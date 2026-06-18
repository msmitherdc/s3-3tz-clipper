mod clip;

use aws_config::BehaviorVersion;
use clap::Parser;
use std::fs::File as StdFile;
use std::io::Read;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::{mpsc, Semaphore};
use flate2::read::DeflateDecoder;
use zip::ZipArchive;
use tracing_subscriber::EnvFilter;
use futures::stream::{FuturesUnordered, StreamExt};

const MAX_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud-Optimized 3dtiles (3tz) & I3S (slpk) Clipper")]
struct Args {
    #[arg(short, long)] bucket: String,
    #[arg(short, long)] key: String,
    #[arg(short, long)] geojson: String,
    #[arg(short, long)] output: String,
    #[arg(short, long)] progress: bool,
    #[arg(short, long, default_value_t = 10)] concurrency: usize,
    #[arg(long, default_value_t = false)] debug: bool,
    #[arg(long, default_value_t = false)] no_sign_request: bool,
}

#[derive(PartialEq, Clone, Debug)]
enum DatasetFormat {
    ThreeDTiles,
    I3S,
}

impl DatasetFormat {
    fn from_key(key: &str) -> Self {
        let lower = key.to_lowercase();
        if lower.ends_with(".slpk") || lower.ends_with(".i3s") || lower.ends_with(".spk") {
            DatasetFormat::I3S
        } else {
            DatasetFormat::ThreeDTiles
        }
    }

    // Return a list of possible index file names.
    fn potential_index_names(&self) -> Vec<&'static str> {
        match self {
            DatasetFormat::ThreeDTiles => vec!["@3dtilesIndex1@"],
            DatasetFormat::I3S => vec!["@speckIndex1@", "@specialIndexFileHASH128@"],
        }
    }

    fn root_file_name(&self) -> &'static str {
        match self {
            DatasetFormat::ThreeDTiles => "tileset.json",
            DatasetFormat::I3S => "3dSceneLayer.json",
        }
    }
}

struct CdEntry {
    filename: String,
    header_offset: u64,
    compressed_size: u64,
    is_deflated: bool,
}

#[derive(Clone)]
enum S3Client {
    Signed(aws_sdk_s3::Client),
    Unsigned(reqwest::Client, String),
}

impl S3Client {
    async fn fetch_size(&self, bucket: &str, key: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            S3Client::Signed(client) => {
                let head = client.head_object().bucket(bucket).key(key).send().await?;
                Ok(head.content_length().unwrap_or(0) as u64)
            }
            S3Client::Unsigned(client, base_url) => {
                let url = format!("{}/{}/{}", base_url, bucket, key);
                let resp = client.head(&url).send().await?;
                let len = resp.headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .ok_or("No content-length header")?
                    .to_str()?
                    .parse::<u64>()?;
                Ok(len)
            }
        }
    }

    async fn fetch_range(&self, bucket: &str, key: &str, start: u64, end: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            S3Client::Signed(client) => {
                let resp = client.get_object()
                    .bucket(bucket)
                    .key(key)
                    .range(format!("bytes={}-{}", start, end))
                    .send()
                    .await?;
                Ok(resp.body.collect().await?.into_bytes().to_vec())
            }
            S3Client::Unsigned(client, base_url) => {
                let url = format!("{}/{}/{}", base_url, bucket, key);
                let resp = client.get(&url)
                    .header(reqwest::header::RANGE, format!("bytes={}-{}", start, end))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    return Err(format!("HTTP Error: {} for url {}", resp.status(), url).into());
                }
                Ok(resp.bytes().await?.to_vec())
            }
        }
    }
}

fn resolve_uri(base: &str, uri: &str) -> Option<String> {
    let clean_uri = if uri.starts_with("./") { &uri[2..] } else { uri };
    let clean_uri = if clean_uri.starts_with('/') { &clean_uri[1..] } else { clean_uri };
    let mut parts: Vec<&str> = base.split('/').collect();
    parts.pop();
    for part in clean_uri.split('/') {
        match part {
            ".." => {
                if parts.is_empty() { return None; }
                parts.pop();
            }
            "." | "" => {}
            _ => parts.push(part),
        }
    }
    Some(parts.join("/"))
}

struct DownloadedFile {
    filename: String,
    data: Vec<u8>,
}

fn decompress_deflate(compressed: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoder = DeflateDecoder::new(compressed);
    let mut buf = Vec::new();
    decoder.by_ref().take(MAX_DECOMPRESSED_BYTES).read_to_end(&mut buf)?;
    let mut probe = [0u8; 1];
    if decoder.read(&mut probe)? != 0 {
        return Err(format!("Decompressed size exceeds {} MiB", MAX_DECOMPRESSED_BYTES / (1024 * 1024)).into());
    }
    Ok(buf)
}

fn parse_central_directory(cd_bytes: &[u8]) -> Vec<CdEntry> {
    let mut entries = Vec::new();
    let mut curr = 0;
    let len = cd_bytes.len();
    while curr + 46 <= len {
        if &cd_bytes[curr..curr + 4] != &[0x50, 0x4b, 0x01, 0x02] { break; }
        let comp_method = u16::from_le_bytes(cd_bytes[curr + 10..curr + 12].try_into().unwrap());
        let is_deflated = comp_method == 8;
        let mut comp_size = u32::from_le_bytes(cd_bytes[curr + 20..curr + 24].try_into().unwrap()) as u64;
        let mut uncomp_size = u32::from_le_bytes(cd_bytes[curr + 24..curr + 28].try_into().unwrap()) as u64;
        let name_len = u16::from_le_bytes(cd_bytes[curr + 28..curr + 30].try_into().unwrap()) as usize;
        let extra_len = u16::from_le_bytes(cd_bytes[curr + 30..curr + 32].try_into().unwrap()) as usize;
        let comment_len = u16::from_le_bytes(cd_bytes[curr + 32..curr + 34].try_into().unwrap()) as usize;
        let mut header_offset = u32::from_le_bytes(cd_bytes[curr + 42..curr + 46].try_into().unwrap()) as u64;
        let name_start = curr + 46;
        let name_end = name_start + name_len;
        if name_end > len {
            eprintln!("[WARN] Truncated CD entry at offset {curr}");
            break;
        }
        let filename = std::str::from_utf8(&cd_bytes[name_start..name_end]).unwrap_or("").to_string();
        if extra_len > 0 && (uncomp_size == 0xFFFFFFFF || comp_size == 0xFFFFFFFF || header_offset == 0xFFFFFFFF) {
            let extra_start = name_end;
            let extra_end = extra_start + extra_len;
            if extra_end <= len {
                let mut ptr = extra_start;
                while ptr + 4 <= extra_end {
                    let tag = u16::from_le_bytes(cd_bytes[ptr..ptr + 2].try_into().unwrap());
                    let sz = u16::from_le_bytes(cd_bytes[ptr + 2..ptr + 4].try_into().unwrap()) as usize;
                    if tag == 0x0001 {
                        let mut data_ptr = ptr + 4;
                        if uncomp_size == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                            uncomp_size = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr + 8].try_into().unwrap());
                            data_ptr += 8;
                        }
                        if comp_size == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                            comp_size = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr + 8].try_into().unwrap());
                            data_ptr += 8;
                        }
                        if header_offset == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                            header_offset = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr + 8].try_into().unwrap());
                        }
                    }
                    ptr += 4 + sz;
                }
            }
        }
        entries.push(CdEntry { filename, header_offset, compressed_size: comp_size, is_deflated });
        curr += 46 + name_len + extra_len + comment_len;
    }
    entries
}

fn load_custom_certs() -> Result<Option<reqwest::Certificate>, Box<dyn std::error::Error + Send + Sync>> {
    let ca_path = match std::env::var("CUSTOM_CA_BUNDLE") {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    println!("[INFO] Loading custom CA Bundle from: {}", ca_path);
    let mut buf = StdFile::open(&ca_path).map_err(|e| format!("Could not open CUSTOM_CA_BUNDLE '{}': {}", ca_path, e))?;
    let mut cert_bytes = Vec::new();
    buf.read_to_end(&mut cert_bytes)?;
    let cert = reqwest::Certificate::from_pem(&cert_bytes).map_err(|e| format!("Failed to parse PEM certificates from '{}': {}", ca_path, e))?;
    Ok(Some(cert))
}

async fn fetch_and_clip_json(client: Arc<S3Client>, bucket: String, key: String, archive_entries: Arc<Vec<CdEntry>>, json_path: String, polygon: Arc<geo::Polygon<f64>>, format: Arc<DatasetFormat>) -> Result<(String, serde_json::Value, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let entry = archive_entries.iter().find(|e| e.filename == json_path).ok_or_else(|| format!("Missing JSON entry: {}", json_path))?;
    let lfh_header = client.fetch_range(&bucket, &key, entry.header_offset, entry.header_offset + 29).await?;
    if lfh_header.len() < 30 { return Err(format!("Short LFH header for '{}'", json_path).into()); }
    let lfh_fname_len = u16::from_le_bytes(lfh_header[26..28].try_into().unwrap()) as u64;
    let lfh_extra_len = u16::from_le_bytes(lfh_header[28..30].try_into().unwrap()) as u64;
    let payload_offset = entry.header_offset + 30 + lfh_fname_len + lfh_extra_len;
    let compressed_bytes = client.fetch_range(&bucket, &key, payload_offset, payload_offset + entry.compressed_size - 1).await?;
    let json_bytes = if entry.is_deflated { decompress_deflate(&compressed_bytes)? } else { compressed_bytes };
    let json_val: serde_json::Value = serde_json::from_slice(&json_bytes)?;
    let mut local_uris = Vec::new();
    let clipped_json = if *format == DatasetFormat::I3S {
        clip::filter_i3s_node(json_val, &polygon, &mut local_uris)
    } else {
        clip::filter_tileset(json_val, &polygon, &mut local_uris)
    };
    Ok((json_path, clipped_json, local_uris))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    if args.debug {
        tracing_subscriber::fmt().with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("aws_config=debug,aws_sdk_s3=debug,reqwest=debug"))).init();
    }
    
    let format = Arc::new(DatasetFormat::from_key(&args.key));
    println!("[INFO] Detected Format: {:?}", format);

    let mut geojson_str = String::new();
    if args.geojson == "-" {
        std::io::stdin().read_to_string(&mut geojson_str)?;
    } else {
        let mut geojson_file = StdFile::open(&args.geojson)?;
        geojson_file.read_to_string(&mut geojson_str)?;
    }
    let clip_polygon = Arc::new(clip::parse_geojson_polygon(&geojson_str).expect("Failed to parse GeoJSON"));
    let custom_endpoint = std::env::var("AWS_S3_ENDPOINT").or_else(|_| std::env::var("AWS_ENDPOINT_URL")).ok();
    let s3_client = if args.no_sign_request {
        let custom_cert = load_custom_certs()?;
        let mut builder = reqwest::Client::builder().use_rustls_tls();
        if let Some(cert) = custom_cert {
            builder = builder.add_root_certificate(cert);
        }
        let reqwest_client = builder.build()?;
        let base_url = custom_endpoint.unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
        if args.debug && base_url != "https://s3.amazonaws.com" {
            println!("[DEBUG] Routing anonymous S3 requests to custom endpoint: {}", base_url);
        }
        S3Client::Unsigned(reqwest_client, base_url)
    } else {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&config).force_path_style(true);
        if let Some(ref endpoint) = custom_endpoint {
            if args.debug { println!("[DEBUG] Routing S3 SDK requests to custom endpoint: {}", endpoint); }
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }
        S3Client::Signed(aws_sdk_s3::Client::from_conf(s3_config_builder.build()))
    };
    println!("Connecting to s3://{}/{}...", args.bucket, args.key);
    let file_size = s3_client.fetch_size(&args.bucket, &args.key).await?;

    let mut cd_offset = 0u64;
    let mut cd_size = 0u64;

    let eocd_read_size = std::cmp::min(file_size, 65536);
    let eocd_start = file_size - eocd_read_size;
    let eocd_bytes = s3_client.fetch_range(&args.bucket, &args.key, eocd_start, file_size - 1).await?;
    for i in (0..eocd_bytes.len().saturating_sub(22)).rev() {
        if &eocd_bytes[i..i + 4] == &[0x50, 0x4b, 0x05, 0x06] {
            cd_size = u32::from_le_bytes(eocd_bytes[i + 12..i + 16].try_into()?) as u64;
            cd_offset = u32::from_le_bytes(eocd_bytes[i + 16..i + 20].try_into()?) as u64;
            break;
        }
    }
    for i in (0..eocd_bytes.len().saturating_sub(20)).rev() {
        if &eocd_bytes[i..i + 4] == &[0x50, 0x4b, 0x06, 0x07] {
            if i + 16 <= eocd_bytes.len() {
                let zip64_eocd_offset = u64::from_le_bytes(eocd_bytes[i + 8..i + 16].try_into()?) as u64;
                let z64_bytes = s3_client.fetch_range(&args.bucket, &args.key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
                if z64_bytes.len() >= 56 && &z64_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x06] {
                    cd_size = u64::from_le_bytes(z64_bytes[40..48].try_into()?);
                    cd_offset = u64::from_le_bytes(z64_bytes[48..56].try_into()?);
                }
                break;
            }
        }
    }

    if cd_size == 0 {
        if args.debug { println!("[DEBUG] Fast EOCD scan failed. Engaging robust seeking scanner..."); }
        const CHUNK_SIZE: u64 = 16384;
        const MAX_EOCD_SEARCH_SIZE: u64 = 1024 * 1024;
        let search_limit = std::cmp::min(file_size, MAX_EOCD_SEARCH_SIZE);
        let mut current_pos = file_size;
        let mut eocd_found = false;
        while current_pos > file_size - search_limit {
            let read_start = current_pos.saturating_sub(CHUNK_SIZE);
            if args.debug { println!("[DEBUG] Scanning for EOCD in range: {}-{}", read_start, current_pos - 1); }
            let buffer = s3_client.fetch_range(&args.bucket, &args.key, read_start, current_pos - 1).await?;
            for i in (0..=buffer.len().saturating_sub(22)).rev() {
                if &buffer[i..i+4] == &[0x50, 0x4b, 0x05, 0x06] {
                    let eocd_absolute_pos = read_start + i as u64;
                    if eocd_absolute_pos >= 20 {
                        let locator_start = eocd_absolute_pos - 20;
                        let locator_bytes = s3_client.fetch_range(&args.bucket, &args.key, locator_start, locator_start + 19).await?;
                        if &locator_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x07] {
                            let zip64_eocd_offset = u64::from_le_bytes(locator_bytes[8..16].try_into()?);
                            let z64_record_bytes = s3_client.fetch_range(&args.bucket, &args.key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
                            if &z64_record_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x06] {
                                cd_size = u64::from_le_bytes(z64_record_bytes[40..48].try_into()?);
                                cd_offset = u64::from_le_bytes(z64_record_bytes[48..56].try_into()?);
                                if args.debug { println!("[DEBUG] Fallback scanner found ZIP64 EOCD. Size: {}, Offset: {}", cd_size, cd_offset); }
                                eocd_found = true;
                                break;
                            }
                        }
                    }
                    cd_size = u32::from_le_bytes(buffer[i+12..i+16].try_into()?) as u64;
                    cd_offset = u32::from_le_bytes(buffer[i+16..i+20].try_into()?) as u64;
                    if args.debug { println!("[DEBUG] Fallback scanner found standard EOCD. Size: {}, Offset: {}", cd_size, cd_offset); }
                    eocd_found = true;
                    break;
                }
            }
            if eocd_found { break; }
            current_pos = read_start;
        }
    }
    
    if cd_size == 0 {
        eprintln!("[ERROR] FATAL: Could not map Central Directory. File may be corrupted or not a valid zip archive.");
        return Ok(());
    }

    println!("[DEBUG] Fetching Central Directory ({} bytes)...", cd_size);
    let cd_bytes = s3_client.fetch_range(&args.bucket, &args.key, cd_offset, cd_offset + cd_size - 1).await?;
    let archive_entries = Arc::new(parse_central_directory(&cd_bytes));
    println!("[DEBUG] Mapped {} file entries.", archive_entries.len());

    let s3_client_arc = Arc::new(s3_client.clone());
    let mut processed_jsons: HashMap<String, serde_json::Value> = HashMap::new();
    let mut keep_uris: HashSet<String> = HashSet::new();
    let mut in_flight = FuturesUnordered::new();
    let mut queued: HashSet<String> = HashSet::new();
    let root = format.root_file_name().to_string();
    keep_uris.insert(root.clone());
    queued.insert(root.clone());
    in_flight.push(fetch_and_clip_json(s3_client_arc.clone(), args.bucket.clone(), args.key.clone(), archive_entries.clone(), root, clip_polygon.clone(), format.clone()));
    while let Some(result) = in_flight.next().await {
        match result {
            Err(e) => eprintln!("[WARN] JSON fetch failed: {}", e),
            Ok((json_path, clipped_json, local_uris)) => {
                for uri in local_uris {
                    let resolved = match resolve_uri(&json_path, &uri) {
                        Some(r) => r,
                        None => { eprintln!("[WARN] Rejected path-traversal URI '{}' in '{}'", uri, json_path); continue; }
                    };
                    if keep_uris.insert(resolved.clone()) {
                        let is_json = resolved.ends_with(".json");
                        let is_i3s_nodepage = *format == DatasetFormat::I3S && !is_json; // I3S node pages might not have a .json extension
                        if (is_json || is_i3s_nodepage) && !queued.contains(&resolved) {
                            queued.insert(resolved.clone());
                            in_flight.push(fetch_and_clip_json(s3_client_arc.clone(), args.bucket.clone(), args.key.clone(), archive_entries.clone(), resolved, clip_polygon.clone(), format.clone()));
                        }
                    }
                }
                processed_jsons.insert(json_path, clipped_json);
            }
        }
    }
    
    println!("Found {} files that intersect, fetching files ...", keep_uris.len());
    let out_file = AsyncFile::create(&args.output).await?;
    let mut zip_writer = ZipFileWriter::new(out_file.compat_write());
    let pb = if args.progress {
        let bar = ProgressBar::new(keep_uris.len() as u64);
        bar.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}").unwrap());
        Some(Arc::new(bar))
    } else { None };
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let (tx, mut rx) = mpsc::channel::<DownloadedFile>(args.concurrency * 2);
    let mut fetch_tasks = Vec::new();
    for entry in archive_entries.iter() {
        if !keep_uris.contains(&entry.filename) { continue; }
        let filename = entry.filename.clone();
        if let Some(clipped_json) = processed_jsons.get(&filename) {
            let data = serde_json::to_string(clipped_json)?.into_bytes();
            let tx_clone = tx.clone();
            fetch_tasks.push(tokio::spawn(async move {
                let _ = tx_clone.send(DownloadedFile { filename, data }).await;
            }));
            continue;
        }
        let header_offset = entry.header_offset;
        let compressed_size = entry.compressed_size;
        let is_deflated = entry.is_deflated;
        let semaphore_clone = semaphore.clone();
        let tx_clone = tx.clone();
        let pb_clone = pb.clone();
        let client_clone = s3_client_arc.clone();
        let bucket_clone = args.bucket.clone();
        let key_clone = args.key.clone();
        let target_filename = filename.clone();
        let task = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire_owned().await.unwrap();
            if let Some(ref bar) = pb_clone { bar.set_message(format!("Fetching {}", target_filename)); }
            let lfh_header = match client_clone.fetch_range(&bucket_clone, &key_clone, header_offset, header_offset + 29).await {
                Ok(b) => b,
                Err(e) => { eprintln!("\n[ERROR] LFH header fetch failed for '{}': {:?}", target_filename, e); return; }
            };
            if lfh_header.len() < 30 { eprintln!("\n[ERROR] Short LFH header for '{}'", target_filename); return; }
            let lfh_fname_len = u16::from_le_bytes(lfh_header[26..28].try_into().unwrap()) as u64;
            let lfh_extra_len = u16::from_le_bytes(lfh_header[28..30].try_into().unwrap()) as u64;
            let payload_start_offset = header_offset + 30 + lfh_fname_len + lfh_extra_len;
            let compressed_data = match client_clone.fetch_range(&bucket_clone, &key_clone, payload_start_offset, payload_start_offset + compressed_size - 1).await {
                Ok(b) => b,
                Err(e) => { eprintln!("\n[ERROR] Payload fetch failed for '{}': {:?}", target_filename, e); return; }
            };
            let data = if is_deflated {
                match decompress_deflate(&compressed_data) {
                    Ok(d) => d,
                    Err(e) => { eprintln!("\n[ERROR] Decompression failed for '{}': {}", target_filename, e); return; }
                }
            } else { compressed_data };
            let _ = tx_clone.send(DownloadedFile { filename: target_filename, data }).await;
        });
        fetch_tasks.push(task);
    }
    drop(tx);
    while let Some(file) = rx.recv().await {
        let builder = ZipEntryBuilder::new(file.filename.clone().into(), Compression::Deflate);
        zip_writer.write_entry_whole(builder, &file.data).await.unwrap();
        if let Some(ref bar) = pb { bar.inc(1); }
    }
    for task in fetch_tasks { task.await.unwrap(); }

    // Discover the correct index filename to use for writing. Default to the first potential name.
    let index_filename_to_write = format.potential_index_names()[0];
    let dummy_index = vec![0u8; keep_uris.len() * 24];
    zip_writer.write_entry_whole(ZipEntryBuilder::new(index_filename_to_write.into(), Compression::Stored), &dummy_index).await?;
    zip_writer.close().await?;
    if let Some(ref bar) = pb { bar.finish_with_message("Done!"); }
    
    println!("Adding spatial index to archive ...");
    let mut file = tokio::fs::OpenOptions::new().read(true).write(true).open(&args.output).await?;
    let read_file = StdFile::open(&args.output)?;
    let mut final_archive = ZipArchive::new(read_file)?;
    struct IndexRecord { md5hash: [u8; 16], offset: u64 }
    let mut tzindex: Vec<IndexRecord> = Vec::new();
    let mut index_header_offset = 0u64;

    // Dynamically find which index file is used
    let potential_indices = format.potential_index_names();
    let mut found_index_name = None;

    for i in 0..final_archive.len() {
        let file_entry = final_archive.by_index(i)?;
        let name = file_entry.name();
        if potential_indices.contains(&name) {
            index_header_offset = file_entry.header_start();
            found_index_name = Some(name.to_string());
        } else {
            let normalized_path = name.replace('\\', "/");
            let digest = md5::compute(normalized_path.as_bytes());
            tzindex.push(IndexRecord { md5hash: digest.0, offset: file_entry.header_start() });
        }
    }
    
    let index_filename = found_index_name.ok_or("Could not find a valid index file in the archive")?;
    println!("[DEBUG] Found and using index file: {}", index_filename);

    tzindex.sort_by_key(|x| (u64::from_le_bytes(x.md5hash[0..8].try_into().unwrap()), u64::from_le_bytes(x.md5hash[8..16].try_into().unwrap())));
    let mut bindex = Vec::with_capacity(tzindex.len() * 24);
    for i in tzindex {
        bindex.extend_from_slice(&i.md5hash);
        bindex.extend_from_slice(&i.offset.to_le_bytes());
    }
    let crc32 = crc32fast::hash(&bindex);
    let index_payload_offset = final_archive.by_name(&index_filename)?.data_start().expect("Index payload offset not found");
    file.seek(SeekFrom::Start(index_payload_offset)).await?;
    file.write_all(&bindex).await?;
    file.seek(SeekFrom::Start(index_header_offset + 14)).await?;
    file.write_all(&crc32.to_le_bytes()).await?;
    println!("Success! Clipped dataset complete");
    Ok(())
}
