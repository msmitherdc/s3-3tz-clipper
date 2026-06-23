mod clip;

use aws_config::BehaviorVersion;
use clap::Parser;
use std::fs::File as StdFile;
use std::io::{Read, Write};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::{mpsc, Semaphore};
use flate2::read::{DeflateDecoder, GzDecoder};
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use zip::ZipArchive;
use tracing_subscriber::EnvFilter;
use futures::stream::{FuturesUnordered, StreamExt};
use geo::{Intersects, BoundingRect};
use std::path::Path;
use crate::clip::{I3SNode, ChildRef};

/// Maximum decompressed size per entry (64 MiB) — prevents zip bomb attacks.
const MAX_DECOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud-Optimized 3dtiles/I3S Clipper")]
struct Args {
    #[arg(short, long)] bucket: String,
    #[arg(short, long)] key: String,
    #[arg(short, long)] geojson: String,
    #[arg(short, long)] output: String,
    #[arg(short, long)] progress: bool,
    #[arg(short, long, default_value_t = 20)] concurrency: usize,
    #[arg(long, default_value_t = false)] debug: bool,
    #[arg(long, default_value_t = false)] no_sign_request: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArchiveFormat {
    Cesium3DTiles,
    EsriI3S,
}

#[derive(Debug, Clone)]
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

struct DownloadedFile {
    filename: String,
    data: Vec<u8>,
}

fn decompress_deflate(compressed: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoder = DeflateDecoder::new(compressed);
    let mut buf = Vec::new();
    decoder.by_ref().take(MAX_DECOMPRESSED_BYTES).read_to_end(&mut buf)?;
    Ok(buf)
}

fn decompress_gzip(compressed: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut decoder = GzDecoder::new(compressed);
    let mut buf = Vec::new();
    decoder.by_ref().take(MAX_DECOMPRESSED_BYTES).read_to_end(&mut buf)?;
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
        let mut filename = std::str::from_utf8(&cd_bytes[name_start..name_end]).unwrap_or("").to_string();
        
        // Normalize Windows backslashes to forward slashes
        filename = filename.replace('\\', "/");
        
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

async fn fetch_file_content(
    client: &S3Client,
    bucket: &str,
    key: &str,
    entry: &CdEntry,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let lfh_header = client.fetch_range(bucket, key, entry.header_offset, entry.header_offset + 29).await?;
    if lfh_header.len() < 30 { return Err(format!("Short LFH header for '{}'", entry.filename).into()); }
    let lfh_fname_len = u16::from_le_bytes(lfh_header[26..28].try_into().unwrap()) as u64;
    let lfh_extra_len = u16::from_le_bytes(lfh_header[28..30].try_into().unwrap()) as u64;
    let payload_start_offset = entry.header_offset + 30 + lfh_fname_len + lfh_extra_len;

    let raw_payload = client.fetch_range(bucket, key, payload_start_offset, payload_start_offset + entry.compressed_size - 1).await?;

    let file_data = if entry.is_deflated {
        decompress_deflate(&raw_payload)?
    } else {
        raw_payload
    };

    if entry.filename.ends_with(".gz") {
        decompress_gzip(&file_data)
    } else {
        Ok(file_data)
    }
}

async fn fetch_and_clip_3dtiles_json(
    client: Arc<S3Client>,
    bucket: String,
    key: String,
    archive_entries: Arc<Vec<CdEntry>>,
    json_path: String,
    polygon: Arc<geo::Polygon<f64>>,
) -> Result<(String, serde_json::Value, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let json_path_gz = format!("{}.gz", json_path);
    let entry = archive_entries.iter().find(|e| e.filename == json_path || e.filename == json_path_gz)
        .ok_or_else(|| format!("Missing JSON entry: {}", json_path))?;
    
    let json_bytes = fetch_file_content(&*client, &bucket, &key, entry).await?;
    let json_val: serde_json::Value = serde_json::from_slice(&json_bytes)?;

    let mut local_uris = Vec::new();
    let clipped_json = clip::filter_tileset(json_val, &json_path, &polygon, &mut local_uris);
    
    Ok((json_path, clipped_json, local_uris))
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let archive_format = if args.key.ends_with(".3tz") {
        ArchiveFormat::Cesium3DTiles
    } else if args.key.ends_with(".slpk") || args.key.ends_with(".spk") {
        ArchiveFormat::EsriI3S
    } else {
        panic!("Unsupported file extension. Please use .3tz, .slpk, or .spk");
    };

    if args.debug {
        tracing_subscriber::fmt().with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("aws_config=debug,aws_sdk_s3=debug,reqwest=debug"))).init();
    }
    let mut geojson_str = String::new();
    if args.geojson == "-" {
        std::io::stdin().read_to_string(&mut geojson_str)?;
    } else {
        let mut geojson_file = StdFile::open(&args.geojson)?;
        geojson_file.read_to_string(&mut geojson_str)?;
    }
    let clip_polygon = Arc::new(clip::parse_geojson_polygon(&geojson_str).expect("Failed to parse GeoJSON"));
    let custom_endpoint = std::env::var("AWS_S3_ENDPOINT")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
        .map(|url| format!("https://{}", url))
        .ok();
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
            cd_size = u32::from_le_bytes(eocd_bytes[i + 12..i + 16].try_into().unwrap()) as u64;
            cd_offset = u32::from_le_bytes(eocd_bytes[i + 16..i + 20].try_into().unwrap()) as u64;
            break;
        }
    }
    for i in (0..eocd_bytes.len().saturating_sub(20)).rev() {
        if &eocd_bytes[i..i + 4] == &[0x50, 0x4b, 0x06, 0x07] {
            if i + 16 <= eocd_bytes.len() {
                let zip64_eocd_offset = u64::from_le_bytes(eocd_bytes[i + 8..i + 16].try_into().unwrap()) as u64;
                let z64_bytes = s3_client.fetch_range(&args.bucket, &args.key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
                if z64_bytes.len() >= 56 && &z64_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x06] {
                    cd_size = u64::from_le_bytes(z64_bytes[40..48].try_into().unwrap());
                    cd_offset = u64::from_le_bytes(z64_bytes[48..56].try_into().unwrap());
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
                            let zip64_eocd_offset = u64::from_le_bytes(locator_bytes[8..16].try_into().unwrap());
                            let z64_record_bytes = s3_client.fetch_range(&args.bucket, &args.key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
                            if &z64_record_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x06] {
                                cd_size = u64::from_le_bytes(z64_record_bytes[40..48].try_into().unwrap());
                                cd_offset = u64::from_le_bytes(z64_record_bytes[48..56].try_into().unwrap());
                                if args.debug { println!("[DEBUG] Fallback scanner found ZIP64 EOCD. Size: {}, Offset: {}", cd_size, cd_offset); }
                                eocd_found = true;
                                break;
                            }
                        }
                    }
                    cd_size = u32::from_le_bytes(buffer[i+12..i+16].try_into().unwrap()) as u64;
                    cd_offset = u32::from_le_bytes(buffer[i+16..i+20].try_into().unwrap()) as u64;
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

    println!("Fetching Central Directory ({} bytes)...", cd_size);
    let cd_bytes = s3_client.fetch_range(&args.bucket, &args.key, cd_offset, cd_offset + cd_size - 1).await?;
    let archive_entries = Arc::new(parse_central_directory(&cd_bytes));
    println!("Mapped {} file entries.", archive_entries.len());

    let s3_client_arc = Arc::new(s3_client.clone());
    let mut keep_uris: HashSet<String> = HashSet::new();
    let mut processed_jsons: HashMap<String, serde_json::Value> = HashMap::new();

    if archive_format == ArchiveFormat::Cesium3DTiles {
        println!("Fetching and clipping 3D Tiles dataset...");
        
        let mut queue = std::collections::VecDeque::new();
        queue.push_back("tileset.json".to_string());
        
        let mut visited = HashSet::new();
        
        while let Some(json_path) = queue.pop_front() {
            if !visited.insert(json_path.clone()) { continue; }
            
            match fetch_and_clip_3dtiles_json(
                s3_client_arc.clone(),
                args.bucket.clone(),
                args.key.clone(),
                archive_entries.clone(),
                json_path.clone(),
                clip_polygon.clone(),
            ).await {
                Ok((path, clipped_json, local_uris)) => {
                    processed_jsons.insert(path.clone(), clipped_json);
                    keep_uris.insert(path);
                    
                    for uri in local_uris {
                        if uri.ends_with(".json") {
                            // Recursively fetch nested tilesets
                            queue.push_back(uri.clone());
                        } else {
                            // Mark data files (e.g., .b3dm, .glb) to be kept
                            keep_uris.insert(uri);
                        }
                    }
                },
                Err(e) => {
                    eprintln!("[ERROR] Failed to fetch and clip 3D Tiles JSON {}: {}", json_path, e);
                }
            }
        }
        
        println!("Finished parsing 3D Tiles dataset. Kept {} files.", keep_uris.len());

       } else if archive_format == ArchiveFormat::EsriI3S {
        let root_json_path = "3dSceneLayer.json".to_string();
        let root_entry = archive_entries.iter().find(|e| e.filename == root_json_path || e.filename == format!("{}.gz", root_json_path)).ok_or("3dSceneLayer.json[.gz] not found")?;
        
        println!("Fetching 3dSceneLayer.json...");
        let scenelayer_bytes = fetch_file_content(&s3_client, &args.bucket, &args.key, root_entry).await?;
        let scenelayer_json: serde_json::Value = serde_json::from_slice(&scenelayer_bytes)?;
        keep_uris.insert(root_json_path.clone());
        
        let mut all_nodes = HashMap::new();
        
        let node_doc_filter = |e: &&CdEntry| {
            let f = &e.filename;
            (f.starts_with("nodepages/") || f.contains("/nodepages/")) 
                && (f.ends_with(".json") || f.ends_with(".json.gz"))
        };

        let is_i3s_17 = archive_entries.iter().any(|e| {
            let f = &e.filename;
            f.starts_with("nodepages/") || f.contains("/nodepages/")
        });

        let mut kept_node_ids: HashSet<String> = HashSet::new();

        if is_i3s_17 {
            println!("Fetching and parsing I3S 1.7+ node pages (Parallel)...");
            let mut fetch_tasks = FuturesUnordered::new();
            let semaphore = Arc::new(Semaphore::new(args.concurrency));

            for entry in archive_entries.iter().filter(node_doc_filter) {
                let entry_clone = entry.clone();
                let client = s3_client_arc.clone();
                let bucket = args.bucket.clone();
                let key = args.key.clone();
                let sem = semaphore.clone();
                
                fetch_tasks.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let bytes_res = fetch_file_content(&*client, &bucket, &key, &entry_clone).await;
                    (entry_clone, bytes_res)
                }));
            }

            while let Some(res) = fetch_tasks.next().await {
                let (entry, bytes_res) = res.unwrap();
                let doc_filename = entry.filename.strip_suffix(".gz").unwrap_or(&entry.filename).to_string();

                match bytes_res {
                    Ok(node_bytes) => {
                        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&node_bytes) {
                            let mut nodes_to_process = Vec::new();

                            if let Some(nodes_arr) = json_val.get("nodes").and_then(|n| n.as_array()) {
                                nodes_to_process.extend(nodes_arr.iter());
                            } else {
                                nodes_to_process.push(&json_val);
                            }

                            for node_val in nodes_to_process {
                                let id_value = node_val.get("id").or_else(|| node_val.get("index"));

                                let id = match id_value {
                                    Some(v) if v.is_string() => v.as_str().unwrap().to_string(),
                                    Some(v) if v.is_number() => v.as_i64().unwrap().to_string(),
                                    _ => continue,
                                };

                                let node_doc_filename = if doc_filename.starts_with("nodes/") {
                                    format!("nodes/{}/3dNodeIndexDocument.json", id)
                                } else {
                                    doc_filename.clone()
                                };

                                let mut mbs = [0.0f64; 4];
                                let mut has_bounds = false;

                                if let Some(mbs_arr) = node_val.get("mbs").and_then(|m| m.as_array()) {
                                    if mbs_arr.len() >= 4 {
                                        for i in 0..4 {
                                            mbs[i] = mbs_arr[i].as_f64().unwrap_or(0.0);
                                        }
                                        has_bounds = true;
                                    }
                                } else if let Some(obb_obj) = node_val.get("obb").and_then(|o| o.as_object()) {
                                    if let (Some(center), Some(half_size)) = (obb_obj.get("center").and_then(|c| c.as_array()), obb_obj.get("halfSize").and_then(|h| h.as_array())) {
                                        if center.len() >= 3 && half_size.len() >= 3 {
                                            mbs[0] = center[0].as_f64().unwrap_or(0.0);
                                            mbs[1] = center[1].as_f64().unwrap_or(0.0);
                                            mbs[2] = center[2].as_f64().unwrap_or(0.0);
                                            let hx = half_size[0].as_f64().unwrap_or(0.0);
                                            let hy = half_size[1].as_f64().unwrap_or(0.0);
                                            let hz = half_size[2].as_f64().unwrap_or(0.0);
                                            mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
                                            has_bounds = true;
                                        }
                                    }
                                } else if let Some(obb_arr) = node_val.get("obb").and_then(|o| o.as_array()) {
                                    if obb_arr.len() >= 6 {
                                        mbs[0] = obb_arr[0].as_f64().unwrap_or(0.0);
                                        mbs[1] = obb_arr[1].as_f64().unwrap_or(0.0);
                                        mbs[2] = obb_arr[2].as_f64().unwrap_or(0.0);
                                        let hx = obb_arr[3].as_f64().unwrap_or(0.0);
                                        let hy = obb_arr[4].as_f64().unwrap_or(0.0);
                                        let hz = obb_arr[5].as_f64().unwrap_or(0.0);
                                        mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
                                        has_bounds = true;
                                    }
                                }

                                if !has_bounds { continue; }

                                let mut children = Vec::new();
                                if let Some(children_arr) = node_val.get("children").and_then(|c| c.as_array()) {
                                    for child_val in children_arr {
                                        let child_id = match child_val {
                                            serde_json::Value::Number(n) => Some(n.to_string()),
                                            serde_json::Value::String(s) => Some(s.clone()),
                                            serde_json::Value::Object(o) => {
                                                o.get("id").or_else(|| o.get("index")).and_then(|v| {
                                                    match v {
                                                        serde_json::Value::String(s) => Some(s.clone()),
                                                        serde_json::Value::Number(n) => Some(n.to_string()),
                                                        _ => None
                                                    }
                                                })
                                            },
                                            _ => None
                                        };
                                        if let Some(cid) = child_id {
                                            children.push(ChildRef { id: cid });
                                        }
                                    }
                                }
                                let new_node = I3SNode {
                                    id: id.clone(),
                                    doc_filename: node_doc_filename,
                                    containing_doc: entry.filename.clone(),
                                    mbs,
                                    children,
                                };

                                all_nodes.insert(id, new_node);
                            }
                        } else {
                            eprintln!("[WARN] Failed to parse valid JSON from: {}", entry.filename);
                        }
                    },
                    Err(e) => {
                        eprintln!("[ERROR] Failed to fetch content for {}: {}", entry.filename, e);
                    }
                }
            }
            
            let node_page_count = archive_entries.iter()
                .filter(|e| e.filename.starts_with("nodepages/"))
                .count();

            println!(
                "Parsed {} I3S nodes into memory ({} node pages).",
                all_nodes.len(),
                node_page_count
            );
            clip::filter_i3s_scenelayer(&scenelayer_json, &all_nodes, &clip_polygon, &mut keep_uris, &mut kept_node_ids);
        } else {
            println!("Detected I3S 1.6 / flat dataset. Traversing tree lazily (Parallel)...");
            
            let root_node_path_str = scenelayer_json
                .get("store")
                .and_then(|s| s.get("rootNode"))
                .and_then(|r| r.as_str())
                .unwrap_or("./nodes/root");
            let root_node_path_norm = root_node_path_str.replace('\\', "/");
            let root_id = Path::new(&root_node_path_norm)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("root")
                .to_string();

            let mut queue = std::collections::VecDeque::new();
            queue.push_back(root_id.clone());

            let mut visited = HashSet::new();
            let mut active_fetches = FuturesUnordered::new();
            let semaphore = Arc::new(Semaphore::new(args.concurrency));

            while !queue.is_empty() || !active_fetches.is_empty() {
                while !queue.is_empty() && active_fetches.len() < args.concurrency {
                    if let Some(node_id) = queue.pop_front() {
                        if !visited.insert(node_id.clone()) {
                            continue;
                        }

                        let client = s3_client_arc.clone();
                        let bucket = args.bucket.clone();
                        let key = args.key.clone();
                        let entries = archive_entries.clone();
                        let sem = semaphore.clone();

                        let fut = async move {
                            let _permit = sem.acquire().await.unwrap();
                            let node_path = format!("nodes/{}/3dNodeIndexDocument.json", node_id);
                            let node_path_gz = format!("{}.gz", node_path);
                            
                            let entry = entries.iter().find(|e| e.filename == node_path || e.filename == node_path_gz);
                            let res: Result<(String, String, Vec<u8>), String> = match entry {
                                None => Err(format!("Node {} not found in archive", node_id)),
                                Some(e) => {
                                    match fetch_file_content(&*client, &bucket, &key, e).await {
                                        Err(err) => Err(format!("Failed to fetch node {}: {}", node_id, err)),
                                        Ok(bytes) => Ok((node_id, e.filename.clone(), bytes))
                                    }
                                }
                            };
                            res
                        };
                        active_fetches.push(tokio::spawn(fut));
                    }
                }

                if let Some(join_res) = active_fetches.next().await {
                    match join_res {
                        Err(join_err) => { eprintln!("[ERROR] Join error in lazy fetch: {}", join_err); }
                        Ok(Err(fetch_err)) => {
                            if args.debug {
                                eprintln!("[WARN] {}", fetch_err);
                            }
                        }
                        Ok(Ok((node_id, containing_doc, bytes))) => {
                            if let Ok(node_val) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                                let mut mbs = [0.0f64; 4];
                                let mut has_bounds = false;
                                if let Some(mbs_arr) = node_val.get("mbs").and_then(|m| m.as_array()) {
                                    if mbs_arr.len() >= 4 {
                                        for i in 0..4 {
                                            mbs[i] = mbs_arr[i].as_f64().unwrap_or(0.0);
                                        }
                                        has_bounds = true;
                                    }
                                } else if let Some(obb_obj) = node_val.get("obb").and_then(|o| o.as_object()) {
                                    if let (Some(center), Some(half_size)) = (obb_obj.get("center").and_then(|c| c.as_array()), obb_obj.get("halfSize").and_then(|h| h.as_array())) {
                                        if center.len() >= 3 && half_size.len() >= 3 {
                                            mbs[0] = center[0].as_f64().unwrap_or(0.0);
                                            mbs[1] = center[1].as_f64().unwrap_or(0.0);
                                            mbs[2] = center[2].as_f64().unwrap_or(0.0);
                                            let hx = half_size[0].as_f64().unwrap_or(0.0);
                                            let hy = half_size[1].as_f64().unwrap_or(0.0);
                                            let hz = half_size[2].as_f64().unwrap_or(0.0);
                                            mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
                                            has_bounds = true;
                                        }
                                    }
                                } else if let Some(obb_arr) = node_val.get("obb").and_then(|o| o.as_array()) {
                                    if obb_arr.len() >= 6 {
                                        mbs[0] = obb_arr[0].as_f64().unwrap_or(0.0);
                                        mbs[1] = obb_arr[1].as_f64().unwrap_or(0.0);
                                        mbs[2] = obb_arr[2].as_f64().unwrap_or(0.0);
                                        let hx = obb_arr[3].as_f64().unwrap_or(0.0);
                                        let hy = obb_arr[4].as_f64().unwrap_or(0.0);
                                        let hz = obb_arr[5].as_f64().unwrap_or(0.0);
                                        mbs[3] = (hx * hx + hy * hy + hz * hz).sqrt();
                                        has_bounds = true;
                                    }
                                }

                                if !has_bounds { continue; }

                                // MBS degree coordinate conversion
                                let mbs_center_x = mbs[0];
                                let mbs_center_y = mbs[1];
                                let mbs_radius_meters = mbs[3];

                                let lat_rad = mbs_center_y.to_radians();
                                let meters_per_deg_lat = 111320.0;
                                let meters_per_deg_lon = (111320.0 * lat_rad.cos()).max(1.0);

                                let radius_deg_x = mbs_radius_meters / meters_per_deg_lon;
                                let radius_deg_y = mbs_radius_meters / meters_per_deg_lat;

                                let node_bbox = geo::Rect::new(
                                    geo::Coord { x: mbs_center_x - radius_deg_x, y: mbs_center_y - radius_deg_y },
                                    geo::Coord { x: mbs_center_x + radius_deg_x, y: mbs_center_y + radius_deg_y },
                                );

                                let polygon_bbox = match clip_polygon.bounding_rect() {
                                    Some(rect) => rect,
                                    None => continue,
                                };

                                let intersects = polygon_bbox.intersects(&node_bbox) && clip_polygon.intersects(&node_bbox);

                                if intersects {
                                    keep_uris.insert(containing_doc);
                                    keep_uris.insert(format!("nodes/{}/3dNodeIndexDocument.json", node_id));
                                    kept_node_ids.insert(node_id.clone());

                                    if let Some(children_arr) = node_val.get("children").and_then(|c| c.as_array()) {
                                        for child_val in children_arr {
                                            let child_id = match child_val {
                                                serde_json::Value::Number(n) => Some(n.to_string()),
                                                serde_json::Value::String(s) => Some(s.clone()),
                                                serde_json::Value::Object(o) => {
                                                    o.get("id").or_else(|| o.get("index")).and_then(|v| {
                                                        match v {
                                                            serde_json::Value::String(s) => Some(s.clone()),
                                                            serde_json::Value::Number(n) => Some(n.to_string()),
                                                            _ => None
                                                        }
                                                    })
                                                },
                                                _ => None
                                            };
                                            if let Some(cid) = child_id {
                                                queue.push_back(cid);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            println!("Lazy traversal finished. Kept {} nodes.", kept_node_ids.len());
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

    let original_filenames: HashMap<String, String> = archive_entries.iter()
        .map(|entry| (entry.filename.strip_suffix(".gz").unwrap_or(&entry.filename).to_string(), entry.filename.clone()))
        .collect();

    for (uncompressed_name, original_name) in original_filenames.iter() {
        if !keep_uris.contains(uncompressed_name) { continue; }

        let entry = archive_entries.iter().find(|e| &e.filename == original_name).unwrap();
        let original_was_gzipped = entry.filename.ends_with(".gz");
        
        if let Some(clipped_json) = processed_jsons.get(uncompressed_name) {
            let data = serde_json::to_string(clipped_json)?.into_bytes();
            let tx_clone = tx.clone();
            // Clone the name so the spawned task owns a 'static String rather than
            // borrowing from `original_filenames` (which doesn't outlive the task).
            let uncompressed_name_owned = uncompressed_name.to_string();

            fetch_tasks.push(tokio::spawn(async move {
                if original_was_gzipped {
                    let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                    if encoder.write_all(&data).is_ok() {
                        if let Ok(gzipped_data) = encoder.finish() {
                            let _ = tx_clone.send(DownloadedFile { filename: format!("{}.gz", uncompressed_name_owned), data: gzipped_data }).await;
                            return;
                        }
                    }
                }
                let _ = tx_clone.send(DownloadedFile { filename: uncompressed_name_owned, data }).await;
            }));
            continue;
        }

        let entry_clone = entry.clone();
        let client_clone = s3_client_arc.clone();
        let bucket_clone = args.bucket.clone();
        let key_clone = args.key.clone();
        let tx_clone = tx.clone();
        let pb_clone = pb.clone();
        let semaphore_clone = semaphore.clone();

        fetch_tasks.push(tokio::spawn(async move {
            let _permit = semaphore_clone.acquire_owned().await.unwrap();
            if let Some(ref bar) = pb_clone { bar.set_message(format!("Fetching {}", entry_clone.filename)); }

            match fetch_file_content(&client_clone, &bucket_clone, &key_clone, &entry_clone).await {
                Ok(mut data) => {
                    if original_was_gzipped {
                        let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                        if encoder.write_all(&data).is_ok() {
                            if let Ok(gzipped_data) = encoder.finish() {
                                data = gzipped_data;
                            }
                        }
                    }
                    let _ = tx_clone.send(DownloadedFile { filename: entry_clone.filename.clone(), data }).await;
                },
                Err(e) => {
                    eprintln!("\n[ERROR] Failed to fetch/decompress '{}': {:?}", entry_clone.filename, e);
                }
            };
        }));
    }
    drop(tx);

    while let Some(file) = rx.recv().await {
        let compression = if file.filename.ends_with(".gz") { Compression::Stored } else { Compression::Deflate };
        let builder = ZipEntryBuilder::new(file.filename.clone().into(), compression);
        zip_writer.write_entry_whole(builder, &file.data).await.unwrap();
        if let Some(ref bar) = pb { bar.inc(1); }
    }
    for task in fetch_tasks { task.await.unwrap(); }

    let index_name = if archive_format == ArchiveFormat::Cesium3DTiles {
        "@3dtilesIndex1@"
    } else {
        "@specialIndexFileHASH128@"
    };
    // The dummy index must be at least as large as the real index written in-place later.
    // The real index has one 24-byte record per non-index zip entry. keep_uris.len() is an
    // upper bound (some fetches may fail), so add 1 record of slack so the slot is never
    // too small, which would cause a truncated/corrupt index.
    let dummy_index = vec![0u8; (keep_uris.len() + 1) * 24];
    zip_writer.write_entry_whole(ZipEntryBuilder::new(index_name.into(), Compression::Stored), &dummy_index).await?;
    zip_writer.close().await?;
    if let Some(ref bar) = pb { bar.finish_with_message("Done!"); }
    println!("Adding index to zipfile ...");
    let mut file = tokio::fs::OpenOptions::new().read(true).write(true).open(&args.output).await?;
    let read_file = StdFile::open(&args.output)?;
    let mut final_archive = ZipArchive::new(read_file)?;
    struct IndexRecord { md5hash: [u8; 16], offset: u64 }
    let mut tzindex: Vec<IndexRecord> = Vec::new();
    let mut index_header_offset = 0u64;
    for i in 0..final_archive.len() {
        let file_entry = final_archive.by_index(i)?;
        if file_entry.name() == index_name {
            index_header_offset = file_entry.header_start();
        } else {
            let normalized_path = file_entry.name().replace('\\', "/");
            let digest = md5::compute(normalized_path.as_bytes());
            tzindex.push(IndexRecord { md5hash: digest.0, offset: file_entry.header_start() });
        }
    }
    
    tzindex.sort_by_key(|x| (u64::from_le_bytes(x.md5hash[0..8].try_into().unwrap()), u64::from_le_bytes(x.md5hash[8..16].try_into().unwrap())));
    
    let mut bindex = Vec::with_capacity(tzindex.len() * 24);
    for i in tzindex {
        bindex.extend_from_slice(&i.md5hash);
        bindex.extend_from_slice(&i.offset.to_le_bytes());
    }
    let crc32 = crc32fast::hash(&bindex);
    let index_payload_offset = final_archive.by_name(index_name)?.data_start().expect("Index payload offset not found");
    file.seek(SeekFrom::Start(index_payload_offset)).await?;
    file.write_all(&bindex).await?;
    file.seek(SeekFrom::Start(index_header_offset + 14)).await?;
    file.write_all(&crc32.to_le_bytes()).await?;

    println!("Success! Clipped dataset complete");
    Ok(())
}
