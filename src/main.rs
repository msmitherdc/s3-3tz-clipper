mod clip;

use aws_config::BehaviorVersion;
use clap::Parser;
use std::fs::File as StdFile;
use std::io::{Read};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use indicatif::{ProgressBar, ProgressStyle};
use md5::{Md5, Digest};
use tokio::sync::{mpsc, Semaphore};
use flate2::read::DeflateDecoder;
use zip::ZipArchive;

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud-Optimized 3tz Clipper")]
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

struct CdEntry {
    filename: String,
    header_offset: u64,
    compressed_size: u64,
    is_deflated: bool,
}

// Thread-safe wrapper allowing us to use either the S3 SDK or Reqwest
#[derive(Clone)]
enum S3Client {
    Signed(aws_sdk_s3::Client),
    Unsigned(reqwest::Client),
}

impl S3Client {
    async fn fetch_size(&self, bucket: &str, key: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            S3Client::Signed(client) => {
                let head = client.head_object().bucket(bucket).key(key).send().await?;
                Ok(head.content_length().unwrap_or(0) as u64)
            }
            S3Client::Unsigned(client) => {
                let url = format!("https://{}.s3.amazonaws.com/{}", bucket, key);
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
            S3Client::Unsigned(client) => {
                let url = format!("https://{}.s3.amazonaws.com/{}", bucket, key);
                let resp = client.get(&url)
                    .header(reqwest::header::RANGE, format!("bytes={}-{}", start, end))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    return Err(format!("HTTP Error: {}", resp.status()).into());
                }
                Ok(resp.bytes().await?.to_vec())
            }
        }
    }
}

fn resolve_uri(base: &str, uri: &str) -> String {
    let clean_uri = if uri.starts_with("./") { &uri[2..] } else { uri };
    if clean_uri.starts_with('/') { return clean_uri[1..].to_string(); }
    let mut parts: Vec<&str> = base.split('/').collect();
    parts.pop();
    for part in clean_uri.split('/') {
        if part == ".." { parts.pop(); } else if part != "." && !part.is_empty() { parts.push(part); }
    }
    parts.join("/")
}

struct DownloadedFile {
    filename: String,
    data: Vec<u8>,
}

fn parse_central_directory(cd_bytes: &[u8]) -> Vec<CdEntry> {
    let mut entries = Vec::new();
    let mut curr = 0;
    let len = cd_bytes.len();
    while curr + 46 <= len {
        if &cd_bytes[curr..curr+4] != &[0x50, 0x4b, 0x01, 0x02] {
            break;
        }
        let comp_method = u16::from_le_bytes(cd_bytes[curr+10..curr+12].try_into().unwrap());
        let is_deflated = comp_method == 8;
        let mut comp_size = u32::from_le_bytes(cd_bytes[curr+20..curr+24].try_into().unwrap()) as u64;
        let mut uncomp_size = u32::from_le_bytes(cd_bytes[curr+24..curr+28].try_into().unwrap()) as u64;
        let name_len = u16::from_le_bytes(cd_bytes[curr+28..curr+30].try_into().unwrap()) as usize;
        let extra_len = u16::from_le_bytes(cd_bytes[curr+30..curr+32].try_into().unwrap()) as usize;
        let comment_len = u16::from_le_bytes(cd_bytes[curr+32..curr+34].try_into().unwrap()) as usize;
        let mut header_offset = u32::from_le_bytes(cd_bytes[curr+42..curr+46].try_into().unwrap()) as u64;
        let filename = std::str::from_utf8(&cd_bytes[curr+46..curr+46+name_len]).unwrap_or("").to_string();
        if extra_len > 0 && (uncomp_size == 0xFFFFFFFF || comp_size == 0xFFFFFFFF || header_offset == 0xFFFFFFFF) {
            let extra_start = curr + 46 + name_len;
            let extra_end = extra_start + extra_len;
            let mut ptr = extra_start;
            while ptr + 4 <= extra_end {
                let tag = u16::from_le_bytes(cd_bytes[ptr..ptr+2].try_into().unwrap());
                let sz = u16::from_le_bytes(cd_bytes[ptr+2..ptr+4].try_into().unwrap()) as usize;
                if tag == 0x0001 {
                    let mut data_ptr = ptr + 4;
                    if uncomp_size == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                        uncomp_size = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr+8].try_into().unwrap());
                        data_ptr += 8;
                    }
                    if comp_size == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                        comp_size = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr+8].try_into().unwrap());
                        data_ptr += 8;
                    }
                    if header_offset == 0xFFFFFFFF && data_ptr + 8 <= ptr + 4 + sz {
                        header_offset = u64::from_le_bytes(cd_bytes[data_ptr..data_ptr+8].try_into().unwrap());
                    }
                }
                ptr += 4 + sz;
            }
        }
        entries.push(CdEntry { filename, header_offset, compressed_size: comp_size, is_deflated });
        curr += 46 + name_len + extra_len + comment_len;
    }
    entries
}

#[tokio::main]
// FIX: Changed main return error type to be Send + Sync
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    
    let mut geojson_str = String::new();
    if args.geojson == "-" {
        std::io::stdin().read_to_string(&mut geojson_str)?;
    } else {
        let mut geojson_file = StdFile::open(&args.geojson)?;
        geojson_file.read_to_string(&mut geojson_str)?;
    }
    let clip_polygon = clip::parse_geojson_polygon(&geojson_str).expect("Failed to parse GeoJSON");

    let s3_client = if args.no_sign_request {
        if args.debug { println!("[DEBUG] Using anonymous reqwest client (no-sign-request)."); }
        S3Client::Unsigned(reqwest::Client::new())
    } else {
        if args.debug { println!("[DEBUG] Using standard authenticated AWS S3 client."); }
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        S3Client::Signed(aws_sdk_s3::Client::new(&config))
    };

    if args.debug { println!("Connecting to s3://{}/{}...", args.bucket, args.key); }
    
    // FIX: Renamed get_size -> fetch_size
    let file_size = s3_client.fetch_size(&args.bucket, &args.key).await?;
    let eocd_read_size = std::cmp::min(file_size, 65536);
    let eocd_start = file_size - eocd_read_size;
    // FIX: Renamed get_range -> fetch_range
    let eocd_bytes = s3_client.fetch_range(&args.bucket, &args.key, eocd_start, file_size - 1).await?;

    let mut cd_offset = 0;
    let mut cd_size = 0;
    for i in (0..eocd_bytes.len().saturating_sub(22)).rev() {
        if &eocd_bytes[i..i+4] == &[0x50, 0x4b, 0x05, 0x06] {
            cd_size = u32::from_le_bytes(eocd_bytes[i+12..i+16].try_into().unwrap()) as u64;
            cd_offset = u32::from_le_bytes(eocd_bytes[i+16..i+20].try_into().unwrap()) as u64;
            break;
        }
    }
     for i in (0..eocd_bytes.len().saturating_sub(20)).rev() {
        if &eocd_bytes[i..i+4] == &[0x50, 0x4b, 0x06, 0x07] {
            let zip64_eocd_offset = u64::from_le_bytes(eocd_bytes[i+8..i+16].try_into().unwrap());
            // FIX: Renamed get_range -> fetch_range
            let z64_bytes = s3_client.fetch_range(&args.bucket, &args.key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
            if &z64_bytes[0..4] == &[0x50, 0x4b, 0x06, 0x06] {
                cd_size = u64::from_le_bytes(z64_bytes[40..48].try_into().unwrap());
                cd_offset = u64::from_le_bytes(z64_bytes[48..56].try_into().unwrap());
            }
            break;
        }
    }

    if args.debug { println!("[DEBUG] Fetching Central Directory ({} bytes)...", cd_size); }
    // FIX: Renamed get_range -> fetch_range
    let cd_bytes = s3_client.fetch_range(&args.bucket, &args.key, cd_offset, cd_offset + cd_size - 1).await?;
    let archive_entries = parse_central_directory(&cd_bytes);
    if args.debug { println!("[DEBUG] Mapped {} file entries.", archive_entries.len()); }

    let mut processed_jsons: HashMap<String, serde_json::Value> = HashMap::new();
    let mut keep_uris: HashSet<String> = HashSet::new();
    let mut queue: Vec<String> = vec!["tileset.json".to_string()];
    keep_uris.insert("tileset.json".to_string());

    while let Some(current_json_path) = queue.pop() {
        let entry = match archive_entries.iter().find(|e| e.filename == current_json_path) {
            Some(e) => e,
            None => { println!("[Warn] Missing JSON: {}", current_json_path); continue; }
        };

        // FIX: Renamed get_range -> fetch_range
        let lfh_bytes = s3_client.fetch_range(&args.bucket, &args.key, entry.header_offset, entry.header_offset + 30 + entry.filename.len() as u64 + 128).await?;
        let lfh_extra_len = u16::from_le_bytes(lfh_bytes[28..30].try_into().unwrap()) as u64;
        let payload_offset = entry.header_offset + 30 + entry.filename.len() as u64 + lfh_extra_len;

        // FIX: Renamed get_range -> fetch_range
        let compressed_bytes = s3_client.fetch_range(&args.bucket, &args.key, payload_offset, payload_offset + entry.compressed_size - 1).await?;

        let mut json_bytes = Vec::new();
        if entry.is_deflated {
            let mut decoder = DeflateDecoder::new(&compressed_bytes[..]);
            decoder.read_to_end(&mut json_bytes)?;
        } else {
            json_bytes.extend_from_slice(&compressed_bytes);
        }
        
        let json_val: serde_json::Value = serde_json::from_slice(&json_bytes)?;
        let mut local_uris = Vec::new();
        let clipped_json = clip::filter_tileset(json_val, &clip_polygon, &mut local_uris);
        processed_jsons.insert(current_json_path.clone(), clipped_json);

        for uri in local_uris {
            let resolved = resolve_uri(&current_json_path, &uri);
            if !keep_uris.contains(&resolved) {
                if resolved.ends_with(".json") { queue.push(resolved.clone()); }
                keep_uris.insert(resolved);
            }
        }
    }
    
    println!("Found {} files to download. Commencing extraction...", keep_uris.len());
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

    let s3_client_shared = Arc::new(s3_client);

    for entry in &archive_entries {
        if keep_uris.contains(&entry.filename) {
            let filename = entry.filename.clone();
            if let Some(clipped_json) = processed_jsons.get(&filename) {
                let data = serde_json::to_string(clipped_json)?.into_bytes();
                let tx_clone = tx.clone();
                let task = tokio::spawn(async move { let _ = tx_clone.send(DownloadedFile { filename, data }).await; });
                fetch_tasks.push(task);
                continue;
            }

            let header_offset = entry.header_offset;
            let compressed_size = entry.compressed_size;
            let filename_len = filename.len();
            let is_deflated = entry.is_deflated;

            let semaphore_clone = semaphore.clone();
            let tx_clone = tx.clone();
            let pb_clone = pb.clone();
            let client_clone = s3_client_shared.clone();
            let bucket_clone = args.bucket.clone();
            let key_clone = args.key.clone();
            let target_filename = filename.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore_clone.acquire_owned().await.unwrap();
                if let Some(ref bar) = pb_clone { bar.set_message(format!("Fetching {}", target_filename)); }
                
                let range_end = header_offset + 30 + filename_len as u64 + 256 + compressed_size;
                // FIX: Renamed get_range -> fetch_range
                let bytes = match client_clone.fetch_range(&bucket_clone, &key_clone, header_offset, range_end).await {
                    Ok(b) => b,
                    Err(e) => { eprintln!("\n[ERROR] Fetch failed for '{}': {:?}", target_filename, e); return; }
                };

                if bytes.len() < 30 { return; }
                let lfh_extra_len = u16::from_le_bytes(bytes[28..30].try_into().unwrap()) as usize;
                let payload_start = 30 + filename_len + lfh_extra_len;
                if bytes.len() < payload_start + compressed_size as usize { return; }

                let compressed_data = &bytes[payload_start..payload_start + compressed_size as usize];
                let mut data = Vec::new();
                if is_deflated {
                    let mut decoder = DeflateDecoder::new(compressed_data);
                    decoder.read_to_end(&mut data).unwrap();
                } else {
                    data.extend_from_slice(compressed_data);
                }
                
                if let Err(e) = tx_clone.send(DownloadedFile { filename: target_filename, data }).await {
                    eprintln!("\n[ERROR] Channel send failed: {}", e);
                }
            });
            fetch_tasks.push(task);
        }
    }
    
    drop(tx);
    while let Some(file) = rx.recv().await {
        let builder = ZipEntryBuilder::new(file.filename.clone().into(), Compression::Deflate);
        zip_writer.write_entry_whole(builder, &file.data).await.unwrap();
        if let Some(ref bar) = pb { bar.inc(1); }
    }
    for task in fetch_tasks {
        task.await.unwrap();
    }
    
    let dummy_index_size = keep_uris.len() * 24;
    let dummy_index = vec![0u8; dummy_index_size];
    zip_writer.write_entry_whole(ZipEntryBuilder::new("@3dtilesIndex1@".into(), Compression::Stored), &dummy_index).await?;
    zip_writer.close().await?;
    if let Some(ref bar) = pb { bar.finish_with_message("Done!"); }

    println!("Clipped files written. Generating and patching binary index...");
    let mut file = tokio::fs::OpenOptions::new().read(true).write(true).open(&args.output).await?;
    let read_file = StdFile::open(&args.output)?;
    let mut final_archive = ZipArchive::new(read_file)?;

    struct IndexRecord { md5hash: [u8; 16], offset: u64 }
    let mut tzindex: Vec<IndexRecord> = Vec::new();
    let mut index_header_offset = 0;
    
    for i in 0..final_archive.len() {
        let file_entry = final_archive.by_index(i)?;
        if file_entry.name() == "@3dtilesIndex1@" {
            index_header_offset = file_entry.header_start();
        } else {
            let mut hasher = Md5::new();
            let normalized_path = file_entry.name().replace('\\', "/");
            hasher.update(normalized_path.as_bytes());
            tzindex.push(IndexRecord { md5hash: hasher.finalize().into(), offset: file_entry.header_start() });
        }
    }
    
    tzindex.sort_by_key(|x| {
        let part1 = u64::from_le_bytes(x.md5hash[0..8].try_into().unwrap());
        let part2 = u64::from_le_bytes(x.md5hash[8..16].try_into().unwrap());
        (part1, part2)
    });

    let mut bindex = Vec::new();
    for i in tzindex {
        bindex.extend_from_slice(&i.md5hash);
        bindex.extend_from_slice(&i.offset.to_le_bytes());
    }

    let crc32 = crc32fast::hash(&bindex);
    let index_payload_offset = final_archive.by_name("@3dtilesIndex1@")?.data_start();
    
    file.seek(SeekFrom::Start(index_payload_offset)).await?;
    file.write_all(&bindex).await?;
    file.seek(SeekFrom::Start(index_header_offset + 14)).await?;
    file.write_all(&crc32.to_le_bytes()).await?;
    
    println!("Success! Clipped dataset is fully 3tz compliant.");
    Ok(())
}
