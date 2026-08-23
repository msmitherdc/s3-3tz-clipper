mod clip;

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::BehaviorVersion;
use clap::Parser;
use std::fs::File as StdFile;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::{mpsc, Semaphore};
use flate2::read::{DeflateDecoder, GzDecoder};
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};
use tracing_subscriber::EnvFilter;
use futures::stream::{FuturesUnordered, StreamExt};
use geo::{Intersects, BoundingRect};
use std::path::Path;
use crate::clip::{I3SNode, ChildRef};

/// Default maximum decompressed size per entry, in MiB - a zip-bomb guard, not a format
/// limit. Entries above it are skipped with an error rather than silently truncated, so the
/// cap has to clear the largest *legitimate* tile payload or clipping quietly loses content.
/// Override with `--max-entry-size`.
const DEFAULT_MAX_ENTRY_MIB: u64 = 256;

/// Peak in-memory decompressed bytes (`--max-entry-size` * `--concurrency`) past which we warn
/// that the configured ceiling could exhaust memory if every in-flight entry were maximal.
const MEMORY_CEILING_WARN_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Knobs threaded through the clipping pipeline. Grouped so the per-archive entry points stay
/// readable as options accumulate.
#[derive(Debug, Clone, Copy)]
struct ClipOptions {
    /// Max concurrent downloads within a single archive's tile fetches.
    concurrency: usize,
    /// Max archives clipped in parallel in `--package` mode.
    archive_concurrency: usize,
    progress: bool,
    debug: bool,
    /// Maximum decompressed size accepted for any single entry.
    max_entry_bytes: u64,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Cloud-Optimized 3dtiles/I3S Clipper")]
struct Args {
    /// Raw name of the S3 bucket (do not prefix with `s3://`). Omit to read from the local
    /// filesystem instead, in which case `--key`/`--package` are interpreted as paths
    /// (relative to `--root` if given, otherwise to the current directory).
    #[arg(short, long)] bucket: Option<String>,
    /// Base directory for local-filesystem reads. Only meaningful when `--bucket` is
    /// omitted; `--key`/`--package` are resolved relative to it.
    #[arg(long, conflicts_with = "bucket")] root: Option<String>,
    /// Full path to a single `.3tz`/`.slpk`/`.spk` archive within the bucket. Mutually
    /// exclusive with `--package`.
    #[arg(short, long, conflicts_with = "package", required_unless_present = "package")] key: Option<String>,
    /// Full path to a "package" tileset.json within the bucket - a bare (non-archive) JSON
    /// file whose root.children each reference their own separate `.3tz`/`.slpk`/`.spk`
    /// archive via `content.uri` (as OWT/Vricon multi-content packages do). Every referenced
    /// archive is clipped independently, in parallel, and written under `--output` at the
    /// same relative path as its `content.uri`; the package's own tileset.json is rewritten
    /// alongside it with each surviving child's (and the root's) `region` shrunk to match.
    /// Mutually exclusive with `--key`.
    #[arg(long)] package: Option<String>,
    #[arg(short, long)] geojson: String,
    /// Output file path in single-archive (`--key`) mode, or output directory in package
    /// (`--package`) mode.
    #[arg(short, long)] output: String,
    #[arg(short, long)] progress: bool,
    /// Max concurrent S3 downloads *within* a single archive's tile fetches.
    #[arg(short, long, default_value_t = 20)] concurrency: usize,
    /// Max archives clipped in parallel in `--package` mode. Each archive additionally uses
    /// up to `--concurrency` connections of its own, so total in-flight connections can
    /// reach `archive_concurrency * concurrency`.
    #[arg(long, default_value_t = 4)] archive_concurrency: usize,
    #[arg(long, default_value_t = false)] debug: bool,
    #[arg(long, default_value_t = false)] no_sign_request: bool,
    /// Named profile from `~/.aws/config`/`~/.aws/credentials` to authenticate with,
    /// overriding the `AWS_PROFILE` environment variable (which is still honored when this
    /// is omitted). Only meaningful for signed requests against `--bucket`.
    #[arg(long, value_name = "NAME")] profile: Option<String>,
    /// AWS region to sign requests for, overriding every other source (`AWS_REGION`, the
    /// profile's own `region`, instance metadata). Needed when the region that signs the
    /// request has to differ from the one the selected profile declares - a `--profile` whose
    /// home region is not where `--bucket` lives, which S3 rejects with
    /// `AuthorizationHeaderMalformed`.
    #[arg(long, value_name = "REGION")] region: Option<String>,
    /// S3 endpoint to send requests to, overriding the `AWS_S3_ENDPOINT` / `AWS_ENDPOINT_URL`
    /// environment variables. A bare host is assumed to be `https://`. Needed when the
    /// environment pins an endpoint in one partition (commercial) while `--bucket` lives in
    /// another (GovCloud, China), which S3 rejects with `AuthorizationHeaderMalformed`.
    #[arg(long, value_name = "URL")] endpoint_url: Option<String>,
    /// Maximum decompressed size accepted for a single archive entry, in MiB. Entries larger
    /// than this are skipped with an error, so raise it if a dataset has legitimately huge
    /// tiles. Guards against zip bombs; peak memory scales with this * `--concurrency`.
    #[arg(long, default_value_t = DEFAULT_MAX_ENTRY_MIB, value_parser = clap::value_parser!(u64).range(1..))]
    max_entry_size: u64,
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
    /// Raw ZIP compression method code (8 = DEFLATE, 93 = Zstandard - what OWT/Vricon .3tz
    /// archives actually use for every entry, 0 = stored/uncompressed).
    comp_method: u16,
}

/// Where archives are read from. `bucket` is ignored by the `Local` variant, which resolves
/// `key` against its own root directory instead.
#[derive(Clone)]
enum ObjectSource {
    Signed(aws_sdk_s3::Client),
    Unsigned(reqwest::Client, String),
    Local(std::path::PathBuf),
}

/// Read `len` bytes at `offset` from a local file without disturbing any shared cursor, so
/// concurrent range reads of the same archive don't need to serialize behind a seek. Runs on
/// the blocking pool: this is a real syscall that would otherwise stall a runtime worker.
async fn local_read_at(path: std::path::PathBuf, offset: u64, len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    tokio::task::spawn_blocking(move || {
        let file = StdFile::open(&path).map_err(|e| format!("Could not open '{}': {}", path.display(), e))?;
        let mut buf = vec![0u8; len];
        let mut filled = 0usize;
        while filled < len {
            let n = {
                #[cfg(unix)]
                { std::os::unix::fs::FileExt::read_at(&file, &mut buf[filled..], offset + filled as u64)? }
                #[cfg(windows)]
                { std::os::windows::fs::FileExt::seek_read(&file, &mut buf[filled..], offset + filled as u64)? }
            };
            // A short read at EOF is expected when a caller's speculative range runs past the
            // end of the file; truncate rather than erroring so the caller sees what exists.
            if n == 0 { break; }
            filled += n;
        }
        buf.truncate(filled);
        Ok(buf)
    })
    .await?
}

impl ObjectSource {
    /// Resolve a key to a local path. Used only by the `Local` variant.
    fn local_path(root: &std::path::Path, key: &str) -> std::path::PathBuf {
        root.join(key)
    }

    /// Human-readable location of `key`, for log messages.
    fn describe(&self, bucket: &str, key: &str) -> String {
        match self {
            ObjectSource::Local(root) => Self::local_path(root, key).display().to_string(),
            _ => format!("s3://{}/{}", bucket, key),
        }
    }

    async fn fetch_size(&self, bucket: &str, key: &str) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ObjectSource::Local(root) => {
                let path = Self::local_path(root, key);
                let meta = tokio::fs::metadata(&path).await
                    .map_err(|e| format!("Could not stat '{}': {}", path.display(), e))?;
                Ok(meta.len())
            }
            ObjectSource::Signed(client) => {
                let head = client.head_object().bucket(bucket).key(key).send().await?;
                Ok(head.content_length().unwrap_or(0) as u64)
            }
            ObjectSource::Unsigned(client, base_url) => {
                let url = format!("{}/{}/{}", base_url, bucket, key);
                let resp = client.head(&url).send().await?;
                if !resp.status().is_success() {
                    return Err(format!("HTTP Error: {} for url {}", resp.status(), url).into());
                }
                let len = resp.headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .ok_or("No content-length header")?
                    .to_str()?
                    .parse::<u64>()?;
                Ok(len)
            }
        }
    }

    /// Fetch the inclusive byte range `[start, end]`. May return fewer bytes than requested
    /// if the range runs past the end of the object; callers that speculatively over-fetch
    /// rely on that.
    async fn fetch_range(&self, bucket: &str, key: &str, start: u64, end: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ObjectSource::Local(root) => {
                let len = end.saturating_sub(start).saturating_add(1) as usize;
                local_read_at(Self::local_path(root, key), start, len).await
            }
            ObjectSource::Signed(client) => {
                let resp = client.get_object()
                    .bucket(bucket)
                    .key(key)
                    .range(format!("bytes={}-{}", start, end))
                    .send()
                    .await?;
                Ok(resp.body.collect().await?.into_bytes().to_vec())
            }
            ObjectSource::Unsigned(client, base_url) => {
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

    /// Fetch a whole object as-is (no Range header) - used for a package's own bare
    /// tileset.json, which isn't a zip archive at all so has no Central Directory to seek
    /// around.
    async fn fetch_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        match self {
            ObjectSource::Local(root) => {
                let path = Self::local_path(root, key);
                tokio::fs::read(&path).await
                    .map_err(|e| format!("Could not read '{}': {}", path.display(), e).into())
            }
            ObjectSource::Signed(client) => {
                let resp = client.get_object().bucket(bucket).key(key).send().await?;
                Ok(resp.body.collect().await?.into_bytes().to_vec())
            }
            ObjectSource::Unsigned(client, base_url) => {
                let url = format!("{}/{}/{}", base_url, bucket, key);
                let resp = client.get(&url).send().await?;
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

/// Read a decompression stream to completion, erroring (rather than silently truncating)
/// if it exceeds `max_bytes`.
fn read_capped(reader: &mut impl Read, max_bytes: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = Vec::new();
    reader.take(max_bytes + 1).read_to_end(&mut buf)?;
    if buf.len() as u64 > max_bytes {
        return Err(format!(
            "Entry exceeds the {} MiB decompressed-size limit (raise --max-entry-size to keep it)",
            max_bytes / (1024 * 1024)
        ).into());
    }
    Ok(buf)
}

fn decompress_deflate(compressed: &[u8], max_bytes: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    read_capped(&mut DeflateDecoder::new(compressed), max_bytes)
}

fn decompress_gzip(compressed: &[u8], max_bytes: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    read_capped(&mut GzDecoder::new(compressed), max_bytes)
}

fn decompress_zstd(compressed: &[u8], max_bytes: u64) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    read_capped(&mut zstd::Decoder::new(compressed)?, max_bytes)
}

fn parse_central_directory(cd_bytes: &[u8]) -> Vec<CdEntry> {
    let mut entries = Vec::new();
    let mut curr = 0;
    let len = cd_bytes.len();
    while curr + 46 <= len {
        if cd_bytes[curr..curr + 4] != [0x50, 0x4b, 0x01, 0x02] { break; }
        let comp_method = u16::from_le_bytes(cd_bytes[curr + 10..curr + 12].try_into().unwrap());
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
        entries.push(CdEntry { filename, header_offset, compressed_size: comp_size, comp_method });
        curr += 46 + name_len + extra_len + comment_len;
    }
    entries
}

/// O(1) lookup of a Central Directory entry by name, transparently falling back to the
/// `.gz` variant of the same name. `index` maps filename -> position in `entries`.
fn lookup_entry<'a>(
    entries: &'a [CdEntry],
    index: &HashMap<String, usize>,
    name: &str,
) -> Option<&'a CdEntry> {
    index
        .get(name)
        .or_else(|| index.get(&format!("{}.gz", name)))
        .map(|&i| &entries[i])
}

/// Scan a tail chunk of the archive for the standard End Of Central Directory record.
/// Returns (cd_size, cd_offset). Searches backwards so the *last* EOCD wins (a file
/// comment could embed the signature bytes). Note the inclusive upper bound: a
/// comment-less archive has its EOCD at exactly `len - 22`.
fn find_eocd(tail: &[u8]) -> Option<(u64, u64)> {
    if tail.len() < 22 {
        return None;
    }
    for i in (0..=tail.len() - 22).rev() {
        if tail[i..i + 4] == [0x50, 0x4b, 0x05, 0x06] {
            let cd_size = u32::from_le_bytes(tail[i + 12..i + 16].try_into().unwrap()) as u64;
            let cd_offset = u32::from_le_bytes(tail[i + 16..i + 20].try_into().unwrap()) as u64;
            return Some((cd_size, cd_offset));
        }
    }
    None
}

/// Scan a tail chunk for the ZIP64 EOCD locator; returns the absolute offset of the ZIP64
/// EOCD record it points at.
fn find_zip64_locator(tail: &[u8]) -> Option<u64> {
    if tail.len() < 20 {
        return None;
    }
    for i in (0..=tail.len() - 20).rev() {
        if tail[i..i + 4] == [0x50, 0x4b, 0x06, 0x07] {
            return Some(u64::from_le_bytes(tail[i + 8..i + 16].try_into().unwrap()));
        }
    }
    None
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

/// Extra-field slop speculatively appended to an entry's Local File Header read so the header
/// and its payload arrive in a *single* range request instead of two.
///
/// A Local File Header's extra field is a per-entry length we can't know from the Central
/// Directory, so the naive implementation reads 30 bytes, learns the length, then issues a
/// second request for the payload - doubling the request count for the whole clip. ZIP64
/// extras are 20-28 bytes and Unix timestamp/uid extras add a few dozen more, so 128 bytes
/// covers every layout seen in practice; over-reading a little is vastly cheaper than a
/// second round trip per entry. The exact-payload path below still handles the rare miss.
const LFH_EXTRA_SLOP: u64 = 128;

/// Fetch an entry's raw (still zip-compressed) payload bytes, skipping past its Local File
/// Header.
async fn fetch_raw_entry(
    client: &ObjectSource,
    bucket: &str,
    key: &str,
    entry: &CdEntry,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    // Directory markers and genuinely empty files have no payload at all. Fetching them would
    // compute an inclusive end offset of `start - 1`, underflowing to u64::MAX.
    if entry.compressed_size == 0 {
        return Ok(Vec::new());
    }

    let speculative_len = 30 + entry.filename.len() as u64 + LFH_EXTRA_SLOP + entry.compressed_size;
    let buf = client
        .fetch_range(bucket, key, entry.header_offset, entry.header_offset + speculative_len - 1)
        .await?;
    if buf.len() < 30 {
        return Err(format!("Short LFH header for '{}'", entry.filename).into());
    }

    let lfh_fname_len = u16::from_le_bytes(buf[26..28].try_into().unwrap()) as u64;
    let lfh_extra_len = u16::from_le_bytes(buf[28..30].try_into().unwrap()) as u64;
    let payload_start = 30 + lfh_fname_len + lfh_extra_len;
    let payload_end = payload_start
        .checked_add(entry.compressed_size)
        .ok_or_else(|| format!("Implausible payload extent for '{}'", entry.filename))?;

    if payload_end <= buf.len() as u64 {
        let mut data = buf;
        data.truncate(payload_end as usize);
        data.drain(..payload_start as usize);
        return Ok(data);
    }

    // The header's extra field ran past our slop - fall back to fetching the payload exactly.
    let abs_start = entry.header_offset + payload_start;
    client
        .fetch_range(bucket, key, abs_start, abs_start + entry.compressed_size - 1)
        .await
}

/// Whether a payload of `len` bytes needs per-entry ZIP64 headers when written.
///
/// The `zip` crate does *not* upgrade an entry automatically: writing more than
/// `spec::ZIP64_BYTES_THR` (== `u32::MAX`) uncompressed bytes with `large_file` unset aborts
/// the entry with "Large file option has not been set". Setting it unconditionally would
/// instead add ZIP64 extra fields to every header in the archive, so the choice is made per
/// entry - which is only possible because entries are written from an in-memory buffer whose
/// length is known up front.
fn needs_zip64(len: usize) -> bool {
    len as u64 > u32::MAX as u64
}

/// Undo an entry's zip compression layer, and - only if `gunzip` - its `.gz` content encoding
/// on top of that. Callers that just want to copy a `.gz` entry through to the output verbatim
/// pass `gunzip = false` and skip an entire decompress/recompress cycle.
fn decode_entry(
    raw: Vec<u8>,
    comp_method: u16,
    gunzip: bool,
    max_bytes: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let file_data = match comp_method {
        8 => decompress_deflate(&raw, max_bytes)?,
        // Zstandard - what OWT/Vricon .3tz archives actually use for every entry.
        93 => decompress_zstd(&raw, max_bytes)?,
        _ => raw,
    };
    if gunzip {
        decompress_gzip(&file_data, max_bytes)
    } else {
        Ok(file_data)
    }
}

async fn fetch_entry_decoded(
    client: &ObjectSource,
    bucket: &str,
    key: &str,
    entry: &CdEntry,
    gunzip: bool,
    max_bytes: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let raw_payload = fetch_raw_entry(client, bucket, key, entry).await?;
    let comp_method = entry.comp_method;

    // Inflate on the blocking pool. Entries decompress to as much as `max_bytes`, and doing
    // that inline parks a tokio worker for the whole decode - stalling the I/O completions of
    // every other in-flight fetch that happens to share the thread. This is what makes the
    // decompression actually parallel across cores.
    tokio::task::spawn_blocking(move || decode_entry(raw_payload, comp_method, gunzip, max_bytes)).await?
}

/// Fetch an entry fully decoded - zip layer *and* any `.gz` content encoding removed - so the
/// caller gets the plaintext bytes (JSON, tile payload, …).
async fn fetch_file_content(
    client: &ObjectSource,
    bucket: &str,
    key: &str,
    entry: &CdEntry,
    max_bytes: u64,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let gunzip = entry.filename.ends_with(".gz");
    fetch_entry_decoded(client, bucket, key, entry, gunzip, max_bytes).await
}

async fn fetch_and_clip_3dtiles_json(
    client: Arc<ObjectSource>,
    bucket: String,
    key: String,
    archive_entries: Arc<Vec<CdEntry>>,
    entry_index: Arc<HashMap<String, usize>>,
    json_path: String,
    polygon: Arc<geo::Polygon<f64>>,
    max_bytes: u64,
) -> Result<(String, serde_json::Value, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let entry = lookup_entry(&archive_entries, &entry_index, &json_path)
        .ok_or_else(|| format!("Missing JSON entry: {}", json_path))?;

    let json_bytes = fetch_file_content(&client, &bucket, &key, entry, max_bytes).await?;
    let json_val: serde_json::Value = serde_json::from_slice(&json_bytes)?;

    let mut local_uris = Vec::new();
    let clipped_json = clip::filter_tileset(json_val, &json_path, &polygon, &mut local_uris);

    Ok((json_path, clipped_json, local_uris))
}

/// Clip a single `.3tz`/`.slpk`/`.spk` archive (whichever `key` names) against
/// `clip_polygon`, writing the result to `output_path`. This is the entire original
/// single-archive pipeline, factored out so both plain `--key` mode and each archive
/// discovered via `--package` mode share the exact same logic.
async fn clip_one_archive(
    s3_client: Arc<ObjectSource>,
    bucket: &str,
    key: &str,
    output_path: &Path,
    clip_polygon: Arc<geo::Polygon<f64>>,
    opts: ClipOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ClipOptions { concurrency, progress, debug, max_entry_bytes, .. } = opts;
    let archive_format = if key.ends_with(".3tz") {
        ArchiveFormat::Cesium3DTiles
    } else if key.ends_with(".slpk") || key.ends_with(".spk") {
        ArchiveFormat::EsriI3S
    } else {
        return Err(format!("Unsupported file extension for '{}'. Please use .3tz, .slpk, or .spk", key).into());
    };

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    println!("Opening {}...", s3_client.describe(bucket, key));
    let file_size = s3_client.fetch_size(bucket, key).await?;

    let mut cd_offset = 0u64;
    let mut cd_size = 0u64;

    if file_size < 22 {
        return Err(format!("'{}' is only {} bytes - too small to be a zip archive.", key, file_size).into());
    }
    let eocd_read_size = std::cmp::min(file_size, 65536);
    let eocd_start = file_size - eocd_read_size;
    let eocd_bytes = s3_client.fetch_range(bucket, key, eocd_start, file_size - 1).await?;
    if let Some((size, offset)) = find_eocd(&eocd_bytes) {
        cd_size = size;
        cd_offset = offset;
    }
    if let Some(zip64_eocd_offset) = find_zip64_locator(&eocd_bytes) {
        let z64_bytes = s3_client.fetch_range(bucket, key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
        if z64_bytes.len() >= 56 && z64_bytes[0..4] == [0x50, 0x4b, 0x06, 0x06] {
            cd_size = u64::from_le_bytes(z64_bytes[40..48].try_into().unwrap());
            cd_offset = u64::from_le_bytes(z64_bytes[48..56].try_into().unwrap());
        }
    }

    if cd_size == 0 {
        if debug { println!("[DEBUG] Fast EOCD scan failed. Engaging robust seeking scanner..."); }
        const CHUNK_SIZE: u64 = 16384;
        const MAX_EOCD_SEARCH_SIZE: u64 = 1024 * 1024;
        let search_limit = std::cmp::min(file_size, MAX_EOCD_SEARCH_SIZE);
        let mut current_pos = file_size;
        let mut eocd_found = false;
        while current_pos > file_size - search_limit {
            let read_start = current_pos.saturating_sub(CHUNK_SIZE);
            if debug { println!("[DEBUG] Scanning for EOCD in range: {}-{}", read_start, current_pos - 1); }
            let buffer = s3_client.fetch_range(bucket, key, read_start, current_pos - 1).await?;
            for i in (0..=buffer.len().saturating_sub(22)).rev() {
                if buffer[i..i+4] == [0x50, 0x4b, 0x05, 0x06] {
                    let eocd_absolute_pos = read_start + i as u64;
                    if eocd_absolute_pos >= 20 {
                        let locator_start = eocd_absolute_pos - 20;
                        let locator_bytes = s3_client.fetch_range(bucket, key, locator_start, locator_start + 19).await?;
                        if locator_bytes[0..4] == [0x50, 0x4b, 0x06, 0x07] {
                            let zip64_eocd_offset = u64::from_le_bytes(locator_bytes[8..16].try_into().unwrap());
                            let z64_record_bytes = s3_client.fetch_range(bucket, key, zip64_eocd_offset, zip64_eocd_offset + 55).await?;
                            if z64_record_bytes[0..4] == [0x50, 0x4b, 0x06, 0x06] {
                                cd_size = u64::from_le_bytes(z64_record_bytes[40..48].try_into().unwrap());
                                cd_offset = u64::from_le_bytes(z64_record_bytes[48..56].try_into().unwrap());
                                if debug { println!("[DEBUG] Fallback scanner found ZIP64 EOCD. Size: {}, Offset: {}", cd_size, cd_offset); }
                                eocd_found = true;
                                break;
                            }
                        }
                    }
                    cd_size = u32::from_le_bytes(buffer[i+12..i+16].try_into().unwrap()) as u64;
                    cd_offset = u32::from_le_bytes(buffer[i+16..i+20].try_into().unwrap()) as u64;
                    if debug { println!("[DEBUG] Fallback scanner found standard EOCD. Size: {}, Offset: {}", cd_size, cd_offset); }
                    eocd_found = true;
                    break;
                }
            }
            if eocd_found { break; }
            current_pos = read_start;
        }
    }

    if cd_size == 0 {
        let msg = format!("Could not map Central Directory for '{}'. File may be corrupted or not a valid zip archive.", key);
        eprintln!("[ERROR] FATAL: {}", msg);
        return Err(msg.into());
    }

    println!("Fetching Central Directory ({} bytes) for {}...", cd_size, key);
    let cd_bytes = s3_client.fetch_range(bucket, key, cd_offset, cd_offset + cd_size - 1).await?;
    let archive_entries = Arc::new(parse_central_directory(&cd_bytes));
    // filename -> index into archive_entries, so per-file lookups are O(1) instead of a
    // linear scan over the whole Central Directory.
    let entry_index: Arc<HashMap<String, usize>> = Arc::new(
        archive_entries.iter().enumerate().map(|(i, e)| (e.filename.clone(), i)).collect(),
    );
    println!("Mapped {} file entries in {}.", archive_entries.len(), key);

    let mut keep_uris: HashSet<String> = HashSet::new();
    let mut processed_jsons: HashMap<String, serde_json::Value> = HashMap::new();

    if archive_format == ArchiveFormat::Cesium3DTiles {
        println!("Fetching and clipping 3D Tiles dataset...");

        let mut queue = std::collections::VecDeque::new();
        queue.push_back("tileset.json".to_string());

        let mut visited = HashSet::new();
        // Walk the external-tileset graph breadth-first with up to `concurrency` fetches in
        // flight. The nested tilesets discovered at one level are independent of each other,
        // so awaiting them one at a time (as this used to) serialized a round trip per
        // external tileset - the dominant cost on datasets that fan out into many of them.
        let mut active = FuturesUnordered::new();
        let semaphore = Arc::new(Semaphore::new(concurrency));

        while !queue.is_empty() || !active.is_empty() {
            while let Some(json_path) = queue.pop_front() {
                if !visited.insert(json_path.clone()) { continue; }

                let client = s3_client.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let entries = archive_entries.clone();
                let index = entry_index.clone();
                let polygon = clip_polygon.clone();
                let sem = semaphore.clone();

                active.push(tokio::spawn(async move {
                    let _permit = sem.acquire_owned().await.unwrap();
                    let result = fetch_and_clip_3dtiles_json(
                        client, bucket, key, entries, index, json_path.clone(), polygon, max_entry_bytes,
                    ).await;
                    (json_path, result)
                }));

                if active.len() >= concurrency { break; }
            }

            let Some(joined) = active.next().await else { continue };
            match joined {
                Err(join_err) => eprintln!("[ERROR] Tileset task failed: {}", join_err),
                Ok((json_path, Err(e))) => {
                    eprintln!("[ERROR] Failed to fetch and clip 3D Tiles JSON {}: {}", json_path, e);
                }
                Ok((_, Ok((path, clipped_json, local_uris)))) => {
                    processed_jsons.insert(path.clone(), clipped_json);
                    keep_uris.insert(path);

                    for uri in local_uris {
                        if uri.ends_with(".json") {
                            // Recursively fetch nested tilesets
                            queue.push_back(uri);
                        } else {
                            // Mark data files (e.g., .b3dm, .glb) to be kept
                            keep_uris.insert(uri);
                        }
                    }
                }
            }
        }

        println!("Finished parsing 3D Tiles dataset. Kept {} files.", keep_uris.len());

    } else if archive_format == ArchiveFormat::EsriI3S {
        let root_json_path = "3dSceneLayer.json".to_string();
        let root_entry = lookup_entry(&archive_entries, &entry_index, &root_json_path).ok_or("3dSceneLayer.json[.gz] not found")?;

        println!("Fetching 3dSceneLayer.json...");
        let scenelayer_bytes = fetch_file_content(&s3_client, bucket, key, root_entry, max_entry_bytes).await?;
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
            let semaphore = Arc::new(Semaphore::new(concurrency));

            for entry in archive_entries.iter().filter(node_doc_filter) {
                let entry_clone = entry.clone();
                let client = s3_client.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let sem = semaphore.clone();

                fetch_tasks.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let bytes_res = fetch_file_content(&client, &bucket, &key, &entry_clone, max_entry_bytes).await;
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

                                let Some(mbs) = clip::parse_node_bounds(node_val) else { continue };

                                let mut children = Vec::new();
                                if let Some(children_arr) = node_val.get("children").and_then(|c| c.as_array()) {
                                    for child_val in children_arr {
                                        if let Some(cid) = clip::child_id_of(child_val) {
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

            // Traversal only keeps the node *documents*; the renderable payloads
            // (geometries/, textures/, attributes/, …) live under nodes/{id}/ and must be
            // expanded per kept node or the clipped archive contains no content.
            let added = clip::expand_i3s_keep_set(
                archive_entries.iter().map(|e| e.filename.strip_suffix(".gz").unwrap_or(&e.filename)),
                &kept_node_ids,
                &mut keep_uris,
            );
            println!(
                "Expanded I3S keep set: +{} entries (nodes/<id>/*, nodepages/*, statistics/*) for {} kept nodes.",
                added,
                kept_node_ids.len()
            );
        } else {
            // Note this walks the *whole* node tree rather than pruning at the first
            // non-intersecting node: see the traversal below for why spatial culling of
            // subtrees is unsound for I3S. Correctness over round trips.
            println!("Detected I3S 1.6 / flat dataset. Traversing full node tree (Parallel)...");

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
            let semaphore = Arc::new(Semaphore::new(concurrency));
            let polygon_bbox = clip_polygon.bounding_rect()
                .ok_or("Clip polygon has no bounding rect")?;

            while !queue.is_empty() || !active_fetches.is_empty() {
                while !queue.is_empty() && active_fetches.len() < concurrency {
                    if let Some(node_id) = queue.pop_front() {
                        if !visited.insert(node_id.clone()) {
                            continue;
                        }

                        let client = s3_client.clone();
                        let bucket = bucket.to_string();
                        let key = key.to_string();
                        let entries = archive_entries.clone();
                        let index = entry_index.clone();
                        let sem = semaphore.clone();

                        let fut = async move {
                            let _permit = sem.acquire().await.unwrap();
                            let node_path = format!("nodes/{}/3dNodeIndexDocument.json", node_id);

                            let entry = lookup_entry(&entries, &index, &node_path);
                            let res: Result<(String, String, Vec<u8>), String> = match entry {
                                None => Err(format!("Node {} not found in archive", node_id)),
                                Some(e) => {
                                    match fetch_file_content(&client, &bucket, &key, e, max_entry_bytes).await {
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
                            if debug {
                                eprintln!("[WARN] {}", fetch_err);
                            }
                        }
                        Ok(Ok((node_id, containing_doc, bytes))) => {
                            let Ok(node_val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                                eprintln!("[WARN] Failed to parse valid JSON from node '{}'.", node_id);
                                continue;
                            };

                            // Always enqueue this node's children, whether or not it
                            // intersects - and whether or not it even declares usable bounds.
                            // The I3S LOD tree is NOT a strict spatial containment hierarchy:
                            // a parent's MBS does not always bound its children's, so pruning
                            // a subtree because its parent missed silently drops descendants
                            // that genuinely do intersect the clip polygon. This matches what
                            // the 1.7+ node-page walk in clip::filter_i3s_scenelayer already
                            // does; only *keeping* a node is gated on the intersection test.
                            if let Some(children_arr) = node_val.get("children").and_then(|c| c.as_array()) {
                                for child_val in children_arr {
                                    if let Some(cid) = clip::child_id_of(child_val) {
                                        if !visited.contains(&cid) {
                                            queue.push_back(cid);
                                        }
                                    }
                                }
                            }

                            // No usable bounds: we can't evaluate this node, but its subtree
                            // has already been queued above.
                            let Some(mbs) = clip::parse_node_bounds(&node_val) else { continue };

                            let node_bbox = clip::mbs_to_rect(&mbs);
                            let intersects = polygon_bbox.intersects(&node_bbox)
                                && clip_polygon.intersects(&node_bbox);

                            if intersects {
                                keep_uris.insert(containing_doc);
                                keep_uris.insert(format!("nodes/{}/3dNodeIndexDocument.json", node_id));
                                kept_node_ids.insert(node_id.clone());
                            }
                        }
                    }
                }
            }
            // Same payload expansion as the 1.7+ path: keep everything under each kept
            // node's nodes/{id}/ directory (geometries, textures, shared resources, …).
            let added = clip::expand_i3s_keep_set(
                archive_entries.iter().map(|e| e.filename.strip_suffix(".gz").unwrap_or(&e.filename)),
                &kept_node_ids,
                &mut keep_uris,
            );
            println!(
                "Traversal finished: visited {} nodes, kept {} (+{} node resource entries).",
                visited.len(), kept_node_ids.len(), added
            );
        }
    }

    println!("Found {} files that intersect, fetching files ...", keep_uris.len());

    let pb = if progress {
        let bar = ProgressBar::new(keep_uris.len() as u64);
        bar.set_style(ProgressStyle::default_bar().template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}").unwrap());
        Some(Arc::new(bar))
    } else { None };
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let (tx, rx) = mpsc::channel::<DownloadedFile>(concurrency * 2);
    let mut fetch_tasks = Vec::new();

    let index_name = if archive_format == ArchiveFormat::Cesium3DTiles {
        "@3dtilesIndex1@"
    } else {
        "@specialIndexFileHASH128@"
    };

    // The archive writer runs on the blocking pool, draining the channel with `blocking_recv`.
    // Writing a zip entry compresses it (Zstandard, for everything but passthrough `.gz`
    // entries), and doing that inline on the runtime would park a worker for every entry -
    // stalling the I/O completions of the fetches still in flight, exactly as the decode path
    // used to. The writer is inherently sequential either way; this just keeps it off the
    // reactor.
    let writer_output_path = output_path.to_path_buf();
    let writer_pb = pb.clone();
    let writer_handle = tokio::task::spawn_blocking(move || -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut rx = rx;
        let file = StdFile::create(&writer_output_path)?;
        let mut zip_writer = ZipWriter::new(BufWriter::new(file));
        let mut written_count = 0usize;

        // Deliberately one entry per `blocking_recv`. Draining in batches via
        // `blocking_recv_many` was measured to be substantially *worse* (roughly 2x the wall
        // clock, and more than double the system time on a local source): holding a batch of
        // payload buffers in flight costs more than the per-entry wakeups it saves.
        while let Some(file) = rx.blocking_recv() {
            // Zstandard (ZIP method 93) per the 3D Tiles Archive Format v1.4 spec
            // (https://github.com/Maxar-Public/3tz-specification) - the same method OWT/Vricon's
            // own archives already use, and a better read-performance/size trade-off than
            // DEFLATE. `.gz` entries are stored as-is: they carry their own gzip encoding, and
            // re-compressing an already-compressed stream only costs time.
            let method = if file.filename.ends_with(".gz") {
                CompressionMethod::Stored
            } else {
                CompressionMethod::Zstd
            };
            // ZIP64 per-entry headers cost bytes on every entry, so only opt in when the
            // payload genuinely can't be described by a 32-bit size.
            let options = SimpleFileOptions::default()
                .compression_method(method)
                .large_file(needs_zip64(file.data.len()));

            zip_writer.start_file(&file.filename, options)?;
            zip_writer.write_all(&file.data)?;
            written_count += 1;
            if let Some(ref bar) = writer_pb { bar.inc(1); }
        }

        // The dummy index's placeholder bytes get overwritten in-place with the real index
        // later, so it must be sized to exactly the real index's length (one 24-byte record per
        // non-index entry) - not `keep_uris.len()` (only an upper bound: some fetches can fail
        // and never reach this loop). Sizing it any larger leaves stale zero-padding after the
        // real index bytes, which the Local File Header still declares as part of the entry's
        // data - producing a CRC32 mismatch against every other zip reader.
        let dummy_index = vec![0u8; written_count * 24];
        zip_writer.start_file(
            index_name,
            SimpleFileOptions::default()
                .compression_method(CompressionMethod::Stored)
                .large_file(needs_zip64(dummy_index.len())),
        )?;
        zip_writer.write_all(&dummy_index)?;
        zip_writer.finish()?.flush()?;
        Ok(written_count)
    });

    // Iterate the Central Directory directly rather than first materializing a
    // stripped-name -> original-name HashMap over *every* entry in the archive: that map cost
    // two extra String allocations per entry (millions, for a large .slpk) to answer a
    // question each entry can answer about itself.
    for entry in archive_entries.iter() {
        let uncompressed_name = entry.filename.strip_suffix(".gz").unwrap_or(&entry.filename);
        if !keep_uris.contains(uncompressed_name) { continue; }

        let original_was_gzipped = entry.filename.ends_with(".gz");

        if let Some(clipped_json) = processed_jsons.get(uncompressed_name) {
            // We rewrote this JSON's contents, so its `.gz` encoding genuinely has to be
            // rebuilt - unlike the passthrough path below.
            let data = serde_json::to_string(clipped_json)?.into_bytes();
            let tx_clone = tx.clone();
            let uncompressed_name_owned = uncompressed_name.to_string();

            fetch_tasks.push(tokio::spawn(async move {
                let file = if original_was_gzipped {
                    let name = format!("{}.gz", uncompressed_name_owned);
                    let encoded = tokio::task::spawn_blocking(move || {
                        let mut encoder = GzEncoder::new(Vec::new(), GzCompression::default());
                        encoder.write_all(&data)?;
                        encoder.finish()
                    })
                    .await;
                    match encoded {
                        Ok(Ok(gzipped_data)) => DownloadedFile { filename: name, data: gzipped_data },
                        // Falling back to the plaintext bytes here would write the entry under
                        // a name the tileset doesn't reference, so surface it instead.
                        Ok(Err(e)) => {
                            eprintln!("\n[ERROR] Failed to gzip rewritten JSON '{}': {}", name, e);
                            return;
                        }
                        Err(e) => {
                            eprintln!("\n[ERROR] gzip task for '{}' failed: {}", name, e);
                            return;
                        }
                    }
                } else {
                    DownloadedFile { filename: uncompressed_name_owned, data }
                };
                let _ = tx_clone.send(file).await;
            }));
            continue;
        }

        let entry_clone = entry.clone();
        let client_clone = s3_client.clone();
        let bucket_clone = bucket.to_string();
        let key_clone = key.to_string();
        let tx_clone = tx.clone();
        let pb_clone = pb.clone();
        let semaphore_clone = semaphore.clone();

        fetch_tasks.push(tokio::spawn(async move {
            let _permit = semaphore_clone.acquire_owned().await.unwrap();
            if let Some(ref bar) = pb_clone { bar.set_message(format!("Fetching {}", entry_clone.filename)); }

            // For an unmodified `.gz` entry the gzip stream is exactly what we want to write
            // back out (the writer stores `.gz` entries uncompressed), so stop decoding after
            // the zip layer. Previously this gunzipped and then re-gzipped at the default
            // level for no change in content - the single most expensive CPU step in the
            // pipeline, paid once per entry, on I3S archives where nearly everything is `.gz`.
            let fetched = fetch_entry_decoded(
                &client_clone,
                &bucket_clone,
                &key_clone,
                &entry_clone,
                false,
                max_entry_bytes,
            ).await;

            match fetched {
                Ok(data) => {
                    let _ = tx_clone.send(DownloadedFile { filename: entry_clone.filename.clone(), data }).await;
                },
                Err(e) => {
                    eprintln!("\n[ERROR] Failed to fetch/decompress '{}': {:?}", entry_clone.filename, e);
                }
            };
        }));
    }
    // Every sender must be gone before the writer's `blocking_recv` loop can end.
    drop(tx);
    for task in fetch_tasks {
        if let Err(e) = task.await {
            eprintln!("[ERROR] Fetch task failed: {}", e);
        }
    }
    let written_count = writer_handle.await??;

    if let Some(ref bar) = pb { bar.finish_with_message("Done!"); }
    println!("Adding index to zipfile ({})...", key);

    // Reading back the finished archive's Central Directory and patching the index in place is
    // all synchronous file I/O over an archive that can be gigabytes - it belongs on the
    // blocking pool too, not on a runtime worker.
    let finalize_path = output_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut final_archive = ZipArchive::new(StdFile::open(&finalize_path)?)?;

        struct IndexRecord { md5hash: [u8; 16], offset: u64 }
        let mut tzindex: Vec<IndexRecord> = Vec::with_capacity(final_archive.len());
        let mut index_header_offset = 0u64;
        let mut index_central_header_start = 0u64;
        for i in 0..final_archive.len() {
            let file_entry = final_archive.by_index(i)?;
            if file_entry.name() == index_name {
                index_header_offset = file_entry.header_start();
                index_central_header_start = file_entry.central_header_start();
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
        let index_payload_offset = final_archive
            .by_name(index_name)?
            .data_start()
            .ok_or("Index payload offset not found")?;
        drop(final_archive);

        let mut file = std::fs::OpenOptions::new().read(true).write(true).open(&finalize_path)?;
        file.seek(SeekFrom::Start(index_payload_offset))?;
        file.write_all(&bindex)?;
        // The CRC-32 must be patched in *both* places a compliant reader might check it: the
        // Local File Header (offset 14 past its signature/version/flags/method/modtime/moddate)
        // and the Central Directory record for the same entry (offset 16 past its own leading
        // fields - it additionally has a 2-byte "version made by"). Standard zip readers
        // (Python's zipfile, unzip, etc.) validate against the Central Directory copy, not the
        // Local File Header - patching only the latter leaves the archive looking corrupt to
        // every reader except this tool's own index-based one.
        file.seek(SeekFrom::Start(index_header_offset + 14))?;
        file.write_all(&crc32.to_le_bytes())?;
        file.seek(SeekFrom::Start(index_central_header_start + 16))?;
        file.write_all(&crc32.to_le_bytes())?;
        file.flush()?;
        Ok(())
    })
    .await??;

    println!("Success! Clipped {} ({} entries) -> {}", key, written_count, output_path.display());
    Ok(())
}

/// One archive discovered while walking a package's root.children, along with whatever it
/// takes to fetch, clip, and (if it survives) rewrite its entry in the outer tileset.
struct PackageChild {
    index: usize,
    /// Resolved (and path-traversal-checked) relative path - safe to join onto
    /// `base_prefix`/`output_dir` directly. See `clip::resolve_uri`.
    uri: String,
    /// Raw `boundingVolume.region`, if the child declared one (position-preserving - a
    /// non-numeric component is defaulted to `0.0`, never dropped, so it can't shift the
    /// remaining west/south/east/north/height values out of alignment).
    region: Option<Vec<f64>>,
}

/// `--package` mode: fetch a bare package tileset.json, follow every root.children
/// `content.uri` to its own separate archive, clip each one independently (in parallel,
/// bounded by `archive_concurrency`) into `output_dir` at the same relative path as its
/// `content.uri`, then rewrite the package's own tileset.json alongside them with each
/// surviving child's (and the overall root's) `region` shrunk to match - see
/// clip::clip_region/union_regions for why that rewrite is necessary: clip_one_archive only
/// ever touches the archive it's given, never the bare outer tileset.json that references
/// it.
async fn run_package(
    s3_client: Arc<ObjectSource>,
    bucket: &str,
    package_key: &str,
    output_dir: &Path,
    clip_polygon: Arc<geo::Polygon<f64>>,
    opts: ClipOptions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ClipOptions { archive_concurrency, debug, .. } = opts;
    println!("Fetching package tileset {}...", s3_client.describe(bucket, package_key));
    let package_bytes = s3_client.fetch_object(bucket, package_key).await?;
    let mut package_json: serde_json::Value = serde_json::from_slice(&package_bytes)?;

    let base_prefix = match package_key.rfind('/') {
        Some(i) => package_key[..=i].to_string(),
        None => String::new(),
    };

    // First pass (read-only): decide which children are worth fetching at all, scoped so
    // the borrow of `package_json` ends before we start spawning tasks below.
    let mut candidates: Vec<PackageChild> = Vec::new();
    let mut processed_indices: HashSet<usize> = HashSet::new();
    let mut seen_archive_keys: HashSet<String> = HashSet::new();
    {
        let children = package_json
            .get("root")
            .and_then(|r| r.get("children"))
            .and_then(|c| c.as_array())
            .ok_or("Package tileset has no root.children to follow")?;

        for (index, child) in children.iter().enumerate() {
            let uri = match child.get("content").and_then(|c| c.get("uri").or_else(|| c.get("url"))).and_then(|u| u.as_str()) {
                Some(u) => u.to_string(),
                None => continue,
            };
            if !(uri.ends_with(".3tz") || uri.ends_with(".slpk") || uri.ends_with(".spk")) {
                if debug { println!("[DEBUG] Skipping non-archive content.uri: {}", uri); }
                continue;
            }

            // From here on we've committed to a decision (clip it, or drop it) - anything
            // we didn't even get this far for (non-archive content) stays untouched below.
            processed_indices.insert(index);

            // content.uri comes straight from S3-hosted, externally-produced data - reject
            // anything that would escape the package's own directory (absolute paths, `..`
            // escapes) the same way filter_node already does for in-archive references,
            // rather than joining it onto output_dir unchecked.
            let Some(resolved_uri) = clip::resolve_uri("", &uri) else {
                eprintln!("[WARN] Dropping content.uri '{}': escapes the package's own directory.", uri);
                continue;
            };

            // tile_intersects already handles region/S2/box/unknown boundingVolume shapes
            // (conservatively keeping what it can't evaluate) - reuse it here instead of
            // only ever pre-filtering region-shaped children.
            if !clip::tile_intersects(child, &clip_polygon) {
                if debug { println!("[DEBUG] Archive '{}' does not intersect clip polygon; dropping.", resolved_uri); }
                continue;
            }

            if !seen_archive_keys.insert(resolved_uri.clone()) {
                eprintln!("[WARN] Dropping duplicate content.uri '{}': already referenced by another child.", resolved_uri);
                continue;
            }

            let region = child
                .get("boundingVolume")
                .and_then(|b| b.get("region"))
                .and_then(|r| r.as_array())
                .map(|arr| arr.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect::<Vec<f64>>());

            candidates.push(PackageChild { index, uri: resolved_uri, region });
        }
    }

    println!(
        "Clipping {} archive(s) referenced by the package tileset (up to {} in parallel)...",
        candidates.len(),
        archive_concurrency
    );
    tokio::fs::create_dir_all(output_dir).await?;

    let semaphore = Arc::new(Semaphore::new(archive_concurrency));
    let mut tasks = FuturesUnordered::new();

    for candidate in candidates {
        let archive_key = format!("{}{}", base_prefix, candidate.uri);
        let output_path = output_dir.join(&candidate.uri);
        let client = s3_client.clone();
        let bucket = bucket.to_string();
        let polygon = clip_polygon.clone();
        let sem = semaphore.clone();

        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            let result = clip_one_archive(
                client,
                &bucket,
                &archive_key,
                &output_path,
                polygon,
                opts,
            ).await;
            (candidate.index, candidate.uri, candidate.region, result)
        }));
    }

    let mut new_regions: HashMap<usize, [f64; 6]> = HashMap::new();
    let mut kept_indices: HashSet<usize> = HashSet::new();
    let mut failures = 0usize;

    while let Some(joined) = tasks.next().await {
        let (index, uri, region, result) = joined?;
        match result {
            Ok(()) => {
                if let Some(new_region) = region.as_ref().and_then(|r| clip::clip_region(r, &clip_polygon)) {
                    new_regions.insert(index, new_region);
                }
                kept_indices.insert(index);
            }
            Err(e) => {
                failures += 1;
                eprintln!("[ERROR] Failed to clip archive '{}': {}", uri, e);
            }
        }
    }

    {
        let children = package_json
            .get_mut("root")
            .and_then(|r| r.get_mut("children"))
            .and_then(|c| c.as_array_mut())
            .ok_or("Package tileset has no root.children to follow")?;

        let mut retained = Vec::new();
        for (index, mut child) in children.drain(..).enumerate() {
            // Only drop children we actually attempted and which didn't survive - anything
            // we never touched (non-archive content) is kept exactly as it was.
            if processed_indices.contains(&index) && !kept_indices.contains(&index) {
                continue;
            }
            if let Some(new_region) = new_regions.get(&index) {
                if let Some(bv) = child.get_mut("boundingVolume") {
                    bv["region"] = serde_json::json!(new_region.to_vec());
                }
            }
            retained.push(child);
        }
        *children = retained;
    }

    // Only recompute the root's region if every kept child had one to contribute - a kept
    // child with a box/S2/missing boundingVolume has an extent we can't shrink or represent
    // as a region, so unioning just the region-shaped children would understate the root's
    // true extent. Leaving the original (pre-clip) region in that case is the safe default.
    let kept_without_region = kept_indices.iter().filter(|i| !new_regions.contains_key(i)).count();
    if kept_indices.is_empty() {
        println!("[WARN] No archives in the package intersected the clip polygon.");
    } else if kept_without_region > 0 {
        println!(
            "[WARN] {} kept archive(s) have no clippable `region` metadata (box/S2/missing boundingVolume); leaving the package's root boundingVolume unchanged rather than risk understating their true extent.",
            kept_without_region
        );
    } else if let Some(new_root_region) = clip::union_regions(&new_regions.values().cloned().collect::<Vec<_>>()) {
        if let Some(bv) = package_json.get_mut("root").and_then(|r| r.get_mut("boundingVolume")) {
            bv["region"] = serde_json::json!(new_root_region.to_vec());
        }
    }

    let output_name = match package_key.rfind('/') {
        Some(i) => &package_key[i + 1..],
        None => package_key,
    };
    let output_tileset_path = output_dir.join(output_name);
    tokio::fs::write(&output_tileset_path, serde_json::to_vec_pretty(&package_json)?).await?;

    println!(
        "Package clip complete: {} archive(s) kept, {} failed. Wrote {}",
        kept_indices.len(),
        failures,
        output_tileset_path.display()
    );

    if failures > 0 {
        return Err(format!("{} of {} package archive(s) failed to clip", failures, kept_indices.len() + failures).into());
    }
    Ok(())
}

/// Warn when a named profile was requested but no profile file is reachable to define it.
///
/// The SDK locates `~/.aws/config` through `HOME` (`USERPROFILE` on Windows), so a caller
/// that hands us a *replaced* environment - `subprocess.run(env={"AWS_PROFILE": ...})` and
/// friends, which substitute the environment rather than extending it - passes the profile
/// name through intact while making the file that defines it invisible. The profile then
/// resolves to nothing and the run dies further downstream with "A region must be set",
/// which says nothing about the real cause. Explicit file overrides win over `HOME`, matching
/// the SDK's own lookup order.
fn warn_if_profile_files_unreachable(profile: &str) {
    let explicit = ["AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"]
        .iter()
        .filter_map(std::env::var_os)
        .any(|path| std::path::Path::new(&path).is_file());
    if explicit {
        return;
    }
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    if let Some(ref home) = home {
        let aws_dir = std::path::Path::new(home).join(".aws");
        if aws_dir.join("config").is_file() || aws_dir.join("credentials").is_file() {
            return;
        }
    }
    eprintln!(
        "[WARN] profile '{}' was requested, but no AWS profile file could be found{} - it will resolve to no credentials and no region. \
If this tool was launched as a subprocess with a replaced environment (e.g. Python's `subprocess.run(env=...)`), pass the existing environment through \
(`env={{**os.environ, \"AWS_PROFILE\": \"{}\"}}`), or set AWS_CONFIG_FILE/AWS_SHARED_CREDENTIALS_FILE to the profile files explicitly.",
        profile,
        if home.is_none() { " (HOME is not set)" } else { " under $HOME/.aws" },
        profile
    );
}

/// The AWS partition a region belongs to. Partitions are separate clouds: an endpoint in one
/// cannot serve a bucket in another, and credentials do not cross between them either.
fn partition_of_region(region: &str) -> &'static str {
    if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("cn-") {
        "aws-cn"
    } else {
        "aws"
    }
}

/// The partition an endpoint serves, or `None` for a host that is not an AWS endpoint at all
/// (MinIO, OVH, Ceph...), where the question does not apply and no guess should be made.
fn partition_of_endpoint(endpoint: &str) -> Option<&'static str> {
    let host = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()?;
    if host.contains("us-gov-") {
        Some("aws-us-gov")
    } else if host.ends_with(".amazonaws.com.cn") {
        Some("aws-cn")
    } else if host.ends_with(".amazonaws.com") {
        Some("aws")
    } else {
        None
    }
}

/// Warn before the first request when the endpoint in force serves a different partition than
/// the region being signed for.
///
/// The trap is an environment-pinned `AWS_S3_ENDPOINT` (set once for a whole deployment,
/// commercial by default) combined with a `--profile` whose region is GovCloud: every request
/// then goes to the commercial cloud carrying a GovCloud signature, and S3 answers with a
/// bare `AuthorizationHeaderMalformed` naming only the region it wanted - never the endpoint,
/// which is the half that is actually wrong.
fn warn_if_endpoint_partition_mismatch(endpoint: &str, region: &str, endpoint_from_flag: bool) {
    let Some(endpoint_partition) = partition_of_endpoint(endpoint) else {
        return;
    };
    let region_partition = partition_of_region(region);
    if endpoint_partition == region_partition {
        return;
    }
    eprintln!(
        "[WARN] endpoint {} serves the '{}' partition, but requests are signed for region '{}' in '{}'. \
S3 will reject these as AuthorizationHeaderMalformed. {} to reach a bucket in '{}'.",
        endpoint,
        endpoint_partition,
        region,
        region_partition,
        if endpoint_from_flag {
            format!("Pass --endpoint-url https://s3.{}.amazonaws.com", region)
        } else {
            format!(
                "Pass --endpoint-url https://s3.{}.amazonaws.com, or clear AWS_S3_ENDPOINT/AWS_ENDPOINT_URL from this process's environment",
                region
            )
        },
        region
    );
}

/// Turn S3's region-mismatch rejection into an actionable hint.
///
/// `AuthorizationHeaderMalformed` names the region the *endpoint* expected, which invites the
/// wrong conclusion: when a pinned endpoint is the thing at fault, signing for the region it
/// names would only move the failure (to a bucket that does not exist in that partition).
/// Both halves are therefore reported - the region signed for, and the endpoint it was sent
/// to - so the operator can tell which one is wrong.
fn hint_region_mismatch(
    err: &(dyn std::error::Error + 'static),
    signing_region: Option<&str>,
    endpoint: Option<&str>,
) {
    let rendered = format!("{:?}", err);
    if !rendered.contains("AuthorizationHeaderMalformed") {
        return;
    }
    // The metadata reads: the region 'us-gov-west-1' is wrong; expecting 'us-east-1'
    let expected = rendered
        .split_once("expecting '")
        .and_then(|(_, rest)| rest.split_once('\''))
        .map(|(region, _)| region);
    let Some(expected) = expected else { return };
    let signed = signing_region.unwrap_or("<unknown>");
    match endpoint {
        Some(endpoint) => eprintln!(
            "[HINT] Signed for region '{}', but endpoint {} expects '{}'. If --bucket really is in '{}', \
the endpoint is what is wrong - pass --endpoint-url https://s3.{}.amazonaws.com (or clear AWS_S3_ENDPOINT/AWS_ENDPOINT_URL). \
If the bucket is in '{}', pass --region {} instead.",
            signed, endpoint, expected, signed, signed, expected, expected
        ),
        None => eprintln!(
            "[HINT] Signed for region '{}', but S3 expects '{}'. Pass --region {} to sign for the bucket's own region \
(a --profile otherwise signs for the region that profile declares).",
            signed, expected, expected
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // reqwest is built with `rustls-tls-webpki-roots-no-provider` so it shares the AWS SDK's
    // aws-lc-rs backend rather than linking a second (ring) crypto library into the binary.
    // That deliberately leaves rustls' process-wide provider unset, so install it before any
    // TLS client is constructed - reqwest's builder panics with "No provider set" otherwise.
    // An Err here means a provider was already installed, which is exactly what we wanted.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args = Args::parse();

    if args.package.is_some() && args.bucket.is_some() {
        const CONCURRENCY_WARN_THRESHOLD: usize = 200;
        let total_connections = args.archive_concurrency.saturating_mul(args.concurrency);
        if total_connections > CONCURRENCY_WARN_THRESHOLD {
            eprintln!(
                "[WARN] --archive-concurrency ({}) * --concurrency ({}) allows up to {} concurrent S3 connections, which may exhaust local sockets or trip S3 rate limits. Consider lowering one of these.",
                args.archive_concurrency, args.concurrency, total_connections
            );
        }
    }

    let max_entry_bytes = args.max_entry_size * 1024 * 1024;
    // Every in-flight fetch holds its decompressed entry in memory at once, so the configured
    // cap is a per-entry ceiling that multiplies by concurrency in the worst case.
    let peak_decompressed = max_entry_bytes.saturating_mul(args.concurrency as u64);
    if peak_decompressed > MEMORY_CEILING_WARN_BYTES {
        eprintln!(
            "[WARN] --max-entry-size ({} MiB) * --concurrency ({}) allows up to {} GiB of decompressed entries in memory at once. Consider lowering one of these.",
            args.max_entry_size, args.concurrency, peak_decompressed / (1024 * 1024 * 1024)
        );
    }

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
    // Explicit flag first: the environment variables are typically set once for a whole
    // deployment, so a single run against a bucket in another partition has to be able to
    // override them without disturbing everything else that reads them.
    let custom_endpoint = args
        .endpoint_url
        .clone()
        .or_else(|| {
            std::env::var("AWS_S3_ENDPOINT")
                .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
                .ok()
        })
        .map(|url| {
            if url.contains("://") { url } else { format!("https://{}", url) }
        });
    // Region the SDK settled on, reported back if S3 rejects the signature (signed mode only).
    let mut signing_region: Option<String> = None;
    let source = if args.bucket.is_none() {
        // Local-filesystem mode: `--key`/`--package` are paths under `--root` (default ".").
        let root = std::path::PathBuf::from(args.root.as_deref().unwrap_or("."));
        if !root.is_dir() {
            return Err(format!("--root '{}' is not a directory", root.display()).into());
        }
        if args.no_sign_request {
            eprintln!("[WARN] --no-sign-request has no effect without --bucket (reading from the local filesystem).");
        }
        if args.profile.is_some() {
            eprintln!("[WARN] --profile/AWS_PROFILE has no effect without --bucket (reading from the local filesystem).");
        }
        ObjectSource::Local(root)
    } else if args.no_sign_request {
        if args.profile.is_some() {
            eprintln!("[WARN] --profile/AWS_PROFILE has no effect with --no-sign-request (requests are anonymous).");
        }
        let custom_cert = load_custom_certs()?;
        let mut builder = reqwest::Client::builder().use_rustls_tls();
        if let Some(cert) = custom_cert {
            builder = builder.add_root_certificate(cert);
        }
        let reqwest_client = builder.build()?;
        let base_url = custom_endpoint
            .clone()
            .unwrap_or_else(|| "https://s3.amazonaws.com".to_string());
        if args.debug && base_url != "https://s3.amazonaws.com" {
            println!("[DEBUG] Routing anonymous S3 requests to custom endpoint: {}", base_url);
        }
        ObjectSource::Unsigned(reqwest_client, base_url)
    } else {
        // `profile_name` feeds the whole default chain - region *and* credentials - so a
        // profile that only sets `region` still works, exactly as `AWS_PROFILE` in the
        // environment would. Left unset, that environment variable is still honored.
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        // `--profile` overrides AWS_PROFILE, but a profile named either way is only usable if
        // the file defining it can be found - see warn_if_profile_files_unreachable.
        let selected_profile = args
            .profile
            .clone()
            .or_else(|| std::env::var("AWS_PROFILE").ok())
            .filter(|profile| !profile.is_empty());
        if let Some(ref profile) = selected_profile {
            if args.debug { println!("[DEBUG] Using AWS profile: {}", profile); }
            warn_if_profile_files_unreachable(profile);
            // Naming a profile has to actually *select* its credentials. The SDK's default
            // chain consults the environment first, so ambient AWS_ACCESS_KEY_ID /
            // AWS_SESSION_TOKEN - which a caller that inherits or copies its parent's
            // environment (`subprocess.run(env={**os.environ, "AWS_PROFILE": ...})`) passes
            // along without meaning to - would otherwise sign every request while the profile
            // contributed nothing but a region. botocore has the same rule ("an explicitly
            // provided profile will negate an EnvProvider"), so this matches what the AWS CLI
            // and boto3 do with the same configuration.
            //
            // The default chain stays on as a *fallback* rather than being removed outright:
            // a profile that sets only a region, or only `role_arn` against instance
            // metadata, still resolves the way it does today. Only a profile that can supply
            // credentials itself changes anything here.
            let profile_credentials = ProfileFileCredentialsProvider::builder()
                .profile_name(profile)
                .build();
            let fallback = DefaultCredentialsChain::builder()
                .profile_name(profile)
                .build()
                .await;
            loader = loader
                .profile_name(profile)
                .credentials_provider(
                    CredentialsProviderChain::first_try("ExplicitProfile", profile_credentials)
                        .or_else("Default", fallback),
                );
        }
        // Last word on region, ahead of AWS_REGION and the profile's own `region`: selecting
        // a profile for its *credentials* also drags in that profile's home region, which is
        // wrong whenever the bucket lives elsewhere (and fatal when the endpoint is pinned by
        // AWS_S3_ENDPOINT - the request reaches one region signed for another).
        if let Some(ref region) = args.region {
            if args.debug { println!("[DEBUG] Signing for region: {}", region); }
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        let config = loader.load().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&config).force_path_style(true);
        if let Some(ref endpoint) = custom_endpoint {
            if args.debug { println!("[DEBUG] Routing S3 SDK requests to custom endpoint: {}", endpoint); }
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }
        signing_region = config.region().map(|region| region.to_string());
        if let (Some(endpoint), Some(region)) = (custom_endpoint.as_deref(), signing_region.as_deref()) {
            warn_if_endpoint_partition_mismatch(endpoint, region, args.endpoint_url.is_some());
        }
        ObjectSource::Signed(aws_sdk_s3::Client::from_conf(s3_config_builder.build()))
    };
    let s3_client = Arc::new(source);
    let bucket = args.bucket.as_deref().unwrap_or("");
    let opts = ClipOptions {
        concurrency: args.concurrency,
        archive_concurrency: args.archive_concurrency,
        progress: args.progress,
        debug: args.debug,
        max_entry_bytes,
    };

    let result = if let Some(ref package_key) = args.package {
        run_package(
            s3_client,
            bucket,
            package_key,
            Path::new(&args.output),
            clip_polygon,
            opts,
        ).await
    } else {
        let key = args.key.as_ref().unwrap();
        clip_one_archive(
            s3_client,
            bucket,
            key,
            Path::new(&args.output),
            clip_polygon,
            opts,
        ).await
    };
    if let Err(ref err) = result {
        hint_region_mismatch(err.as_ref(), signing_region.as_deref(), custom_endpoint.as_deref());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- helpers to build synthetic zip structures ---

    /// Build one Central Directory record. `sizes` = (compressed, uncompressed),
    /// `extra` = raw extra-field bytes.
    fn cd_record(name: &str, comp_method: u16, sizes: (u32, u32), header_offset: u32, extra: &[u8]) -> Vec<u8> {
        let mut rec = Vec::new();
        rec.extend_from_slice(&[0x50, 0x4b, 0x01, 0x02]); // signature
        rec.extend_from_slice(&[20, 0]); // version made by
        rec.extend_from_slice(&[20, 0]); // version needed
        rec.extend_from_slice(&[0, 0]); // flags
        rec.extend_from_slice(&comp_method.to_le_bytes());
        rec.extend_from_slice(&[0; 4]); // mod time/date
        rec.extend_from_slice(&[0; 4]); // crc32
        rec.extend_from_slice(&sizes.0.to_le_bytes());
        rec.extend_from_slice(&sizes.1.to_le_bytes());
        rec.extend_from_slice(&(name.len() as u16).to_le_bytes());
        rec.extend_from_slice(&(extra.len() as u16).to_le_bytes());
        rec.extend_from_slice(&[0, 0]); // comment len
        rec.extend_from_slice(&[0; 4]); // disk start / internal attrs
        rec.extend_from_slice(&[0; 4]); // external attrs
        rec.extend_from_slice(&header_offset.to_le_bytes());
        rec.extend_from_slice(name.as_bytes());
        rec.extend_from_slice(extra);
        rec
    }

    /// Build a standard EOCD record with the given cd_size/cd_offset and comment.
    fn eocd_record(cd_size: u32, cd_offset: u32, comment: &[u8]) -> Vec<u8> {
        let mut rec = Vec::new();
        rec.extend_from_slice(&[0x50, 0x4b, 0x05, 0x06]);
        rec.extend_from_slice(&[0; 8]); // disk numbers, entry counts
        rec.extend_from_slice(&cd_size.to_le_bytes());
        rec.extend_from_slice(&cd_offset.to_le_bytes());
        rec.extend_from_slice(&(comment.len() as u16).to_le_bytes());
        rec.extend_from_slice(comment);
        rec
    }

    #[test]
    fn parse_cd_basic_entry() {
        let bytes = cd_record("tileset.json", 8, (100, 400), 42, &[]);
        let entries = parse_central_directory(&bytes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "tileset.json");
        assert_eq!(entries[0].comp_method, 8);
        assert_eq!(entries[0].compressed_size, 100);
        assert_eq!(entries[0].header_offset, 42);
    }

    #[test]
    fn parse_cd_multiple_entries_and_backslash_normalization() {
        let mut bytes = cd_record("a\\b\\tile.b3dm", 0, (10, 10), 0, &[]);
        bytes.extend(cd_record("nodes/0/geometries/0.bin", 93, (5, 20), 100, &[]));
        let entries = parse_central_directory(&bytes);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].filename, "a/b/tile.b3dm");
        assert_eq!(entries[1].comp_method, 93);
    }

    #[test]
    fn parse_cd_zip64_extra_field() {
        // compressed size and header offset deferred to the zip64 extra field
        let mut extra = Vec::new();
        extra.extend_from_slice(&0x0001u16.to_le_bytes()); // zip64 tag
        extra.extend_from_slice(&24u16.to_le_bytes()); // size of extra data
        extra.extend_from_slice(&(5_000_000_000u64).to_le_bytes()); // uncompressed
        extra.extend_from_slice(&(4_100_000_000u64).to_le_bytes()); // compressed
        extra.extend_from_slice(&(6_000_000_000u64).to_le_bytes()); // header offset
        let bytes = cd_record("big.glb", 8, (0xFFFFFFFF, 0xFFFFFFFF), 0xFFFFFFFF, &extra);
        let entries = parse_central_directory(&bytes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].compressed_size, 4_100_000_000);
        assert_eq!(entries[0].header_offset, 6_000_000_000);
    }

    #[test]
    fn parse_cd_truncated_name_stops_cleanly() {
        let mut bytes = cd_record("ok.json", 0, (1, 1), 0, &[]);
        let mut bad = cd_record("truncated-name.json", 0, (1, 1), 0, &[]);
        bad.truncate(bad.len() - 5); // cut into the filename
        bytes.extend(bad);
        let entries = parse_central_directory(&bytes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].filename, "ok.json");
    }

    #[test]
    fn find_eocd_at_exact_end_of_buffer() {
        // The common case: no archive comment, EOCD is the last 22 bytes. This position
        // was previously missed by an exclusive-range scan, forcing the slow fallback.
        let mut buf = vec![0xAAu8; 100];
        buf.extend(eocd_record(1234, 5678, &[]));
        assert_eq!(find_eocd(&buf), Some((1234, 5678)));
    }

    #[test]
    fn find_eocd_with_trailing_comment() {
        let mut buf = vec![0u8; 10];
        buf.extend(eocd_record(11, 22, b"a comment"));
        assert_eq!(find_eocd(&buf), Some((11, 22)));
    }

    #[test]
    fn find_eocd_minimal_and_missing() {
        assert_eq!(find_eocd(&eocd_record(7, 9, &[])), Some((7, 9)));
        assert_eq!(find_eocd(&[0u8; 21]), None); // too short
        assert_eq!(find_eocd(&[0u8; 200]), None); // no signature
    }

    #[test]
    fn find_zip64_locator_returns_offset() {
        let mut buf = vec![0u8; 30];
        buf.extend_from_slice(&[0x50, 0x4b, 0x06, 0x07]);
        buf.extend_from_slice(&[0; 4]); // disk number
        buf.extend_from_slice(&(9_876_543_210u64).to_le_bytes());
        buf.extend_from_slice(&[1, 0, 0, 0]); // total disks
        buf.extend(eocd_record(0xFFFFFFFF, 0xFFFFFFFF, &[]));
        assert_eq!(find_zip64_locator(&buf), Some(9_876_543_210));
        assert_eq!(find_zip64_locator(&[0u8; 19]), None);
    }

    #[test]
    fn lookup_entry_exact_and_gz_fallback() {
        let entries = vec![
            CdEntry { filename: "tileset.json.gz".into(), header_offset: 0, compressed_size: 1, comp_method: 0 },
            CdEntry { filename: "tile.b3dm".into(), header_offset: 10, compressed_size: 2, comp_method: 0 },
        ];
        let index: HashMap<String, usize> = entries.iter().enumerate().map(|(i, e)| (e.filename.clone(), i)).collect();
        assert_eq!(lookup_entry(&entries, &index, "tile.b3dm").unwrap().header_offset, 10);
        // falls back to the .gz variant
        assert_eq!(lookup_entry(&entries, &index, "tileset.json").unwrap().filename, "tileset.json.gz");
        assert!(lookup_entry(&entries, &index, "missing.json").is_none());
    }

    /// A cap comfortably above every payload the tests below build.
    const TEST_CAP: u64 = 8 * 1024 * 1024;

    #[test]
    fn deflate_and_gzip_roundtrip() {
        let payload = b"3d tiles payload data".repeat(100);

        let mut deflated = Vec::new();
        let mut enc = flate2::write::DeflateEncoder::new(&mut deflated, GzCompression::default());
        enc.write_all(&payload).unwrap();
        enc.finish().unwrap();
        assert_eq!(decompress_deflate(&deflated, TEST_CAP).unwrap(), payload);

        let mut gzipped = Vec::new();
        let mut enc = GzEncoder::new(&mut gzipped, GzCompression::default());
        enc.write_all(&payload).unwrap();
        enc.finish().unwrap();
        assert_eq!(decompress_gzip(&gzipped, TEST_CAP).unwrap(), payload);
    }

    #[test]
    fn zstd_roundtrip() {
        let payload = b"zstandard entry payload".repeat(50);
        let compressed = zstd::encode_all(&payload[..], 3).unwrap();
        assert_eq!(decompress_zstd(&compressed, TEST_CAP).unwrap(), payload);
    }

    #[test]
    fn oversized_decompression_errors_instead_of_truncating() {
        // A zero payload 1 MiB over the cap compresses to almost nothing but must error,
        // not silently truncate to corrupt data.
        let cap = 4 * 1024 * 1024;
        let payload = vec![0u8; (cap + 1024 * 1024) as usize];
        let mut deflated = Vec::new();
        let mut enc = flate2::write::DeflateEncoder::new(&mut deflated, GzCompression::fast());
        enc.write_all(&payload).unwrap();
        enc.finish().unwrap();
        let err = decompress_deflate(&deflated, cap).unwrap_err();
        assert!(err.to_string().contains("decompressed-size limit"), "unexpected error: {err}");
        // The message must point at the knob that fixes it.
        assert!(err.to_string().contains("--max-entry-size"), "unexpected error: {err}");

        let compressed = zstd::encode_all(&payload[..], 1).unwrap();
        assert!(decompress_zstd(&compressed, cap).is_err());

        // The very same payload succeeds once the cap is raised past it - i.e. the limit is
        // configurable, not a hard format ceiling.
        assert_eq!(decompress_deflate(&deflated, cap * 4).unwrap().len(), payload.len());
    }

    #[test]
    fn needs_zip64_only_past_the_u32_boundary() {
        // `zip`'s own threshold is `spec::ZIP64_BYTES_THR == u32::MAX`, and it aborts an entry
        // that crosses it without `large_file`. Being off by one here either corrupts >4 GiB
        // entries or bloats every header in every archive with ZIP64 extra fields, so pin the
        // exact boundary - a real >4 GiB entry is far too expensive to write in a unit test.
        assert!(!needs_zip64(0));
        assert!(!needs_zip64(u32::MAX as usize - 1));
        assert!(!needs_zip64(u32::MAX as usize), "exactly u32::MAX still fits");
        assert!(needs_zip64(u32::MAX as usize + 1), "one byte past must opt in");
        assert!(needs_zip64(8 * 1024 * 1024 * 1024));
    }

    #[test]
    fn decode_entry_honours_the_gunzip_flag() {
        // The `.gz` passthrough path relies on being able to stop after the zip layer, so a
        // gzip stream stored verbatim must come back out byte-identical.
        let plaintext = b"i3s node payload".repeat(40);
        let mut gzipped = Vec::new();
        let mut enc = GzEncoder::new(&mut gzipped, GzCompression::default());
        enc.write_all(&plaintext).unwrap();
        enc.finish().unwrap();

        let stored = zstd::encode_all(&gzipped[..], 3).unwrap();
        assert_eq!(decode_entry(stored.clone(), 93, false, TEST_CAP).unwrap(), gzipped);
        assert_eq!(decode_entry(stored, 93, true, TEST_CAP).unwrap(), plaintext);
    }

    // --- local-filesystem source / speculative range read ---

    /// Build one Local File Header immediately followed by its payload.
    fn lfh(name: &str, comp_method: u16, extra: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut rec = Vec::new();
        rec.extend_from_slice(&[0x50, 0x4b, 0x03, 0x04]); // signature
        rec.extend_from_slice(&[20, 0]); // version needed
        rec.extend_from_slice(&[0, 0]); // flags
        rec.extend_from_slice(&comp_method.to_le_bytes());
        rec.extend_from_slice(&[0; 4]); // mod time/date
        rec.extend_from_slice(&[0; 4]); // crc32
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // compressed size
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes()); // uncompressed size
        rec.extend_from_slice(&(name.len() as u16).to_le_bytes());
        rec.extend_from_slice(&(extra.len() as u16).to_le_bytes());
        rec.extend_from_slice(name.as_bytes());
        rec.extend_from_slice(extra);
        rec.extend_from_slice(payload);
        rec
    }

    /// Assemble a whole zip from (name, method, lfh_extra, payload) tuples and write it to a
    /// scratch dir, returning (root, filename, parsed entries).
    fn write_archive(tag: &str, files: &[(&str, u16, Vec<u8>, Vec<u8>)]) -> (std::path::PathBuf, String, Vec<CdEntry>) {
        let mut body = Vec::new();
        let mut offsets = Vec::new();
        for (name, method, extra, payload) in files {
            offsets.push(body.len() as u32);
            body.extend(lfh(name, *method, extra, payload));
        }
        let cd_offset = body.len() as u32;
        let mut cd = Vec::new();
        for ((name, method, _, payload), off) in files.iter().zip(&offsets) {
            cd.extend(cd_record(name, *method, (payload.len() as u32, payload.len() as u32), *off, &[]));
        }
        let cd_size = cd.len() as u32;
        body.extend(&cd);
        body.extend(eocd_record(cd_size, cd_offset, &[]));

        let root = std::env::temp_dir().join(format!("s3-3tz-clipper-test-{}-{}", std::process::id(), tag));
        std::fs::create_dir_all(&root).unwrap();
        let name = "fixture.3tz".to_string();
        std::fs::write(root.join(&name), &body).unwrap();
        (root, name, parse_central_directory(&cd))
    }

    #[tokio::test]
    async fn local_source_reads_entries_through_speculative_range() {
        let plain = b"stored tile payload".repeat(20);
        let deflated = {
            let mut out = Vec::new();
            let mut enc = flate2::write::DeflateEncoder::new(&mut out, GzCompression::default());
            enc.write_all(b"deflated tileset json").unwrap();
            enc.finish().unwrap();
            out
        };
        // An extra field far larger than LFH_EXTRA_SLOP, to force the exact-payload refetch.
        let fat_extra = vec![0u8; (LFH_EXTRA_SLOP as usize) * 3];

        let (root, key, entries) = write_archive("range", &[
            ("stored.b3dm", 0, Vec::new(), plain.clone()),
            ("tileset.json", 8, Vec::new(), deflated),
            ("empty.bin", 0, Vec::new(), Vec::new()),
            ("fat-extra.b3dm", 0, fat_extra, plain.clone()),
        ]);

        let source = ObjectSource::Local(root.clone());
        let by_name = |n: &str| entries.iter().find(|e| e.filename == n).unwrap().clone();

        assert_eq!(source.fetch_size("", &key).await.unwrap(), std::fs::metadata(root.join(&key)).unwrap().len());

        // Common case: header + payload arrive in one speculative read.
        assert_eq!(fetch_raw_entry(&source, "", &key, &by_name("stored.b3dm")).await.unwrap(), plain);
        // Deflated entry decodes through the full path.
        assert_eq!(fetch_file_content(&source, "", &key, &by_name("tileset.json"), TEST_CAP).await.unwrap(), b"deflated tileset json");
        // Zero-length entry: must not underflow its inclusive end offset to u64::MAX.
        assert!(fetch_raw_entry(&source, "", &key, &by_name("empty.bin")).await.unwrap().is_empty());
        // Slop miss: the extra field overruns the speculative window, forcing the refetch.
        assert_eq!(fetch_raw_entry(&source, "", &key, &by_name("fat-extra.b3dm")).await.unwrap(), plain);

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn local_source_reports_missing_files_and_describes_paths() {
        let (root, key, _) = write_archive("missing", &[("a.b3dm", 0, Vec::new(), b"x".to_vec())]);
        let source = ObjectSource::Local(root.clone());

        assert!(source.fetch_size("", "nope.3tz").await.is_err());
        assert!(source.fetch_object("", "nope.3tz").await.is_err());
        // Local mode must not describe its inputs as s3:// URLs.
        let described = source.describe("ignored-bucket", &key);
        assert!(described.ends_with("fixture.3tz"), "unexpected description: {described}");
        assert!(!described.contains("s3://"));

        std::fs::remove_dir_all(&root).ok();
    }
}
