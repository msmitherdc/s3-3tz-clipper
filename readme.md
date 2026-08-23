
# S3 3tz Clipper 🛰️
### Cloud-Optimized, Multi-Threaded 3D Tiles with 3tz index Clipping Tool

`s3-3tz-clipper` is a Rust-based command-line interface (CLI) for clipping 3D Tiles (`.3tz`) and I3S (`.spk`, `.slpk`) archives directly over S3, **or from a local filesystem path**. Operating on any size dataset, it requires **zero local storage overhead** for the source file, using  HTTP range requests (or positional reads, locally) to stream only the required tiles based on a GeoJSON polygon.

The application features multi-threaded, concurrent S3 downloads, parallel CPU-side decompression, recursive parsing of external nested tilesets, and compliance with the Maxar `.3tz` specification (incorporating the sorted 24-byte binary index `@3dtilesIndex1@`) and the ESRI `.spk` specification (`1.6+`)

---

## 🛠️ Features

*   **Zero-Download Remote Reads**: Streams `.3tz`, `.spk` files directly from any S3 bucket. No local download of the source dataset is ever required.
*   **Named AWS Profiles**: `--profile <NAME>` (or the `AWS_PROFILE` environment variable) signs requests with a specific profile's credentials and region.
*   **Local Filesystem Sources**: Omit `--bucket` to clip an archive already on disk, using the exact same pipeline (positional reads in place of HTTP range requests) - no need to stage it in a bucket first.
*   **Multi-Threaded Parallel Fetching**: Spawns concurrent background workers to stream and decompress multiple tiles simultaneously from S3
*   **Parallel Decompression**: Offloads decompression to CPU cores in parallel (`flate2`/`zlib-rs` and `zstd`) on a dedicated blocking pool, so decoding never stalls the in-flight S3 fetches sharing the async runtime. Archive writing - and the Zstandard re-compression it performs - runs off-runtime for the same reason.
*   **Recursive Tileset Resolution**: Recursively resolves and filters nested external tilesets (`.json` files pointing to other `.json` files), ensuring all levels of detail are correctly mapped and clipped.
*   **Sound I3S Tree Traversal**: The I3S LOD tree is *not* a strict spatial containment hierarchy - a parent's bounding sphere does not always bound its children's. Traversal therefore descends through **every** node regardless of whether it intersects, gating only whether a node is *kept*. Culling a subtree at the first non-intersecting node is faster but silently drops descendants that do intersect.
*   **Standard & S2 Bounding Volume Support**:
    *   ✅ **Geographic `region`**: Full, exact support for WGS84 bounding volumes.
    *   ✅ **S2 Cells**: Full, exact support for `3DTILES_bounding_volume_S2` cell tokens.
    *   ⚠️ **oriented `box` / `sphere`**: Safely defaults to keeping the tiles to prevent accidental data loss.
*   **`.3tz` Compliance**: Automatically generates a sorted, 24-byte record binary search index (`@3dtilesIndex1@`) as the first entry inside the output archive, and patches the ZIP Local File Headers and Central Directory CRC-32 checksums.
* ESRI I3S (`1.6+`) support for `.spk` and `.slpk` files
*   **UNIX Pipeline-Ready**: Accepts clipping boundaries piped directly into standard input (`stdin`).

---

## 📦 Prerequisites

This environment is can be instantiated vai via `mamba` / `conda-forge`.

### 1. Install the Rust Compiler Toolchain
Install the Rust compiler and package manager (`cargo`) from `conda-forge`. To cross-compile for an AMD64 Linux target from a macOS Apple Silicon host, install the pre-compiled standard library package for `gnu` Linux.
Install `zig` (used as the cross-linker) and `cargo-zigbuild`:
```bash
mamba install -c conda-forge rust rust-std-x86_64-unknown-linux-gnu cargo-zigbuild 
```
---

## 🚀 Compilation

Compile the project for either your native host environment or cross-compile it for target environments:

### Compile Natively (macOS Apple Silicon)
```bash
cargo build --release --target aarch64-apple-darwin
```
*The compiled binary will be placed at: `target/aarch64-apple-darwin/release/s3-3tz-clipper`*

### Cross-Compile for Linux AMD64
```bash
cargo zigbuild --release --target x86_64-unknown-linux-gnu
```
*The compiled statically linked binary will be placed at: `target/x86_64-unknown-linux-gnu/release/s3-3tz-clipper`*

---

## 💻 Usage

```text
s3-3tz-clipper [OPTIONS] [--bucket <BUCKET> | --root <DIR>] (--key <KEY> | --package <KEY>) --geojson <GEOJSON> --output <OUTPUT>
```

| Flag | Argument | Description |
|---|---|---|
| `-b`, `--bucket` | `<BUCKET>` | Raw name of the S3 bucket (do not prefix with `s3://`). **Omit to read from the local filesystem instead** - see `--root`. |
| `--root` | `<DIR>` | *(Optional)* Base directory for local-filesystem reads. Only meaningful when `--bucket` is omitted; `--key`/`--package` are resolved relative to it. Defaults to the current directory. Mutually exclusive with `--bucket`. |
| `-k`, `--key` | `<KEY>` | Full path to a single `.3tz`/`.slpk`/`.spk` archive within the bucket (do not start with `/`). Mutually exclusive with `--package`. |
| `--package` | `<KEY>` | Full path to a *package* tileset.json - a bare (non-archive) JSON file whose `root.children` each reference their own separate archive via `content.uri` (as OWT/Vricon multi-content packages do). Every referenced archive is clipped independently and written under `--output` at the same relative path as its `content.uri`; the package's own tileset.json is rewritten alongside it with each surviving child's (and the root's) `region` shrunk to match. Mutually exclusive with `--key`. |
| `-g`, `--geojson` | `<GEOJSON>` | Path to the GeoJSON boundary file, or **`-`** to read from `stdin`. |
| `-o`, `--output` | `<OUTPUT>` | Output file path in `--key` mode, or output **directory** in `--package` mode. |
| `-p`, `--progress` | | *(Optional)* Show an interactive progress bar. |
| `-c`, `--concurrency` | `<NUM>` | *(Optional)* Max concurrent S3 downloads within a single archive's tile fetches. Defaults to `20`. |
| `--archive-concurrency` | `<NUM>` | *(Optional, `--package` mode only)* Max archives clipped in parallel. Defaults to `4`. Each archive additionally uses up to `--concurrency` connections of its own, so total in-flight connections can reach `archive-concurrency * concurrency`. |
| `--max-entry-size` | `<MiB>` | *(Optional)* Maximum **decompressed** size accepted for a single archive entry. Defaults to `256`. Entries above it are skipped with an error rather than silently truncated, so raise this if a dataset has legitimately huge tiles. It is a zip-bomb guard, not a format limit; peak memory scales with `max-entry-size * concurrency`. |
| `--profile` | `<NAME>` | *(Optional)* Named profile from `~/.aws/config`/`~/.aws/credentials` to sign requests with, overriding the `AWS_PROFILE` environment variable (still honored when this is omitted). Only meaningful alongside `--bucket`, and without `--no-sign-request`. |
| `-d`, `--debug` | | *(Optional)* Print verbose debugging logs. |

---

## 💡 Examples

### Example 1: Standard File-Based Clipping
Clips the dataset using a local GeoJSON file, displaying an interactive progress bar with 30 concurrent S3 connection workers:
```bash
./target/release/s3-3tz-clipper \
  --bucket "mybucket" \
  --key "3dtiles11.3dtiles.3tz" \
  --geojson "~/myboundary.geojson" \
  --output "~/myboundary.3tz" \
  --progress \
  --concurrency 20
```

### Example 2: Piping GeoJSON from standard input (`stdin`)
Integrates directly with UNIX pipes by passing `-` as the `--geojson` argument:
```bash
cat ~/myboundary.geojson | ./target/release/s3-3tz-clipper \
  --bucket "mybucket" \
  --key "3dtiles11.3dtiles.3tz" \
  --geojson "-" \
  --output "~/myboundary.3tz" \
  --progress
```

### Example 3: Clipping an Archive Already on Disk
Omit `--bucket` to read from the local filesystem. `--key` is resolved relative to `--root`
(or the current directory if `--root` is omitted):
```bash
./target/release/s3-3tz-clipper \
  --root "/data/tilesets" \
  --key "3dtiles11.3dtiles.3tz" \
  --geojson "~/myboundary.geojson" \
  --output "~/myboundary.3tz" \
  --progress
```
An absolute `--key` works too, in which case `--root` can be left off entirely:
```bash
./target/release/s3-3tz-clipper \
  --key "/data/tilesets/3dtiles11.3dtiles.3tz" \
  --geojson "-" \
  --output "~/myboundary.3tz" < ~/myboundary.geojson
```

### Example 4: Clipping a Package Tileset
Follows `product_package_88e0c/vricon_ste_refined/tileset.json`'s `root.children` out to each of its own per-layer archives (e.g. `terrain.3tz`, `vectors/Aeronautic/HelipadPnt.3tz`, ...), clips up to 8 of them at a time, and mirrors the same relative directory layout - plus a rewritten `tileset.json` - under `--output`:
```bash
./target/release/s3-3tz-clipper \
  --bucket "mybucket" \
  --package "owt/product_package_88e0c/vricon_ste_refined/tileset.json" \
  --geojson "~/myboundary.geojson" \
  --output "~/clipped_88e0c/" \
  --archive-concurrency 8 \
  --progress
```

### Example 5: Selecting an AWS Profile
Signs requests with the credentials (and region) of a named profile instead of the default chain:
```bash
./target/release/s3-3tz-clipper \
  --profile "nrl" \
  --bucket "vantor-jvt-terrain" \
  --package "Pendleton/.../3d_terrain_pack_refined/tileset.json" \
  --geojson "~/myboundary.geojson" \
  --output "/u02/tmp_exports/pendleton" \
  --archive-concurrency 4 \
  --concurrency 10
```
Equivalent to exporting `AWS_PROFILE=nrl` in the environment; `--profile` wins if both are set.

Naming a profile - by either route - makes that profile's credentials take precedence over
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` already present in the
environment, which the SDK's default chain would otherwise consult first. This matters when
the tool is launched as a subprocess that inherits or copies its parent's environment
(`subprocess.run(env={**os.environ, "AWS_PROFILE": "nrl"})`): without it, the ambient
credentials sign every request while the named profile contributes nothing but a region.
botocore applies the same rule, so this matches what the AWS CLI and boto3 do with the same
configuration. A profile that cannot supply credentials of its own (one that sets only a
region, say) still falls back to the normal chain. Run with `--debug` to see which provider
won - look for `loaded credentials provider=`.

Profiles that authenticate through `credential_process`, static keys, `source_profile`
role assumption, or web identity all work. **AWS SSO profiles do not** - the `sso` feature of
`aws-config` is left out of the build to keep the binary small; add it to `Cargo.toml` if you
need one.

---
## 💡 Environment

### To configure for custom certificate bundles, use:
```
CUSTOM_CA_BUNDLE=/path/to/enterprise-ca.pem  
AWS_CA_BUNDLE=/path/to/enterprise-ca.pem
``` 
both the reqwest anonymous client and the standard aws-sdk-s3 client will automatically mount the custom TLS certificates 

### To configure for a custom s3 endpoint use:
```bash
AWS_S3_ENDPOINT=/path/to/custom/aws-s3-endpoint
```

### To sign with a named AWS profile use:
```bash
AWS_PROFILE=nrl
```
equivalent to `--profile nrl`, which takes precedence if both are given.