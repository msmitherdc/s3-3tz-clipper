
# S3 3tz Clipper 🛰️
### Cloud-Optimized, Multi-Threaded 3D Tiles with 3tz index Clipping Tool

`s3-3tz-clipper` is a Rust-based command-line interface (CLI) for clipping 3D Tiles (`.3tz`) and I3S (`.spk`, `.slpk`) archives directly over S3. Operating on any size dataset, it requires **zero local storage overhead** for the source file, using  HTTP range requests to stream only the required tiles based on a GeoJSON polygon.

The application features multi-threaded, concurrent S3 downloads, parallel CPU-side decompression, recursive parsing of external nested tilesets, and compliance with the Maxar `.3tz` specification (incorporating the sorted 24-byte binary index `@3dtilesIndex1@`) and the ESRI `.spk` specification (`1.6+`)

---

## 🛠️ Features

*   **Zero-Download Remote Reads**: Streams `.3tz`, `.spk` files directly from any S3 bucket. No local download of the source dataset is ever required.
*   **Multi-Threaded Parallel Fetching**: Spawns concurrent background workers to stream and decompress multiple tiles simultaneously from S3
*   **Parallel Decompression**: Offloads decompression tasks to CPU cores in parallel via the `flate2` crate, bypassing S3 CPU overhead.
*   **Recursive Tileset Resolution**: Recursively resolves and filters nested external tilesets (`.json` files pointing to other `.json` files), ensuring all levels of detail are correctly mapped and clipped.
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
s3-3tz-clipper [OPTIONS] --bucket <BUCKET> (--key <KEY> | --package <KEY>) --geojson <GEOJSON> --output <OUTPUT>
```

| Flag | Argument | Description |
|---|---|---|
| `-b`, `--bucket` | `<BUCKET>` | Raw name of the S3 bucket (do not prefix with `s3://`). |
| `-k`, `--key` | `<KEY>` | Full path to a single `.3tz`/`.slpk`/`.spk` archive within the bucket (do not start with `/`). Mutually exclusive with `--package`. |
| `--package` | `<KEY>` | Full path to a *package* tileset.json - a bare (non-archive) JSON file whose `root.children` each reference their own separate archive via `content.uri` (as OWT/Vricon multi-content packages do). Every referenced archive is clipped independently and written under `--output` at the same relative path as its `content.uri`; the package's own tileset.json is rewritten alongside it with each surviving child's (and the root's) `region` shrunk to match. Mutually exclusive with `--key`. |
| `-g`, `--geojson` | `<GEOJSON>` | Path to the GeoJSON boundary file, or **`-`** to read from `stdin`. |
| `-o`, `--output` | `<OUTPUT>` | Output file path in `--key` mode, or output **directory** in `--package` mode. |
| `-p`, `--progress` | | *(Optional)* Show an interactive progress bar. |
| `-c`, `--concurrency` | `<NUM>` | *(Optional)* Max concurrent S3 downloads within a single archive's tile fetches. Defaults to `20`. |
| `--archive-concurrency` | `<NUM>` | *(Optional, `--package` mode only)* Max archives clipped in parallel. Defaults to `4`. Each archive additionally uses up to `--concurrency` connections of its own, so total in-flight connections can reach `archive-concurrency * concurrency`. |
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

### Example 3: Clipping a Package Tileset
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