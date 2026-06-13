
# S3 3tz Clipper 🛰️
### Cloud-Optimized, Multi-Threaded 3D Tiles Clipping Tool

`s3-3tz-clipper` is a Rust-based command-line interface (CLI) for clipping 3D Tiles (`.3tz`) archives directly over S3. Operating on any size dataset, it requires **zero local storage overhead** for the source file, using  HTTP range requests to stream only the required tiles based on a GeoJSON polygon.

The application features multi-threaded, concurrent S3 downloads, parallel CPU-side decompression, recursive parsing of external nested tilesets, and compliance with the Maxar `.3tz` specification (incorporating the sorted 24-byte binary index `@3dtilesIndex1@`).

---

## 🛠️ Features

*   **Zero-Download Remote Reads**: Streams `.3tz` files directly from any S3 bucket. No local download of the source dataset is ever required.
*   **Multi-Threaded Parallel Fetching**: Spawns concurrent background workers to stream and decompress multiple tiles simultaneously from S3
*   **Parallel Decompression**: Offloads decompression tasks to CPU cores in parallel via the `flate2` crate, bypassing S3 CPU overhead.
*   **Recursive Tileset Resolution**: Recursively resolves and filters nested external tilesets (`.json` files pointing to other `.json` files), ensuring all levels of detail are correctly mapped and clipped.
*   **Standard & S2 Bounding Volume Support**:
    *   ✅ **Geographic `region`**: Full, exact support for WGS84 bounding volumes.
    *   ✅ **S2 Cells**: Full, exact support for `3DTILES_bounding_volume_S2` cell tokens.
    *   ⚠️ **oriented `box` / `sphere`**: Safely defaults to keeping the tiles to prevent accidental data loss.
*   **`.3tz` Compliance**: Automatically generates a sorted, 24-byte record binary search index (`@3dtilesIndex1@`) as the first entry inside the output archive, and patches the ZIP Local File Headers and Central Directory CRC-32 checksums.
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
s3-3tz-clipper [OPTIONS] --bucket <BUCKET> --key <KEY> --geojson <GEOJSON> --output <OUTPUT>
```

| Flag | Argument | Description |
|---|---|---|
| `-b`, `--bucket` | `<BUCKET>` | Raw name of the S3 bucket (do not prefix with `s3://`). |
| `-k`, `--key` | `<KEY>` | Full path to the `.3tz` file within the bucket (do not start with `/`). |
| `-g`, `--geojson` | `<GEOJSON>` | Path to the GeoJSON boundary file, or **`-`** to read from `stdin`. |
| `-o`, `--output` | `<OUTPUT>` | Local output path where the clipped `.3tz` file will be saved. |
| `-p`, `--progress` | | *(Optional)* Show an interactive progress bar. |
| `-c`, `--concurrency` | `<NUM>` | *(Optional)* Max concurrent S3 downloads. Defaults to `10`. |
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

---
