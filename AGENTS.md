# AGENTS.md

Guidance for coding agents (and humans) contributing to `s3-3tz-clipper`. The goal is PRs
that a reviewer can read top to bottom and approve without reverse-engineering intent.

## Project at a glance

A Rust CLI that clips 3D Tiles (`.3tz`) and I3S (`.slpk`/`.spk`) zip archives to a GeoJSON
polygon, reading only the bytes it needs via S3 range requests or local positional reads.

| Path | What lives there |
|---|---|
| `src/main.rs` | CLI args, `ObjectSource` (S3 signed / unsigned / local), zip parsing and writing, the fetch/decode pipeline, AWS region/endpoint diagnostics |
| `src/clip.rs` | Pure geometry and JSON logic: GeoJSON parsing, bounding-volume tests, tileset and I3S filtering. No I/O |
| `tests/run_test.sh` | End-to-end script against a public S3 bucket plus synthetic local fixtures |
| `Cargo.toml` | Dependencies, each non-obvious feature choice explained inline |
| `readme.md` | User docs, including the CLI flag table |
| `CHANGELOG.md`, version in `Cargo.toml` | Owned by release-plz. Do not edit by hand |

Keep new pure logic in `clip.rs` (easy to unit test). Keep I/O, async, and archive plumbing in `main.rs`.

## Build and test

```bash
cargo build --release
cargo test                      # unit tests; fast, no network
cargo clippy                    # should add no new warnings
./tests/run_test.sh             # needs a release build and network access to public S3
RUN_ZIP64_TEST=1 ./tests/run_test.sh   # also runs the >4 GiB ZIP64 case (~4.5 GiB RAM)
```

CI (`.github/workflows/ci.yml`) runs `cargo build --release`, `cargo test`, and `tests/run_test.sh`.
Run at least `cargo test` and `cargo clippy` before opening a PR, and the script too when you
touch fetching, archive reading/writing, or traversal.

If `cargo` is not on `PATH`, the toolchain may be installed from conda-forge (see `readme.md`).

Python 3.14 or newer is required for `tests/run_test.sh` and any ad hoc checks. Older
`zipfile` can't read zstd entries, which is what this tool writes.

## Coding style

**Match the surrounding code.** This repo is not `rustfmt`-formatted. Do **not** run
`cargo fmt` on whole files; it rewrites hundreds of unrelated lines and buries your change.
Format only what you write, consistent with its neighbours.

Conventions already in use:

- **Errors:** functions return `Result<T, Box<dyn std::error::Error + Send + Sync>>`. Wrap
  failures with context the operator can act on:
  `.map_err(|e| format!("Could not open '{}': {}", path.display(), e))?`.
- **Logging:** plain `println!`/`eprintln!` with a bracketed level prefix:
  `[INFO]`, `[WARN]`, `[ERROR]`, `[DEBUG]` (gated on `debug`), `[HINT]`. Warnings and hints
  should say what to do next (which flag to pass, which env var to clear).
- **Never lose data silently.** Prefer erroring over truncating (`read_capped`), and keep a
  tile when its bounding volume can't be evaluated (`box`/`sphere`) rather than dropping it.
- **Async:** CPU-heavy or blocking work (decompression, file reads, zip writing) goes on
  `tokio::task::spawn_blocking` so it never stalls in-flight fetches. Bound concurrency with
  a `Semaphore`.
- **Options:** new pipeline knobs go in `ClipOptions` rather than growing argument lists.
- **Named constants** for magic numbers, with a `///` saying why that value
  (`DEFAULT_MAX_ENTRY_MIB`, `MEMORY_CEILING_WARN_BYTES`).
- **Punctuation:** use an ASCII hyphen with spaces (` - `) in comments and messages, not em dashes.
- **Scope:** do not refactor, rename, or reformat code your change doesn't need. Propose
  cleanups as separate PRs.

## Comments

Comments explain *why*, or a contract the code can't express. Keep them short; one or two
lines is the norm. If a comment restates the code, delete it.

- `///` on functions, structs, fields, and constants: what it guarantees or why it exists.
  ```rust
  /// Human-readable location of `key`, for log messages.
  ```
- `//` inline only for a non-obvious decision or trap:
  ```rust
  // TryFrom errors (and thus yields None) for any non-Polygon geometry.
  ```
- `// --- Section name ---` separates groups of related functions and, in test modules,
  groups tests by the function under test.
- Byte-layout code labels fields at the end of the line: `rec.extend_from_slice(&[0, 0]); // flags`.
- A longer block is justified only for a real trap that already bit someone (see the
  `rustls` comment in `Cargo.toml` or the TLS provider note at the top of `main()`). Even then,
  state the constraint and consequence; skip the story.

Avoid:

- Narrating the change ("Now we also...", "Updated to...", "Previously this..."). History
  belongs in the commit message.
- Restating the signature or the next line of code.
- Multi-paragraph doc comments on simple helpers.
- `TODO`s without an owner or an issue number.

## Tests

Every behavioural change ships with a test, unless it truly can't be tested (say so in the PR).

**Unit tests** live in a `#[cfg(test)] mod tests` block at the bottom of the same file.

- Name tests for the behaviour asserted: `resolve_uri_rejects_escapes`,
  `parse_cd_truncated_name_stops_cleanly`.
- Build fixtures in code; don't commit binary files. Reuse the existing helpers (`cd_record`,
  `eocd_record`, `write_archive`, `unit_polygon`, `region_deg`) before writing new ones.
- Use `#[tokio::test]` for async paths; remove any temp directories you create.
- For a bug fix, add a test that fails without the fix, with a one-line comment naming what
  it guards:
  ```rust
  // Zero-length entry: must not underflow its inclusive end offset to u64::MAX.
  ```
- Cover edge cases the code explicitly handles: ZIP64 sentinels, `.gz` fallbacks, truncated
  input, out-of-polygon ancestors.

**End-to-end tests** go in `tests/run_test.sh` when a change affects the output archive's
shape. Assert on contents with `assert_zip_contains`, and gate anything slow or memory-heavy
behind an env var, as `RUN_ZIP64_TEST` does.

When a change touches archive writing or the index patch, also confirm every entry's CRC
in the outputs the script leaves behind:

```bash
python3 -c "import sys, zipfile; [print(f, zipfile.ZipFile(f).testzip() or 'OK') for f in sys.argv[1:]]" clipped-*.3tz clipped-*.spk
```

## External dependencies

Read the real source for the exact version in use; don't rely on remembered APIs.

```bash
grep -A1 'name = "zip"' Cargo.lock          # exact resolved version
cargo tree -i rustls                        # who pulls a crate in
cargo tree -e features -i rustls            # which features are enabled, and by whom
cargo tree -d                               # duplicate versions of the same crate
cargo doc -p aws-sdk-s3 --open              # local docs for the locked version
ls ~/.cargo/registry/src/*/zip-*/           # downloaded crate source (after a build)
```

Online, use `https://docs.rs/<crate>/<version>` with the version from `Cargo.lock`.

When adding or changing a dependency:

- Prefer `default-features = false` and enable only what's used.
- Add a short comment in `Cargo.toml` when a feature choice isn't obvious.
- Check `cargo tree -d` so a new crate doesn't duplicate an HTTP, TLS, or compression stack
  that's already present.
- `rustls` must stay on the same major version that `reqwest` and the AWS SDK resolve to.
- Routine version bumps come from Dependabot; don't mix them into feature PRs.

## Commit messages

release-plz builds `CHANGELOG.md` and picks the next version from commit subjects, so the
prefix matters.

```
<type>: <imperative summary, lowercase, no trailing period>

Why the change is needed: the symptom a user saw, or the constraint involved.
Then what changed, if the diff doesn't make it obvious.

- Bullets are fine for several distinct changes.
```

- **Types:** `feat` (new user-facing behaviour, minor bump), `fix` (bug fix, patch bump),
  `docs`, `test`, `refactor`, `ci`, `chore`.
- Subject at most ~72 characters, saying what changed for the user, not which file was touched.
- Wrap the body at ~72 columns. Name the error message or symptom in it, so it's searchable.
- One logical change per commit. Don't mix a fix with a reformat or a dependency bump.

Good, from this repo's history:

```
fix: stop release-plz publishing to crates.io

git_only alone is not enough: release-plz only rejects git_only + publish when
publish is explicitly true, so an unset publish fell back to its default of
true and the release job failed trying to cargo publish. Set it explicitly.
```

Avoid: `update`, `cleanup pass, fix bugs, update dependencies`, `Update Cargo.toml`.

## Pull requests

- **Branch:** `feat/<topic>`, `fix/<topic>`, `docs/<topic>`, or `issues/<n>-<topic>`.
- **Small and focused.** One concern per PR. A reviewer should be able to say what it does
  in one sentence.
- **Title:** same form as the commit subject.
- **Description:** what problem it solves, the approach, and how it was tested (the commands
  run, and the dataset or fixture used). Link the issue if there is one.
- **User-facing changes:** a new or changed CLI flag needs its `///` doc on `Args` and its row
  in the `readme.md` flag table.
- **Don't touch** `CHANGELOG.md` or the `version` in `Cargo.toml`; release-plz owns them.

Checklist before opening:

- [ ] `cargo test` passes, with new tests for the change
- [ ] `cargo clippy` adds no new warnings
- [ ] `tests/run_test.sh` run if fetching, archive I/O, or traversal changed
- [ ] No unrelated formatting, renames, or refactors in the diff
- [ ] Comments are short and explain why
- [ ] `readme.md` updated for user-visible changes
