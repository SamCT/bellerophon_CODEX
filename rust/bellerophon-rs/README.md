# bellerophon-rs

Side-by-side Rust implementation of the Bellerophon pipeline.

## Build

```bash
cargo build --release --manifest-path rust/bellerophon-rs/Cargo.toml
```

Binary output:

- `target/release/bellerophon-rs`

## Test and format

```bash
cargo fmt --manifest-path rust/bellerophon-rs/Cargo.toml
cargo test --manifest-path rust/bellerophon-rs/Cargo.toml
```

## Pipeline support

- `--pipeline legacy-temp`: mirrors Python temp-BAM flow.
- `--pipeline pair-temp`: pair-aware filter, temp BAM only for surviving pairs.
- `--pipeline direct`: pair-aware filter, writes final BAM directly.

## Thread-tuning environment variables

- `BELLEROPHON_DIRECT_MAX_READ_THREADS` (optional): caps per-reader BGZF workers in `--pipeline direct`.
- `BELLEROPHON_DIRECT_MAX_WRITE_THREADS` (optional): caps output writer BGZF workers in `--pipeline direct`.

If unset, direct mode scales worker counts with `--threads` instead of using fixed default caps.
The default split is adaptive: small thread counts keep 1-2 writer workers, and larger counts reserve ~20% for writer/compression and split the rest across the two readers.
