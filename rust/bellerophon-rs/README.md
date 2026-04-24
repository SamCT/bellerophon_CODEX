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
