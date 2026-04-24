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

## Current pipeline support

- `--pipeline legacy-temp`: implemented.
- `--pipeline pair-temp`: not implemented yet.
- `--pipeline direct`: not implemented yet.
