# bellerophon

Rust-first implementation of the Bellerophon pipeline: filter mapped reads where the mapping spans a junction, retaining the 5-prime read.

This repository now ships the optimized direct Rust pipeline as the primary and only supported implementation.

## Quick start (Pixi)

```bash
pixi install
pixi run bellerophon --help
```

## Build and test

```bash
pixi run cargo-build-rs
pixi run cargo-test-rs
```

## Manual cargo usage

```bash
cargo build --release --manifest-path rust/bellerophon-rs/Cargo.toml
cargo run --release --manifest-path rust/bellerophon-rs/Cargo.toml -- --help
```

## Binary

Release binary path:

- `target/release/bellerophon-rs`
