# bellerophon-rs

Optimized direct Rust implementation of the Bellerophon pipeline.

## Build

```bash
cargo build --release --manifest-path rust/bellerophon-rs/Cargo.toml
```

Binary output:

- `target/release/bellerophon-rs`

## Install

Install into `~/.local/bin` from the repository root:

```bash
scripts/install_bellerophon_rs.sh "$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

Install into a shared prefix:

```bash
scripts/install_bellerophon_rs.sh /nfs7/path/to/bellerophon-rs
export PATH="/nfs7/path/to/bellerophon-rs/bin:$PATH"
```

After install, run `bellerophon-rs` directly from any working directory. Do not
use `pixi run` for production jobs.

## Test and format

```bash
cargo fmt --manifest-path rust/bellerophon-rs/Cargo.toml
cargo test --manifest-path rust/bellerophon-rs/Cargo.toml
```
