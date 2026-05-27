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

## Direct pipeline debugging

To isolate parallel reader/synchronizer EOF issues, run the direct pipeline with
the serial reader:

```bash
bellerophon-rs \
  --forward R1.bam \
  --reverse R2.bam \
  --threads 16 \
  --quality 0 \
  --direct-reader-mode serial \
  --output out.serial.bam
```

For shutdown diagnostics, add a stall warning timeout:

```bash
bellerophon-rs \
  --forward R1.bam \
  --reverse R2.bam \
  --threads 16 \
  --quality 0 \
  --stall-timeout-seconds 60 \
  --abort-on-stall \
  --output out.debug.bam
```
