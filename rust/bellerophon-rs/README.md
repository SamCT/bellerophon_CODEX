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

## Thread model

- `--threads` is the total thread budget.
- Resolved total threads are `min(--threads, available_parallelism())`.
- There is no hidden default cap (for example no implicit 32-thread ceiling).
- Uses a single shared HTSlib BGZF thread pool (`set_thread_pool`) attached to both readers and the writer.
- The resolved total is split adaptively into:
  - total BGZF workers
  - per-reader BGZF workers (target share)
  - writer BGZF workers (target share)
  - compute workers for batch processing

## Benchmarking matrix

Use even thread counts and focus on thread-scaling effects:

```bash
for t in 16 32 64 128; do
  /usr/bin/time -f "threads=${t} elapsed=%e" \
    target/release/bellerophon-rs \
    --forward /scratch/forward.bam \
    --reverse /scratch/reverse.bam \
    --output /scratch/out.t${t}.bam \
    --threads "${t}" \
    --quality 20 \
    --log-level info
done
```
