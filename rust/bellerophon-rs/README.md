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

## Direct mode thread model

- `--threads` is the total thread budget for direct mode.
- Resolved total threads are `min(--threads, available_parallelism())`, unless an explicit cap is set with `BELLEROPHON_THREADS_CAP`.
- There is no hidden default cap (for example no implicit 32-thread ceiling).
- Direct mode uses a single shared HTSlib BGZF thread pool (`set_thread_pool`) attached to both readers and the writer.
- The resolved total is split adaptively into:
  - total BGZF workers
  - per-reader BGZF workers (target share)
  - writer BGZF workers (target share)
  - compute workers for batch processing

### Thread-related environment variables

- `BELLEROPHON_THREADS_CAP` (optional): explicit upper bound for resolved total threads.

## Direct-mode benchmarking matrix

Use even thread counts and log both thread scaling and compression-level effects:

```bash
for t in 16 32 64 128; do
  for c in 1 3 6; do
    /usr/bin/time -f "threads=${t} comp=${c} elapsed=%e" \
      target/release/bellerophon-rs \
      --pipeline direct \
      --forward /scratch/forward.bam \
      --reverse /scratch/reverse.bam \
      --output /scratch/out.t${t}.c${c}.bam \
      --threads "${t}" \
      --compression-level "${c}" \
      --quality 20 \
      --log-level info
  done
done
```
