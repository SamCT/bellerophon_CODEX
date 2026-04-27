# bellerophon

Rust-first implementation of the Bellerophon pipeline: filter mapped reads where the mapping spans a junction, retaining the 5-prime read.

This repository now ships the optimized direct Rust pipeline as the primary and only supported implementation.

## Quick start (Pixi) and installation

```bash
pixi install
pixi run bellerophon --help
```

```bash
pixi run cargo-build-rs
pixi run cargo-test-rs
```

## Manual cargo usage

```bash
cargo build --release --manifest-path rust/bellerophon-rs/Cargo.toml
cargo run --release --manifest-path rust/bellerophon-rs/Cargo.toml -- --help
```

## Example command
```bash
pixi run bellerophon \
  --forward /path/to/sample_R1.bam \
  --reverse /path/to/sample_R2.bam \
  --output sample.bellerophon.q0.bam \
  --quality 0 \
  --threads 16
```

## Help
bellerophon [-h] --forward FORWARD --reverse REVERSE --output OUTPUT [--quality QUALITY] [--threads THREADS] [--log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG}] [--version]

Filter chimeric reads.

options:
  -h, --help            show this help message and exit
  --forward FORWARD, -f FORWARD
                        SAM/BAM/CRAM file with the first set of reads.
  --reverse REVERSE, -r REVERSE
                        SAM/BAM/CRAM file with the second set of reads.
  --output OUTPUT, -o OUTPUT
                        Output BAM file for filtered and paired reads.
  --quality QUALITY, -q QUALITY
                        Minimum mapping quality.
  --threads THREADS, -t THREADS
                        Threads.
  --log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG}, -l {CRITICAL,ERROR,WARNING,INFO,DEBUG}
                        Log level.
  --version             show program's version number and exit

Filter two single-end BAM, SAM, or CRAM files for reads where there is high-quality mapping on both sides of a ligation junction, retaining the 5´ side of that mapping, then merge them into one paired-end
BAM file.

## How optimized is it?


Python3 optimization speedup, versus a series of rust speedups of the current commit. 5GB BAM R1/R2 file combining at MAPQ=0, across multiple thread intervals.

<img width="1448" height="1086" alt="runtime_RUST1" src="https://github.com/user-attachments/assets/c647121d-1be7-4430-a165-1d6d7a83cb83" />
