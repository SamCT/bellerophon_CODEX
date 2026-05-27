# bellerophon

This branch was developed using OpenAI's Codex, non-integrated into the HPC. End use is for teaching and research purposes.

Rust-first implementation of the Bellerophon pipeline: filter mapped reads where the mapping spans a junction, retaining the 5-prime read.

This repository now ships the optimized direct Rust pipeline as the primary and only supported implementation.

## Quick start

```bash
bellerophon-rs \
  --forward R1.bam \
  --reverse R2.bam \
  --threads 64 \
  --quality 10 \
  --output out.bam
```

Relative input and output paths are resolved relative to the shell directory
where `bellerophon-rs` is launched.

## Install

Install into `~/.local/bin`:

```bash
scripts/install_bellerophon_rs.sh "$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

Install into a custom shared prefix:

```bash
scripts/install_bellerophon_rs.sh /nfs7/path/to/bellerophon-rs
export PATH="/nfs7/path/to/bellerophon-rs/bin:$PATH"
```

## Manual cargo usage

```bash
cargo build --release --manifest-path rust/bellerophon-rs/Cargo.toml
cargo run --release --manifest-path rust/bellerophon-rs/Cargo.toml -- \
  --forward test_data/test_1500_forward.bam \
  --reverse test_data/test_1500_reverse.bam \
  --threads 2 \
  --quality 10 \
  --output /tmp/bellerophon-rs.example.bam
```

Pixi is kept for development tasks only:

```bash
pixi run build
pixi run test
pixi run help
pixi run install-local
```

## Help
bellerophon-rs [-h] --forward FORWARD --reverse REVERSE --output OUTPUT [--quality QUALITY] [--threads THREADS] [--log-level {CRITICAL,ERROR,WARNING,INFO,DEBUG}] [--version]

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
