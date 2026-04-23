[![Coverage Status](https://coveralls.io/repos/github/davebx/bellerophon/badge.svg?branch=main)](https://coveralls.io/github/davebx/bellerophon?branch=main)

# bellerophon
Filter mapped reads where the mapping spans a junction, retaining the 5-prime read.

## Performance validation workflow

Use this sequence before claiming a speedup:

1. **Correctness equivalence** (`--quality 0` and `--quality 20`) using `samtools view`, `samtools flagstat`, and record-level diffs.
2. **Environment capture**:
   - `git rev-parse HEAD`
   - `python -VV`
   - `python -c 'import pysam; print("pysam", pysam.__version__); print("samtools", getattr(pysam, "__samtools_version__", "unknown"))'`
   - `uname -a`
   - `lscpu`
   - `df -h .`
3. **Timing** with `/usr/bin/time -v`.
4. **Stage logs** from `--log-level INFO` (look for `STAGE ...` entries).
5. **Input-shape audit**:
   - `python scripts/input_shape_audit.py --forward FWD.bam --reverse REV.bam --output input_shape.json`
6. **Cluster matrix submission (one job per setting, `/scratch` outputs):**
   - `python scripts/perf_matrix_hqsub.py --forward FWD.bam --reverse REV.bam --scratch-dir /scratch --queue boris --project-cpus-mode thread --resource-prefix perf > submit_perf.sh`
   - `bash submit_perf.sh`
   - `python scripts/perf_matrix_collect.py --matrix-dir /scratch/perf_matrix --output-csv perf_matrix_summary.csv`
