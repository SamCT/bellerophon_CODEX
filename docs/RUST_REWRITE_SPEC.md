# Rust Rewrite Specification

## Scope and intent

This document defines the **current Python behavior** that a future Rust rewrite must match before any optimization claims are made.

This is a specification document only:

- Do **not** modify Python runtime behavior as part of this specification task.
- Do **not** add Rust implementation code as part of this specification task.

---

## 1) CLI compatibility contract

A Rust CLI rewrite must preserve these options and semantics:

- `--forward` / `-f`: required; path to first input alignment file.
- `--reverse` / `-r`: required; path to second input alignment file.
- `--output` / `-o`: required; output BAM path.
- `--quality` / `-q`: optional minimum MAPQ threshold, default `20`.
- `--threads` / `-t`: optional thread count control.
- `--log-level` / `-l`: optional logging level (`CRITICAL|ERROR|WARNING|INFO|DEBUG`).

Compatibility target is the externally visible interface and default behavior as implemented in Python.

---

## 2) Input contract

### Header compatibility

Forward and reverse inputs are valid only when they have:

1. identical reference name lists, and
2. identical reference length lists.

When either differs, processing is treated as invalid input for merge semantics.

### Grouping model

Records are processed in **query-name groups** (`query_name` adjacency groups in stream order).

- Group boundaries are defined by changes in consecutive query names.
- Group processing is done independently per stream during filtering.

### Synchronization assumption (current Python)

The current merge step assumes **ordinal group synchronization** between filtered forward and filtered reverse temp BAM streams:

- filtered outputs are consumed via positional zipping,
- not by hash/key join,
- therefore correctness depends on compatible ordering and group cardinality effects in both streams.

---

## 3) Current filtering semantics (Python-equivalent)

Each side (forward, reverse) is filtered independently and writes **exactly one temp BAM record per query-name group**.

### Selection rule per group

For a group size of **1 or 2**:

- if there is **exactly one** 5-prime read in the group, retain that read.

Otherwise:

- write the group’s first read with the `unmapped` flag set.

### Current 5-prime classification (M-only behavior)

Preserve existing CIGAR interpretation exactly:

- A **non-reverse** read is 5-prime when the **first** CIGAR operation is `M`.
- A **reverse** read is 5-prime when the **last** CIGAR operation is `M`.

Do **not** reinterpret `=` or `X` as `M` unless a future, explicitly named option introduces that semantics.

Notes for compatibility:

- The current behavior is intentionally tied to this M-only criterion.
- Any broadening of match-op handling is a future feature, not part of baseline equivalence.

---

## 4) Current merge semantics (Python-equivalent)

Merge consumes the two filtered temp BAMs using a `zip`-style positional iteration.

### Pair acceptance / skip criteria

Given each zipped forward/reverse filtered record pair:

1. Skip if query names mismatch.
2. Skip if either side is unmapped.
3. Skip if either MAPQ is below `--quality`.

### Output pair field behavior

For retained pairs, set fields to match current Python output behavior, including:

- read roles: `read1` (forward), `read2` (reverse),
- paired/proper-pair flags,
- mate reference ID and mate start,
- mate reverse flags,
- signed template lengths (including sign direction conventions and zero when references differ).

Rust parity is defined by record-level equivalence to current Python outputs under identical inputs/options.

---

## 5) Rust rewrite variants (implementation phases to build later)

The rewrite plan contains three strategies to be implemented in later tasks:

1. **legacy-temp**
   - Rust implementation of the current temp-BAM algorithm.
   - Purpose: language/runtime comparison with equivalent pipeline structure.

2. **pair-temp**
   - Pair-aware algorithm writing compressed temp BAM records only for candidates that can survive final output.

3. **direct**
   - Pair-aware algorithm writing final BAM directly with bounded memory and without the legacy temp materialization pattern.

Each variant must be benchmarked and validated against the same equivalence harness.

---

## 6) Performance and validation contract

### Equivalence gate before speed claims

No speed claim is valid unless record-level output equivalence is demonstrated at:

- `q=0`, and
- `q=20`.

### Change isolation

Do not bundle unrelated optimizations into rewrite-comparison runs.

- Keep algorithm/runtime strategy changes isolated.
- Report exactly what changed per run.

### Required matrix fields

Every benchmark matrix row must include all of the following:

- runner
- pipeline
- strategy
- quality
- threads
- wall time
- CPU percent
- RSS
- filesystem inputs/outputs
- stage times
- final pairs
- skip counts
- temp sizes
- output size

A row is incomplete if any required field is missing.
