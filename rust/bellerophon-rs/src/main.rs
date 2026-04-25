use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError};
use rust_htslib::bam;
use rust_htslib::bam::record::{Cigar, Record};
use rust_htslib::bam::{Read, Writer};
use rust_htslib::tpool::ThreadPool;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use tempfile::Builder;

#[derive(Clone, Debug, ValueEnum, Eq, PartialEq)]
enum Pipeline {
    LegacyTemp,
    PairTemp,
    Direct,
}

#[derive(Clone, Debug, ValueEnum, Eq, PartialEq, Ord, PartialOrd)]
enum LogLevel {
    Critical,
    Error,
    Warning,
    Info,
    Debug,
}

#[derive(Parser, Debug)]
#[command(name = "bellerophon-rs")]
struct Cli {
    #[arg(short = 'f', long = "forward")]
    forward: PathBuf,
    #[arg(short = 'r', long = "reverse")]
    reverse: PathBuf,
    #[arg(short = 'o', long = "output")]
    output: PathBuf,
    #[arg(short = 'q', long = "quality", default_value_t = 20)]
    quality: u8,
    /// Total thread budget for the selected pipeline.
    /// Direct mode resolves this to min(--threads, available_parallelism, optional BELLEROPHON_THREADS_CAP).
    #[arg(short = 't', long = "threads", default_value_t = 1)]
    threads: usize,
    #[arg(short = 'l', long = "log-level", default_value = "error")]
    log_level: LogLevel,
    #[arg(long = "pipeline", value_enum)]
    pipeline: Pipeline,
    #[arg(long = "tmp-dir", default_value = ".")]
    tmp_dir: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct ThreadRoles {
    input: usize,
    temp_write: usize,
    temp_read: usize,
    output: usize,
}

#[derive(Default)]
struct FilterStats {
    processed: u64,
    written: u64,
    selected_groups: u64,
    placeholder_groups: u64,
}

#[derive(Default)]
struct PairFilterStats {
    groups: u64,
    candidate_groups_fwd: u64,
    candidate_groups_rev: u64,
    candidate_pairs: u64,
    missing_candidate: u64,
    low_mapq: u64,
    final_pairs: u64,
    mismatched: u64,
}

#[derive(Default)]
struct MergeStats {
    pairs: u64,
    mismatched: u64,
    unmapped: u64,
    low_mapq: u64,
}

#[derive(Clone, Debug)]
struct DirectThreadResolution {
    requested_threads: usize,
    detected_available_parallelism: usize,
    explicit_user_cap: Option<usize>,
    resolved_total_threads: usize,
    total_bgzf_workers: usize,
    per_reader_bgzf_workers: usize,
    writer_bgzf_workers: usize,
    compute_workers: usize,
}

#[derive(Debug)]
struct DirectInputBatch {
    sequence: u64,
    groups: Vec<(DirectRecordGroup, DirectRecordGroup)>,
}

#[derive(Default, Clone, Debug)]
struct DirectBatchStats {
    groups: u64,
    candidate_groups_fwd: u64,
    candidate_groups_rev: u64,
    candidate_pairs: u64,
    missing_candidate: u64,
    low_mapq: u64,
    final_pairs: u64,
}

#[derive(Debug)]
struct DirectOutputBatch {
    sequence: u64,
    records: Vec<(Record, Record)>,
    stats: DirectBatchStats,
    process_seconds: f64,
}

#[derive(Debug)]
struct DirectRecordGroup {
    first: Record,
    second: Option<Record>,
    extra_count: usize,
}

#[derive(Clone, Copy)]
enum CandidateSlot {
    First,
    Second,
}

impl DirectRecordGroup {
    fn new(first: Record) -> Self {
        Self {
            first,
            second: None,
            extra_count: 0,
        }
    }

    fn len(&self) -> usize {
        1 + usize::from(self.second.is_some()) + self.extra_count
    }

    fn qname(&self) -> &[u8] {
        self.first.qname()
    }

    fn push_same_qname(&mut self, record: Record) {
        if self.second.is_none() {
            self.second = Some(record);
        } else {
            self.extra_count += 1;
        }
    }

    fn mapq_at(&self, slot: CandidateSlot) -> u8 {
        match slot {
            CandidateSlot::First => self.first.mapq(),
            CandidateSlot::Second => self.second.as_ref().map(|r| r.mapq()).unwrap_or_default(),
        }
    }

    fn take_candidate(self, slot: CandidateSlot) -> Record {
        match slot {
            CandidateSlot::First => self.first,
            CandidateSlot::Second => self.second.expect("candidate slot must exist"),
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.pipeline {
        Pipeline::LegacyTemp => run_legacy_temp(&cli),
        Pipeline::PairTemp => run_pair_temp(&cli),
        Pipeline::Direct => run_direct(&cli),
    }
}

fn run_legacy_temp(cli: &Cli) -> Result<()> {
    let thread_roles = resolve_thread_roles(cli.threads);

    let open_start = Instant::now();
    let mut forward_reader = bam::Reader::from_path(&cli.forward)
        .with_context(|| format!("failed to open forward input {}", cli.forward.display()))?;
    forward_reader
        .set_threads(thread_roles.input)
        .context("failed to set forward input threads")?;
    let mut reverse_reader = bam::Reader::from_path(&cli.reverse)
        .with_context(|| format!("failed to open reverse input {}", cli.reverse.display()))?;
    reverse_reader
        .set_threads(thread_roles.input)
        .context("failed to set reverse input threads")?;

    verify_matching_references(forward_reader.header(), reverse_reader.header())?;

    stage_log(
        cli,
        format!(
            "STAGE open_headers seconds={:.6} input_threads={}",
            open_start.elapsed().as_secs_f64(),
            thread_roles.input
        ),
    );
    let forward_header = bam::Header::from_template(forward_reader.header());
    let reverse_header = bam::Header::from_template(reverse_reader.header());
    let forward_temp = make_temp_path(&cli.tmp_dir, "filtered_forward")?;
    let reverse_temp = make_temp_path(&cli.tmp_dir, "filtered_reverse")?;

    filter_one_side(
        cli,
        &mut forward_reader,
        &forward_header,
        &forward_temp,
        thread_roles,
        input_label(&cli.forward),
    )?;
    filter_one_side(
        cli,
        &mut reverse_reader,
        &reverse_header,
        &reverse_temp,
        thread_roles,
        input_label(&cli.reverse),
    )?;

    merge_filtered(cli, &forward_temp, &reverse_temp, thread_roles)?;

    let cleanup_start = Instant::now();
    let mut deleted = Vec::new();
    for path in [&forward_temp, &reverse_temp] {
        if path.exists() {
            fs::remove_file(path)
                .with_context(|| format!("failed to delete temp file {}", path.display()))?;
            deleted.push(path.display().to_string());
        }
    }
    stage_log(
        cli,
        format!(
            "STAGE cleanup_temp seconds={:.6} deleted={}",
            cleanup_start.elapsed().as_secs_f64(),
            deleted.join(",")
        ),
    );

    Ok(())
}

fn run_pair_temp(cli: &Cli) -> Result<()> {
    let thread_roles = resolve_thread_roles(cli.threads);
    let mut forward_reader = bam::Reader::from_path(&cli.forward)
        .with_context(|| format!("failed to open forward input {}", cli.forward.display()))?;
    let mut reverse_reader = bam::Reader::from_path(&cli.reverse)
        .with_context(|| format!("failed to open reverse input {}", cli.reverse.display()))?;
    forward_reader
        .set_threads(thread_roles.input)
        .context("failed to set forward input threads")?;
    reverse_reader
        .set_threads(thread_roles.input)
        .context("failed to set reverse input threads")?;
    verify_matching_references(forward_reader.header(), reverse_reader.header())?;

    let forward_temp = make_temp_path(&cli.tmp_dir, "pair_filtered_forward")?;
    let reverse_temp = make_temp_path(&cli.tmp_dir, "pair_filtered_reverse")?;
    let header = bam::Header::from_template(forward_reader.header());
    let mut forward_temp_writer = Writer::from_path(&forward_temp, &header, bam::Format::Bam)
        .with_context(|| format!("failed to create temp BAM {}", forward_temp.display()))?;
    let mut reverse_temp_writer = Writer::from_path(&reverse_temp, &header, bam::Format::Bam)
        .with_context(|| format!("failed to create temp BAM {}", reverse_temp.display()))?;
    forward_temp_writer
        .set_threads(thread_roles.temp_write)
        .context("failed to set forward temp writer threads")?;
    reverse_temp_writer
        .set_threads(thread_roles.temp_write)
        .context("failed to set reverse temp writer threads")?;

    let filter_start = Instant::now();
    let mut stats = PairFilterStats::default();
    let mut forward_iter = forward_reader.records();
    let mut reverse_iter = reverse_reader.records();
    let mut forward_pending = None;
    let mut reverse_pending = None;

    loop {
        let next_forward = next_group(&mut forward_iter, &mut forward_pending)?;
        let next_reverse = next_group(&mut reverse_iter, &mut reverse_pending)?;
        match (next_forward, next_reverse) {
            (Some((f_name, f_group)), Some((r_name, r_group))) => {
                stats.groups += 1;
                if f_name != r_name {
                    bail!(
                        "pair-temp group name mismatch at group {}: forward={} reverse={}",
                        stats.groups,
                        String::from_utf8_lossy(&f_name),
                        String::from_utf8_lossy(&r_name)
                    );
                }
                let f_candidate = select_group_candidate(&f_group);
                let r_candidate = select_group_candidate(&r_group);
                if f_candidate.is_some() {
                    stats.candidate_groups_fwd += 1;
                }
                if r_candidate.is_some() {
                    stats.candidate_groups_rev += 1;
                }
                let (Some(f_candidate), Some(r_candidate)) = (f_candidate, r_candidate) else {
                    stats.missing_candidate += 1;
                    continue;
                };
                stats.candidate_pairs += 1;
                if f_candidate.mapq() < cli.quality || r_candidate.mapq() < cli.quality {
                    stats.low_mapq += 1;
                    continue;
                }
                forward_temp_writer
                    .write(&f_candidate)
                    .context("failed to write forward pair-temp record")?;
                reverse_temp_writer
                    .write(&r_candidate)
                    .context("failed to write reverse pair-temp record")?;
                stats.final_pairs += 1;
            }
            (None, None) => break,
            _ => bail!("pair-temp input BAMs contained a different number of query-name groups"),
        }
    }
    stage_log(
        cli,
        format!(
            "STAGE pair_filter groups={} candidate_pairs={} selected_groups_fwd={} selected_groups_rev={} missing_candidate={} low_mapq={} mismatched={} seconds={:.6} input_threads={}",
            stats.groups,
            stats.candidate_pairs,
            stats.candidate_groups_fwd,
            stats.candidate_groups_rev,
            stats.missing_candidate,
            stats.low_mapq,
            stats.mismatched,
            filter_start.elapsed().as_secs_f64(),
            thread_roles.input
        ),
    );
    drop(forward_temp_writer);
    drop(reverse_temp_writer);
    stage_log(
        cli,
        format!(
            "STAGE pair_temp pairs={} seconds={:.6} temp_fwd={} temp_rev={} temp_fwd_mb={:.3} temp_rev_mb={:.3}",
            stats.final_pairs,
            filter_start.elapsed().as_secs_f64(),
            forward_temp.display(),
            reverse_temp.display(),
            size_mb(&forward_temp)?,
            size_mb(&reverse_temp)?,
        ),
    );

    merge_pair_temp(cli, &forward_temp, &reverse_temp, thread_roles)?;
    fs::remove_file(&forward_temp)
        .with_context(|| format!("failed to delete temp file {}", forward_temp.display()))?;
    fs::remove_file(&reverse_temp)
        .with_context(|| format!("failed to delete temp file {}", reverse_temp.display()))?;
    Ok(())
}

fn run_direct(cli: &Cli) -> Result<()> {
    let setup_start = Instant::now();
    let thread_resolution = resolve_direct_thread_roles(cli.threads);
    stage_log(
        cli,
        format!(
            "STAGE direct_thread_resolution requested_threads={} detected_available_parallelism={} explicit_user_cap={} resolved_total_threads={} total_bgzf_workers={} per_reader_bgzf_workers={} writer_bgzf_workers={} compute_workers={}",
            thread_resolution.requested_threads,
            thread_resolution.detected_available_parallelism,
            thread_resolution
                .explicit_user_cap
                .map(|v| v.to_string())
                .unwrap_or_else(|| "none".to_string()),
            thread_resolution.resolved_total_threads,
            thread_resolution.total_bgzf_workers,
            thread_resolution.per_reader_bgzf_workers,
            thread_resolution.writer_bgzf_workers,
            thread_resolution.compute_workers
        ),
    );
    if thread_resolution.requested_threads > thread_resolution.detected_available_parallelism {
        stage_log(
            cli,
            format!(
                "STAGE direct_thread_backoff reason=available_parallelism_limit requested_threads={} detected_available_parallelism={} resolved_total_threads={}",
                thread_resolution.requested_threads,
                thread_resolution.detected_available_parallelism,
                thread_resolution.resolved_total_threads
            ),
        );
    }
    if let Some(cap) = thread_resolution.explicit_user_cap {
        if thread_resolution.requested_threads > cap {
            stage_log(
                cli,
                format!(
                    "STAGE direct_thread_backoff reason=explicit_user_cap requested_threads={} explicit_user_cap={} resolved_total_threads={}",
                    thread_resolution.requested_threads,
                    cap,
                    thread_resolution.resolved_total_threads
                ),
            );
        }
    }

    let mut forward_reader = bam::Reader::from_path(&cli.forward)
        .with_context(|| format!("failed to open forward input {}", cli.forward.display()))?;
    let mut reverse_reader = bam::Reader::from_path(&cli.reverse)
        .with_context(|| format!("failed to open reverse input {}", cli.reverse.display()))?;
    let bgzf_pool = ThreadPool::new(thread_resolution.total_bgzf_workers as u32)
        .context("failed to create shared BGZF thread pool")?;
    forward_reader
        .set_thread_pool(&bgzf_pool)
        .context("failed to attach BGZF pool to forward input")?;
    reverse_reader
        .set_thread_pool(&bgzf_pool)
        .context("failed to attach BGZF pool to reverse input")?;
    verify_matching_references(forward_reader.header(), reverse_reader.header())?;

    let header = bam::Header::from_template(forward_reader.header());
    let mut output = Writer::from_path(&cli.output, &header, bam::Format::Bam)
        .with_context(|| format!("failed to create output {}", cli.output.display()))?;
    output
        .set_thread_pool(&bgzf_pool)
        .context("failed to attach BGZF pool to output")?;

    stage_log(
        cli,
        format!(
            "STAGE direct_open_setup seconds={:.6} total_bgzf_workers={} compute_workers={}",
            setup_start.elapsed().as_secs_f64(),
            thread_resolution.total_bgzf_workers,
            thread_resolution.compute_workers
        ),
    );

    let mut stats = PairFilterStats::default();
    let mut read_match_seconds = 0.0f64;
    let mut process_seconds = 0.0f64;
    let mut write_compress_seconds = 0.0f64;
    let mut batch_sequence: u64 = 0;
    let mut received_batches: u64 = 0;
    let mut next_write_sequence: u64 = 0;
    let mut pending_batches: BTreeMap<u64, DirectOutputBatch> = BTreeMap::new();
    let (batch_tx, batch_rx) =
        bounded::<Option<DirectInputBatch>>(thread_resolution.compute_workers * 2);
    let (result_tx, result_rx) =
        bounded::<DirectOutputBatch>(thread_resolution.compute_workers * 2);
    let mut workers = Vec::with_capacity(thread_resolution.compute_workers);
    for _ in 0..thread_resolution.compute_workers {
        let worker_rx = batch_rx.clone();
        let worker_tx = result_tx.clone();
        let quality = cli.quality;
        workers.push(thread::spawn(move || {
            worker_process_direct_batches(worker_rx, worker_tx, quality)
        }));
    }
    drop(result_tx);

    let mut forward_pending = None;
    let mut reverse_pending = None;
    let mut forward_record = Record::new();
    let mut reverse_record = Record::new();
    let mut active_batch: Vec<(DirectRecordGroup, DirectRecordGroup)> = Vec::with_capacity(1024);

    loop {
        drain_completed_batches(
            &result_rx,
            &mut pending_batches,
            &mut output,
            &mut next_write_sequence,
            &mut write_compress_seconds,
            &mut stats,
            &mut process_seconds,
            &mut received_batches,
        )?;
        let read_start = Instant::now();
        let next_forward = next_group_records_read(
            &mut forward_reader,
            &mut forward_pending,
            &mut forward_record,
        )?;
        let next_reverse = next_group_records_read(
            &mut reverse_reader,
            &mut reverse_pending,
            &mut reverse_record,
        )?;
        match (next_forward, next_reverse) {
            (Some(f_group), Some(r_group)) => {
                if f_group.qname() != r_group.qname() {
                    bail!(
                        "direct group name mismatch at group {}: forward={} reverse={}",
                        batch_sequence + active_batch.len() as u64 + 1,
                        String::from_utf8_lossy(f_group.qname()),
                        String::from_utf8_lossy(r_group.qname())
                    );
                }
                active_batch.push((f_group, r_group));
                if active_batch.len() >= 1024 {
                    let groups = std::mem::replace(&mut active_batch, Vec::with_capacity(1024));
                    send_direct_batch(&batch_tx, batch_sequence, groups)?;
                    batch_sequence += 1;
                }
            }
            (None, None) => {
                if !active_batch.is_empty() {
                    let groups = std::mem::replace(&mut active_batch, Vec::with_capacity(1024));
                    send_direct_batch(&batch_tx, batch_sequence, groups)?;
                    batch_sequence += 1;
                }
                read_match_seconds += read_start.elapsed().as_secs_f64();
                break;
            }
            _ => bail!("direct input BAMs contained a different number of query-name groups"),
        }
        read_match_seconds += read_start.elapsed().as_secs_f64();
    }
    for _ in 0..thread_resolution.compute_workers {
        batch_tx
            .send(None)
            .context("failed to send direct worker termination signal")?;
    }
    drop(batch_tx);
    while received_batches < batch_sequence {
        receive_and_write_next_batch(
            &result_rx,
            &mut pending_batches,
            &mut output,
            &mut next_write_sequence,
            &mut write_compress_seconds,
            &mut stats,
            &mut process_seconds,
            &mut received_batches,
        )?;
    }
    for handle in workers {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("direct worker thread panicked"))??;
    }
    if write_compress_seconds > process_seconds && stats.final_pairs > 0 {
        stage_log(
            cli,
            format!(
                "STAGE direct_saturation reason=write_compress_dominates write_compress_seconds={:.6} process_seconds={:.6} note=consider_faster_storage_or_lower_compression",
                write_compress_seconds, process_seconds
            ),
        );
    }
    stage_log(
        cli,
        format!(
            "STAGE direct_process groups={} candidate_pairs={} selected_groups_fwd={} selected_groups_rev={} missing_candidate={} low_mapq={} mismatched={} read_match_seconds={:.6} process_seconds={:.6} write_compress_seconds={:.6}",
            stats.groups,
            stats.candidate_pairs,
            stats.candidate_groups_fwd,
            stats.candidate_groups_rev,
            stats.missing_candidate,
            stats.low_mapq,
            stats.mismatched,
            read_match_seconds,
            process_seconds,
            write_compress_seconds
        ),
    );

    let close_start = Instant::now();
    drop(output);
    stage_log(
        cli,
        format!(
            "STAGE flush_finalize_seconds pairs={} seconds={:.6} output={} output_mb={:.3}",
            stats.final_pairs,
            close_start.elapsed().as_secs_f64(),
            cli.output.display(),
            size_mb(&cli.output)?
        ),
    );
    Ok(())
}

fn send_direct_batch(
    batch_tx: &Sender<Option<DirectInputBatch>>,
    sequence: u64,
    groups: Vec<(DirectRecordGroup, DirectRecordGroup)>,
) -> Result<()> {
    batch_tx
        .send(Some(DirectInputBatch { sequence, groups }))
        .context("failed to enqueue direct processing batch")
}

fn worker_process_direct_batches(
    batch_rx: Receiver<Option<DirectInputBatch>>,
    result_tx: Sender<DirectOutputBatch>,
    quality: u8,
) -> Result<()> {
    loop {
        match batch_rx.recv() {
            Ok(Some(batch)) => {
                let output = process_direct_batch(batch, quality);
                result_tx
                    .send(output)
                    .context("failed to send processed direct batch")?;
            }
            Ok(None) => return Ok(()),
            Err(_) => return Ok(()),
        }
    }
}

fn process_direct_batch(batch: DirectInputBatch, quality: u8) -> DirectOutputBatch {
    let process_start = Instant::now();
    let mut stats = DirectBatchStats::default();
    let mut selected = Vec::with_capacity(batch.groups.len());
    for (f_group, r_group) in batch.groups {
        stats.groups += 1;
        let f_candidate_slot = select_group_candidate_slot(&f_group);
        let r_candidate_slot = select_group_candidate_slot(&r_group);
        if f_candidate_slot.is_some() {
            stats.candidate_groups_fwd += 1;
        }
        if r_candidate_slot.is_some() {
            stats.candidate_groups_rev += 1;
        }
        let (Some(f_candidate_slot), Some(r_candidate_slot)) = (f_candidate_slot, r_candidate_slot)
        else {
            stats.missing_candidate += 1;
            continue;
        };
        stats.candidate_pairs += 1;
        if f_group.mapq_at(f_candidate_slot) < quality
            || r_group.mapq_at(r_candidate_slot) < quality
        {
            stats.low_mapq += 1;
            continue;
        }
        let mut f_candidate = f_group.take_candidate(f_candidate_slot);
        let mut r_candidate = r_group.take_candidate(r_candidate_slot);
        let (forward_len, reverse_len) = signed_template_lengths(&f_candidate, &r_candidate);
        set_output_flags(&mut f_candidate, true, r_candidate.is_reverse());
        set_output_flags(&mut r_candidate, false, f_candidate.is_reverse());
        f_candidate.set_mtid(r_candidate.tid());
        r_candidate.set_mtid(f_candidate.tid());
        f_candidate.set_mpos(r_candidate.pos());
        r_candidate.set_mpos(f_candidate.pos());
        f_candidate.set_insert_size(forward_len);
        r_candidate.set_insert_size(reverse_len);
        selected.push((f_candidate, r_candidate));
        stats.final_pairs += 1;
    }
    DirectOutputBatch {
        sequence: batch.sequence,
        records: selected,
        stats,
        process_seconds: process_start.elapsed().as_secs_f64(),
    }
}

fn drain_completed_batches(
    result_rx: &Receiver<DirectOutputBatch>,
    pending_batches: &mut BTreeMap<u64, DirectOutputBatch>,
    output: &mut Writer,
    next_write_sequence: &mut u64,
    write_compress_seconds: &mut f64,
    stats: &mut PairFilterStats,
    process_seconds: &mut f64,
    received_batches: &mut u64,
) -> Result<()> {
    loop {
        match result_rx.try_recv() {
            Ok(batch) => {
                *received_batches += 1;
                pending_batches.insert(batch.sequence, batch);
                write_ready_batches(
                    pending_batches,
                    output,
                    next_write_sequence,
                    write_compress_seconds,
                    stats,
                    process_seconds,
                )?;
            }
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn receive_and_write_next_batch(
    result_rx: &Receiver<DirectOutputBatch>,
    pending_batches: &mut BTreeMap<u64, DirectOutputBatch>,
    output: &mut Writer,
    next_write_sequence: &mut u64,
    write_compress_seconds: &mut f64,
    stats: &mut PairFilterStats,
    process_seconds: &mut f64,
    received_batches: &mut u64,
) -> Result<()> {
    let batch = result_rx
        .recv()
        .context("direct result channel closed before all batches were processed")?;
    *received_batches += 1;
    pending_batches.insert(batch.sequence, batch);
    write_ready_batches(
        pending_batches,
        output,
        next_write_sequence,
        write_compress_seconds,
        stats,
        process_seconds,
    )
}

fn write_ready_batches(
    pending_batches: &mut BTreeMap<u64, DirectOutputBatch>,
    output: &mut Writer,
    next_write_sequence: &mut u64,
    write_compress_seconds: &mut f64,
    stats: &mut PairFilterStats,
    process_seconds: &mut f64,
) -> Result<()> {
    while let Some(batch) = pending_batches.remove(next_write_sequence) {
        stats.groups += batch.stats.groups;
        stats.candidate_groups_fwd += batch.stats.candidate_groups_fwd;
        stats.candidate_groups_rev += batch.stats.candidate_groups_rev;
        stats.candidate_pairs += batch.stats.candidate_pairs;
        stats.missing_candidate += batch.stats.missing_candidate;
        stats.low_mapq += batch.stats.low_mapq;
        stats.final_pairs += batch.stats.final_pairs;
        *process_seconds += batch.process_seconds;
        let write_start = Instant::now();
        for (f_record, r_record) in &batch.records {
            output
                .write(f_record)
                .context("failed to write direct output forward record")?;
            output
                .write(r_record)
                .context("failed to write direct output reverse record")?;
        }
        *write_compress_seconds += write_start.elapsed().as_secs_f64();
        *next_write_sequence += 1;
    }
    Ok(())
}

fn filter_one_side(
    cli: &Cli,
    reader: &mut bam::Reader,
    header: &bam::Header,
    temp_path: &Path,
    thread_roles: ThreadRoles,
    input_name: String,
) -> Result<()> {
    let mut writer = Writer::from_path(temp_path, header, bam::Format::Bam)
        .with_context(|| format!("failed to create temp BAM {}", temp_path.display()))?;
    writer
        .set_threads(thread_roles.temp_write)
        .context("failed to set temp writer threads")?;

    let start = Instant::now();
    let mut stats = FilterStats::default();

    let mut previous_qname: Option<Vec<u8>> = None;
    let mut group_records: Vec<Record> = Vec::new();
    let mut five_prime_count: usize = 0;
    let mut five_prime_record: Option<Record> = None;

    for read_result in reader.records() {
        let read = read_result.context("failed to read BAM record")?;
        stats.processed += 1;

        if let Some(prev) = &previous_qname {
            if prev.as_slice() != read.qname() {
                flush_group(
                    &mut writer,
                    &mut stats,
                    &group_records,
                    five_prime_count,
                    five_prime_record.as_ref(),
                )?;
                group_records.clear();
                five_prime_count = 0;
                five_prime_record = None;
            }
        }

        previous_qname = Some(read.qname().to_vec());
        if is_five_prime_m_only(&read) {
            five_prime_count += 1;
            if five_prime_record.is_none() {
                five_prime_record = Some(read.clone());
            }
        }
        group_records.push(read);
    }

    if !group_records.is_empty() {
        flush_group(
            &mut writer,
            &mut stats,
            &group_records,
            five_prime_count,
            five_prime_record.as_ref(),
        )?;
    }

    drop(writer);
    stage_log(
        cli,
        format!(
            "STAGE filter input={} processed={} written={} selected_groups={} placeholder_groups={} seconds={:.6} temp={} temp_mb={:.3} input_threads={} temp_write_threads={}",
            input_name,
            stats.processed,
            stats.written,
            stats.selected_groups,
            stats.placeholder_groups,
            start.elapsed().as_secs_f64(),
            temp_path.display(),
            size_mb(temp_path)?,
            thread_roles.input,
            thread_roles.temp_write,
        ),
    );

    Ok(())
}

fn flush_group(
    writer: &mut Writer,
    stats: &mut FilterStats,
    group_records: &[Record],
    five_prime_count: usize,
    five_prime_record: Option<&Record>,
) -> Result<()> {
    if (group_records.len() == 1 || group_records.len() == 2) && five_prime_count == 1 {
        if let Some(record) = five_prime_record {
            writer
                .write(record)
                .context("failed to write selected record")?;
            stats.written += 1;
            stats.selected_groups += 1;
            return Ok(());
        }
    }

    let mut placeholder = group_records
        .first()
        .context("group unexpectedly empty")?
        .clone();
    placeholder.set_unmapped();
    writer
        .write(&placeholder)
        .context("failed to write placeholder record")?;
    stats.written += 1;
    stats.placeholder_groups += 1;
    Ok(())
}

fn merge_filtered(
    cli: &Cli,
    filtered_forward: &Path,
    filtered_reverse: &Path,
    thread_roles: ThreadRoles,
) -> Result<()> {
    let open_start = Instant::now();
    let mut forward = bam::Reader::from_path(filtered_forward).with_context(|| {
        format!(
            "failed to open filtered forward {}",
            filtered_forward.display()
        )
    })?;
    let mut reverse = bam::Reader::from_path(filtered_reverse).with_context(|| {
        format!(
            "failed to open filtered reverse {}",
            filtered_reverse.display()
        )
    })?;
    forward
        .set_threads(thread_roles.temp_read)
        .context("failed to set filtered forward read threads")?;
    reverse
        .set_threads(thread_roles.temp_read)
        .context("failed to set filtered reverse read threads")?;

    let out_header = bam::Header::from_template(forward.header());
    let mut output = Writer::from_path(&cli.output, &out_header, bam::Format::Bam)
        .with_context(|| format!("failed to create output {}", cli.output.display()))?;
    output
        .set_threads(thread_roles.output)
        .context("failed to set output threads")?;

    stage_log(
        cli,
        format!(
            "STAGE open_merge_inputs seconds={:.6} temp_read_threads={} output_threads={}",
            open_start.elapsed().as_secs_f64(),
            thread_roles.temp_read,
            thread_roles.output,
        ),
    );

    let start = Instant::now();
    let mut stats = MergeStats::default();

    let mut forward_iter = forward.records();
    let mut reverse_iter = reverse.records();
    loop {
        let next_forward = forward_iter.next();
        let next_reverse = reverse_iter.next();

        match (next_forward, next_reverse) {
            (Some(Ok(mut f)), Some(Ok(mut r))) => {
                if f.qname() != r.qname() {
                    stats.mismatched += 1;
                    continue;
                }
                if f.is_unmapped() || r.is_unmapped() {
                    stats.unmapped += 1;
                    continue;
                }
                if f.mapq() < cli.quality || r.mapq() < cli.quality {
                    stats.low_mapq += 1;
                    continue;
                }

                let (forward_len, reverse_len) = signed_template_lengths(&f, &r);
                set_output_flags(&mut f, true, r.is_reverse());
                set_output_flags(&mut r, false, f.is_reverse());

                f.set_mtid(r.tid());
                r.set_mtid(f.tid());
                f.set_mpos(r.pos());
                r.set_mpos(f.pos());
                f.set_insert_size(forward_len);
                r.set_insert_size(reverse_len);

                output
                    .write(&f)
                    .context("failed to write output forward record")?;
                output
                    .write(&r)
                    .context("failed to write output reverse record")?;
                stats.pairs += 1;
            }
            (Some(Err(e)), _) | (_, Some(Err(e))) => {
                return Err(anyhow::Error::from(e).context("failed reading filtered record"));
            }
            _ => break,
        }
    }

    drop(output);
    stage_log(
        cli,
        format!(
            "STAGE merge pairs={} mismatched={} unmapped={} low_mapq={} seconds={:.6} output={} output_mb={:.3}",
            stats.pairs,
            stats.mismatched,
            stats.unmapped,
            stats.low_mapq,
            start.elapsed().as_secs_f64(),
            cli.output.display(),
            size_mb(&cli.output)?
        ),
    );

    Ok(())
}

fn merge_pair_temp(
    cli: &Cli,
    filtered_forward: &Path,
    filtered_reverse: &Path,
    thread_roles: ThreadRoles,
) -> Result<()> {
    let mut forward = bam::Reader::from_path(filtered_forward).with_context(|| {
        format!(
            "failed to open filtered forward {}",
            filtered_forward.display()
        )
    })?;
    let mut reverse = bam::Reader::from_path(filtered_reverse).with_context(|| {
        format!(
            "failed to open filtered reverse {}",
            filtered_reverse.display()
        )
    })?;
    forward
        .set_threads(thread_roles.temp_read)
        .context("failed to set filtered forward read threads")?;
    reverse
        .set_threads(thread_roles.temp_read)
        .context("failed to set filtered reverse read threads")?;

    let out_header = bam::Header::from_template(forward.header());
    let mut output = Writer::from_path(&cli.output, &out_header, bam::Format::Bam)
        .with_context(|| format!("failed to create output {}", cli.output.display()))?;
    output
        .set_threads(thread_roles.output)
        .context("failed to set output threads")?;

    let start = Instant::now();
    let mut stats = MergeStats::default();
    let mut forward_iter = forward.records();
    let mut reverse_iter = reverse.records();
    loop {
        let next_forward = forward_iter.next();
        let next_reverse = reverse_iter.next();
        match (next_forward, next_reverse) {
            (Some(Ok(mut f)), Some(Ok(mut r))) => {
                if f.qname() != r.qname() {
                    bail!(
                        "pair-temp merge qname mismatch: forward={} reverse={}",
                        String::from_utf8_lossy(f.qname()),
                        String::from_utf8_lossy(r.qname())
                    );
                }
                if f.is_unmapped() || r.is_unmapped() {
                    stats.unmapped += 1;
                    continue;
                }
                if f.mapq() < cli.quality || r.mapq() < cli.quality {
                    stats.low_mapq += 1;
                    continue;
                }
                let (forward_len, reverse_len) = signed_template_lengths(&f, &r);
                set_output_flags(&mut f, true, r.is_reverse());
                set_output_flags(&mut r, false, f.is_reverse());
                f.set_mtid(r.tid());
                r.set_mtid(f.tid());
                f.set_mpos(r.pos());
                r.set_mpos(f.pos());
                f.set_insert_size(forward_len);
                r.set_insert_size(reverse_len);
                output
                    .write(&f)
                    .context("failed to write output forward record")?;
                output
                    .write(&r)
                    .context("failed to write output reverse record")?;
                stats.pairs += 1;
            }
            (Some(Err(e)), _) | (_, Some(Err(e))) => {
                return Err(anyhow::Error::from(e).context("failed reading filtered record"));
            }
            (Some(_), None) | (None, Some(_)) => {
                bail!("pair-temp merge inputs have different record counts")
            }
            (None, None) => break,
        }
    }
    drop(output);
    stage_log(
        cli,
        format!(
            "STAGE merge pairs={} mismatched={} unmapped={} low_mapq={} seconds={:.6} output={} output_mb={:.3}",
            stats.pairs,
            stats.mismatched,
            stats.unmapped,
            stats.low_mapq,
            start.elapsed().as_secs_f64(),
            cli.output.display(),
            size_mb(&cli.output)?
        ),
    );
    Ok(())
}

fn next_group<I>(
    iter: &mut I,
    pending: &mut Option<Record>,
) -> Result<Option<(Vec<u8>, Vec<Record>)>>
where
    I: Iterator<Item = std::result::Result<Record, rust_htslib::errors::Error>>,
{
    let first = if let Some(record) = pending.take() {
        record
    } else {
        match iter.next() {
            Some(record) => record.context("failed to read BAM record")?,
            None => return Ok(None),
        }
    };
    let qname = first.qname().to_vec();
    let mut group = vec![first];
    loop {
        match iter.next() {
            Some(next) => {
                let next = next.context("failed to read BAM record")?;
                if next.qname() == qname.as_slice() {
                    group.push(next);
                } else {
                    *pending = Some(next);
                    break;
                }
            }
            None => break,
        }
    }
    Ok(Some((qname, group)))
}

fn next_group_records_read(
    reader: &mut bam::Reader,
    pending: &mut Option<Record>,
    scratch: &mut Record,
) -> Result<Option<DirectRecordGroup>> {
    let first = if let Some(record) = pending.take() {
        record
    } else {
        match reader.read(scratch) {
            Some(read_result) => {
                read_result.context("failed to read BAM record")?;
                std::mem::replace(scratch, Record::new())
            }
            None => return Ok(None),
        }
    };
    let mut group = DirectRecordGroup::new(first);
    loop {
        match reader.read(scratch) {
            Some(read_result) => {
                read_result.context("failed to read BAM record")?;
                let next = std::mem::replace(scratch, Record::new());
                if next.qname() == group.qname() {
                    group.push_same_qname(next);
                } else {
                    *pending = Some(next);
                    break;
                }
            }
            None => break,
        }
    }
    Ok(Some(group))
}

fn select_group_candidate(group_records: &[Record]) -> Option<Record> {
    if group_records.len() != 1 && group_records.len() != 2 {
        return None;
    }
    let mut candidate: Option<Record> = None;
    let mut five_prime_count = 0;
    for record in group_records {
        if is_five_prime_m_only(record) {
            five_prime_count += 1;
            if candidate.is_none() {
                candidate = Some(record.clone());
            }
        }
    }
    if five_prime_count == 1 {
        candidate
    } else {
        None
    }
}

fn select_group_candidate_slot(group_records: &DirectRecordGroup) -> Option<CandidateSlot> {
    if group_records.len() != 1 && group_records.len() != 2 {
        return None;
    }
    let first_ok = is_five_prime_m_only(&group_records.first);
    let second_ok = group_records
        .second
        .as_ref()
        .map(is_five_prime_m_only)
        .unwrap_or(false);
    match (first_ok, second_ok) {
        (true, false) => Some(CandidateSlot::First),
        (false, true) => Some(CandidateSlot::Second),
        _ => None,
    }
}

fn signed_template_lengths(forward: &Record, reverse: &Record) -> (i64, i64) {
    if forward.tid() != reverse.tid() {
        return (0, 0);
    }
    let distance = (forward.pos() - reverse.pos()).abs();
    if forward.pos() >= reverse.pos() {
        (-distance, distance)
    } else {
        (distance, -distance)
    }
}

fn set_output_flags(record: &mut Record, is_forward: bool, mate_is_reverse: bool) {
    clear_flag(record, 0x100);
    clear_flag(record, 0x800);
    clear_flag(record, 0x4);
    set_flag(record, 0x1);
    set_flag(record, 0x2);
    clear_flag(record, 0x8);

    if is_forward {
        set_flag(record, 0x40);
        clear_flag(record, 0x80);
    } else {
        clear_flag(record, 0x40);
        set_flag(record, 0x80);
    }

    if mate_is_reverse {
        set_flag(record, 0x20);
    } else {
        clear_flag(record, 0x20);
    }
}

fn set_flag(record: &mut Record, bit: u16) {
    record.set_flags(record.flags() | bit);
}

fn clear_flag(record: &mut Record, bit: u16) {
    record.set_flags(record.flags() & !bit);
}

fn is_five_prime_m_only(record: &Record) -> bool {
    if record.is_reverse() {
        last_cigar_op_is_m(&record.cigar())
    } else {
        first_cigar_op_is_m(&record.cigar())
    }
}

fn first_cigar_op_is_m(cigar: &[Cigar]) -> bool {
    matches!(cigar.first(), Some(Cigar::Match(_))) || cigar_has_clipped_middle_m(cigar)
}

fn last_cigar_op_is_m(cigar: &[Cigar]) -> bool {
    matches!(cigar.last(), Some(Cigar::Match(_))) || cigar_has_clipped_middle_m(cigar)
}

fn cigar_has_clipped_middle_m(cigar: &[Cigar]) -> bool {
    match (cigar.first(), cigar.last()) {
        (Some(first), Some(last)) if is_clip_op(first) && is_clip_op(last) => {
            cigar.iter().any(|op| matches!(op, Cigar::Match(_)))
        }
        _ => false,
    }
}

fn is_clip_op(op: &Cigar) -> bool {
    matches!(op, Cigar::SoftClip(_) | Cigar::HardClip(_))
}

fn resolve_thread_roles(thread_count: usize) -> ThreadRoles {
    let strategy = env::var("BELLEROPHON_IO_THREADS_STRATEGY")
        .unwrap_or_else(|_| "legacy".to_string())
        .trim()
        .to_ascii_lowercase();
    let requested = thread_count.max(1);
    let base_threads = if strategy == "legacy" {
        requested
    } else {
        requested.min(4)
    };
    let read_threads = (base_threads as isize - 1).max(1) as usize;
    ThreadRoles {
        input: read_threads,
        temp_write: 1,
        temp_read: read_threads,
        output: 1,
    }
}

fn resolve_direct_thread_roles(thread_count: usize) -> DirectThreadResolution {
    let requested = thread_count.max(1);
    let detected_available_parallelism = thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(1);
    let explicit_user_cap = parse_env_usize("BELLEROPHON_THREADS_CAP");
    let capped_by_user = explicit_user_cap
        .map(|cap| requested.min(cap))
        .unwrap_or(requested);
    let resolved_total_threads = capped_by_user.min(detected_available_parallelism).max(1);

    let total_bgzf_workers = if resolved_total_threads >= 8 {
        (resolved_total_threads / 3).max(2)
    } else if resolved_total_threads >= 3 {
        2
    } else {
        1
    };
    let writer_bgzf_workers = (total_bgzf_workers / 3).max(1);
    let remaining_reader_workers = total_bgzf_workers.saturating_sub(writer_bgzf_workers);
    let per_reader_bgzf_workers = (remaining_reader_workers / 2).max(1);
    let compute_workers = resolved_total_threads
        .saturating_sub(total_bgzf_workers)
        .max(1);

    DirectThreadResolution {
        requested_threads: requested,
        detected_available_parallelism,
        explicit_user_cap,
        resolved_total_threads,
        total_bgzf_workers,
        per_reader_bgzf_workers,
        writer_bgzf_workers,
        compute_workers,
    }
}

fn parse_env_usize(key: &str) -> Option<usize> {
    env::var(key)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn verify_matching_references(left: &bam::HeaderView, right: &bam::HeaderView) -> Result<()> {
    if left.target_count() != right.target_count() {
        bail!("the input files do not have the same sequence names or lengths");
    }

    for tid in 0..left.target_count() {
        let left_name = left.tid2name(tid);
        let right_name = right.tid2name(tid);
        if left_name != right_name || left.target_len(tid) != right.target_len(tid) {
            bail!("the input files do not have the same sequence names or lengths");
        }
    }
    Ok(())
}

fn make_temp_path(tmp_dir: &Path, prefix: &str) -> Result<PathBuf> {
    fs::create_dir_all(tmp_dir)
        .with_context(|| format!("failed to create tmp directory {}", tmp_dir.display()))?;
    let file = Builder::new()
        .prefix(prefix)
        .suffix(".bam")
        .tempfile_in(tmp_dir)
        .with_context(|| format!("failed to create tempfile in {}", tmp_dir.display()))?;
    let (_persist_file, path) = file.keep().context("failed to persist temporary file")?;
    Ok(path)
}

fn input_label(path: &Path) -> String {
    path.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

fn size_mb(path: &Path) -> Result<f64> {
    let size = fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();
    Ok(size as f64 / 1024.0 / 1024.0)
}

fn stage_log(cli: &Cli, message: String) {
    if cli.log_level >= LogLevel::Info {
        println!("{message}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_htslib::bam::record::CigarString;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn make_record(name: &[u8], reverse: bool, mapq: u8, cigar: CigarString) -> Record {
        let mut record = Record::new();
        record.set(name, Some(&cigar), b"A", &[30]);
        record.set_reverse();
        if !reverse {
            clear_flag(&mut record, 0x10);
        }
        record.set_mapq(mapq);
        record
    }

    #[test]
    fn first_cigar_classifier() {
        let cigar = CigarString(vec![Cigar::Match(10), Cigar::SoftClip(3)]);
        assert!(first_cigar_op_is_m(&cigar));
        let cigar = CigarString(vec![Cigar::SoftClip(3), Cigar::Match(10)]);
        assert!(!first_cigar_op_is_m(&cigar));
        let cigar = CigarString(vec![
            Cigar::SoftClip(3),
            Cigar::Match(10),
            Cigar::HardClip(2),
        ]);
        assert!(first_cigar_op_is_m(&cigar));
    }

    #[test]
    fn last_cigar_classifier() {
        let cigar = CigarString(vec![Cigar::SoftClip(3), Cigar::Match(10)]);
        assert!(last_cigar_op_is_m(&cigar));
        let cigar = CigarString(vec![Cigar::Match(10), Cigar::SoftClip(3)]);
        assert!(!last_cigar_op_is_m(&cigar));
        let cigar = CigarString(vec![
            Cigar::SoftClip(3),
            Cigar::Match(10),
            Cigar::HardClip(2),
        ]);
        assert!(last_cigar_op_is_m(&cigar));
    }

    #[test]
    fn clipped_middle_match_classifier() {
        let cigar = CigarString(vec![
            Cigar::SoftClip(10),
            Cigar::Match(80),
            Cigar::SoftClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));
        let cigar = CigarString(vec![
            Cigar::HardClip(10),
            Cigar::Match(80),
            Cigar::HardClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));
        let cigar = CigarString(vec![
            Cigar::SoftClip(10),
            Cigar::Match(80),
            Cigar::HardClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));
        let cigar = CigarString(vec![
            Cigar::SoftClip(10),
            Cigar::Del(80),
            Cigar::SoftClip(10),
        ]);
        assert!(!cigar_has_clipped_middle_m(&cigar));
    }

    #[test]
    fn cigar_regex_semantics_edge_cases() {
        let cigar = CigarString(vec![Cigar::Match(100)]);
        assert!(first_cigar_op_is_m(&cigar));
        assert!(last_cigar_op_is_m(&cigar));

        let cigar = CigarString(vec![Cigar::SoftClip(10), Cigar::Match(90)]);
        assert!(!first_cigar_op_is_m(&cigar));
        assert!(last_cigar_op_is_m(&cigar));

        let cigar = CigarString(vec![Cigar::Match(90), Cigar::SoftClip(10)]);
        assert!(first_cigar_op_is_m(&cigar));
        assert!(!last_cigar_op_is_m(&cigar));

        let cigar = CigarString(vec![
            Cigar::SoftClip(10),
            Cigar::Match(80),
            Cigar::SoftClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));
        assert!(first_cigar_op_is_m(&cigar));
        assert!(last_cigar_op_is_m(&cigar));

        let cigar = CigarString(vec![
            Cigar::HardClip(10),
            Cigar::Match(80),
            Cigar::HardClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));

        let cigar = CigarString(vec![
            Cigar::SoftClip(10),
            Cigar::Match(80),
            Cigar::HardClip(10),
        ]);
        assert!(cigar_has_clipped_middle_m(&cigar));

        let cigar = CigarString(vec![Cigar::Match(80), Cigar::Del(10), Cigar::Match(20)]);
        assert!(first_cigar_op_is_m(&cigar));
        assert!(last_cigar_op_is_m(&cigar));
        assert!(!cigar_has_clipped_middle_m(&cigar));

        let cigar = CigarString(vec![]);
        assert!(!first_cigar_op_is_m(&cigar));
        assert!(!last_cigar_op_is_m(&cigar));
        assert!(!cigar_has_clipped_middle_m(&cigar));
    }

    #[test]
    fn template_length_signs() {
        let mut f = Record::new();
        let mut r = Record::new();
        f.set_tid(0);
        r.set_tid(0);
        f.set_pos(200);
        r.set_pos(120);
        assert_eq!(signed_template_lengths(&f, &r), (-80, 80));

        f.set_pos(20);
        r.set_pos(120);
        assert_eq!(signed_template_lengths(&f, &r), (100, -100));

        r.set_tid(1);
        assert_eq!(signed_template_lengths(&f, &r), (0, 0));
    }

    #[test]
    fn flag_mutation_helper() {
        let mut rec = Record::new();
        rec.set_flags(0x100 | 0x800 | 0x4 | 0x20);
        set_output_flags(&mut rec, true, false);

        assert!(!rec.is_secondary());
        assert!(!rec.is_supplementary());
        assert!(!rec.is_unmapped());
        assert!(rec.is_paired());
        assert!(rec.is_proper_pair());
        assert!(rec.is_first_in_template());
        assert!(!rec.is_last_in_template());
        assert!(!rec.is_mate_reverse());
    }

    #[test]
    fn group_candidate_requires_single_five_prime_read() {
        let good = make_record(
            b"q1",
            false,
            60,
            CigarString(vec![Cigar::Match(10), Cigar::SoftClip(3)]),
        );
        let bad = make_record(
            b"q1",
            false,
            60,
            CigarString(vec![Cigar::SoftClip(3), Cigar::Match(10)]),
        );
        let one = vec![good.clone()];
        assert!(select_group_candidate(&one).is_some());
        let both = vec![good, bad];
        assert!(select_group_candidate(&both).is_some());
    }

    #[test]
    fn group_candidate_rejects_invalid_groups() {
        let read_a = make_record(
            b"q1",
            false,
            60,
            CigarString(vec![Cigar::Match(10), Cigar::SoftClip(3)]),
        );
        let read_b = make_record(
            b"q1",
            false,
            60,
            CigarString(vec![Cigar::Match(12), Cigar::SoftClip(1)]),
        );
        assert!(select_group_candidate(&[read_a.clone(), read_b]).is_none());
        assert!(select_group_candidate(&[read_a.clone(), read_a.clone(), read_a]).is_none());
    }

    #[test]
    fn next_group_keeps_state_between_calls() {
        let a1 = make_record(
            b"a",
            false,
            60,
            CigarString(vec![Cigar::Match(10), Cigar::SoftClip(3)]),
        );
        let a2 = make_record(
            b"a",
            true,
            60,
            CigarString(vec![Cigar::SoftClip(2), Cigar::Match(10)]),
        );
        let b1 = make_record(b"b", false, 60, CigarString(vec![Cigar::Match(10)]));
        let mut iter = vec![Ok(a1), Ok(a2), Ok(b1)].into_iter();
        let mut pending = None;
        let g1 = next_group(&mut iter, &mut pending)
            .expect("group read")
            .expect("group exists");
        assert_eq!(g1.0, b"a".to_vec());
        assert_eq!(g1.1.len(), 2);
        let g2 = next_group(&mut iter, &mut pending)
            .expect("group read")
            .expect("group exists");
        assert_eq!(g2.0, b"b".to_vec());
        assert_eq!(g2.1.len(), 1);
        assert!(next_group(&mut iter, &mut pending)
            .expect("group read")
            .is_none());
    }

    #[test]
    fn direct_thread_roles_use_requested_threads_without_hidden_32_cap() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("BELLEROPHON_THREADS_CAP");
        let resolution = resolve_direct_thread_roles(128);
        assert_eq!(
            resolution.resolved_total_threads,
            resolution.detected_available_parallelism
        );
        assert!(resolution.total_bgzf_workers >= 1);
        assert!(resolution.compute_workers >= 1);
    }

    #[test]
    fn direct_thread_roles_respect_explicit_cap_env_var() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("BELLEROPHON_THREADS_CAP", "12");
        let resolution = resolve_direct_thread_roles(64);
        assert_eq!(resolution.explicit_user_cap, Some(12));
        assert_eq!(
            resolution.resolved_total_threads,
            resolution.detected_available_parallelism.min(12)
        );
        std::env::remove_var("BELLEROPHON_THREADS_CAP");
    }
}
