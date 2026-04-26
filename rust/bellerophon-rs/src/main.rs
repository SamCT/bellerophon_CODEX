use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rust_htslib::bam;
use rust_htslib::bam::record::{Cigar, Record};
use rust_htslib::bam::{CompressionLevel, Read, Writer};
use rust_htslib::tpool::ThreadPool;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Instant;
use tempfile::Builder;

#[derive(Clone, Debug, ValueEnum, Eq, PartialEq)]
enum Pipeline {
    LegacyTemp,
    PairTemp,
    Direct,
}

#[derive(Clone, Debug, ValueEnum, Eq, PartialEq)]
enum DirectHtslibPoolMode {
    Shared,
    #[value(name = "split-per-handle")]
    SplitPerHandle,
}

#[derive(Clone, Debug, ValueEnum, Eq, PartialEq)]
enum DirectReaderMode {
    Serial,
    #[value(name = "dual-thread-diagnostic")]
    DualThreadDiagnostic,
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
    /// Total thread budget.
    /// Resolved to min(--threads, available_parallelism()).
    #[arg(short = 't', long = "threads", default_value_t = 1)]
    threads: usize,
    /// BAM compression level for output (0-9).
    /// If omitted, HTSlib default is used.
    #[arg(long = "compression-level", value_parser = clap::value_parser!(u8).range(0..=9))]
    compression_level: Option<u8>,
    #[arg(short = 'l', long = "log-level", default_value = "error")]
    log_level: LogLevel,
    #[arg(long = "pipeline", value_enum, hide = true, default_value = "direct")]
    pipeline: Pipeline,
    /// Number of query-name groups per direct pipeline batch.
    #[arg(long = "direct-batch-size", default_value_t = 1024)]
    direct_batch_size: usize,
    /// HTSlib BGZF pool wiring mode for direct pipeline diagnostics.
    #[arg(
        long = "direct-htslib-pool-mode",
        value_enum,
        default_value = "shared",
        help = "HTSlib BGZF pool mode. 'split-per-handle' is experimental and slower/pathological on current HPC evidence."
    )]
    direct_htslib_pool_mode: DirectHtslibPoolMode,
    /// Reader execution mode for direct pipeline diagnostics.
    #[arg(
        long = "direct-reader-mode",
        value_enum,
        default_value = "serial",
        help = "Reader mode. 'dual-thread-diagnostic' is experimental diagnostic-only and not production tuned."
    )]
    direct_reader_mode: DirectReaderMode,
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
    shared_pool_intended_per_reader_bgzf_workers: usize,
    shared_pool_intended_writer_bgzf_workers: usize,
    compute_workers: usize,
    assigned_threads: usize,
    unused_threads: usize,
    htslib_pool_enabled: bool,
}

#[derive(Debug)]
struct DirectInputBatch {
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
    records: Vec<(Record, Record)>,
    stats: DirectBatchStats,
    process_seconds: f64,
}

#[derive(Default)]
struct DirectWorkerStats {
    pair_stats: PairFilterStats,
    writer_loop_seconds: f64,
    writer_recv_wait_seconds: f64,
    process_seconds: f64,
    write_call_seconds: f64,
    output_drop_close_seconds: f64,
    bgzf_flush_seconds: f64,
    hts_close_seconds: f64,
    file_sync_or_drop_seconds: f64,
    batches_processed: u64,
    total_batch_size: u64,
    max_batch_size: usize,
    records_written: u64,
    estimated_uncompressed_bytes_written: u64,
}

#[derive(Default, Clone, Debug)]
struct ReaderDecodeStats {
    wall_seconds: f64,
    decode_seconds: f64,
    records_decoded: u64,
}

#[derive(Default, Clone, Copy, Debug)]
struct SplitBgzfWorkers {
    forward: usize,
    reverse: usize,
    output: usize,
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
            "STAGE direct_thread_resolution requested_threads={} detected_available_parallelism={} explicit_user_cap={} resolved_total_threads={} shared_bgzf_pool_workers={} shared_pool_intended_per_reader_bgzf_workers={} shared_pool_intended_writer_bgzf_workers={} compute_workers={} assigned_threads={} unused_threads={} htslib_pool_enabled={} allocation_mode=auto_shared_pool",
            thread_resolution.requested_threads,
            thread_resolution.detected_available_parallelism,
            thread_resolution
                .explicit_user_cap
                .map(|v| v.to_string())
                .unwrap_or_else(|| "none".to_string()),
            thread_resolution.resolved_total_threads,
            thread_resolution.total_bgzf_workers,
            thread_resolution.shared_pool_intended_per_reader_bgzf_workers,
            thread_resolution.shared_pool_intended_writer_bgzf_workers,
            thread_resolution.compute_workers,
            thread_resolution.assigned_threads,
            thread_resolution.unused_threads,
            thread_resolution.htslib_pool_enabled
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
                    thread_resolution.requested_threads, cap, thread_resolution.resolved_total_threads
                ),
            );
        }
    }
    if thread_resolution.unused_threads > 0 {
        stage_log(
            cli,
            format!(
                "STAGE direct_thread_backoff reason=internal_allocation_limit requested_threads={} resolved_total_threads={} assigned_threads={} unused_threads={} note=unexpected_unused_budget",
                thread_resolution.requested_threads,
                thread_resolution.resolved_total_threads,
                thread_resolution.assigned_threads,
                thread_resolution.unused_threads
            ),
        );
    }
    if cli.direct_htslib_pool_mode == DirectHtslibPoolMode::SplitPerHandle {
        stage_log(
            cli,
            "STAGE direct_experimental_mode mode=direct_htslib_pool_mode value=split_per_handle note=experimental_slower_or_pathological_in_current_hpc_matrix"
                .to_string(),
        );
    }
    if cli.direct_reader_mode == DirectReaderMode::DualThreadDiagnostic {
        stage_log(
            cli,
            "STAGE direct_experimental_mode mode=direct_reader_mode value=dual_thread_diagnostic note=diagnostic_only_not_default_and_not_performance_tuned"
                .to_string(),
        );
    }

    if cli.direct_batch_size == 0 {
        bail!("--direct-batch-size must be at least 1");
    }

    let mut forward_reader = bam::Reader::from_path(&cli.forward)
        .with_context(|| format!("failed to open forward input {}", cli.forward.display()))?;
    let mut reverse_reader = bam::Reader::from_path(&cli.reverse)
        .with_context(|| format!("failed to open reverse input {}", cli.reverse.display()))?;
    let split_workers = resolve_split_bgzf_workers(thread_resolution.total_bgzf_workers);
    let mut shared_bgzf_pool: Option<ThreadPool> = None;
    let mut forward_bgzf_pool: Option<ThreadPool> = None;
    let mut reverse_bgzf_pool: Option<ThreadPool> = None;
    let mut output_bgzf_pool: Option<ThreadPool> = None;
    if thread_resolution.htslib_pool_enabled {
        match cli.direct_htslib_pool_mode {
            DirectHtslibPoolMode::Shared => {
                let pool = ThreadPool::new(thread_resolution.total_bgzf_workers as u32)
                    .context("failed to create shared BGZF thread pool")?;
                forward_reader
                    .set_thread_pool(&pool)
                    .context("failed to attach shared BGZF pool to forward input")?;
                reverse_reader
                    .set_thread_pool(&pool)
                    .context("failed to attach shared BGZF pool to reverse input")?;
                shared_bgzf_pool = Some(pool);
            }
            DirectHtslibPoolMode::SplitPerHandle => {
                if split_workers.forward > 0 {
                    let pool = ThreadPool::new(split_workers.forward as u32)
                        .context("failed to create forward BGZF thread pool")?;
                    forward_reader
                        .set_thread_pool(&pool)
                        .context("failed to attach forward BGZF pool to forward input")?;
                    forward_bgzf_pool = Some(pool);
                }
                if split_workers.reverse > 0 {
                    let pool = ThreadPool::new(split_workers.reverse as u32)
                        .context("failed to create reverse BGZF thread pool")?;
                    reverse_reader
                        .set_thread_pool(&pool)
                        .context("failed to attach reverse BGZF pool to reverse input")?;
                    reverse_bgzf_pool = Some(pool);
                }
            }
        }
    }
    verify_matching_references(forward_reader.header(), reverse_reader.header())?;

    let header = bam::Header::from_template(forward_reader.header());
    let mut output = Writer::from_path(&cli.output, &header, bam::Format::Bam)
        .with_context(|| format!("failed to create output {}", cli.output.display()))?;
    match cli.direct_htslib_pool_mode {
        DirectHtslibPoolMode::Shared => {
            if let Some(pool) = shared_bgzf_pool.as_ref() {
                output
                    .set_thread_pool(pool)
                    .context("failed to attach shared BGZF pool to output")?;
            }
        }
        DirectHtslibPoolMode::SplitPerHandle => {
            if split_workers.output > 0 {
                let pool = ThreadPool::new(split_workers.output as u32)
                    .context("failed to create output BGZF thread pool")?;
                output
                    .set_thread_pool(&pool)
                    .context("failed to attach output BGZF pool to output writer")?;
                output_bgzf_pool = Some(pool);
            }
        }
    }
    if let Some(level) = cli.compression_level {
        output
            .set_compression_level(compression_level_from_u8(level))
            .with_context(|| format!("failed to set output compression level {level}"))?;
    }
    let (batch_sender, batch_receiver) = sync_channel::<DirectInputBatch>(8);
    let queue_depth = Arc::new(AtomicUsize::new(0));
    let max_queue_depth = Arc::new(AtomicUsize::new(0));
    let quality = cli.quality;
    let writer_handle = thread::spawn({
        let queue_depth = Arc::clone(&queue_depth);
        move || direct_writer_thread(batch_receiver, output, quality, queue_depth)
    });

    stage_log(
        cli,
        format!(
            "STAGE direct_open_setup seconds={:.6} total_bgzf_workers={} compute_workers={} direct_batch_size={} compression_level={} htslib_pool_enabled={} htslib_pool_mode={} split_forward_bgzf_workers={} split_reverse_bgzf_workers={} split_output_bgzf_workers={} reader_mode={}",
            setup_start.elapsed().as_secs_f64(),
            thread_resolution.total_bgzf_workers,
            thread_resolution.compute_workers,
            cli.direct_batch_size,
            cli.compression_level
                .map(|v| v.to_string())
                .unwrap_or_else(|| "htslib_default".to_string()),
            thread_resolution.htslib_pool_enabled,
            direct_htslib_pool_mode_name(&cli.direct_htslib_pool_mode),
            split_workers.forward,
            split_workers.reverse,
            split_workers.output,
            direct_reader_mode_name(&cli.direct_reader_mode)
        ),
    );

    let read_match_start = Instant::now();
    let mut forward_reader_stats = ReaderDecodeStats::default();
    let mut reverse_reader_stats = ReaderDecodeStats::default();
    let mut pair_match_assembly_seconds = 0.0f64;
    let mut batch_enqueue_wait_seconds = 0.0f64;
    let mut groups_seen: u64 = 0;
    let mut active_batch: Vec<(DirectRecordGroup, DirectRecordGroup)> =
        Vec::with_capacity(cli.direct_batch_size);
    let mut reader_threads_spawned = 0usize;
    let mut reader_threads_active_high_watermark = 1usize;

    match cli.direct_reader_mode {
        DirectReaderMode::Serial => {
            let mut forward_pending = None;
            let mut reverse_pending = None;
            let mut forward_record = Record::new();
            let mut reverse_record = Record::new();
            loop {
                let next_forward = next_group_records_read(
                    &mut forward_reader,
                    &mut forward_pending,
                    &mut forward_record,
                    &mut forward_reader_stats,
                )?;
                let next_reverse = next_group_records_read(
                    &mut reverse_reader,
                    &mut reverse_pending,
                    &mut reverse_record,
                    &mut reverse_reader_stats,
                )?;
                let done = handle_direct_group_pair(
                    next_forward,
                    next_reverse,
                    &mut active_batch,
                    &batch_sender,
                    &queue_depth,
                    &max_queue_depth,
                    &mut batch_enqueue_wait_seconds,
                    cli.direct_batch_size,
                    &mut groups_seen,
                    &mut pair_match_assembly_seconds,
                )?;
                if done {
                    break;
                }
            }
        }
        DirectReaderMode::DualThreadDiagnostic => {
            reader_threads_spawned = 2;
            reader_threads_active_high_watermark = 2;
            let (forward_tx, forward_rx) = sync_channel::<Result<Option<DirectRecordGroup>>>(2);
            let (reverse_tx, reverse_rx) = sync_channel::<Result<Option<DirectRecordGroup>>>(2);
            let forward_handle =
                thread::spawn(move || read_groups_producer(forward_reader, forward_tx, "forward"));
            let reverse_handle =
                thread::spawn(move || read_groups_producer(reverse_reader, reverse_tx, "reverse"));
            loop {
                let next_forward = forward_rx
                    .recv()
                    .context("failed to receive next forward group")??;
                let next_reverse = reverse_rx
                    .recv()
                    .context("failed to receive next reverse group")??;
                let done = handle_direct_group_pair(
                    next_forward,
                    next_reverse,
                    &mut active_batch,
                    &batch_sender,
                    &queue_depth,
                    &max_queue_depth,
                    &mut batch_enqueue_wait_seconds,
                    cli.direct_batch_size,
                    &mut groups_seen,
                    &mut pair_match_assembly_seconds,
                )?;
                if done {
                    break;
                }
            }
            forward_reader_stats = forward_handle
                .join()
                .map_err(|_| anyhow::anyhow!("forward reader thread panicked"))??;
            reverse_reader_stats = reverse_handle
                .join()
                .map_err(|_| anyhow::anyhow!("reverse reader thread panicked"))??;
        }
    }

    if cli.direct_reader_mode == DirectReaderMode::Serial {
        forward_reader_stats.wall_seconds = forward_reader_stats.decode_seconds;
        reverse_reader_stats.wall_seconds = reverse_reader_stats.decode_seconds;
    }

    let read_match_seconds = read_match_start.elapsed().as_secs_f64();
    let read_decode_seconds =
        forward_reader_stats.decode_seconds + reverse_reader_stats.decode_seconds;
    let qname_group_seconds =
        (read_match_seconds - read_decode_seconds - pair_match_assembly_seconds).max(0.0);
    let pending_batches_before_close = queue_depth.load(Ordering::Relaxed);
    drop(batch_sender);
    drop(shared_bgzf_pool);
    drop(forward_bgzf_pool);
    drop(reverse_bgzf_pool);
    drop(output_bgzf_pool);
    let writer_wait_start = Instant::now();
    let writer_stats = writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("direct writer thread panicked"))??;
    let writer_drain_seconds = writer_wait_start.elapsed().as_secs_f64();
    let stats = writer_stats.pair_stats;
    let process_seconds = writer_stats.process_seconds;
    let writer_loop_seconds = writer_stats.writer_loop_seconds;
    let writer_recv_wait_seconds = writer_stats.writer_recv_wait_seconds;
    let writer_idle_seconds = writer_recv_wait_seconds;
    let write_call_seconds = writer_stats.write_call_seconds;
    let output_drop_close_seconds = writer_stats.output_drop_close_seconds;
    let batches_processed = writer_stats.batches_processed;
    let total_batch_size = writer_stats.total_batch_size;
    let max_batch_size = writer_stats.max_batch_size;
    let forward_decode_rps = if forward_reader_stats.wall_seconds > 0.0 {
        forward_reader_stats.records_decoded as f64 / forward_reader_stats.wall_seconds
    } else {
        0.0
    };
    let reverse_decode_rps = if reverse_reader_stats.wall_seconds > 0.0 {
        reverse_reader_stats.records_decoded as f64 / reverse_reader_stats.wall_seconds
    } else {
        0.0
    };

    if write_call_seconds > process_seconds && stats.final_pairs > 0 {
        stage_log(
            cli,
            format!(
                "STAGE direct_saturation reason=write_call_dominates write_call_seconds={:.6} process_seconds={:.6} note=consider_faster_storage_or_lower_compression",
                write_call_seconds, process_seconds
            ),
        );
    }
    stage_log(
        cli,
        format!(
            "STAGE direct_process groups={} candidate_pairs={} selected_groups_fwd={} selected_groups_rev={} missing_candidate={} low_mapq={} mismatched={} read_match_seconds={:.6} bam_read_decode_seconds={:.6} bam_read_decode_forward_seconds={:.6} bam_read_decode_reverse_seconds={:.6} qname_group_seconds={:.6} match_assembly_seconds={:.6} writer_recv_wait_seconds={:.6} writer_loop_seconds={:.6} process_seconds={:.6} write_call_seconds={:.6} output_drop_close_seconds={:.6}",
            stats.groups,
            stats.candidate_pairs,
            stats.candidate_groups_fwd,
            stats.candidate_groups_rev,
            stats.missing_candidate,
            stats.low_mapq,
            stats.mismatched,
            read_match_seconds,
            read_decode_seconds,
            forward_reader_stats.decode_seconds,
            reverse_reader_stats.decode_seconds,
            qname_group_seconds,
            pair_match_assembly_seconds,
            writer_recv_wait_seconds,
            writer_loop_seconds,
            process_seconds,
            write_call_seconds,
            output_drop_close_seconds
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_reader_diagnostics forward_reader_wall_seconds={:.6} reverse_reader_wall_seconds={:.6} forward_records_decoded={} reverse_records_decoded={} forward_decode_records_per_second={:.3} reverse_decode_records_per_second={:.3} reader_threads_spawned={} reader_threads_active_high_watermark={}",
            forward_reader_stats.wall_seconds,
            reverse_reader_stats.wall_seconds,
            forward_reader_stats.records_decoded,
            reverse_reader_stats.records_decoded,
            forward_decode_rps,
            reverse_decode_rps,
            reader_threads_spawned,
            reader_threads_active_high_watermark
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_phase_timings read_decode_seconds={:.6} qname_group_seconds={:.6} pair_match_assembly_seconds={:.6} batch_enqueue_wait_seconds={:.6} writer_drain_seconds={:.6}",
            read_decode_seconds,
            qname_group_seconds,
            pair_match_assembly_seconds,
            batch_enqueue_wait_seconds,
            writer_drain_seconds
        ),
    );
    let max_queue_depth = max_queue_depth.load(Ordering::Relaxed).max(1);
    let average_batch_size = if batches_processed > 0 {
        total_batch_size as f64 / batches_processed as f64
    } else {
        0.0
    };
    stage_log(
        cli,
        format!(
            "STAGE direct_pipeline_diagnostics writer_wait_for_sequence_seconds={:.6} max_queue_depth={} producer_blocked_seconds={:.6} writer_idle_seconds={:.6} batches_processed={} average_batch_size={:.3} max_batch_size={} pending_batches_before_close={} records_written={} estimated_uncompressed_bytes_written={} bgzf_worker_utilization=not_exposed_by_htslib_api",
            writer_recv_wait_seconds,
            max_queue_depth,
            batch_enqueue_wait_seconds,
            writer_idle_seconds,
            batches_processed,
            average_batch_size,
            max_batch_size,
            pending_batches_before_close,
            writer_stats.records_written,
            writer_stats.estimated_uncompressed_bytes_written
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_close_diagnostics write_call_seconds={:.6} hts_close_seconds={:.6} close_to_write_ratio={:.3} note=high_ratio_suggests_writer_backlog_flushed_during_close",
            write_call_seconds,
            writer_stats.hts_close_seconds,
            if write_call_seconds > 0.0 {
                writer_stats.hts_close_seconds / write_call_seconds
            } else {
                0.0
            }
        ),
    );

    let finalize_start = Instant::now();
    stage_log(
        cli,
        format!(
            "STAGE flush_finalize_seconds pairs={} seconds={:.6} output_drop_close_seconds={:.6} bgzf_flush_seconds={:.6} hts_close_seconds={:.6} file_sync_or_drop_seconds={:.6} output={} output_mb={:.3}",
            stats.final_pairs,
            finalize_start.elapsed().as_secs_f64(),
            output_drop_close_seconds,
            writer_stats.bgzf_flush_seconds,
            writer_stats.hts_close_seconds,
            writer_stats.file_sync_or_drop_seconds,
            cli.output.display(),
            size_mb(&cli.output)?
        ),
    );
    Ok(())
}

fn flush_direct_batch(
    active_batch: &mut Vec<(DirectRecordGroup, DirectRecordGroup)>,
    batch_sender: &SyncSender<DirectInputBatch>,
    queue_depth: &Arc<AtomicUsize>,
    max_queue_depth: &Arc<AtomicUsize>,
    batch_enqueue_wait_seconds: &mut f64,
    direct_batch_size: usize,
) -> Result<()> {
    if active_batch.is_empty() {
        return Ok(());
    }
    let enqueue_start = Instant::now();
    let batch = DirectInputBatch {
        groups: std::mem::replace(active_batch, Vec::with_capacity(direct_batch_size)),
    };
    batch_sender
        .send(batch)
        .context("failed to send direct batch to writer thread")?;
    let depth = queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
    loop {
        let previous_max = max_queue_depth.load(Ordering::Relaxed);
        if depth <= previous_max {
            break;
        }
        if max_queue_depth
            .compare_exchange(previous_max, depth, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
    *batch_enqueue_wait_seconds += enqueue_start.elapsed().as_secs_f64();
    Ok(())
}

fn handle_direct_group_pair(
    next_forward: Option<DirectRecordGroup>,
    next_reverse: Option<DirectRecordGroup>,
    active_batch: &mut Vec<(DirectRecordGroup, DirectRecordGroup)>,
    batch_sender: &SyncSender<DirectInputBatch>,
    queue_depth: &Arc<AtomicUsize>,
    max_queue_depth: &Arc<AtomicUsize>,
    batch_enqueue_wait_seconds: &mut f64,
    direct_batch_size: usize,
    groups_seen: &mut u64,
    pair_match_assembly_seconds: &mut f64,
) -> Result<bool> {
    let match_start = Instant::now();
    match (next_forward, next_reverse) {
        (Some(f_group), Some(r_group)) => {
            if f_group.qname() != r_group.qname() {
                bail!(
                    "direct group name mismatch at group {}: forward={} reverse={}",
                    *groups_seen + active_batch.len() as u64 + 1,
                    String::from_utf8_lossy(f_group.qname()),
                    String::from_utf8_lossy(r_group.qname())
                );
            }
            active_batch.push((f_group, r_group));
            *groups_seen += 1;
            if active_batch.len() >= direct_batch_size {
                flush_direct_batch(
                    active_batch,
                    batch_sender,
                    queue_depth,
                    max_queue_depth,
                    batch_enqueue_wait_seconds,
                    direct_batch_size,
                )?;
            }
        }
        (None, None) => {
            if !active_batch.is_empty() {
                flush_direct_batch(
                    active_batch,
                    batch_sender,
                    queue_depth,
                    max_queue_depth,
                    batch_enqueue_wait_seconds,
                    direct_batch_size,
                )?;
            }
            *pair_match_assembly_seconds += match_start.elapsed().as_secs_f64();
            return Ok(true);
        }
        _ => bail!("direct input BAMs contained a different number of query-name groups"),
    }
    *pair_match_assembly_seconds += match_start.elapsed().as_secs_f64();
    Ok(false)
}

fn read_groups_producer(
    mut reader: bam::Reader,
    sender: SyncSender<Result<Option<DirectRecordGroup>>>,
    label: &'static str,
) -> Result<ReaderDecodeStats> {
    let wall_start = Instant::now();
    let mut stats = ReaderDecodeStats::default();
    let mut pending = None;
    let mut scratch = Record::new();
    loop {
        let next =
            match next_group_records_read(&mut reader, &mut pending, &mut scratch, &mut stats) {
                Ok(next) => next,
                Err(e) => {
                    let _ = sender.send(Err(e));
                    break;
                }
            };
        let done = next.is_none();
        sender
            .send(Ok(next))
            .with_context(|| format!("failed to send {label} group from producer"))?;
        if done {
            break;
        }
    }
    stats.wall_seconds = wall_start.elapsed().as_secs_f64();
    Ok(stats)
}

fn direct_writer_thread(
    batch_receiver: Receiver<DirectInputBatch>,
    mut output: Writer,
    quality: u8,
    queue_depth: Arc<AtomicUsize>,
) -> Result<DirectWorkerStats> {
    let writer_loop_start = Instant::now();
    let mut worker_stats = DirectWorkerStats::default();
    loop {
        let recv_wait_start = Instant::now();
        let batch = match batch_receiver.recv() {
            Ok(batch) => batch,
            Err(_) => break,
        };
        worker_stats.writer_recv_wait_seconds += recv_wait_start.elapsed().as_secs_f64();
        queue_depth.fetch_sub(1, Ordering::Relaxed);
        worker_stats.batches_processed += 1;
        worker_stats.total_batch_size += batch.groups.len() as u64;
        worker_stats.max_batch_size = worker_stats.max_batch_size.max(batch.groups.len());
        let output_batch = process_direct_batch(batch, quality);
        worker_stats.process_seconds += output_batch.process_seconds;
        worker_stats.pair_stats.groups += output_batch.stats.groups;
        worker_stats.pair_stats.candidate_groups_fwd += output_batch.stats.candidate_groups_fwd;
        worker_stats.pair_stats.candidate_groups_rev += output_batch.stats.candidate_groups_rev;
        worker_stats.pair_stats.candidate_pairs += output_batch.stats.candidate_pairs;
        worker_stats.pair_stats.missing_candidate += output_batch.stats.missing_candidate;
        worker_stats.pair_stats.low_mapq += output_batch.stats.low_mapq;
        worker_stats.pair_stats.final_pairs += output_batch.stats.final_pairs;
        let write_start = Instant::now();
        for (f_record, r_record) in &output_batch.records {
            output
                .write(f_record)
                .context("failed to write direct output forward record")?;
            output
                .write(r_record)
                .context("failed to write direct output reverse record")?;
            worker_stats.records_written += 2;
            worker_stats.estimated_uncompressed_bytes_written +=
                (f_record.inner().l_data + r_record.inner().l_data) as u64;
        }
        worker_stats.write_call_seconds += write_start.elapsed().as_secs_f64();
    }

    worker_stats.writer_loop_seconds = writer_loop_start.elapsed().as_secs_f64();
    let output_drop_start = Instant::now();
    drop(output);
    worker_stats.output_drop_close_seconds = output_drop_start.elapsed().as_secs_f64();
    worker_stats.hts_close_seconds = worker_stats.output_drop_close_seconds;
    let sync_start = Instant::now();
    worker_stats.file_sync_or_drop_seconds = sync_start.elapsed().as_secs_f64();
    worker_stats.bgzf_flush_seconds = 0.0;
    Ok(worker_stats)
}

fn process_direct_batch(batch: DirectInputBatch, quality: u8) -> DirectOutputBatch {
    let process_start = Instant::now();
    let mut results = Vec::with_capacity(batch.groups.len());
    for (f_group, r_group) in batch.groups {
        results.push(process_group_pair(f_group, r_group, quality));
    }
    let mut stats = DirectBatchStats::default();
    let mut selected = Vec::with_capacity(results.len());
    for (item_stats, pair) in results {
        stats.groups += item_stats.groups;
        stats.candidate_groups_fwd += item_stats.candidate_groups_fwd;
        stats.candidate_groups_rev += item_stats.candidate_groups_rev;
        stats.candidate_pairs += item_stats.candidate_pairs;
        stats.missing_candidate += item_stats.missing_candidate;
        stats.low_mapq += item_stats.low_mapq;
        stats.final_pairs += item_stats.final_pairs;
        if let Some(pair) = pair {
            selected.push(pair);
        }
    }
    DirectOutputBatch {
        records: selected,
        stats,
        process_seconds: process_start.elapsed().as_secs_f64(),
    }
}

fn approx_record_payload_bytes(record: &Record) -> u64 {
    let seq_len = record.seq_len() as u64;
    let qual_len = record.qual().len() as u64;
    let qname_len = record.qname().len() as u64;
    let cigar_ops = record.cigar().len() as u64;
    qname_len + seq_len + qual_len + (cigar_ops * 4)
}

fn process_group_pair(
    f_group: DirectRecordGroup,
    r_group: DirectRecordGroup,
    quality: u8,
) -> (DirectBatchStats, Option<(Record, Record)>) {
    let mut stats = DirectBatchStats {
        groups: 1,
        ..Default::default()
    };
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
        return (stats, None);
    };
    stats.candidate_pairs += 1;
    if f_group.mapq_at(f_candidate_slot) < quality || r_group.mapq_at(r_candidate_slot) < quality {
        stats.low_mapq += 1;
        return (stats, None);
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
    stats.final_pairs += 1;
    (stats, Some((f_candidate, r_candidate)))
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
    reader_stats: &mut ReaderDecodeStats,
) -> Result<Option<DirectRecordGroup>> {
    let decode_start = Instant::now();
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
    reader_stats.records_decoded += 1;
    loop {
        match reader.read(scratch) {
            Some(read_result) => {
                read_result.context("failed to read BAM record")?;
                let next = std::mem::replace(scratch, Record::new());
                if next.qname() == group.qname() {
                    group.push_same_qname(next);
                    reader_stats.records_decoded += 1;
                } else {
                    *pending = Some(next);
                    break;
                }
            }
            None => break,
        }
    }
    reader_stats.decode_seconds += decode_start.elapsed().as_secs_f64();
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
    let explicit_user_cap = None;
    let resolved_total_threads = requested.min(detected_available_parallelism).max(1);

    // Allocate requested threads proportionally across:
    // - a small compute budget for pair assembly/filtering
    // - BGZF workers for the two input readers
    // - BGZF workers for the output writer
    let htslib_pool_enabled = true;
    let mut compute_workers = 1usize;
    if compute_workers >= resolved_total_threads {
        compute_workers = resolved_total_threads.saturating_sub(1);
    }
    let total_bgzf_workers = resolved_total_threads
        .saturating_sub(compute_workers)
        .max(1);
    let (shared_pool_intended_per_reader_bgzf_workers, shared_pool_intended_writer_bgzf_workers) =
        if total_bgzf_workers <= 1 {
            (0, total_bgzf_workers)
        } else {
            let per_reader = (total_bgzf_workers / 5).max(1);
            let mut writer = total_bgzf_workers.saturating_sub(per_reader * 2);
            if writer == 0 {
                writer = 1;
            }
            if writer <= per_reader {
                writer = per_reader + 1;
            }
            (per_reader, writer.min(total_bgzf_workers))
        };
    let assigned_threads = total_bgzf_workers + compute_workers;
    let unused_threads = resolved_total_threads.saturating_sub(assigned_threads);

    DirectThreadResolution {
        requested_threads: requested,
        detected_available_parallelism,
        explicit_user_cap,
        resolved_total_threads,
        total_bgzf_workers,
        shared_pool_intended_per_reader_bgzf_workers,
        shared_pool_intended_writer_bgzf_workers,
        compute_workers,
        assigned_threads,
        unused_threads,
        htslib_pool_enabled,
    }
}

fn resolve_split_bgzf_workers(total_bgzf_workers: usize) -> SplitBgzfWorkers {
    if total_bgzf_workers == 0 {
        return SplitBgzfWorkers::default();
    }
    if total_bgzf_workers == 1 {
        return SplitBgzfWorkers {
            forward: 1,
            reverse: 0,
            output: 0,
        };
    }
    if total_bgzf_workers == 2 {
        return SplitBgzfWorkers {
            forward: 1,
            reverse: 1,
            output: 0,
        };
    }
    let mut output = (total_bgzf_workers / 10).max(1);
    if output >= total_bgzf_workers {
        output = total_bgzf_workers - 1;
    }
    let reader_budget = total_bgzf_workers - output;
    let forward = reader_budget / 2;
    let reverse = reader_budget - forward;
    SplitBgzfWorkers {
        forward,
        reverse,
        output,
    }
}

fn direct_htslib_pool_mode_name(mode: &DirectHtslibPoolMode) -> &'static str {
    match mode {
        DirectHtslibPoolMode::Shared => "shared",
        DirectHtslibPoolMode::SplitPerHandle => "split_per_handle",
    }
}

fn direct_reader_mode_name(mode: &DirectReaderMode) -> &'static str {
    match mode {
        DirectReaderMode::Serial => "serial",
        DirectReaderMode::DualThreadDiagnostic => "dual_thread_diagnostic",
    }
}

fn compression_level_from_u8(level: u8) -> CompressionLevel {
    match level {
        0 => CompressionLevel::Uncompressed,
        1 => CompressionLevel::Fastest,
        9 => CompressionLevel::Maximum,
        value => CompressionLevel::Level(value as u32),
    }
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
        let resolution = resolve_direct_thread_roles(128);
        assert_eq!(
            resolution.resolved_total_threads,
            resolution.detected_available_parallelism
        );
        assert!(resolution.total_bgzf_workers >= 1);
        assert!(resolution.compute_workers >= 1);
        assert_eq!(resolution.unused_threads, 0);
        assert_eq!(
            resolution.assigned_threads,
            resolution.resolved_total_threads
        );
    }

    #[test]
    fn direct_thread_roles_only_back_off_for_available_parallelism() {
        let requested = 64usize;
        let resolution = resolve_direct_thread_roles(requested);
        assert_eq!(
            resolution.resolved_total_threads,
            requested.min(resolution.detected_available_parallelism)
        );
        assert_eq!(resolution.unused_threads, 0);
        assert_eq!(
            resolution.assigned_threads,
            resolution.resolved_total_threads
        );
    }

    #[test]
    fn direct_thread_roles_writer_gets_majority_of_bgzf_budget() {
        let resolution = resolve_direct_thread_roles(64);
        if resolution.resolved_total_threads >= 6 {
            assert!(
                resolution.shared_pool_intended_writer_bgzf_workers
                    > resolution.shared_pool_intended_per_reader_bgzf_workers
            );
            assert!(
                resolution.shared_pool_intended_writer_bgzf_workers > resolution.compute_workers
            );
        }
    }

    #[test]
    fn direct_thread_roles_keep_small_fixed_compute_budget() {
        let resolution = resolve_direct_thread_roles(16);
        assert_eq!(resolution.compute_workers, 1);
        assert_eq!(resolution.htslib_pool_enabled, true);
    }

    #[test]
    fn direct_thread_roles_allocate_proportional_reader_and_writer_workers() {
        let resolution = resolve_direct_thread_roles(96);
        assert!(resolution.shared_pool_intended_per_reader_bgzf_workers >= 1);
        assert!(resolution.shared_pool_intended_writer_bgzf_workers >= 1);
        assert!(
            resolution.shared_pool_intended_writer_bgzf_workers
                > resolution.shared_pool_intended_per_reader_bgzf_workers
        );
        assert!(resolution.compute_workers >= 1);
        assert_eq!(
            resolution.compute_workers + resolution.total_bgzf_workers,
            resolution.resolved_total_threads
        );
    }

    #[test]
    fn direct_thread_roles_scale_per_reader_workers_with_threads() {
        let low = resolve_direct_thread_roles(64);
        let high = resolve_direct_thread_roles(128);
        if low.resolved_total_threads == 64 && high.resolved_total_threads == 128 {
            assert!(
                high.shared_pool_intended_per_reader_bgzf_workers
                    > low.shared_pool_intended_per_reader_bgzf_workers
            );
            assert!(
                high.shared_pool_intended_writer_bgzf_workers
                    > low.shared_pool_intended_writer_bgzf_workers
            );
            assert!(high.compute_workers > low.compute_workers);
        }
    }

    #[test]
    fn compression_level_mapping_is_stable() {
        assert!(matches!(
            compression_level_from_u8(0),
            CompressionLevel::Uncompressed
        ));
        assert!(matches!(
            compression_level_from_u8(1),
            CompressionLevel::Fastest
        ));
        assert!(matches!(
            compression_level_from_u8(9),
            CompressionLevel::Maximum
        ));
        assert!(matches!(
            compression_level_from_u8(6),
            CompressionLevel::Level(6)
        ));
    }
}
