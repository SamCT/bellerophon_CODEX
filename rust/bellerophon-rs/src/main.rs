use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rust_htslib::bam;
use rust_htslib::bam::record::{Cigar, Record};
use rust_htslib::bam::{CompressionLevel, Read, Writer};
use rust_htslib::tpool::ThreadPool;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};
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
    #[value(name = "parallel-chunked")]
    ParallelChunked,
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
        default_value = "parallel-chunked",
        help = "Reader mode. 'parallel-chunked' runs one owned reader thread per input and synchronizes by QNAME in bounded memory."
    )]
    direct_reader_mode: DirectReaderMode,
    /// Number of query-name groups per reader chunk in parallel chunked mode.
    #[arg(long = "direct-reader-chunk-groups", default_value_t = 512)]
    direct_reader_chunk_groups: usize,
    /// Maximum number of unmatched groups allowed in bounded QNAME synchronization.
    #[arg(long = "direct-sync-lookahead-groups", default_value_t = 8192)]
    direct_sync_lookahead_groups: usize,
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
    batch_id: u64,
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
    batch_id: u64,
    records: Vec<(Record, Record)>,
    stats: DirectBatchStats,
    filter_seconds: f64,
}

#[derive(Default)]
struct DirectWorkerStats {
    pair_stats: PairFilterStats,
    writer_loop_seconds: f64,
    writer_recv_wait_seconds: f64,
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
    ordered_writer_pending_batches_max: usize,
    ordered_writer_wait_for_next_batch_seconds: f64,
    ordered_writer_missing_batch_wait_events: u64,
    ordered_writer_max_gap: u64,
    ordered_writer_pending_map_max_size: usize,
    ordered_writer_next_expected_batch_id: u64,
    ordered_writer_largest_received_batch_id: u64,
    max_completed_batch_gap_at_writer: u64,
    ordered_writer_waiting_for_batch_topn: String,
    writer_periodic_flush_count: u64,
    writer_periodic_flush_seconds: f64,
    writer_pre_close_flush_seconds: f64,
    output_finalize_non_tail_seconds: f64,
    total_output_drain_seconds: f64,
    writer_probe_reason: String,
    writer_probe_started_with_output_debt_bytes: u64,
    writer_probe_started_with_output_debt_seconds: f64,
    writer_probe_started_with_pending_batches: u64,
    writer_probe_started_with_writer_progress_age_seconds: f64,
    writer_probe_elapsed_seconds: f64,
    writer_probe_changed_output_bytes: u64,
    writer_probe_executed_count: u64,
    writer_probe_skipped_count: u64,
    writer_probe_skip_reason: String,
    writer_probe_last_skip_reason: String,
    writer_probe_skipped_output_debt_seconds: f64,
    writer_probe_skipped_pending_batches: u64,
    writer_probe_skipped_writer_progress_age_seconds: f64,
    writer_probe_skipped_estimated_bytes: u64,
    output_bytes_before_close: u64,
    output_bytes_after_close: u64,
    records_written_before_close: u64,
    pending_batches_before_close: usize,
}

#[derive(Clone, Copy, Debug)]
struct DirectWriterRuntimeConfig {
    flow_target_backlog_seconds: f64,
    flow_min_inflight_bytes: u64,
    flow_max_inflight_bytes: u64,
    flow_max_queue_backlog_batches: u64,
    flow_stale_progress_micros: u64,
    flow_wait_poll_micros: u64,
    flow_soft_queue_backlog_batches: u64,
    drain_min_interval_micros: u64,
    drain_min_base_bytes: u64,
    drain_bytes_per_probe_second: u64,
    drain_expensive_threshold_micros: u64,
    drain_backoff_shift_max: u8,
}

#[derive(Clone, Debug)]
struct OutputDrainController {
    last_probe_instant: Instant,
    last_probe_estimated_uncompressed_bytes: u64,
    next_probe_estimated_uncompressed_bytes: u64,
    expensive_backoff_shift: u8,
    drain_duration_ema_seconds: f64,
}

impl OutputDrainController {
    fn new(initial_probe_bytes: u64) -> Self {
        Self {
            last_probe_instant: Instant::now(),
            last_probe_estimated_uncompressed_bytes: 0,
            next_probe_estimated_uncompressed_bytes: initial_probe_bytes,
            expensive_backoff_shift: 0,
            drain_duration_ema_seconds: 0.0,
        }
    }

    fn should_probe(
        &self,
        estimated_uncompressed_bytes: u64,
        writer_bps: u64,
        pending_batches: usize,
        output_debt_bytes: u64,
        output_debt_seconds: f64,
        writer_progress_age_seconds: f64,
        runtime_config: &DirectWriterRuntimeConfig,
    ) -> Option<&'static str> {
        const PROBE_SUPPRESS_RECENT_PROGRESS_SECONDS: f64 = 0.750;
        const PROBE_LOW_DEBT_SECONDS: f64 = 0.250;
        if estimated_uncompressed_bytes < self.next_probe_estimated_uncompressed_bytes {
            return None;
        }
        let elapsed_micros = self
            .last_probe_instant
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        if elapsed_micros < runtime_config.drain_min_interval_micros {
            return None;
        }
        let backlog_evidence_batches = (pending_batches as u64).saturating_mul(8);
        if backlog_evidence_batches > runtime_config.flow_max_queue_backlog_batches {
            return Some("pending_batches_pressure");
        }
        let probe_period_seconds = (self.drain_duration_ema_seconds * 8.0).max(20.0).min(240.0);
        let throughput_scaled_bytes = writer_bps.saturating_mul(probe_period_seconds as u64);
        let bytes_since_last_probe = estimated_uncompressed_bytes
            .saturating_sub(self.last_probe_estimated_uncompressed_bytes);
        let dynamic_probe_floor = runtime_config
            .drain_min_base_bytes
            .max(throughput_scaled_bytes)
            .max(
                runtime_config
                    .drain_min_base_bytes
                    .saturating_mul(backlog_evidence_batches + 1),
            );
        if output_debt_bytes <= runtime_config.drain_min_base_bytes.saturating_mul(2)
            && output_debt_seconds <= PROBE_LOW_DEBT_SECONDS
            && writer_progress_age_seconds <= PROBE_SUPPRESS_RECENT_PROGRESS_SECONDS
        {
            return None;
        }
        if writer_progress_age_seconds >= runtime_config.flow_target_backlog_seconds.max(0.5)
            && output_debt_seconds >= PROBE_LOW_DEBT_SECONDS
        {
            return Some("stale_writer_progress");
        }
        if bytes_since_last_probe >= dynamic_probe_floor {
            return Some("byte_progress_checkpoint");
        }
        None
    }

    fn on_probe(
        &mut self,
        estimated_uncompressed_bytes: u64,
        probe_micros: u64,
        writer_bps: u64,
        runtime_config: &DirectWriterRuntimeConfig,
    ) {
        let probe_seconds = probe_micros as f64 / 1e6f64;
        if self.drain_duration_ema_seconds == 0.0 {
            self.drain_duration_ema_seconds = probe_seconds;
        } else {
            self.drain_duration_ema_seconds =
                (self.drain_duration_ema_seconds * 0.8) + (probe_seconds * 0.2);
        }
        if probe_micros >= runtime_config.drain_expensive_threshold_micros {
            self.expensive_backoff_shift = self
                .expensive_backoff_shift
                .saturating_add(1)
                .min(runtime_config.drain_backoff_shift_max);
        } else if self.expensive_backoff_shift > 0 {
            self.expensive_backoff_shift -= 1;
        }
        let ema_scaled_seconds = (self.drain_duration_ema_seconds * 12.0)
            .max(20.0)
            .min(480.0) as u64;
        let throughput_scaled_bytes = writer_bps.saturating_mul(ema_scaled_seconds);
        let base_next_probe = runtime_config
            .drain_min_base_bytes
            .max(throughput_scaled_bytes)
            .max(
                runtime_config
                    .drain_bytes_per_probe_second
                    .saturating_mul(ema_scaled_seconds),
            );
        let shifted_probe = base_next_probe
            .checked_shl(self.expensive_backoff_shift as u32)
            .unwrap_or(u64::MAX);
        self.next_probe_estimated_uncompressed_bytes = estimated_uncompressed_bytes
            .saturating_add(shifted_probe.max(runtime_config.drain_min_base_bytes));
        self.last_probe_estimated_uncompressed_bytes = estimated_uncompressed_bytes;
        self.last_probe_instant = Instant::now();
    }

    fn on_probe_skipped(&mut self) {
        self.last_probe_instant = Instant::now();
    }
}

fn should_skip_pending_batches_probe(
    output_debt_seconds: f64,
    pending_batches: usize,
    writer_progress_age_seconds: f64,
    worker_stats: &DirectWorkerStats,
) -> Option<&'static str> {
    const MAX_NEGLIGIBLE_OUTPUT_DEBT_SECONDS: f64 = 0.020;
    const MAX_SMALL_PENDING_BATCHES: usize = 8;
    const MAX_RECENT_WRITER_PROGRESS_SECONDS: f64 = 0.050;
    if output_debt_seconds > MAX_NEGLIGIBLE_OUTPUT_DEBT_SECONDS
        || pending_batches > MAX_SMALL_PENDING_BATCHES
        || writer_progress_age_seconds > MAX_RECENT_WRITER_PROGRESS_SECONDS
    {
        return None;
    }
    if worker_stats.writer_probe_executed_count > 0
        && worker_stats.writer_probe_changed_output_bytes == 0
    {
        return Some("previous_probe_noop");
    }
    Some("below_min_probe_value")
}

#[derive(Default, Clone, Debug)]
struct WriterDrainDiagnostics {
    join_wait_seconds: f64,
    queue_backlog_seconds: f64,
    byte_backlog_seconds: f64,
    ordered_backlog_seconds: f64,
    bgzf_or_writer_work_seconds: f64,
    max_queue_backlog_batches: u64,
    max_byte_backlog: u64,
    max_ordered_backlog_batches: u64,
}

const UNSET_ELAPSED_MICROS: u64 = u64::MAX;

#[derive(Debug)]
struct PipelineLifecycleMarkers {
    readers_started_micros: AtomicU64,
    writer_thread_started_micros: AtomicU64,
    first_input_batch_submitted_micros: AtomicU64,
    first_output_batch_submitted_micros: AtomicU64,
    first_writer_batch_received_micros: AtomicU64,
    first_writer_batch_written_micros: AtomicU64,
    last_input_batch_submitted_micros: AtomicU64,
    producers_done_micros: AtomicU64,
    compute_done_micros: AtomicU64,
    output_senders_dropped_micros: AtomicU64,
    writer_last_batch_received_micros: AtomicU64,
    writer_last_batch_written_micros: AtomicU64,
    writer_finalize_start_micros: AtomicU64,
    writer_finalize_done_micros: AtomicU64,
    writer_thread_exit_micros: AtomicU64,
    writer_join_start_micros: AtomicU64,
    writer_join_done_micros: AtomicU64,
    pipeline_end_micros: AtomicU64,
}

impl Default for PipelineLifecycleMarkers {
    fn default() -> Self {
        Self {
            readers_started_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_thread_started_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            first_input_batch_submitted_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            first_output_batch_submitted_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            first_writer_batch_received_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            first_writer_batch_written_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            last_input_batch_submitted_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            producers_done_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            compute_done_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            output_senders_dropped_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_last_batch_received_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_last_batch_written_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_finalize_start_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_finalize_done_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_thread_exit_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_join_start_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            writer_join_done_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
            pipeline_end_micros: AtomicU64::new(UNSET_ELAPSED_MICROS),
        }
    }
}

fn mark_elapsed_once(target: &AtomicU64, pipeline_start: Instant) {
    let elapsed = pipeline_start
        .elapsed()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64;
    let _ = target.compare_exchange(
        UNSET_ELAPSED_MICROS,
        elapsed,
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
}

fn elapsed_micros_to_seconds(value: u64) -> f64 {
    if value == UNSET_ELAPSED_MICROS {
        -1.0
    } else {
        value as f64 / 1_000_000.0
    }
}

#[derive(Default, Clone, Debug)]
struct OutputFlowStats {
    input_flow_control_wait_seconds: f64,
    compute_start_flow_control_wait_seconds: f64,
    output_submit_flow_control_wait_seconds: f64,
    flow_control_wait_events: u64,
    adaptive_compute_active_min: usize,
    adaptive_compute_active_max: usize,
    adaptive_compute_active_mean: f64,
}

#[derive(Copy, Clone, Debug)]
enum OutputFlowWaitKind {
    InputEnqueue,
    ComputeStart,
    OutputSubmit,
}

#[derive(Default, Clone, Debug)]
struct OutputFlowWaitDiagnostics {
    input_wait_seconds_total: f64,
    input_wait_events: u64,
    input_wait_max_seconds: f64,
    compute_start_wait_seconds_total: f64,
    compute_start_wait_events: u64,
    compute_start_wait_max_seconds: f64,
    output_submit_wait_seconds_total: f64,
    output_submit_wait_events: u64,
    output_submit_wait_max_seconds: f64,
}

#[derive(Default, Clone, Debug)]
struct OutputFlowSnapshot {
    submitted_batches: u64,
    received_batches: u64,
    written_batches: u64,
    submitted_output_bytes_estimate: u64,
    written_output_bytes_estimate: u64,
    output_debt_batches: u64,
    output_debt_bytes: u64,
    output_debt_seconds: f64,
    output_debt_max_bytes: u64,
    output_debt_max_batches: u64,
    output_debt_max_seconds: f64,
    output_debt_mean_seconds: f64,
    next_expected_batch_id: u64,
    largest_completed_batch_id: u64,
    completed_ahead_gap: u64,
    completed_ahead_gap_max: u64,
    ordered_pending_batches: u64,
    ordered_pending_bytes_estimate: u64,
    ordered_pending_max_batches: u64,
    ordered_pending_max_bytes: u64,
    compute_to_writer_queue_depth: u64,
    compute_to_writer_queue_max_depth: u64,
    oldest_unwritten_batch_age_seconds: f64,
    writer_write_bps_ema: f64,
    writer_last_progress_age_seconds: f64,
    producers_done: bool,
    output_batches_submitted_at_producer_done: u64,
    output_batches_received_at_producer_done: u64,
    output_batches_written_at_producer_done: u64,
    output_bytes_submitted_at_producer_done: u64,
    output_bytes_written_at_producer_done: u64,
    output_debt_bytes_at_producer_done: u64,
    output_debt_batches_at_producer_done: u64,
    ordered_pending_batches_at_producer_done: u64,
    ordered_pending_bytes_at_producer_done: u64,
    next_expected_batch_id_at_producer_done: u64,
    largest_completed_batch_id_at_producer_done: u64,
    completed_ahead_gap_at_producer_done: u64,
    input_wait_seconds_total: f64,
    input_wait_events: u64,
    input_wait_max_seconds: f64,
    compute_start_wait_seconds_total: f64,
    compute_start_wait_events: u64,
    compute_start_wait_max_seconds: f64,
    output_submit_wait_seconds_total: f64,
    output_submit_wait_events: u64,
    output_submit_wait_max_seconds: f64,
}

#[derive(Debug)]
struct OutputFlowInner {
    submitted_batches: u64,
    received_batches: u64,
    written_batches: u64,
    submitted_output_bytes_estimate: u64,
    written_output_bytes_estimate: u64,
    queued_batches: HashMap<u64, (u64, Instant)>,
    next_expected_batch_id: u64,
    largest_completed_batch_id: u64,
    completed_ahead_gap_max: u64,
    ordered_pending_bytes_estimate: u64,
    ordered_pending_max_batches: u64,
    ordered_pending_max_bytes: u64,
    compute_to_writer_queue_max_depth: u64,
    writer_write_bps_ema: f64,
    writer_last_progress_time: Instant,
    debt_seconds_sum: f64,
    debt_seconds_samples: u64,
    output_debt_max_bytes: u64,
    output_debt_max_batches: u64,
    output_debt_max_seconds: f64,
    producers_done: bool,
    compute_active: usize,
    compute_active_sum: u64,
    compute_active_samples: u64,
    compute_active_min: usize,
    compute_active_max: usize,
    output_batches_submitted_at_producer_done: Option<u64>,
    output_batches_received_at_producer_done: Option<u64>,
    output_batches_written_at_producer_done: Option<u64>,
    output_bytes_submitted_at_producer_done: Option<u64>,
    output_bytes_written_at_producer_done: Option<u64>,
    output_debt_bytes_at_producer_done: Option<u64>,
    output_debt_batches_at_producer_done: Option<u64>,
    ordered_pending_batches_at_producer_done: Option<u64>,
    ordered_pending_bytes_at_producer_done: Option<u64>,
    next_expected_batch_id_at_producer_done: Option<u64>,
    largest_completed_batch_id_at_producer_done: Option<u64>,
    completed_ahead_gap_at_producer_done: Option<u64>,
    wait_diagnostics: OutputFlowWaitDiagnostics,
}

#[derive(Debug)]
struct OutputFlowController {
    inner: Mutex<OutputFlowInner>,
    cv: Condvar,
    started: Instant,
    max_compute_workers: usize,
}

#[derive(Default, Clone, Debug)]
struct DirectComputeStats {
    compute_workers: usize,
    compute_batches_processed: u64,
    compute_records_selected: u64,
    compute_filter_seconds_total: f64,
    compute_filter_wall_seconds: f64,
    compute_input_wait_thread_seconds_total: f64,
    compute_output_send_wait_thread_seconds_total: f64,
    compute_input_wait_wall_seconds: f64,
    compute_output_send_wait_wall_seconds: f64,
    compute_output_queue_full_events: u64,
    compute_flow_control_wait_thread_seconds_total: f64,
    compute_flow_control_wait_wall_seconds: f64,
    compute_flow_control_wait_events: u64,
    compute_start_flow_control_wait_seconds_total: f64,
    output_submit_flow_control_wait_seconds_total: f64,
    compute_to_writer_queue_max_depth: usize,
    per_worker_batches_processed: Vec<u64>,
    per_worker_max_batch_filter_seconds: Vec<f64>,
    per_worker_mean_batch_filter_seconds: Vec<f64>,
    slowest_batch_id: u64,
    slowest_batch_filter_seconds: f64,
    compute_batch_duration_p50_seconds: f64,
    compute_batch_duration_p95_seconds: f64,
    compute_batch_duration_p99_seconds: f64,
}

#[derive(Default, Clone, Debug)]
struct DirectComputeWorkerStats {
    worker_id: usize,
    compute_batches_processed: u64,
    compute_records_selected: u64,
    compute_filter_seconds_total: f64,
    compute_input_wait_thread_seconds_total: f64,
    compute_output_send_wait_thread_seconds_total: f64,
    compute_input_wait_wall_seconds: f64,
    compute_output_send_wait_wall_seconds: f64,
    compute_output_queue_full_events: u64,
    compute_flow_control_wait_thread_seconds_total: f64,
    compute_flow_control_wait_wall_seconds: f64,
    compute_flow_control_wait_events: u64,
    compute_start_flow_control_wait_seconds_total: f64,
    output_submit_flow_control_wait_seconds_total: f64,
    compute_filter_wall_seconds: f64,
    batch_filter_seconds_total: f64,
    max_batch_filter_seconds: f64,
    slowest_batch_id: u64,
    slowest_batch_filter_seconds: f64,
    batch_filter_samples_seconds: Vec<f64>,
}

#[derive(Default, Clone, Debug)]
struct ReaderDecodeStats {
    wall_seconds: f64,
    decode_seconds: f64,
    decode_only_seconds: f64,
    group_build_seconds: f64,
    htslib_read_seconds: f64,
    records_decoded: u64,
    send_wait_seconds: f64,
    chunks_sent: u64,
    total_chunk_groups: u64,
    max_chunk_groups: usize,
    queue_full_events: u64,
    queue_occupancy_sum: u64,
    queue_occupancy_samples: u64,
    chunk_interval_seconds_total: f64,
    chunk_interval_samples: u64,
    chunk_interval_min_seconds: f64,
    chunk_interval_max_seconds: f64,
    chunk_interval_sample_cursor: usize,
    chunk_interval_sample_window: Vec<f64>,
}

#[derive(Debug)]
struct ReaderChunk {
    groups: Vec<DirectRecordGroup>,
}

#[derive(Default, Clone, Debug)]
struct QueueDepthStats {
    max_depth: usize,
    full_events: u64,
    occupancy_sum: u64,
    occupancy_samples: u64,
}

#[derive(Clone, Debug)]
struct SyncDiagnostics {
    wait_for_forward_chunk_seconds: f64,
    wait_for_reverse_chunk_seconds: f64,
    match_loop_seconds: f64,
    output_enqueue_seconds: f64,
    forward_recv_calls: u64,
    reverse_recv_calls: u64,
    forward_try_recv_hits: u64,
    reverse_try_recv_hits: u64,
    forward_blocking_recv_when_reverse_work_available: u64,
    reverse_blocking_recv_when_forward_work_available: u64,
    slow_side_detected: &'static str,
    slow_side_switch_count: u64,
    max_consecutive_waits_forward: u64,
    max_consecutive_waits_reverse: u64,
}

impl Default for SyncDiagnostics {
    fn default() -> Self {
        Self {
            wait_for_forward_chunk_seconds: 0.0,
            wait_for_reverse_chunk_seconds: 0.0,
            match_loop_seconds: 0.0,
            output_enqueue_seconds: 0.0,
            forward_recv_calls: 0,
            reverse_recv_calls: 0,
            forward_try_recv_hits: 0,
            reverse_try_recv_hits: 0,
            forward_blocking_recv_when_reverse_work_available: 0,
            reverse_blocking_recv_when_forward_work_available: 0,
            slow_side_detected: "unknown",
            slow_side_switch_count: 0,
            max_consecutive_waits_forward: 0,
            max_consecutive_waits_reverse: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct DirectQueuePolicy {
    output_queue_capacity: usize,
    reader_queue_capacity: usize,
    reader_chunk_groups: usize,
    batch_size: usize,
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
    if cli.direct_batch_size == 0 {
        bail!("--direct-batch-size must be at least 1");
    }
    if cli.direct_reader_chunk_groups == 0 {
        bail!("--direct-reader-chunk-groups must be at least 1");
    }
    if cli.direct_sync_lookahead_groups == 0 {
        bail!("--direct-sync-lookahead-groups must be at least 1");
    }
    let queue_policy = resolve_direct_queue_policy(thread_resolution.requested_threads, cli);

    let forward_header_reader = bam::Reader::from_path(&cli.forward)
        .with_context(|| format!("failed to open forward input {}", cli.forward.display()))?;
    let reverse_header_reader = bam::Reader::from_path(&cli.reverse)
        .with_context(|| format!("failed to open reverse input {}", cli.reverse.display()))?;
    let split_workers = resolve_split_bgzf_workers(thread_resolution.total_bgzf_workers);
    let mut shared_bgzf_pool: Option<ThreadPool> = None;
    let mut output_bgzf_pool: Option<ThreadPool> = None;
    if thread_resolution.htslib_pool_enabled
        && cli.direct_htslib_pool_mode == DirectHtslibPoolMode::Shared
        && thread_resolution.total_bgzf_workers > 0
    {
        shared_bgzf_pool = Some(
            ThreadPool::new(thread_resolution.total_bgzf_workers as u32)
                .context("failed to create shared BGZF thread pool")?,
        );
    }
    verify_matching_references(
        forward_header_reader.header(),
        reverse_header_reader.header(),
    )?;

    let header = bam::Header::from_template(forward_header_reader.header());
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
    let (input_batch_sender, input_batch_receiver) =
        sync_channel::<DirectInputBatch>(queue_policy.output_queue_capacity);
    let (output_batch_sender, output_batch_receiver) =
        sync_channel::<DirectOutputBatch>(queue_policy.output_queue_capacity);
    let input_queue_depth = Arc::new(AtomicUsize::new(0));
    let input_queue_max_depth = Arc::new(AtomicUsize::new(0));
    let output_queue_submitted = Arc::new(AtomicU64::new(0));
    let output_queue_received = Arc::new(AtomicU64::new(0));
    let output_queue_max_depth = Arc::new(AtomicUsize::new(0));
    let output_bytes_submitted = Arc::new(AtomicU64::new(0));
    let output_bytes_written = Arc::new(AtomicU64::new(0));
    let writer_bytes_per_second_estimate = Arc::new(AtomicU64::new(0));
    let writer_last_progress_micros = Arc::new(AtomicU64::new(0));
    let pipeline_start = Instant::now();
    let lifecycle = Arc::new(PipelineLifecycleMarkers::default());
    let writer_runtime_config = resolve_direct_writer_runtime_config(
        thread_resolution.requested_threads,
        thread_resolution.shared_pool_intended_writer_bgzf_workers,
    );
    let ordered_writer_next_expected_batch_id = Arc::new(AtomicU64::new(0));
    let output_flow_controller =
        Arc::new(OutputFlowController::new(thread_resolution.compute_workers));
    let input_batch_receiver = Arc::new(Mutex::new(input_batch_receiver));
    let mut compute_handles = Vec::with_capacity(thread_resolution.compute_workers);
    for worker_id in 0..thread_resolution.compute_workers {
        let worker_receiver = Arc::clone(&input_batch_receiver);
        let worker_sender = output_batch_sender.clone();
        let worker_quality = cli.quality;
        let worker_input_queue_depth = Arc::clone(&input_queue_depth);
        let worker_output_queue_submitted = Arc::clone(&output_queue_submitted);
        let worker_output_queue_received = Arc::clone(&output_queue_received);
        let worker_output_queue_max_depth = Arc::clone(&output_queue_max_depth);
        let worker_output_bytes_submitted = Arc::clone(&output_bytes_submitted);
        let worker_output_bytes_written = Arc::clone(&output_bytes_written);
        let worker_writer_bytes_per_second_estimate = Arc::clone(&writer_bytes_per_second_estimate);
        let worker_writer_last_progress_micros = Arc::clone(&writer_last_progress_micros);
        let worker_writer_runtime_config = writer_runtime_config;
        let worker_writer_next_expected_batch_id =
            Arc::clone(&ordered_writer_next_expected_batch_id);
        let worker_output_flow_controller = Arc::clone(&output_flow_controller);
        let worker_output_queue_capacity = queue_policy.output_queue_capacity;
        let worker_pipeline_start = pipeline_start;
        let worker_lifecycle = Arc::clone(&lifecycle);
        compute_handles.push(thread::spawn(move || {
            direct_compute_thread(
                worker_id,
                worker_receiver,
                worker_sender,
                worker_quality,
                worker_input_queue_depth,
                worker_output_queue_submitted,
                worker_output_queue_received,
                worker_output_queue_max_depth,
                worker_output_bytes_submitted,
                worker_output_bytes_written,
                worker_writer_bytes_per_second_estimate,
                worker_writer_last_progress_micros,
                worker_writer_next_expected_batch_id,
                worker_output_flow_controller,
                worker_output_queue_capacity,
                worker_writer_runtime_config,
                worker_pipeline_start,
                worker_lifecycle,
            )
        }));
    }
    drop(output_batch_sender);
    let writer_output_queue_received = Arc::clone(&output_queue_received);
    let writer_output_bytes_submitted = Arc::clone(&output_bytes_submitted);
    let writer_output_bytes_written = Arc::clone(&output_bytes_written);
    let writer_bytes_per_second_estimate = Arc::clone(&writer_bytes_per_second_estimate);
    let writer_last_progress_micros = Arc::clone(&writer_last_progress_micros);
    let writer_next_expected_batch_id = Arc::clone(&ordered_writer_next_expected_batch_id);
    let writer_next_expected_batch_id_for_thread = Arc::clone(&writer_next_expected_batch_id);
    let writer_output_flow_controller = Arc::clone(&output_flow_controller);
    let writer_lifecycle = Arc::clone(&lifecycle);
    let output_path_for_writer = cli.output.clone();
    let writer_handle = thread::spawn(move || {
        direct_writer_thread(
            output_batch_receiver,
            output,
            output_path_for_writer,
            writer_output_queue_received,
            writer_output_bytes_submitted,
            writer_output_bytes_written,
            writer_bytes_per_second_estimate,
            writer_last_progress_micros,
            writer_next_expected_batch_id_for_thread,
            writer_output_flow_controller,
            writer_runtime_config,
            pipeline_start,
            writer_lifecycle,
        )
    });

    stage_log(
        cli,
        format!(
            "STAGE direct_open_setup seconds={:.6} total_bgzf_workers={} compute_workers={} direct_batch_size={} compression_level={} htslib_pool_enabled={} htslib_pool_mode={} split_forward_bgzf_workers={} split_reverse_bgzf_workers={} split_output_bgzf_workers={} writer_drain_min_interval_micros={} writer_drain_min_base_bytes={} writer_drain_bytes_per_probe_second={} flow_target_backlog_seconds={:.3} flow_min_inflight_bytes={} flow_max_inflight_bytes={} flow_max_queue_backlog_batches={} flow_soft_queue_backlog_batches={} reader_mode={} direct_output_queue_capacity={} direct_reader_queue_capacity={} direct_reader_chunk_groups={} direct_queue_policy=auto_bounded",
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
            writer_runtime_config.drain_min_interval_micros,
            writer_runtime_config.drain_min_base_bytes,
            writer_runtime_config.drain_bytes_per_probe_second,
            writer_runtime_config.flow_target_backlog_seconds,
            writer_runtime_config.flow_min_inflight_bytes,
            writer_runtime_config.flow_max_inflight_bytes,
            writer_runtime_config.flow_max_queue_backlog_batches,
            writer_runtime_config.flow_soft_queue_backlog_batches,
            direct_reader_mode_name(&cli.direct_reader_mode),
            queue_policy.output_queue_capacity,
            queue_policy.reader_queue_capacity,
            queue_policy.reader_chunk_groups
        ),
    );

    let read_match_start = Instant::now();
    mark_elapsed_once(&lifecycle.readers_started_micros, pipeline_start);
    let mut forward_reader_stats = ReaderDecodeStats::default();
    let mut reverse_reader_stats = ReaderDecodeStats::default();
    let mut pair_match_assembly_seconds = 0.0f64;
    let mut batch_enqueue_wait_seconds = 0.0f64;
    let mut input_flow_control_wait_seconds = 0.0f64;
    let mut groups_seen: u64 = 0;
    let mut active_batch: Vec<(DirectRecordGroup, DirectRecordGroup)> =
        Vec::with_capacity(cli.direct_batch_size);
    let mut reader_threads_spawned = 0usize;
    let mut reader_threads_active_high_watermark = 1usize;
    let mut output_queue_full_events = 0u64;
    let mut reader_chunk_queue_stats = QueueDepthStats::default();
    let mut sync_diagnostics = SyncDiagnostics::default();
    let mut next_batch_id = 0u64;

    match cli.direct_reader_mode {
        DirectReaderMode::Serial => {
            let mut forward_reader = bam::Reader::from_path(&cli.forward).with_context(|| {
                format!("failed to open forward input {}", cli.forward.display())
            })?;
            let mut reverse_reader = bam::Reader::from_path(&cli.reverse).with_context(|| {
                format!("failed to open reverse input {}", cli.reverse.display())
            })?;
            if split_workers.forward > 0 {
                forward_reader
                    .set_threads(split_workers.forward)
                    .context("failed to set forward reader threads")?;
            }
            if split_workers.reverse > 0 {
                reverse_reader
                    .set_threads(split_workers.reverse)
                    .context("failed to set reverse reader threads")?;
            }
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
                    &input_batch_sender,
                    &input_queue_depth,
                    &input_queue_max_depth,
                    &mut batch_enqueue_wait_seconds,
                    cli.direct_batch_size,
                    &mut groups_seen,
                    &mut pair_match_assembly_seconds,
                    queue_policy.output_queue_capacity,
                    &mut output_queue_full_events,
                    &mut next_batch_id,
                    &mut input_flow_control_wait_seconds,
                    &output_flow_controller,
                    pipeline_start,
                    &lifecycle,
                )?;
                if done {
                    break;
                }
            }
        }
        DirectReaderMode::ParallelChunked => {
            reader_threads_spawned = 2;
            reader_threads_active_high_watermark = 2;
            let (forward_tx, forward_rx) =
                sync_channel::<Result<Option<ReaderChunk>>>(queue_policy.reader_queue_capacity);
            let (reverse_tx, reverse_rx) =
                sync_channel::<Result<Option<ReaderChunk>>>(queue_policy.reader_queue_capacity);
            let forward_chunk_depth = Arc::new(AtomicUsize::new(0));
            let reverse_chunk_depth = Arc::new(AtomicUsize::new(0));
            let forward_chunk_max_depth = Arc::new(AtomicUsize::new(0));
            let reverse_chunk_max_depth = Arc::new(AtomicUsize::new(0));
            let forward_path = cli.forward.clone();
            let reverse_path = cli.reverse.clone();
            let chunk_groups = queue_policy.reader_chunk_groups;
            let forward_threads = split_workers.forward.max(1);
            let reverse_threads = split_workers.reverse.max(1);
            let forward_chunk_depth_reader = Arc::clone(&forward_chunk_depth);
            let forward_chunk_max_depth_reader = Arc::clone(&forward_chunk_max_depth);
            let forward_handle = thread::spawn(move || {
                read_group_chunks_producer(
                    forward_path,
                    forward_tx,
                    chunk_groups,
                    forward_threads,
                    "forward",
                    queue_policy.reader_queue_capacity,
                    forward_chunk_depth_reader,
                    forward_chunk_max_depth_reader,
                )
            });
            let reverse_chunk_depth_reader = Arc::clone(&reverse_chunk_depth);
            let reverse_chunk_max_depth_reader = Arc::clone(&reverse_chunk_max_depth);
            let reverse_handle = thread::spawn(move || {
                read_group_chunks_producer(
                    reverse_path,
                    reverse_tx,
                    chunk_groups,
                    reverse_threads,
                    "reverse",
                    queue_policy.reader_queue_capacity,
                    reverse_chunk_depth_reader,
                    reverse_chunk_max_depth_reader,
                )
            });
            sync_parallel_reader_groups(
                &forward_rx,
                &reverse_rx,
                &forward_chunk_depth,
                &reverse_chunk_depth,
                &mut active_batch,
                &input_batch_sender,
                &input_queue_depth,
                &input_queue_max_depth,
                &mut batch_enqueue_wait_seconds,
                cli.direct_batch_size,
                &mut groups_seen,
                &mut pair_match_assembly_seconds,
                cli.direct_sync_lookahead_groups,
                queue_policy.reader_queue_capacity,
                queue_policy.output_queue_capacity,
                &mut output_queue_full_events,
                &mut sync_diagnostics,
                &mut next_batch_id,
                &mut input_flow_control_wait_seconds,
                &output_flow_controller,
                pipeline_start,
                &lifecycle,
            )?;
            forward_reader_stats = forward_handle
                .join()
                .map_err(|_| anyhow::anyhow!("forward reader thread panicked"))??;
            reverse_reader_stats = reverse_handle
                .join()
                .map_err(|_| anyhow::anyhow!("reverse reader thread panicked"))??;
            reader_chunk_queue_stats.max_depth = forward_chunk_max_depth
                .load(Ordering::Relaxed)
                .max(reverse_chunk_max_depth.load(Ordering::Relaxed));
            reader_chunk_queue_stats.full_events =
                forward_reader_stats.queue_full_events + reverse_reader_stats.queue_full_events;
            reader_chunk_queue_stats.occupancy_sum =
                forward_reader_stats.queue_occupancy_sum + reverse_reader_stats.queue_occupancy_sum;
            reader_chunk_queue_stats.occupancy_samples = forward_reader_stats
                .queue_occupancy_samples
                + reverse_reader_stats.queue_occupancy_samples;
        }
    }

    if cli.direct_reader_mode == DirectReaderMode::Serial {
        forward_reader_stats.wall_seconds = forward_reader_stats.decode_seconds;
        reverse_reader_stats.wall_seconds = reverse_reader_stats.decode_seconds;
    }

    let read_match_seconds = read_match_start.elapsed().as_secs_f64();
    let read_decode_seconds =
        forward_reader_stats.decode_seconds + reverse_reader_stats.decode_seconds;
    let forward_reader_decode_only_seconds = forward_reader_stats.decode_only_seconds;
    let reverse_reader_decode_only_seconds = reverse_reader_stats.decode_only_seconds;
    let qname_group_seconds =
        (read_match_seconds - read_decode_seconds - pair_match_assembly_seconds).max(0.0);
    let pending_batches_before_close = input_queue_depth.load(Ordering::Relaxed);
    let input_batches_submitted = next_batch_id;
    drop(input_batch_sender);
    output_flow_controller.on_producers_done();
    mark_elapsed_once(&lifecycle.producers_done_micros, pipeline_start);
    let producer_done_flow_snapshot = output_flow_controller.snapshot();
    stage_log(
        cli,
        format!(
            "STAGE output_flow_state_at_producer_done submitted_batches={} received_batches={} written_batches={} output_debt_bytes={} output_debt_batches={} ordered_pending_batches={} ordered_pending_bytes={} next_expected_batch_id={} largest_completed_batch_id={} input_wait_seconds_total={:.6} input_wait_events={} input_wait_max_seconds={:.6} compute_start_wait_seconds_total={:.6} compute_start_wait_events={} compute_start_wait_max_seconds={:.6} output_submit_wait_seconds_total={:.6} output_submit_wait_events={} output_submit_wait_max_seconds={:.6}",
            producer_done_flow_snapshot.submitted_batches,
            producer_done_flow_snapshot.received_batches,
            producer_done_flow_snapshot.written_batches,
            producer_done_flow_snapshot.output_debt_bytes,
            producer_done_flow_snapshot.output_debt_batches,
            producer_done_flow_snapshot.ordered_pending_batches,
            producer_done_flow_snapshot.ordered_pending_bytes_estimate,
            producer_done_flow_snapshot.next_expected_batch_id,
            producer_done_flow_snapshot.largest_completed_batch_id,
            producer_done_flow_snapshot.input_wait_seconds_total,
            producer_done_flow_snapshot.input_wait_events,
            producer_done_flow_snapshot.input_wait_max_seconds,
            producer_done_flow_snapshot.compute_start_wait_seconds_total,
            producer_done_flow_snapshot.compute_start_wait_events,
            producer_done_flow_snapshot.compute_start_wait_max_seconds,
            producer_done_flow_snapshot.output_submit_wait_seconds_total,
            producer_done_flow_snapshot.output_submit_wait_events,
            producer_done_flow_snapshot.output_submit_wait_max_seconds
        ),
    );
    let mut compute_stats = DirectComputeStats {
        compute_workers: thread_resolution.compute_workers,
        ..Default::default()
    };
    let mut per_worker_stats = Vec::with_capacity(thread_resolution.compute_workers);
    for handle in compute_handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow::anyhow!("direct compute thread panicked"))??;
        per_worker_stats.push(worker_stats.clone());
        compute_stats.compute_batches_processed += worker_stats.compute_batches_processed;
        compute_stats.compute_records_selected += worker_stats.compute_records_selected;
        compute_stats.compute_filter_seconds_total += worker_stats.compute_filter_seconds_total;
        compute_stats.compute_input_wait_thread_seconds_total +=
            worker_stats.compute_input_wait_thread_seconds_total;
        compute_stats.compute_output_send_wait_thread_seconds_total +=
            worker_stats.compute_output_send_wait_thread_seconds_total;
        compute_stats.compute_input_wait_wall_seconds = compute_stats
            .compute_input_wait_wall_seconds
            .max(worker_stats.compute_input_wait_wall_seconds);
        compute_stats.compute_output_send_wait_wall_seconds = compute_stats
            .compute_output_send_wait_wall_seconds
            .max(worker_stats.compute_output_send_wait_wall_seconds);
        compute_stats.compute_output_queue_full_events +=
            worker_stats.compute_output_queue_full_events;
        compute_stats.compute_flow_control_wait_thread_seconds_total +=
            worker_stats.compute_flow_control_wait_thread_seconds_total;
        compute_stats.compute_flow_control_wait_wall_seconds = compute_stats
            .compute_flow_control_wait_wall_seconds
            .max(worker_stats.compute_flow_control_wait_wall_seconds);
        compute_stats.compute_flow_control_wait_events +=
            worker_stats.compute_flow_control_wait_events;
        compute_stats.compute_start_flow_control_wait_seconds_total +=
            worker_stats.compute_start_flow_control_wait_seconds_total;
        compute_stats.output_submit_flow_control_wait_seconds_total +=
            worker_stats.output_submit_flow_control_wait_seconds_total;
        compute_stats.compute_filter_wall_seconds = compute_stats
            .compute_filter_wall_seconds
            .max(worker_stats.compute_filter_wall_seconds);
    }
    mark_elapsed_once(&lifecycle.compute_done_micros, pipeline_start);
    mark_elapsed_once(&lifecycle.output_senders_dropped_micros, pipeline_start);
    per_worker_stats.sort_by_key(|entry| entry.worker_id);
    compute_stats.per_worker_batches_processed = per_worker_stats
        .iter()
        .map(|entry| entry.compute_batches_processed)
        .collect();
    compute_stats.per_worker_max_batch_filter_seconds = per_worker_stats
        .iter()
        .map(|entry| entry.max_batch_filter_seconds)
        .collect();
    compute_stats.per_worker_mean_batch_filter_seconds = per_worker_stats
        .iter()
        .map(|entry| {
            if entry.compute_batches_processed > 0 {
                entry.batch_filter_seconds_total / entry.compute_batches_processed as f64
            } else {
                0.0
            }
        })
        .collect();
    let mut all_batch_filter_samples_seconds: Vec<f64> = per_worker_stats
        .iter()
        .flat_map(|entry| entry.batch_filter_samples_seconds.iter().copied())
        .collect();
    if let Some(slowest) = per_worker_stats.iter().max_by(|a, b| {
        a.slowest_batch_filter_seconds
            .partial_cmp(&b.slowest_batch_filter_seconds)
            .unwrap_or(CmpOrdering::Equal)
    }) {
        compute_stats.slowest_batch_id = slowest.slowest_batch_id;
        compute_stats.slowest_batch_filter_seconds = slowest.slowest_batch_filter_seconds;
    }
    compute_stats.compute_batch_duration_p50_seconds =
        percentile_seconds(&mut all_batch_filter_samples_seconds, 50.0);
    compute_stats.compute_batch_duration_p95_seconds =
        percentile_seconds(&mut all_batch_filter_samples_seconds, 95.0);
    compute_stats.compute_batch_duration_p99_seconds =
        percentile_seconds(&mut all_batch_filter_samples_seconds, 99.0);
    compute_stats.compute_to_writer_queue_max_depth =
        output_queue_max_depth.load(Ordering::Relaxed);
    let producer_done_seconds = pipeline_start.elapsed().as_secs_f64();
    let writer_wait_start = Instant::now();
    let mut writer_drain_diagnostics = WriterDrainDiagnostics::default();
    let mut last_sample_instant = Instant::now();
    while !writer_handle.is_finished() {
        let now = Instant::now();
        let delta = now.duration_since(last_sample_instant).as_secs_f64();
        let submitted = output_queue_submitted.load(Ordering::Relaxed);
        let received = output_queue_received.load(Ordering::Relaxed);
        let queue_backlog_batches = submitted.saturating_sub(received);
        let bytes_backlog = output_bytes_submitted
            .load(Ordering::Relaxed)
            .saturating_sub(output_bytes_written.load(Ordering::Relaxed));
        let next_expected_batch = writer_next_expected_batch_id.load(Ordering::Relaxed);
        let ordered_backlog_batches = received.saturating_sub(next_expected_batch);
        writer_drain_diagnostics.max_queue_backlog_batches = writer_drain_diagnostics
            .max_queue_backlog_batches
            .max(queue_backlog_batches);
        writer_drain_diagnostics.max_byte_backlog =
            writer_drain_diagnostics.max_byte_backlog.max(bytes_backlog);
        writer_drain_diagnostics.max_ordered_backlog_batches = writer_drain_diagnostics
            .max_ordered_backlog_batches
            .max(ordered_backlog_batches);
        if queue_backlog_batches > 0 {
            writer_drain_diagnostics.queue_backlog_seconds += delta;
        }
        if bytes_backlog > 0 {
            writer_drain_diagnostics.byte_backlog_seconds += delta;
        }
        if ordered_backlog_batches > 0 {
            writer_drain_diagnostics.ordered_backlog_seconds += delta;
        }
        if queue_backlog_batches == 0 && bytes_backlog > 0 {
            writer_drain_diagnostics.bgzf_or_writer_work_seconds += delta;
        }
        last_sample_instant = now;
        thread::sleep(Duration::from_micros(
            writer_runtime_config.flow_wait_poll_micros.max(250),
        ));
    }
    mark_elapsed_once(&lifecycle.writer_join_start_micros, pipeline_start);
    let writer_stats = writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("direct writer thread panicked"))??;
    mark_elapsed_once(&lifecycle.writer_join_done_micros, pipeline_start);
    let writer_drain_seconds = writer_wait_start.elapsed().as_secs_f64();
    let output_flow_snapshot = output_flow_controller.snapshot();
    stage_log(
        cli,
        format!(
            "STAGE output_flow_state_at_writer_join submitted_batches={} received_batches={} written_batches={} output_debt_bytes={} output_debt_batches={} ordered_pending_batches={} ordered_pending_bytes={} next_expected_batch_id={} largest_completed_batch_id={} input_wait_seconds_total={:.6} input_wait_events={} input_wait_max_seconds={:.6} compute_start_wait_seconds_total={:.6} compute_start_wait_events={} compute_start_wait_max_seconds={:.6} output_submit_wait_seconds_total={:.6} output_submit_wait_events={} output_submit_wait_max_seconds={:.6}",
            output_flow_snapshot.submitted_batches,
            output_flow_snapshot.received_batches,
            output_flow_snapshot.written_batches,
            output_flow_snapshot.output_debt_bytes,
            output_flow_snapshot.output_debt_batches,
            output_flow_snapshot.ordered_pending_batches,
            output_flow_snapshot.ordered_pending_bytes_estimate,
            output_flow_snapshot.next_expected_batch_id,
            output_flow_snapshot.largest_completed_batch_id,
            output_flow_snapshot.input_wait_seconds_total,
            output_flow_snapshot.input_wait_events,
            output_flow_snapshot.input_wait_max_seconds,
            output_flow_snapshot.compute_start_wait_seconds_total,
            output_flow_snapshot.compute_start_wait_events,
            output_flow_snapshot.compute_start_wait_max_seconds,
            output_flow_snapshot.output_submit_wait_seconds_total,
            output_flow_snapshot.output_submit_wait_events,
            output_flow_snapshot.output_submit_wait_max_seconds
        ),
    );
    writer_drain_diagnostics.join_wait_seconds = writer_drain_seconds;
    let shared_pool_drop_start = Instant::now();
    drop(shared_bgzf_pool);
    let shared_pool_drop_seconds = shared_pool_drop_start.elapsed().as_secs_f64();
    let output_pool_drop_start = Instant::now();
    drop(output_bgzf_pool);
    let output_pool_drop_seconds = output_pool_drop_start.elapsed().as_secs_f64();
    let stats = writer_stats.pair_stats;
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
    let reader_decode_wall_seconds = forward_reader_stats
        .wall_seconds
        .max(reverse_reader_stats.wall_seconds);
    let reader_decode_thread_seconds =
        forward_reader_stats.decode_seconds + reverse_reader_stats.decode_seconds;
    let forward_chunk_interval_mean_seconds = if forward_reader_stats.chunk_interval_samples > 0 {
        forward_reader_stats.chunk_interval_seconds_total
            / forward_reader_stats.chunk_interval_samples as f64
    } else {
        0.0
    };
    let reverse_chunk_interval_mean_seconds = if reverse_reader_stats.chunk_interval_samples > 0 {
        reverse_reader_stats.chunk_interval_seconds_total
            / reverse_reader_stats.chunk_interval_samples as f64
    } else {
        0.0
    };
    let forward_chunk_interval_p95_seconds =
        sample_percentile(&forward_reader_stats.chunk_interval_sample_window, 95.0);
    let reverse_chunk_interval_p95_seconds =
        sample_percentile(&reverse_reader_stats.chunk_interval_sample_window, 95.0);
    let max_rss_kb = read_max_rss_kb();

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
            0.0f64,
            write_call_seconds,
            output_drop_close_seconds
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_reader_diagnostics forward_reader_wall_seconds={:.6} reverse_reader_wall_seconds={:.6} reader_decode_wall_seconds={:.6} reader_decode_thread_seconds={:.6} forward_reader_decode_only_seconds={:.6} reverse_reader_decode_only_seconds={:.6} forward_reader_group_build_seconds={:.6} reverse_reader_group_build_seconds={:.6} forward_reader_htslib_read_seconds={:.6} reverse_reader_htslib_read_seconds={:.6} forward_records_decoded={} reverse_records_decoded={} forward_decode_records_per_second={:.3} reverse_decode_records_per_second={:.3} forward_chunk_interval_samples={} reverse_chunk_interval_samples={} forward_chunk_interval_mean_seconds={:.6} reverse_chunk_interval_mean_seconds={:.6} forward_chunk_interval_min_seconds={:.6} reverse_chunk_interval_min_seconds={:.6} forward_chunk_interval_p95_seconds={:.6} reverse_chunk_interval_p95_seconds={:.6} forward_chunk_interval_max_seconds={:.6} reverse_chunk_interval_max_seconds={:.6} reader_threads_spawned={} reader_threads_active_high_watermark={} max_rss_kb={}",
            forward_reader_stats.wall_seconds,
            reverse_reader_stats.wall_seconds,
            reader_decode_wall_seconds,
            reader_decode_thread_seconds,
            forward_reader_decode_only_seconds,
            reverse_reader_decode_only_seconds,
            forward_reader_stats.group_build_seconds,
            reverse_reader_stats.group_build_seconds,
            forward_reader_stats.htslib_read_seconds,
            reverse_reader_stats.htslib_read_seconds,
            forward_reader_stats.records_decoded,
            reverse_reader_stats.records_decoded,
            forward_decode_rps,
            reverse_decode_rps,
            forward_reader_stats.chunk_interval_samples,
            reverse_reader_stats.chunk_interval_samples,
            forward_chunk_interval_mean_seconds,
            reverse_chunk_interval_mean_seconds,
            forward_reader_stats.chunk_interval_min_seconds,
            reverse_reader_stats.chunk_interval_min_seconds,
            forward_chunk_interval_p95_seconds,
            reverse_chunk_interval_p95_seconds,
            forward_reader_stats.chunk_interval_max_seconds,
            reverse_reader_stats.chunk_interval_max_seconds,
            reader_threads_spawned,
            reader_threads_active_high_watermark,
            max_rss_kb
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
    stage_log(
        cli,
        format!(
            "STAGE writer_drain_diagnostics writer_drain_seconds={:.6} writer_join_wait_seconds={:.6} writer_drain_queue_backlog_seconds={:.6} writer_drain_byte_backlog_seconds={:.6} writer_drain_ordered_backlog_seconds={:.6} writer_drain_bgzf_or_writer_work_seconds={:.6} writer_drain_max_queue_backlog_batches={} writer_drain_max_ordered_backlog_batches={} writer_drain_max_byte_backlog={} writer_drain_interpretation=join_wait_after_compute_queue_or_ordering_or_bgzf_backlog",
            writer_drain_seconds,
            writer_drain_diagnostics.join_wait_seconds,
            writer_drain_diagnostics.queue_backlog_seconds,
            writer_drain_diagnostics.byte_backlog_seconds,
            writer_drain_diagnostics.ordered_backlog_seconds,
            writer_drain_diagnostics.bgzf_or_writer_work_seconds,
            writer_drain_diagnostics.max_queue_backlog_batches,
            writer_drain_diagnostics.max_ordered_backlog_batches,
            writer_drain_diagnostics.max_byte_backlog
        ),
    );
    let max_input_queue_depth = input_queue_max_depth.load(Ordering::Relaxed);
    let max_output_queue_depth = compute_stats.compute_to_writer_queue_max_depth;
    let average_batch_size = if batches_processed > 0 {
        total_batch_size as f64 / batches_processed as f64
    } else {
        0.0
    };
    let matcher_send_wait_seconds = batch_enqueue_wait_seconds;
    let writer_receive_count = writer_stats.batches_processed;
    let writer_records_per_second = if writer_loop_seconds > 0.0 {
        writer_stats.records_written as f64 / writer_loop_seconds
    } else {
        0.0
    };
    let writer_batches_per_second = if writer_loop_seconds > 0.0 {
        batches_processed as f64 / writer_loop_seconds
    } else {
        0.0
    };
    let total_chunks_sent = forward_reader_stats.chunks_sent + reverse_reader_stats.chunks_sent;
    let total_chunk_groups =
        forward_reader_stats.total_chunk_groups + reverse_reader_stats.total_chunk_groups;
    let average_chunk_groups = if total_chunks_sent > 0 {
        total_chunk_groups as f64 / total_chunks_sent as f64
    } else {
        0.0
    };
    let max_chunk_groups = forward_reader_stats
        .max_chunk_groups
        .max(reverse_reader_stats.max_chunk_groups);
    let reader_chunk_queue_occupancy_mean = if reader_chunk_queue_stats.occupancy_samples > 0 {
        reader_chunk_queue_stats.occupancy_sum as f64
            / reader_chunk_queue_stats.occupancy_samples as f64
    } else {
        0.0
    };
    stage_log(
        cli,
        format!(
            "STAGE direct_pipeline_diagnostics writer_wait_for_sequence_seconds={:.6} input_to_compute_queue_max_depth={} input_to_compute_queue_capacity={} compute_to_writer_queue_max_depth={} compute_to_writer_queue_capacity={} matcher_output_queue_max_depth={} matcher_output_queue_capacity={} reader_chunk_queue_max_depth={} reader_chunk_queue_capacity={} producer_blocked_seconds={:.6} matcher_send_wait_seconds={:.6} forward_reader_send_wait_seconds={:.6} reverse_reader_send_wait_seconds={:.6} writer_idle_seconds={:.6} sync_wait_for_forward_chunk_seconds={:.6} sync_wait_for_reverse_chunk_seconds={:.6} sync_match_loop_seconds={:.6} sync_output_enqueue_seconds={:.6} sync_forward_recv_calls={} sync_reverse_recv_calls={} sync_forward_try_recv_hits={} sync_reverse_try_recv_hits={} sync_forward_blocking_recv_when_reverse_work_available={} sync_reverse_blocking_recv_when_forward_work_available={} sync_slow_side_detected={} sync_slow_side_switch_count={} sync_max_consecutive_waits_forward={} sync_max_consecutive_waits_reverse={} batches_processed={} writer_receive_count={} average_batch_size={:.3} max_batch_size={} input_batches_submitted={} output_batches_submitted={} batches_created={} pending_batches_before_close={} records_written={} writer_records_per_second={:.3} writer_batches_per_second={:.3} writer_write_call_seconds={:.6} writer_process_seconds={:.6} writer_filter_seconds={:.6} writer_actual_bam_write_seconds={:.6} writer_output_queue_receive_wait_seconds={:.6} average_records_per_writer_batch={:.3} estimated_uncompressed_bytes_written={} input_to_compute_queue_full_events={} compute_to_writer_queue_full_events={} matcher_output_queue_full_events={} reader_chunk_queue_full_events={} total_queue_full_events={} average_chunk_groups={:.3} max_chunk_groups={} reader_chunk_queue_occupancy_mean={:.3} forward_reader_chunks_sent={} reverse_reader_chunks_sent={} direct_output_queue_capacity={} direct_reader_queue_capacity={} direct_reader_chunk_groups={} direct_batch_size={} forward_chunk_interval_p95_seconds={:.6} reverse_chunk_interval_p95_seconds={:.6} max_rss_kb={} direct_queue_policy=auto_bounded bgzf_worker_utilization=not_exposed_by_htslib_api",
            writer_recv_wait_seconds,
            max_input_queue_depth,
            queue_policy.output_queue_capacity,
            max_output_queue_depth,
            queue_policy.output_queue_capacity,
            max_output_queue_depth,
            queue_policy.output_queue_capacity,
            reader_chunk_queue_stats.max_depth,
            queue_policy.reader_queue_capacity,
            batch_enqueue_wait_seconds,
            matcher_send_wait_seconds,
            forward_reader_stats.send_wait_seconds,
            reverse_reader_stats.send_wait_seconds,
            writer_idle_seconds,
            sync_diagnostics.wait_for_forward_chunk_seconds,
            sync_diagnostics.wait_for_reverse_chunk_seconds,
            sync_diagnostics.match_loop_seconds,
            sync_diagnostics.output_enqueue_seconds,
            sync_diagnostics.forward_recv_calls,
            sync_diagnostics.reverse_recv_calls,
            sync_diagnostics.forward_try_recv_hits,
            sync_diagnostics.reverse_try_recv_hits,
            sync_diagnostics.forward_blocking_recv_when_reverse_work_available,
            sync_diagnostics.reverse_blocking_recv_when_forward_work_available,
            sync_diagnostics.slow_side_detected,
            sync_diagnostics.slow_side_switch_count,
            sync_diagnostics.max_consecutive_waits_forward,
            sync_diagnostics.max_consecutive_waits_reverse,
            batches_processed,
            writer_receive_count,
            average_batch_size,
            max_batch_size,
            input_batches_submitted,
            output_queue_submitted.load(Ordering::Relaxed),
            input_batches_submitted,
            pending_batches_before_close,
            writer_stats.records_written,
            writer_records_per_second,
            writer_batches_per_second,
            write_call_seconds,
            0.0f64,
            0.0f64,
            write_call_seconds,
            writer_recv_wait_seconds,
            average_batch_size * 2.0,
            writer_stats.estimated_uncompressed_bytes_written,
            output_queue_full_events,
            compute_stats.compute_output_queue_full_events,
            output_queue_full_events,
            reader_chunk_queue_stats.full_events,
            output_queue_full_events
                + compute_stats.compute_output_queue_full_events
                + reader_chunk_queue_stats.full_events,
            average_chunk_groups,
            max_chunk_groups,
            reader_chunk_queue_occupancy_mean,
            forward_reader_stats.chunks_sent,
            reverse_reader_stats.chunks_sent,
            queue_policy.output_queue_capacity,
            queue_policy.reader_queue_capacity,
            queue_policy.reader_chunk_groups,
            queue_policy.batch_size
            ,
            forward_chunk_interval_p95_seconds,
            reverse_chunk_interval_p95_seconds,
            max_rss_kb
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_compute_diagnostics compute_workers={} compute_batches_processed={} compute_records_selected={} compute_filter_seconds_total={:.6} compute_filter_wall_seconds={:.6} compute_input_wait_thread_seconds_total={:.6} compute_output_send_wait_thread_seconds_total={:.6} compute_input_wait_wall_seconds={:.6} compute_output_send_wait_wall_seconds={:.6} compute_output_queue_full_events={} compute_flow_control_wait_thread_seconds_total={:.6} compute_flow_control_wait_wall_seconds={:.6} compute_flow_control_wait_events={} per_worker_batches_processed={} per_worker_max_batch_filter_seconds={} per_worker_mean_batch_filter_seconds={} slowest_batch_id={} slowest_batch_filter_seconds={:.6} compute_batch_duration_p50_seconds={:.6} compute_batch_duration_p95_seconds={:.6} compute_batch_duration_p99_seconds={:.6} ordered_writer_pending_batches_max={} ordered_writer_wait_for_next_batch_seconds={:.6} ordered_writer_missing_batch_wait_events={} ordered_writer_max_gap={} ordered_writer_pending_map_max_size={} ordered_writer_waiting_for_batch_topn={} max_completed_batch_gap_at_writer={} ordered_writer_next_expected_batch_id={} ordered_writer_largest_received_batch_id={} ordered_writer_write_seconds={:.6} ordered_writer_records_per_second={:.3}",
            compute_stats.compute_workers,
            compute_stats.compute_batches_processed,
            compute_stats.compute_records_selected,
            compute_stats.compute_filter_seconds_total,
            compute_stats.compute_filter_wall_seconds,
            compute_stats.compute_input_wait_thread_seconds_total,
            compute_stats.compute_output_send_wait_thread_seconds_total,
            compute_stats.compute_input_wait_wall_seconds,
            compute_stats.compute_output_send_wait_wall_seconds,
            compute_stats.compute_output_queue_full_events,
            compute_stats.compute_flow_control_wait_thread_seconds_total,
            compute_stats.compute_flow_control_wait_wall_seconds,
            compute_stats.compute_flow_control_wait_events,
            format_u64_slice(&compute_stats.per_worker_batches_processed),
            format_f64_slice(&compute_stats.per_worker_max_batch_filter_seconds),
            format_f64_slice(&compute_stats.per_worker_mean_batch_filter_seconds),
            compute_stats.slowest_batch_id,
            compute_stats.slowest_batch_filter_seconds,
            compute_stats.compute_batch_duration_p50_seconds,
            compute_stats.compute_batch_duration_p95_seconds,
            compute_stats.compute_batch_duration_p99_seconds,
            writer_stats.ordered_writer_pending_batches_max,
            writer_stats.ordered_writer_wait_for_next_batch_seconds,
            writer_stats.ordered_writer_missing_batch_wait_events,
            writer_stats.ordered_writer_max_gap,
            writer_stats.ordered_writer_pending_map_max_size,
            if writer_stats.ordered_writer_waiting_for_batch_topn.is_empty() {
                "none".to_string()
            } else {
                writer_stats.ordered_writer_waiting_for_batch_topn.clone()
            },
            writer_stats.max_completed_batch_gap_at_writer,
            writer_stats.ordered_writer_next_expected_batch_id,
            writer_stats.ordered_writer_largest_received_batch_id,
            write_call_seconds,
            writer_records_per_second
        ),
    );
    let close_to_write_ratio = if write_call_seconds > 0.0 {
        writer_stats.hts_close_seconds / write_call_seconds
    } else {
        0.0
    };
    let close_note = if close_to_write_ratio >= 0.25 {
        "high_ratio_suggests_writer_backlog_flushed_during_close"
    } else {
        "close_time_not_dominant"
    };
    let output_probe_seconds = writer_stats.writer_periodic_flush_seconds;
    let output_pool_drop_seconds_total = shared_pool_drop_seconds + output_pool_drop_seconds;
    let output_finalize_seconds = writer_stats.hts_close_seconds
        + writer_stats.writer_pre_close_flush_seconds
        + output_pool_drop_seconds_total;
    let total_output_pressure_seconds =
        writer_drain_seconds + output_probe_seconds + output_finalize_seconds;
    let output_finalize_non_tail_seconds = output_probe_seconds
        + writer_stats.writer_pre_close_flush_seconds
        + writer_stats.hts_close_seconds
        + shared_pool_drop_seconds
        + output_pool_drop_seconds;
    let writer_tail_channel_recv_seconds = (writer_drain_diagnostics.queue_backlog_seconds
        - writer_drain_diagnostics.ordered_backlog_seconds)
        .max(0.0);
    let writer_tail_order_wait_seconds = writer_drain_diagnostics.ordered_backlog_seconds;
    let writer_tail_write_call_seconds = writer_drain_diagnostics.bgzf_or_writer_work_seconds;
    let writer_tail_bgzf_finalize_seconds = writer_stats.hts_close_seconds;
    let writer_tail_pool_drop_seconds = output_pool_drop_seconds_total;
    let writer_tail_join_overhead_seconds = (writer_drain_seconds
        - writer_tail_channel_recv_seconds
        - writer_tail_order_wait_seconds
        - writer_tail_write_call_seconds)
        .max(0.0);
    let _writer_tail_unclassified_seconds = (writer_drain_seconds
        - writer_tail_channel_recv_seconds
        - writer_tail_order_wait_seconds
        - writer_tail_write_call_seconds
        - writer_tail_bgzf_finalize_seconds
        - writer_tail_pool_drop_seconds
        - writer_tail_join_overhead_seconds)
        .max(0.0);
    let writer_drain_primary_cause = classify_primary_bottleneck(&[
        ("channel_drain", writer_tail_channel_recv_seconds),
        ("ordered_backlog", writer_tail_order_wait_seconds),
        ("write_calls", writer_tail_write_call_seconds),
        ("bgzf_finalize", writer_tail_bgzf_finalize_seconds),
        ("pool_drop", writer_tail_pool_drop_seconds),
    ]);
    let pipeline_start_elapsed = 0.0f64;
    let readers_started_elapsed =
        elapsed_micros_to_seconds(lifecycle.readers_started_micros.load(Ordering::Relaxed));
    let writer_thread_started_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .writer_thread_started_micros
            .load(Ordering::Relaxed),
    );
    let first_input_batch_submitted_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .first_input_batch_submitted_micros
            .load(Ordering::Relaxed),
    );
    let first_output_batch_submitted_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .first_output_batch_submitted_micros
            .load(Ordering::Relaxed),
    );
    let first_writer_batch_received_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .first_writer_batch_received_micros
            .load(Ordering::Relaxed),
    );
    let first_writer_batch_written_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .first_writer_batch_written_micros
            .load(Ordering::Relaxed),
    );
    let last_input_batch_submitted_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .last_input_batch_submitted_micros
            .load(Ordering::Relaxed),
    );
    let producers_done_elapsed =
        elapsed_micros_to_seconds(lifecycle.producers_done_micros.load(Ordering::Relaxed));
    let compute_done_elapsed =
        elapsed_micros_to_seconds(lifecycle.compute_done_micros.load(Ordering::Relaxed));
    let output_senders_dropped_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .output_senders_dropped_micros
            .load(Ordering::Relaxed),
    );
    let writer_last_batch_received_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .writer_last_batch_received_micros
            .load(Ordering::Relaxed),
    );
    let writer_last_batch_written_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .writer_last_batch_written_micros
            .load(Ordering::Relaxed),
    );
    let writer_finalize_start_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .writer_finalize_start_micros
            .load(Ordering::Relaxed),
    );
    let writer_finalize_done_elapsed = elapsed_micros_to_seconds(
        lifecycle
            .writer_finalize_done_micros
            .load(Ordering::Relaxed),
    );
    let writer_thread_exit_elapsed =
        elapsed_micros_to_seconds(lifecycle.writer_thread_exit_micros.load(Ordering::Relaxed));
    let writer_join_start_elapsed =
        elapsed_micros_to_seconds(lifecycle.writer_join_start_micros.load(Ordering::Relaxed));
    let writer_join_done_elapsed =
        elapsed_micros_to_seconds(lifecycle.writer_join_done_micros.load(Ordering::Relaxed));
    let producer_phase_seconds = producers_done_elapsed.max(0.0);
    let compute_drain_after_producer_done_seconds =
        (compute_done_elapsed - producers_done_elapsed).max(0.0);
    let output_queue_drain_after_compute_done_seconds =
        (writer_last_batch_received_elapsed - compute_done_elapsed).max(0.0);
    let ordered_writer_drain_after_compute_done_seconds =
        (writer_last_batch_written_elapsed - writer_last_batch_received_elapsed).max(0.0);
    let writer_write_after_compute_done_seconds =
        (writer_last_batch_written_elapsed - compute_done_elapsed).max(0.0);
    let writer_finalize_seconds =
        (writer_finalize_done_elapsed - writer_finalize_start_elapsed).max(0.0);
    let writer_join_overhead_seconds =
        (writer_join_done_elapsed - writer_join_start_elapsed - writer_drain_seconds).max(0.0);
    let writer_tail_unclassified_seconds = (writer_drain_seconds
        - compute_drain_after_producer_done_seconds
        - output_queue_drain_after_compute_done_seconds
        - ordered_writer_drain_after_compute_done_seconds
        - writer_finalize_seconds
        - writer_join_overhead_seconds)
        .max(0.0);
    let writer_tail_accounting_error_seconds = writer_drain_seconds
        - (compute_drain_after_producer_done_seconds
            + output_queue_drain_after_compute_done_seconds
            + ordered_writer_drain_after_compute_done_seconds
            + writer_finalize_seconds
            + writer_join_overhead_seconds
            + writer_tail_unclassified_seconds);
    let writer_tail_primary_cause = writer_drain_primary_cause;
    let lifecycle_unattributed_seconds = (pipeline_start.elapsed().as_secs_f64()
        - producer_phase_seconds
        - compute_drain_after_producer_done_seconds
        - output_queue_drain_after_compute_done_seconds
        - ordered_writer_drain_after_compute_done_seconds
        - writer_finalize_seconds
        - writer_join_overhead_seconds)
        .max(0.0);
    let primary_bottleneck = classify_primary_bottleneck(&[
        ("producer_phase", producer_phase_seconds),
        ("compute_drain", compute_drain_after_producer_done_seconds),
        (
            "output_queue_drain",
            output_queue_drain_after_compute_done_seconds,
        ),
        (
            "ordered_writer_drain",
            ordered_writer_drain_after_compute_done_seconds,
        ),
        ("writer_finalize", writer_finalize_seconds),
        ("writer_join_overhead", writer_join_overhead_seconds),
        ("unattributed", lifecycle_unattributed_seconds),
    ]);
    stage_log(
        cli,
        format!(
            "STAGE direct_lifecycle_timestamps pipeline_start_elapsed={:.6} readers_started_elapsed={:.6} writer_thread_started_elapsed={:.6} first_input_batch_submitted_elapsed={:.6} first_output_batch_submitted_elapsed={:.6} first_writer_batch_received_elapsed={:.6} first_writer_batch_written_elapsed={:.6} last_input_batch_submitted_elapsed={:.6} producers_done_elapsed={:.6} compute_done_elapsed={:.6} output_senders_dropped_elapsed={:.6} writer_last_batch_received_elapsed={:.6} writer_last_batch_written_elapsed={:.6} writer_finalize_start_elapsed={:.6} writer_finalize_done_elapsed={:.6} writer_thread_exit_elapsed={:.6} writer_join_start_elapsed={:.6} writer_join_done_elapsed={:.6}",
            pipeline_start_elapsed, readers_started_elapsed, writer_thread_started_elapsed, first_input_batch_submitted_elapsed, first_output_batch_submitted_elapsed, first_writer_batch_received_elapsed, first_writer_batch_written_elapsed, last_input_batch_submitted_elapsed, producers_done_elapsed, compute_done_elapsed, output_senders_dropped_elapsed, writer_last_batch_received_elapsed, writer_last_batch_written_elapsed, writer_finalize_start_elapsed, writer_finalize_done_elapsed, writer_thread_exit_elapsed, writer_join_start_elapsed, writer_join_done_elapsed
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE writer_tail_breakdown_exclusive producer_phase_wall_seconds_exclusive={:.6} compute_drain_after_producer_done_wall_seconds_exclusive={:.6} output_queue_drain_after_compute_done_wall_seconds_exclusive={:.6} ordered_writer_drain_after_compute_done_wall_seconds_exclusive={:.6} writer_finalize_wall_seconds_exclusive={:.6} writer_join_overhead_wall_seconds_exclusive={:.6} lifecycle_unattributed_wall_seconds_exclusive={:.6} writer_write_after_compute_done_seconds_cumulative={:.6} writer_tail_accounting_error_seconds={:.6} writer_tail_primary_cause={}",
            producer_phase_seconds, compute_drain_after_producer_done_seconds, output_queue_drain_after_compute_done_seconds, ordered_writer_drain_after_compute_done_seconds, writer_finalize_seconds, writer_join_overhead_seconds, lifecycle_unattributed_seconds, writer_write_after_compute_done_seconds, writer_tail_accounting_error_seconds, writer_tail_primary_cause
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_lifecycle_exclusive end_to_end_seconds={:.6} producer_phase_wall_seconds_exclusive={:.6} compute_drain_wall_seconds_exclusive={:.6} output_queue_drain_wall_seconds_exclusive={:.6} ordered_writer_drain_wall_seconds_exclusive={:.6} writer_finalize_wall_seconds_exclusive={:.6} writer_join_overhead_wall_seconds_exclusive={:.6} lifecycle_unattributed_wall_seconds_exclusive={:.6} primary_bottleneck={}",
            pipeline_start.elapsed().as_secs_f64(),
            producer_phase_seconds,
            compute_drain_after_producer_done_seconds,
            output_queue_drain_after_compute_done_seconds,
            ordered_writer_drain_after_compute_done_seconds,
            writer_finalize_seconds,
            writer_join_overhead_seconds,
            lifecycle_unattributed_seconds,
            primary_bottleneck
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_close_diagnostics write_call_seconds={:.6} hts_close_seconds={:.6} close_to_write_ratio={:.3} note={} total_output_drain_seconds={:.6} writer_periodic_flush_count={} writer_periodic_flush_seconds={:.6} writer_pre_close_flush_seconds={:.6} pending_batches_before_close={} records_written_before_close={} output_bytes_before_close={} output_bytes_after_close={} output_bytes_delta_close={} estimated_uncompressed_bytes_written={} writer_probe_reason={} writer_probe_started_with_output_debt_bytes={} writer_probe_started_with_output_debt_seconds={:.6} writer_probe_started_with_pending_batches={} writer_probe_started_with_writer_progress_age_seconds={:.6} writer_probe_elapsed_seconds={:.6} writer_probe_changed_output_bytes={} writer_probe_executed_count={} writer_probe_skipped_count={} writer_probe_skip_reason={} writer_probe_last_skip_reason={} writer_probe_skipped_output_debt_seconds={:.6} writer_probe_skipped_pending_batches={} writer_probe_skipped_writer_progress_age_seconds={:.6} writer_probe_skipped_estimated_bytes={} shared_bgzf_pool_drop_seconds={:.6} output_bgzf_pool_drop_seconds={:.6}",
            write_call_seconds,
            writer_stats.hts_close_seconds,
            close_to_write_ratio,
            close_note,
            output_finalize_non_tail_seconds,
            writer_stats.writer_periodic_flush_count,
            writer_stats.writer_periodic_flush_seconds,
            writer_stats.writer_pre_close_flush_seconds,
            writer_stats.pending_batches_before_close,
            writer_stats.records_written_before_close,
            writer_stats.output_bytes_before_close,
            writer_stats.output_bytes_after_close,
            writer_stats
                .output_bytes_after_close
                .saturating_sub(writer_stats.output_bytes_before_close),
            writer_stats.estimated_uncompressed_bytes_written,
            if writer_stats.writer_probe_reason.is_empty() {
                "none".to_string()
            } else {
                writer_stats.writer_probe_reason.clone()
            },
            writer_stats.writer_probe_started_with_output_debt_bytes,
            writer_stats.writer_probe_started_with_output_debt_seconds,
            writer_stats.writer_probe_started_with_pending_batches,
            writer_stats.writer_probe_started_with_writer_progress_age_seconds,
            writer_stats.writer_probe_elapsed_seconds,
            writer_stats.writer_probe_changed_output_bytes,
            writer_stats.writer_probe_executed_count,
            writer_stats.writer_probe_skipped_count,
            if writer_stats.writer_probe_skip_reason.is_empty() {
                "none".to_string()
            } else {
                writer_stats.writer_probe_skip_reason.clone()
            },
            if writer_stats.writer_probe_last_skip_reason.is_empty() {
                "none".to_string()
            } else {
                writer_stats.writer_probe_last_skip_reason.clone()
            },
            writer_stats.writer_probe_skipped_output_debt_seconds,
            writer_stats.writer_probe_skipped_pending_batches,
            writer_stats.writer_probe_skipped_writer_progress_age_seconds,
            writer_stats.writer_probe_skipped_estimated_bytes,
            shared_pool_drop_seconds,
            output_pool_drop_seconds
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE writer_tail_breakdown writer_tail_seconds={:.6} writer_tail_channel_recv_seconds={:.6} writer_tail_order_wait_seconds={:.6} writer_tail_write_call_seconds={:.6} writer_tail_bgzf_finalize_seconds={:.6} writer_tail_pool_drop_seconds={:.6} writer_tail_join_overhead_seconds={:.6} writer_tail_unclassified_seconds={:.6} writer_drain_primary_cause={}",
            writer_drain_seconds,
            writer_tail_channel_recv_seconds,
            writer_tail_order_wait_seconds,
            writer_tail_write_call_seconds,
            writer_tail_bgzf_finalize_seconds,
            writer_tail_pool_drop_seconds,
            writer_tail_join_overhead_seconds,
            writer_tail_unclassified_seconds,
            writer_drain_primary_cause
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE output_flow_controller_summary submitted_batches={} received_batches={} written_batches={} output_debt_max_bytes={} output_debt_max_batches={} output_debt_max_seconds={:.6} output_debt_mean_seconds={:.6} ordered_pending_max_batches={} ordered_pending_max_bytes={} max_completed_batch_gap_at_writer={} compute_to_writer_queue_max_depth={} writer_write_bps_ema_final={:.3} output_batches_submitted_at_producer_done={} output_batches_received_at_producer_done={} output_batches_written_at_producer_done={} output_bytes_submitted_at_producer_done={} output_bytes_written_at_producer_done={} output_debt_bytes_at_producer_done={} output_debt_batches_at_producer_done={} ordered_pending_batches_at_producer_done={} ordered_pending_bytes_at_producer_done={} next_expected_batch_id_at_producer_done={} largest_completed_batch_id_at_producer_done={} completed_ahead_gap_at_producer_done={} input_wait_seconds_cumulative={:.6} input_wait_events={} input_wait_max_seconds={:.6} compute_start_wait_seconds_cumulative={:.6} compute_start_wait_events={} compute_start_wait_max_seconds={:.6} output_submit_wait_seconds_cumulative={:.6} output_submit_wait_events={} output_submit_wait_max_seconds={:.6}",
            output_flow_snapshot.submitted_batches,
            output_flow_snapshot.received_batches,
            output_flow_snapshot.written_batches,
            output_flow_snapshot.output_debt_max_bytes,
            output_flow_snapshot.output_debt_max_batches,
            output_flow_snapshot.output_debt_max_seconds,
            output_flow_snapshot.output_debt_mean_seconds,
            output_flow_snapshot.ordered_pending_max_batches,
            output_flow_snapshot.ordered_pending_max_bytes,
            output_flow_snapshot.completed_ahead_gap_max,
            output_flow_snapshot.compute_to_writer_queue_max_depth,
            output_flow_snapshot.writer_write_bps_ema,
            output_flow_snapshot.output_batches_submitted_at_producer_done,
            output_flow_snapshot.output_batches_received_at_producer_done,
            output_flow_snapshot.output_batches_written_at_producer_done,
            output_flow_snapshot.output_bytes_submitted_at_producer_done,
            output_flow_snapshot.output_bytes_written_at_producer_done,
            output_flow_snapshot.output_debt_bytes_at_producer_done,
            output_flow_snapshot.output_debt_batches_at_producer_done,
            output_flow_snapshot.ordered_pending_batches_at_producer_done,
            output_flow_snapshot.ordered_pending_bytes_at_producer_done,
            output_flow_snapshot.next_expected_batch_id_at_producer_done,
            output_flow_snapshot.largest_completed_batch_id_at_producer_done,
            output_flow_snapshot.completed_ahead_gap_at_producer_done,
            output_flow_snapshot.input_wait_seconds_total,
            output_flow_snapshot.input_wait_events,
            output_flow_snapshot.input_wait_max_seconds,
            output_flow_snapshot.compute_start_wait_seconds_total,
            output_flow_snapshot.compute_start_wait_events,
            output_flow_snapshot.compute_start_wait_max_seconds,
            output_flow_snapshot.output_submit_wait_seconds_total,
            output_flow_snapshot.output_submit_wait_events,
            output_flow_snapshot.output_submit_wait_max_seconds
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
    mark_elapsed_once(&lifecycle.pipeline_end_micros, pipeline_start);
    let end_to_end_seconds = pipeline_start.elapsed().as_secs_f64();
    let output_gib = size_mb(&cli.output)? / 1024.0;
    let groups_million = if stats.groups > 0 {
        stats.groups as f64 / 1_000_000.0
    } else {
        0.0
    };
    let end_to_end_seconds_per_output_gib = if output_gib > 0.0 {
        end_to_end_seconds / output_gib
    } else {
        0.0
    };
    let writer_tail_seconds_per_output_gib = if output_gib > 0.0 {
        writer_drain_seconds / output_gib
    } else {
        0.0
    };
    let producer_phase_seconds_per_million_groups = if groups_million > 0.0 {
        producer_phase_seconds / groups_million
    } else {
        0.0
    };
    let writer_tail_seconds_per_million_groups = if groups_million > 0.0 {
        writer_drain_seconds / groups_million
    } else {
        0.0
    };
    stage_log(
        cli,
        format!(
            "STAGE direct_output_flow_summary end_to_end_seconds={:.6} producer_done_seconds={:.6} writer_tail_seconds={:.6} output_finalize_seconds={:.6} output_probe_seconds={:.6} output_pool_drop_seconds={:.6} total_output_pressure_seconds={:.6} output_debt_max_seconds={:.6} output_debt_mean_seconds={:.6} output_debt_max_bytes={} output_debt_max_batches={} output_debt_bytes_at_producer_done={} output_debt_batches_at_producer_done={} ordered_pending_max_batches={} ordered_pending_max_bytes={} ordered_pending_batches_at_producer_done={} ordered_pending_bytes_at_producer_done={} max_completed_batch_gap_at_writer={} completed_ahead_gap_at_producer_done={} compute_to_writer_queue_max_depth={} writer_drain_primary_cause={} primary_bottleneck={} end_to_end_seconds_per_output_gib={:.6} writer_tail_seconds_per_output_gib={:.6} producer_phase_seconds_per_million_groups={:.6} writer_tail_seconds_per_million_groups={:.6}",
            end_to_end_seconds,
            producer_done_seconds,
            writer_drain_seconds,
            output_finalize_seconds,
            output_probe_seconds,
            output_pool_drop_seconds_total,
            total_output_pressure_seconds,
            output_flow_snapshot.output_debt_max_seconds,
            output_flow_snapshot.output_debt_mean_seconds,
            output_flow_snapshot.output_debt_max_bytes,
            output_flow_snapshot.output_debt_max_batches,
            output_flow_snapshot.output_debt_bytes_at_producer_done,
            output_flow_snapshot.output_debt_batches_at_producer_done,
            output_flow_snapshot.ordered_pending_max_batches,
            output_flow_snapshot.ordered_pending_max_bytes,
            output_flow_snapshot.ordered_pending_batches_at_producer_done,
            output_flow_snapshot.ordered_pending_bytes_at_producer_done,
            writer_stats.max_completed_batch_gap_at_writer,
            output_flow_snapshot.completed_ahead_gap_at_producer_done,
            output_flow_snapshot.compute_to_writer_queue_max_depth,
            writer_drain_primary_cause,
            primary_bottleneck,
            end_to_end_seconds_per_output_gib,
            writer_tail_seconds_per_output_gib,
            producer_phase_seconds_per_million_groups,
            writer_tail_seconds_per_million_groups
        ),
    );
    stage_log(
        cli,
        format!(
            "STAGE direct_total_summary requested_threads={} end_to_end_seconds={:.6} producer_done_seconds={:.6} writer_tail_seconds={:.6} read_match_seconds={:.6} output_finalize_seconds={:.6} output_probe_seconds={:.6} output_pool_drop_seconds={:.6} total_output_pressure_seconds={:.6} records_written={} output_mb={:.3} output_gib={:.6} writer_write_bps_ema_final={:.3} writer_drain_primary_cause={} primary_bottleneck={} end_to_end_seconds_per_output_gib={:.6} writer_tail_seconds_per_output_gib={:.6} producer_phase_seconds_per_million_groups={:.6} writer_tail_seconds_per_million_groups={:.6}",
            thread_resolution.requested_threads,
            end_to_end_seconds,
            producer_done_seconds,
            writer_drain_seconds,
            read_match_seconds,
            output_finalize_seconds,
            output_probe_seconds,
            output_pool_drop_seconds_total,
            total_output_pressure_seconds,
            writer_stats.records_written,
            size_mb(&cli.output)?,
            output_gib,
            output_flow_snapshot.writer_write_bps_ema,
            writer_drain_primary_cause,
            primary_bottleneck,
            end_to_end_seconds_per_output_gib,
            writer_tail_seconds_per_output_gib,
            producer_phase_seconds_per_million_groups,
            writer_tail_seconds_per_million_groups
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
    matcher_output_queue_capacity: usize,
    output_queue_full_events: &mut u64,
    next_batch_id: &mut u64,
    input_flow_control_wait_seconds: &mut f64,
    output_flow_controller: &Arc<OutputFlowController>,
    pipeline_start: Instant,
    lifecycle: &Arc<PipelineLifecycleMarkers>,
) -> Result<()> {
    if active_batch.is_empty() {
        return Ok(());
    }
    let input_wait = output_flow_controller.wait_before_input_batch_enqueue();
    *input_flow_control_wait_seconds += input_wait;
    *batch_enqueue_wait_seconds += input_wait;
    if queue_depth.load(Ordering::Relaxed) >= matcher_output_queue_capacity {
        *output_queue_full_events += 1;
    }
    let enqueue_start = Instant::now();
    if *next_batch_id == 0 {
        mark_elapsed_once(
            &lifecycle.first_input_batch_submitted_micros,
            pipeline_start,
        );
    }
    let batch = DirectInputBatch {
        batch_id: *next_batch_id,
        groups: std::mem::replace(active_batch, Vec::with_capacity(direct_batch_size)),
    };
    *next_batch_id += 1;
    batch_sender
        .send(batch)
        .context("failed to send direct batch to writer thread")?;
    let depth = queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
    lifecycle.last_input_batch_submitted_micros.store(
        pipeline_start
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
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
    matcher_output_queue_capacity: usize,
    output_queue_full_events: &mut u64,
    next_batch_id: &mut u64,
    input_flow_control_wait_seconds: &mut f64,
    output_flow_controller: &Arc<OutputFlowController>,
    pipeline_start: Instant,
    lifecycle: &Arc<PipelineLifecycleMarkers>,
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
                    matcher_output_queue_capacity,
                    output_queue_full_events,
                    next_batch_id,
                    input_flow_control_wait_seconds,
                    output_flow_controller,
                    pipeline_start,
                    lifecycle,
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
                    matcher_output_queue_capacity,
                    output_queue_full_events,
                    next_batch_id,
                    input_flow_control_wait_seconds,
                    output_flow_controller,
                    pipeline_start,
                    lifecycle,
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

fn read_group_chunks_producer(
    input_path: PathBuf,
    sender: SyncSender<Result<Option<ReaderChunk>>>,
    chunk_groups: usize,
    reader_threads: usize,
    label: &'static str,
    reader_chunk_queue_capacity: usize,
    chunk_queue_depth: Arc<AtomicUsize>,
    chunk_queue_max_depth: Arc<AtomicUsize>,
) -> Result<ReaderDecodeStats> {
    let mut reader = bam::Reader::from_path(&input_path)
        .with_context(|| format!("failed to open {label} input {}", input_path.display()))?;
    reader
        .set_threads(reader_threads)
        .with_context(|| format!("failed to set {label} reader threads"))?;
    let wall_start = Instant::now();
    let mut stats = ReaderDecodeStats::default();
    let mut pending = None;
    let mut scratch = Record::new();
    let mut last_chunk_send_end: Option<Instant> = None;
    loop {
        let mut groups = Vec::with_capacity(chunk_groups);
        for _ in 0..chunk_groups {
            let next = match next_group_records_read(
                &mut reader,
                &mut pending,
                &mut scratch,
                &mut stats,
            ) {
                Ok(next) => next,
                Err(e) => {
                    let _ = sender.send(Err(e));
                    stats.wall_seconds = wall_start.elapsed().as_secs_f64();
                    return Ok(stats);
                }
            };
            match next {
                Some(group) => groups.push(group),
                None => break,
            }
        }
        if groups.is_empty() {
            if chunk_queue_depth.load(Ordering::Relaxed) >= reader_chunk_queue_capacity {
                stats.queue_full_events += 1;
            }
            let send_start = Instant::now();
            sender
                .send(Ok(None))
                .with_context(|| format!("failed to send {label} EOF from producer"))?;
            stats.send_wait_seconds += send_start.elapsed().as_secs_f64();
            let depth = chunk_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
            stats.queue_occupancy_sum += depth as u64;
            stats.queue_occupancy_samples += 1;
            loop {
                let previous_max = chunk_queue_max_depth.load(Ordering::Relaxed);
                if depth <= previous_max {
                    break;
                }
                if chunk_queue_max_depth
                    .compare_exchange(previous_max, depth, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            break;
        }
        if chunk_queue_depth.load(Ordering::Relaxed) >= reader_chunk_queue_capacity {
            stats.queue_full_events += 1;
        }
        let group_count = groups.len();
        if let Some(previous_send_end) = last_chunk_send_end {
            let interval_seconds = previous_send_end.elapsed().as_secs_f64();
            stats.chunk_interval_seconds_total += interval_seconds;
            stats.chunk_interval_samples += 1;
            push_bounded_sample(
                &mut stats.chunk_interval_sample_window,
                &mut stats.chunk_interval_sample_cursor,
                interval_seconds,
                READER_CHUNK_INTERVAL_SAMPLE_WINDOW,
            );
            if stats.chunk_interval_samples == 1 {
                stats.chunk_interval_min_seconds = interval_seconds;
                stats.chunk_interval_max_seconds = interval_seconds;
            } else {
                stats.chunk_interval_min_seconds =
                    stats.chunk_interval_min_seconds.min(interval_seconds);
                stats.chunk_interval_max_seconds =
                    stats.chunk_interval_max_seconds.max(interval_seconds);
            }
        }
        let send_start = Instant::now();
        sender
            .send(Ok(Some(ReaderChunk { groups })))
            .with_context(|| format!("failed to send {label} chunk from producer"))?;
        stats.send_wait_seconds += send_start.elapsed().as_secs_f64();
        last_chunk_send_end = Some(Instant::now());
        stats.chunks_sent += 1;
        stats.total_chunk_groups += group_count as u64;
        stats.max_chunk_groups = stats.max_chunk_groups.max(group_count);
        let depth = chunk_queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        stats.queue_occupancy_sum += depth as u64;
        stats.queue_occupancy_samples += 1;
        loop {
            let previous_max = chunk_queue_max_depth.load(Ordering::Relaxed);
            if depth <= previous_max {
                break;
            }
            if chunk_queue_max_depth
                .compare_exchange(previous_max, depth, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }
    stats.wall_seconds = wall_start.elapsed().as_secs_f64();
    Ok(stats)
}

fn drain_reader_try_recv(
    rx: &Receiver<Result<Option<ReaderChunk>>>,
    queue: &mut VecDeque<DirectRecordGroup>,
    done: &mut bool,
    chunk_depth: &Arc<AtomicUsize>,
    prefetch_groups: usize,
    try_recv_hits: &mut u64,
    label: &'static str,
) -> Result<()> {
    loop {
        if *done || queue.len() >= prefetch_groups {
            break;
        }
        match rx.try_recv() {
            Ok(next_chunk) => {
                *try_recv_hits += 1;
                chunk_depth.fetch_sub(1, Ordering::Relaxed);
                match next_chunk? {
                    Some(chunk) => {
                        for group in chunk.groups {
                            queue.push_back(group);
                        }
                    }
                    None => {
                        *done = true;
                        break;
                    }
                }
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                bail!("failed to receive {label} chunk: channel disconnected")
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn sync_parallel_reader_groups(
    forward_rx: &Receiver<Result<Option<ReaderChunk>>>,
    reverse_rx: &Receiver<Result<Option<ReaderChunk>>>,
    forward_chunk_depth: &Arc<AtomicUsize>,
    reverse_chunk_depth: &Arc<AtomicUsize>,
    active_batch: &mut Vec<(DirectRecordGroup, DirectRecordGroup)>,
    batch_sender: &SyncSender<DirectInputBatch>,
    queue_depth: &Arc<AtomicUsize>,
    max_queue_depth: &Arc<AtomicUsize>,
    batch_enqueue_wait_seconds: &mut f64,
    direct_batch_size: usize,
    groups_seen: &mut u64,
    pair_match_assembly_seconds: &mut f64,
    max_lookahead_groups: usize,
    reader_chunk_queue_capacity: usize,
    matcher_output_queue_capacity: usize,
    output_queue_full_events: &mut u64,
    sync_diagnostics: &mut SyncDiagnostics,
    next_batch_id: &mut u64,
    input_flow_control_wait_seconds: &mut f64,
    output_flow_controller: &Arc<OutputFlowController>,
    pipeline_start: Instant,
    lifecycle: &Arc<PipelineLifecycleMarkers>,
) -> Result<()> {
    const SYNC_PREFETCH_TARGET_CHUNKS: usize = 2;
    const SYNC_PREFETCH_IMBALANCED_CHUNKS: usize = 4;
    const SYNC_SLOW_SIDE_MIN_WAITS: u64 = 8;
    const SYNC_SLOW_SIDE_WAIT_RATIO: f64 = 1.6;
    const SYNC_SLOW_SIDE_WAIT_SECONDS_DELTA: f64 = 0.050;
    const SYNC_BLOCKING_RECV_TIMEOUT: Duration = Duration::from_millis(2);
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum SlowSide {
        Forward,
        Reverse,
        Balanced,
        Unknown,
    }
    impl SlowSide {
        fn as_stage_value(self) -> &'static str {
            match self {
                SlowSide::Forward => "forward",
                SlowSide::Reverse => "reverse",
                SlowSide::Balanced => "balanced",
                SlowSide::Unknown => "unknown",
            }
        }
    }
    let classify_slow_side = |diag: &SyncDiagnostics| -> SlowSide {
        let total_wait_calls = diag.forward_recv_calls + diag.reverse_recv_calls;
        if total_wait_calls < SYNC_SLOW_SIDE_MIN_WAITS {
            return SlowSide::Unknown;
        }
        let forward_wait = diag.wait_for_forward_chunk_seconds.max(0.0);
        let reverse_wait = diag.wait_for_reverse_chunk_seconds.max(0.0);
        if reverse_wait > forward_wait * SYNC_SLOW_SIDE_WAIT_RATIO
            && reverse_wait - forward_wait > SYNC_SLOW_SIDE_WAIT_SECONDS_DELTA
        {
            SlowSide::Reverse
        } else if forward_wait > reverse_wait * SYNC_SLOW_SIDE_WAIT_RATIO
            && forward_wait - reverse_wait > SYNC_SLOW_SIDE_WAIT_SECONDS_DELTA
        {
            SlowSide::Forward
        } else {
            SlowSide::Balanced
        }
    };
    let mut forward_queue: VecDeque<DirectRecordGroup> = VecDeque::new();
    let mut reverse_queue: VecDeque<DirectRecordGroup> = VecDeque::new();
    let mut pending_forward: BTreeMap<Vec<u8>, DirectRecordGroup> = BTreeMap::new();
    let mut pending_reverse: BTreeMap<Vec<u8>, DirectRecordGroup> = BTreeMap::new();
    let mut forward_done = false;
    let mut reverse_done = false;
    let mut last_slow_side = SlowSide::Unknown;
    let mut consecutive_waits_forward = 0u64;
    let mut consecutive_waits_reverse = 0u64;

    loop {
        let current_slow_side = classify_slow_side(sync_diagnostics);
        if current_slow_side != SlowSide::Unknown && current_slow_side != last_slow_side {
            if last_slow_side != SlowSide::Unknown {
                sync_diagnostics.slow_side_switch_count += 1;
            }
            last_slow_side = current_slow_side;
        }
        sync_diagnostics.slow_side_detected = current_slow_side.as_stage_value();
        let adaptive_prefetch_chunks = match current_slow_side {
            SlowSide::Forward | SlowSide::Reverse => SYNC_PREFETCH_IMBALANCED_CHUNKS,
            SlowSide::Balanced | SlowSide::Unknown => SYNC_PREFETCH_TARGET_CHUNKS,
        }
        .min(reader_chunk_queue_capacity.max(1));
        let adaptive_prefetch_groups = adaptive_prefetch_chunks * direct_batch_size;
        drain_reader_try_recv(
            forward_rx,
            &mut forward_queue,
            &mut forward_done,
            forward_chunk_depth,
            adaptive_prefetch_groups,
            &mut sync_diagnostics.forward_try_recv_hits,
            "forward",
        )?;
        drain_reader_try_recv(
            reverse_rx,
            &mut reverse_queue,
            &mut reverse_done,
            reverse_chunk_depth,
            adaptive_prefetch_groups,
            &mut sync_diagnostics.reverse_try_recv_hits,
            "reverse",
        )?;

        let can_make_progress_with_reverse_only =
            !pending_forward.is_empty() && reverse_queue.is_empty();
        let can_make_progress_with_forward_only =
            !pending_reverse.is_empty() && forward_queue.is_empty();

        if !forward_done && forward_queue.is_empty() && !can_make_progress_with_reverse_only {
            let reverse_work_available = !reverse_queue.is_empty() || !pending_reverse.is_empty();
            if reverse_work_available {
                sync_diagnostics.forward_blocking_recv_when_reverse_work_available += 1;
            }
            let wait_start = Instant::now();
            sync_diagnostics.forward_recv_calls += 1;
            let next_forward_chunk = if reverse_work_available {
                match forward_rx.recv_timeout(SYNC_BLOCKING_RECV_TIMEOUT) {
                    Ok(chunk) => Some(chunk),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => {
                        bail!("failed to receive forward chunk: channel disconnected")
                    }
                }
            } else {
                Some(
                    forward_rx
                        .recv()
                        .context("failed to receive forward chunk")?,
                )
            };
            let waited = wait_start.elapsed().as_secs_f64();
            sync_diagnostics.wait_for_forward_chunk_seconds += waited;
            consecutive_waits_forward += 1;
            consecutive_waits_reverse = 0;
            sync_diagnostics.max_consecutive_waits_forward = sync_diagnostics
                .max_consecutive_waits_forward
                .max(consecutive_waits_forward);
            if let Some(next_forward_chunk) = next_forward_chunk {
                forward_chunk_depth.fetch_sub(1, Ordering::Relaxed);
                match next_forward_chunk? {
                    Some(chunk) => {
                        for group in chunk.groups {
                            forward_queue.push_back(group);
                        }
                    }
                    None => forward_done = true,
                }
            } else {
                drain_reader_try_recv(
                    reverse_rx,
                    &mut reverse_queue,
                    &mut reverse_done,
                    reverse_chunk_depth,
                    adaptive_prefetch_groups,
                    &mut sync_diagnostics.reverse_try_recv_hits,
                    "reverse",
                )?;
            }
        }
        if !reverse_done && reverse_queue.is_empty() && !can_make_progress_with_forward_only {
            let forward_work_available = !forward_queue.is_empty() || !pending_forward.is_empty();
            if forward_work_available {
                sync_diagnostics.reverse_blocking_recv_when_forward_work_available += 1;
            }
            let wait_start = Instant::now();
            sync_diagnostics.reverse_recv_calls += 1;
            let next_reverse_chunk = if forward_work_available {
                match reverse_rx.recv_timeout(SYNC_BLOCKING_RECV_TIMEOUT) {
                    Ok(chunk) => Some(chunk),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => {
                        bail!("failed to receive reverse chunk: channel disconnected")
                    }
                }
            } else {
                Some(
                    reverse_rx
                        .recv()
                        .context("failed to receive reverse chunk")?,
                )
            };
            let waited = wait_start.elapsed().as_secs_f64();
            sync_diagnostics.wait_for_reverse_chunk_seconds += waited;
            consecutive_waits_reverse += 1;
            consecutive_waits_forward = 0;
            sync_diagnostics.max_consecutive_waits_reverse = sync_diagnostics
                .max_consecutive_waits_reverse
                .max(consecutive_waits_reverse);
            if let Some(next_reverse_chunk) = next_reverse_chunk {
                reverse_chunk_depth.fetch_sub(1, Ordering::Relaxed);
                match next_reverse_chunk? {
                    Some(chunk) => {
                        for group in chunk.groups {
                            reverse_queue.push_back(group);
                        }
                    }
                    None => reverse_done = true,
                }
            } else {
                drain_reader_try_recv(
                    forward_rx,
                    &mut forward_queue,
                    &mut forward_done,
                    forward_chunk_depth,
                    adaptive_prefetch_groups,
                    &mut sync_diagnostics.forward_try_recv_hits,
                    "forward",
                )?;
            }
        }

        if let (Some(f), Some(r)) = (forward_queue.front(), reverse_queue.front()) {
            let match_start = Instant::now();
            consecutive_waits_forward = 0;
            consecutive_waits_reverse = 0;
            match f.qname().cmp(r.qname()) {
                CmpOrdering::Equal => {
                    let f_group = forward_queue.pop_front().expect("queue checked");
                    let r_group = reverse_queue.pop_front().expect("queue checked");
                    active_batch.push((f_group, r_group));
                    *groups_seen += 1;
                    if active_batch.len() >= direct_batch_size {
                        let enqueue_start = Instant::now();
                        flush_direct_batch(
                            active_batch,
                            batch_sender,
                            queue_depth,
                            max_queue_depth,
                            batch_enqueue_wait_seconds,
                            direct_batch_size,
                            matcher_output_queue_capacity,
                            output_queue_full_events,
                            next_batch_id,
                            input_flow_control_wait_seconds,
                            output_flow_controller,
                            pipeline_start,
                            lifecycle,
                        )?;
                        sync_diagnostics.output_enqueue_seconds +=
                            enqueue_start.elapsed().as_secs_f64();
                    }
                }
                CmpOrdering::Less => {
                    let f_group = forward_queue.pop_front().expect("queue checked");
                    let key = f_group.qname().to_vec();
                    if let Some(r_group) = pending_reverse.remove(&key) {
                        active_batch.push((f_group, r_group));
                        *groups_seen += 1;
                        if active_batch.len() >= direct_batch_size {
                            let enqueue_start = Instant::now();
                            flush_direct_batch(
                                active_batch,
                                batch_sender,
                                queue_depth,
                                max_queue_depth,
                                batch_enqueue_wait_seconds,
                                direct_batch_size,
                                matcher_output_queue_capacity,
                                output_queue_full_events,
                                next_batch_id,
                                input_flow_control_wait_seconds,
                                output_flow_controller,
                                pipeline_start,
                                lifecycle,
                            )?;
                            sync_diagnostics.output_enqueue_seconds +=
                                enqueue_start.elapsed().as_secs_f64();
                        }
                    } else {
                        if pending_forward.insert(key.clone(), f_group).is_some() {
                            bail!(
                                "duplicate unmatched forward QNAME encountered during bounded synchronization: {}",
                                String::from_utf8_lossy(&key)
                            );
                        }
                    }
                }
                CmpOrdering::Greater => {
                    let r_group = reverse_queue.pop_front().expect("queue checked");
                    let key = r_group.qname().to_vec();
                    if let Some(f_group) = pending_forward.remove(&key) {
                        active_batch.push((f_group, r_group));
                        *groups_seen += 1;
                        if active_batch.len() >= direct_batch_size {
                            let enqueue_start = Instant::now();
                            flush_direct_batch(
                                active_batch,
                                batch_sender,
                                queue_depth,
                                max_queue_depth,
                                batch_enqueue_wait_seconds,
                                direct_batch_size,
                                matcher_output_queue_capacity,
                                output_queue_full_events,
                                next_batch_id,
                                input_flow_control_wait_seconds,
                                output_flow_controller,
                                pipeline_start,
                                lifecycle,
                            )?;
                            sync_diagnostics.output_enqueue_seconds +=
                                enqueue_start.elapsed().as_secs_f64();
                        }
                    } else {
                        if pending_reverse.insert(key.clone(), r_group).is_some() {
                            bail!(
                                "duplicate unmatched reverse QNAME encountered during bounded synchronization: {}",
                                String::from_utf8_lossy(&key)
                            );
                        }
                    }
                }
            }
            let elapsed = match_start.elapsed().as_secs_f64();
            *pair_match_assembly_seconds += elapsed;
            sync_diagnostics.match_loop_seconds += elapsed;
        } else if forward_done
            && reverse_done
            && forward_queue.is_empty()
            && reverse_queue.is_empty()
        {
            break;
        }

        if pending_forward.len() + pending_reverse.len() > max_lookahead_groups {
            bail!(
                "direct parallel synchronization exceeded bounded lookahead ({} groups); inputs are not compatible with bounded QNAME synchronization",
                max_lookahead_groups
            );
        }
    }

    if !pending_forward.is_empty() || !pending_reverse.is_empty() {
        bail!("direct input BAMs contained a different number of query-name groups");
    }
    if !active_batch.is_empty() {
        let enqueue_start = Instant::now();
        flush_direct_batch(
            active_batch,
            batch_sender,
            queue_depth,
            max_queue_depth,
            batch_enqueue_wait_seconds,
            direct_batch_size,
            matcher_output_queue_capacity,
            output_queue_full_events,
            next_batch_id,
            input_flow_control_wait_seconds,
            output_flow_controller,
            pipeline_start,
            lifecycle,
        )?;
        sync_diagnostics.output_enqueue_seconds += enqueue_start.elapsed().as_secs_f64();
    }
    Ok(())
}

fn direct_compute_thread(
    worker_id: usize,
    batch_receiver: Arc<Mutex<Receiver<DirectInputBatch>>>,
    output_sender: SyncSender<DirectOutputBatch>,
    quality: u8,
    input_queue_depth: Arc<AtomicUsize>,
    output_queue_submitted: Arc<AtomicU64>,
    output_queue_received: Arc<AtomicU64>,
    output_queue_max_depth: Arc<AtomicUsize>,
    output_bytes_submitted: Arc<AtomicU64>,
    output_bytes_written: Arc<AtomicU64>,
    writer_bytes_per_second_estimate: Arc<AtomicU64>,
    writer_last_progress_micros: Arc<AtomicU64>,
    writer_next_expected_batch_id: Arc<AtomicU64>,
    flow_controller: Arc<OutputFlowController>,
    output_queue_capacity: usize,
    runtime_config: DirectWriterRuntimeConfig,
    pipeline_start: Instant,
    lifecycle: Arc<PipelineLifecycleMarkers>,
) -> Result<DirectComputeWorkerStats> {
    const COMPUTE_AHEAD_WINDOW_BATCHES_PER_WORKER: u64 = 2;
    let worker_start = Instant::now();
    let mut compute_input_wait_wall_seconds = 0.0f64;
    let mut compute_output_send_wait_wall_seconds = 0.0f64;
    let mut stats = DirectComputeWorkerStats {
        worker_id,
        ..Default::default()
    };
    loop {
        let recv_wait_start = Instant::now();
        let batch = {
            let receiver_guard = batch_receiver
                .lock()
                .map_err(|_| anyhow::anyhow!("compute input receiver mutex poisoned"))?;
            receiver_guard.recv()
        };
        match batch {
            Ok(batch) => {
                let compute_start_wait = flow_controller.wait_before_compute_start();
                stats.compute_flow_control_wait_thread_seconds_total += compute_start_wait;
                stats.compute_flow_control_wait_wall_seconds += compute_start_wait;
                stats.compute_start_flow_control_wait_seconds_total += compute_start_wait;
                if compute_start_wait > 0.0 {
                    stats.compute_flow_control_wait_events += 1;
                }
                let input_wait_seconds = recv_wait_start.elapsed().as_secs_f64();
                stats.compute_input_wait_thread_seconds_total += input_wait_seconds;
                compute_input_wait_wall_seconds += input_wait_seconds;
                input_queue_depth.fetch_sub(1, Ordering::Relaxed);
                stats.compute_batches_processed += 1;
                let output_batch = process_direct_batch(batch, quality);
                let output_batch_id = output_batch.batch_id;
                stats.compute_records_selected += (output_batch.records.len() * 2) as u64;
                stats.compute_filter_seconds_total += output_batch.filter_seconds;
                stats.batch_filter_seconds_total += output_batch.filter_seconds;
                stats.max_batch_filter_seconds = stats
                    .max_batch_filter_seconds
                    .max(output_batch.filter_seconds);
                if output_batch.filter_seconds > stats.slowest_batch_filter_seconds {
                    stats.slowest_batch_filter_seconds = output_batch.filter_seconds;
                    stats.slowest_batch_id = output_batch_id;
                }
                stats
                    .batch_filter_samples_seconds
                    .push(output_batch.filter_seconds);
                let output_batch_bytes = estimate_output_batch_bytes(&output_batch);
                let submit_wait = flow_controller.wait_before_output_submit();
                stats.compute_flow_control_wait_thread_seconds_total += submit_wait;
                stats.compute_flow_control_wait_wall_seconds += submit_wait;
                stats.output_submit_flow_control_wait_seconds_total += submit_wait;
                if submit_wait > 0.0 {
                    stats.compute_flow_control_wait_events += 1;
                }
                let queue_depth_before_send = output_queue_submitted
                    .load(Ordering::Relaxed)
                    .saturating_sub(output_queue_received.load(Ordering::Relaxed));
                if queue_depth_before_send >= output_queue_capacity as u64 {
                    stats.compute_output_queue_full_events += 1;
                }
                let send_wait_start = Instant::now();
                if output_sender.send(output_batch).is_err() {
                    flow_controller.on_compute_done();
                    break;
                }
                let output_wait_seconds = send_wait_start.elapsed().as_secs_f64();
                stats.compute_output_send_wait_thread_seconds_total += output_wait_seconds;
                compute_output_send_wait_wall_seconds += output_wait_seconds;
                output_bytes_submitted.fetch_add(output_batch_bytes, Ordering::Relaxed);
                let submitted = output_queue_submitted.fetch_add(1, Ordering::Relaxed) + 1;
                if submitted == 1 {
                    mark_elapsed_once(
                        &lifecycle.first_output_batch_submitted_micros,
                        pipeline_start,
                    );
                }
                flow_controller.on_output_submitted(output_batch_id, output_batch_bytes);
                let received = output_queue_received.load(Ordering::Relaxed);
                let depth = submitted.saturating_sub(received) as usize;
                loop {
                    let previous_max = output_queue_max_depth.load(Ordering::Relaxed);
                    if depth <= previous_max {
                        break;
                    }
                    if output_queue_max_depth
                        .compare_exchange(previous_max, depth, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
                flow_controller.on_compute_done();
            }
            Err(_) => break,
        }
    }
    stats.compute_filter_wall_seconds = worker_start.elapsed().as_secs_f64();
    stats.compute_input_wait_wall_seconds = compute_input_wait_wall_seconds;
    stats.compute_output_send_wait_wall_seconds = compute_output_send_wait_wall_seconds;
    Ok(stats)
}

fn estimate_output_batch_bytes(batch: &DirectOutputBatch) -> u64 {
    batch
        .records
        .iter()
        .map(|(f_record, r_record)| (f_record.inner().l_data + r_record.inner().l_data) as u64)
        .sum()
}

fn should_throttle_compute_submission(
    output_batch_id: u64,
    writer_window_batches: u64,
    writer_next_expected_batch_id: &Arc<AtomicU64>,
    output_queue_submitted: &Arc<AtomicU64>,
    output_queue_received: &Arc<AtomicU64>,
    output_bytes_submitted: &Arc<AtomicU64>,
    output_bytes_written: &Arc<AtomicU64>,
    writer_bytes_per_second_estimate: &Arc<AtomicU64>,
    writer_last_progress_micros: &Arc<AtomicU64>,
    runtime_config: DirectWriterRuntimeConfig,
    pipeline_start: Instant,
) -> bool {
    let max_allowed_batch_id = writer_next_expected_batch_id
        .load(Ordering::Relaxed)
        .saturating_add(writer_window_batches);
    if output_batch_id > max_allowed_batch_id {
        return true;
    }
    let submitted = output_queue_submitted.load(Ordering::Relaxed);
    let received = output_queue_received.load(Ordering::Relaxed);
    let queue_backlog_batches = submitted.saturating_sub(received);
    let backlog_bytes = output_bytes_submitted
        .load(Ordering::Relaxed)
        .saturating_sub(output_bytes_written.load(Ordering::Relaxed));
    let writer_bps = writer_bytes_per_second_estimate.load(Ordering::Relaxed);
    let dynamic_limit = if writer_bps == 0 {
        runtime_config.flow_min_inflight_bytes
    } else {
        ((writer_bps as f64) * runtime_config.flow_target_backlog_seconds) as u64
    }
    .clamp(
        runtime_config.flow_min_inflight_bytes,
        runtime_config.flow_max_inflight_bytes,
    );
    if queue_backlog_batches >= runtime_config.flow_max_queue_backlog_batches {
        return true;
    }
    if queue_backlog_batches >= runtime_config.flow_soft_queue_backlog_batches
        && backlog_bytes >= dynamic_limit / 2
    {
        return true;
    }
    if backlog_bytes > dynamic_limit {
        return true;
    }
    let last_progress_micros = writer_last_progress_micros.load(Ordering::Relaxed);
    let now_micros = pipeline_start.elapsed().as_micros() as u64;
    let stale_writer = now_micros.saturating_sub(last_progress_micros)
        >= runtime_config.flow_stale_progress_micros;
    stale_writer && queue_backlog_batches >= runtime_config.flow_max_queue_backlog_batches
}

impl OutputFlowController {
    fn new(max_compute_workers: usize) -> Self {
        Self {
            inner: Mutex::new(OutputFlowInner {
                writer_last_progress_time: Instant::now(),
                compute_active_min: max_compute_workers,
                ..OutputFlowInner {
                    submitted_batches: 0,
                    received_batches: 0,
                    written_batches: 0,
                    submitted_output_bytes_estimate: 0,
                    written_output_bytes_estimate: 0,
                    queued_batches: HashMap::new(),
                    next_expected_batch_id: 0,
                    largest_completed_batch_id: 0,
                    completed_ahead_gap_max: 0,
                    ordered_pending_bytes_estimate: 0,
                    ordered_pending_max_batches: 0,
                    ordered_pending_max_bytes: 0,
                    compute_to_writer_queue_max_depth: 0,
                    writer_write_bps_ema: 0.0,
                    writer_last_progress_time: Instant::now(),
                    debt_seconds_sum: 0.0,
                    debt_seconds_samples: 0,
                    output_debt_max_bytes: 0,
                    output_debt_max_batches: 0,
                    output_debt_max_seconds: 0.0,
                    producers_done: false,
                    compute_active: 0,
                    compute_active_sum: 0,
                    compute_active_samples: 0,
                    compute_active_min: max_compute_workers,
                    compute_active_max: 0,
                    output_batches_submitted_at_producer_done: None,
                    output_batches_received_at_producer_done: None,
                    output_batches_written_at_producer_done: None,
                    output_bytes_submitted_at_producer_done: None,
                    output_bytes_written_at_producer_done: None,
                    output_debt_bytes_at_producer_done: None,
                    output_debt_batches_at_producer_done: None,
                    ordered_pending_batches_at_producer_done: None,
                    ordered_pending_bytes_at_producer_done: None,
                    next_expected_batch_id_at_producer_done: None,
                    largest_completed_batch_id_at_producer_done: None,
                    completed_ahead_gap_at_producer_done: None,
                    wait_diagnostics: OutputFlowWaitDiagnostics::default(),
                }
            }),
            cv: Condvar::new(),
            started: Instant::now(),
            max_compute_workers,
        }
    }
    fn debt_bytes(inner: &OutputFlowInner) -> u64 {
        inner
            .submitted_output_bytes_estimate
            .saturating_sub(inner.written_output_bytes_estimate)
    }
    fn debt_batches(inner: &OutputFlowInner) -> u64 {
        inner
            .submitted_batches
            .saturating_sub(inner.written_batches)
    }
    fn dynamic_limits(
        inner: &OutputFlowInner,
        max_compute_workers: usize,
    ) -> (u64, u64, u64, usize) {
        let bps = inner.writer_write_bps_ema.max(4.0 * 1024.0 * 1024.0);
        let density = if inner.submitted_batches > 0 {
            inner.submitted_output_bytes_estimate as f64 / inner.submitted_batches as f64
        } else {
            1.0 * 1024.0 * 1024.0
        };
        let target_lag_seconds = if density > 4.0 * 1024.0 * 1024.0 {
            0.35
        } else {
            0.9
        };
        let debt_byte_limit = (bps * target_lag_seconds).max(8.0 * 1024.0 * 1024.0) as u64;
        let debt_batch_limit =
            ((debt_byte_limit as f64 / density.max(256.0)).ceil() as u64).clamp(4, 128);
        let ordered_batch_limit = (debt_batch_limit / 2).max(2);
        let ordered_byte_limit = debt_byte_limit / 2;
        let lag_seconds = Self::debt_bytes(inner) as f64 / bps.max(1.0);
        let adaptive_compute = if lag_seconds > target_lag_seconds * 3.0 {
            1
        } else if lag_seconds > target_lag_seconds * 2.0 {
            (max_compute_workers / 2).max(1)
        } else {
            max_compute_workers.max(1)
        };
        (
            debt_byte_limit.max(1),
            debt_batch_limit.max(1),
            ordered_batch_limit.max(1).min(ordered_byte_limit.max(1)),
            adaptive_compute,
        )
    }
    fn should_wait_input(inner: &OutputFlowInner, max_compute_workers: usize) -> bool {
        let debt_bytes = Self::debt_bytes(inner);
        let debt_batches = Self::debt_batches(inner);
        let (debt_byte_limit, debt_batch_limit, _, _) =
            Self::dynamic_limits(inner, max_compute_workers);
        debt_bytes > debt_byte_limit || debt_batches > debt_batch_limit
    }
    fn should_wait_compute_start(inner: &OutputFlowInner, max_compute_workers: usize) -> bool {
        let debt_bytes = Self::debt_bytes(inner);
        let debt_batches = Self::debt_batches(inner);
        let completed_gap = inner
            .largest_completed_batch_id
            .saturating_sub(inner.next_expected_batch_id);
        let (debt_byte_limit, debt_batch_limit, ordered_limit, adaptive_compute) =
            Self::dynamic_limits(inner, max_compute_workers);
        if debt_bytes > debt_byte_limit || debt_batches > debt_batch_limit {
            return true;
        }
        if inner.ordered_pending_bytes_estimate > debt_byte_limit
            && completed_gap > ordered_limit.saturating_mul(2)
        {
            return true;
        }
        let moderate_pressure = debt_bytes > debt_byte_limit / 2
            || debt_batches > debt_batch_limit / 2
            || inner.ordered_pending_bytes_estimate > debt_byte_limit / 2
            || completed_gap > ordered_limit;
        moderate_pressure && inner.compute_active >= adaptive_compute
    }
    fn should_wait_output_submit(inner: &OutputFlowInner, max_compute_workers: usize) -> bool {
        const OUTPUT_SUBMIT_RECENT_PROGRESS_SECONDS: f64 = 0.24;
        let debt_bytes = Self::debt_bytes(inner);
        let debt_batches = Self::debt_batches(inner);
        let completed_gap = inner
            .largest_completed_batch_id
            .saturating_sub(inner.next_expected_batch_id);
        let (debt_byte_limit, debt_batch_limit, ordered_limit, _) =
            Self::dynamic_limits(inner, max_compute_workers);
        let writer_progress_age = inner.writer_last_progress_time.elapsed().as_secs_f64();
        let writer_recent_progress = writer_progress_age <= OUTPUT_SUBMIT_RECENT_PROGRESS_SECONDS;
        let severe_pressure = debt_bytes > debt_byte_limit.saturating_mul(2)
            || debt_batches > debt_batch_limit.saturating_mul(2)
            || inner.ordered_pending_bytes_estimate > debt_byte_limit.saturating_mul(2)
            || completed_gap > ordered_limit.saturating_mul(2);
        if writer_recent_progress && !severe_pressure {
            return false;
        }
        if debt_bytes > debt_byte_limit || debt_batches > debt_batch_limit {
            return true;
        }
        inner.ordered_pending_bytes_estimate > debt_byte_limit || completed_gap > ordered_limit
    }
    fn wait_gate(&self, wait_kind: OutputFlowWaitKind) -> f64 {
        let start = Instant::now();
        let mut guard = self.inner.lock().expect("flow mutex poisoned");
        let mut waited = false;
        while !guard.producers_done {
            let should_wait = match wait_kind {
                OutputFlowWaitKind::InputEnqueue => {
                    Self::should_wait_input(&guard, self.max_compute_workers)
                }
                OutputFlowWaitKind::ComputeStart => {
                    Self::should_wait_compute_start(&guard, self.max_compute_workers)
                }
                OutputFlowWaitKind::OutputSubmit => {
                    Self::should_wait_output_submit(&guard, self.max_compute_workers)
                }
            };
            if !should_wait {
                break;
            }
            waited = true;
            guard = self.cv.wait(guard).expect("flow condvar wait failed");
        }
        let elapsed = start.elapsed().as_secs_f64();
        if waited {
            let diagnostics = &mut guard.wait_diagnostics;
            match wait_kind {
                OutputFlowWaitKind::InputEnqueue => {
                    diagnostics.input_wait_seconds_total += elapsed;
                    diagnostics.input_wait_events += 1;
                    diagnostics.input_wait_max_seconds =
                        diagnostics.input_wait_max_seconds.max(elapsed);
                }
                OutputFlowWaitKind::ComputeStart => {
                    diagnostics.compute_start_wait_seconds_total += elapsed;
                    diagnostics.compute_start_wait_events += 1;
                    diagnostics.compute_start_wait_max_seconds =
                        diagnostics.compute_start_wait_max_seconds.max(elapsed);
                }
                OutputFlowWaitKind::OutputSubmit => {
                    diagnostics.output_submit_wait_seconds_total += elapsed;
                    diagnostics.output_submit_wait_events += 1;
                    diagnostics.output_submit_wait_max_seconds =
                        diagnostics.output_submit_wait_max_seconds.max(elapsed);
                }
            }
            if elapsed > 1.0 {
                eprintln!(
                    "STAGE output_flow_wait_over_1s method={:?} wait_seconds={:.6} submitted_batches={} received_batches={} written_batches={} debt_bytes={} debt_batches={} ordered_pending_bytes={} next_expected_batch_id={} largest_completed_batch_id={} compute_active={} producers_done={}",
                    wait_kind,
                    elapsed,
                    guard.submitted_batches,
                    guard.received_batches,
                    guard.written_batches,
                    Self::debt_bytes(&guard),
                    Self::debt_batches(&guard),
                    guard.ordered_pending_bytes_estimate,
                    guard.next_expected_batch_id,
                    guard.largest_completed_batch_id,
                    guard.compute_active,
                    guard.producers_done
                );
            }
        }
        elapsed
    }
    fn wait_before_input_batch_enqueue(&self) -> f64 {
        self.wait_gate(OutputFlowWaitKind::InputEnqueue)
    }
    fn wait_before_compute_start(&self) -> f64 {
        let waited = self.wait_gate(OutputFlowWaitKind::ComputeStart);
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.compute_active += 1;
        g.compute_active_min = g.compute_active_min.min(g.compute_active);
        g.compute_active_max = g.compute_active_max.max(g.compute_active);
        g.compute_active_sum += g.compute_active as u64;
        g.compute_active_samples += 1;
        waited
    }
    fn on_compute_done(&self) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.compute_active = g.compute_active.saturating_sub(1);
        self.cv.notify_all();
    }
    fn wait_before_output_submit(&self) -> f64 {
        self.wait_gate(OutputFlowWaitKind::OutputSubmit)
    }
    fn on_output_submitted(&self, batch_id: u64, estimated_output_bytes: u64) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.submitted_batches += 1;
        g.submitted_output_bytes_estimate += estimated_output_bytes;
        g.queued_batches
            .insert(batch_id, (estimated_output_bytes, Instant::now()));
        g.compute_to_writer_queue_max_depth = g
            .compute_to_writer_queue_max_depth
            .max(g.submitted_batches.saturating_sub(g.received_batches));
        g.output_debt_max_bytes = g.output_debt_max_bytes.max(Self::debt_bytes(&g));
        g.output_debt_max_batches = g.output_debt_max_batches.max(Self::debt_batches(&g));
        self.cv.notify_all();
    }
    fn on_writer_received(&self, batch_id: u64) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.received_batches += 1;
        if batch_id > g.next_expected_batch_id {
            let gap = batch_id.saturating_sub(g.next_expected_batch_id);
            g.completed_ahead_gap_max = g.completed_ahead_gap_max.max(gap);
        }
        let ordered_pending = g.received_batches.saturating_sub(g.next_expected_batch_id);
        g.ordered_pending_max_batches = g.ordered_pending_max_batches.max(ordered_pending);
        g.ordered_pending_max_bytes = g
            .ordered_pending_max_bytes
            .max(g.ordered_pending_bytes_estimate);
        self.cv.notify_all();
    }
    fn on_writer_ordered_pending(&self, estimated_output_bytes: u64) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.ordered_pending_bytes_estimate += estimated_output_bytes;
        g.ordered_pending_max_bytes = g
            .ordered_pending_max_bytes
            .max(g.ordered_pending_bytes_estimate);
        self.cv.notify_all();
    }
    fn on_writer_batch_written(
        &self,
        batch_id: u64,
        estimated_output_bytes: u64,
        write_elapsed_seconds: f64,
    ) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        g.written_batches += 1;
        g.written_output_bytes_estimate += estimated_output_bytes;
        g.next_expected_batch_id = batch_id.saturating_add(1);
        g.largest_completed_batch_id = g.largest_completed_batch_id.max(batch_id);
        g.ordered_pending_bytes_estimate = g
            .ordered_pending_bytes_estimate
            .saturating_sub(estimated_output_bytes);
        g.queued_batches.remove(&batch_id);
        if write_elapsed_seconds > 0.0 && estimated_output_bytes > 0 {
            let bps = estimated_output_bytes as f64 / write_elapsed_seconds;
            g.writer_write_bps_ema = if g.writer_write_bps_ema == 0.0 {
                bps
            } else {
                g.writer_write_bps_ema * 0.8 + bps * 0.2
            };
        }
        g.writer_last_progress_time = Instant::now();
        let debt_seconds =
            Self::debt_bytes(&g) as f64 / g.writer_write_bps_ema.max(4.0 * 1024.0 * 1024.0);
        g.debt_seconds_sum += debt_seconds;
        g.debt_seconds_samples += 1;
        g.output_debt_max_seconds = g.output_debt_max_seconds.max(debt_seconds);
        self.cv.notify_all();
    }
    fn on_producers_done(&self) {
        let mut g = self.inner.lock().expect("flow mutex poisoned");
        if !g.producers_done {
            g.producers_done = true;
            g.output_batches_submitted_at_producer_done = Some(g.submitted_batches);
            g.output_batches_received_at_producer_done = Some(g.received_batches);
            g.output_batches_written_at_producer_done = Some(g.written_batches);
            g.output_bytes_submitted_at_producer_done = Some(g.submitted_output_bytes_estimate);
            g.output_bytes_written_at_producer_done = Some(g.written_output_bytes_estimate);
            g.output_debt_bytes_at_producer_done = Some(Self::debt_bytes(&g));
            g.output_debt_batches_at_producer_done = Some(Self::debt_batches(&g));
            g.ordered_pending_batches_at_producer_done =
                Some(g.received_batches.saturating_sub(g.next_expected_batch_id));
            g.ordered_pending_bytes_at_producer_done = Some(g.ordered_pending_bytes_estimate);
            g.next_expected_batch_id_at_producer_done = Some(g.next_expected_batch_id);
            g.largest_completed_batch_id_at_producer_done = Some(g.largest_completed_batch_id);
            g.completed_ahead_gap_at_producer_done = Some(
                g.largest_completed_batch_id
                    .saturating_sub(g.next_expected_batch_id),
            );
        }
        self.cv.notify_all();
    }
    fn snapshot(&self) -> OutputFlowSnapshot {
        let g = self.inner.lock().expect("flow mutex poisoned");
        let debt_seconds_mean = if g.debt_seconds_samples > 0 {
            g.debt_seconds_sum / g.debt_seconds_samples as f64
        } else {
            0.0
        };
        let oldest_unwritten_batch_age_seconds = g
            .queued_batches
            .values()
            .map(|(_, t)| t.elapsed().as_secs_f64())
            .fold(0.0, f64::max);
        OutputFlowSnapshot {
            submitted_batches: g.submitted_batches,
            received_batches: g.received_batches,
            written_batches: g.written_batches,
            submitted_output_bytes_estimate: g.submitted_output_bytes_estimate,
            written_output_bytes_estimate: g.written_output_bytes_estimate,
            output_debt_batches: Self::debt_batches(&g),
            output_debt_bytes: Self::debt_bytes(&g),
            output_debt_seconds: Self::debt_bytes(&g) as f64
                / g.writer_write_bps_ema.max(4.0 * 1024.0 * 1024.0),
            output_debt_max_bytes: g.output_debt_max_bytes,
            output_debt_max_batches: g.output_debt_max_batches,
            output_debt_max_seconds: g.output_debt_max_seconds,
            output_debt_mean_seconds: debt_seconds_mean,
            next_expected_batch_id: g.next_expected_batch_id,
            largest_completed_batch_id: g.largest_completed_batch_id,
            completed_ahead_gap: g
                .largest_completed_batch_id
                .saturating_sub(g.next_expected_batch_id),
            completed_ahead_gap_max: g.completed_ahead_gap_max,
            ordered_pending_batches: g.received_batches.saturating_sub(g.next_expected_batch_id),
            ordered_pending_bytes_estimate: g.ordered_pending_bytes_estimate,
            ordered_pending_max_batches: g.ordered_pending_max_batches,
            ordered_pending_max_bytes: g.ordered_pending_max_bytes,
            compute_to_writer_queue_depth: g.submitted_batches.saturating_sub(g.received_batches),
            compute_to_writer_queue_max_depth: g.compute_to_writer_queue_max_depth,
            oldest_unwritten_batch_age_seconds,
            writer_write_bps_ema: g.writer_write_bps_ema,
            writer_last_progress_age_seconds: g.writer_last_progress_time.elapsed().as_secs_f64(),
            producers_done: g.producers_done,
            output_batches_submitted_at_producer_done: g
                .output_batches_submitted_at_producer_done
                .unwrap_or(0),
            output_batches_received_at_producer_done: g
                .output_batches_received_at_producer_done
                .unwrap_or(0),
            output_batches_written_at_producer_done: g
                .output_batches_written_at_producer_done
                .unwrap_or(0),
            output_bytes_submitted_at_producer_done: g
                .output_bytes_submitted_at_producer_done
                .unwrap_or(0),
            output_bytes_written_at_producer_done: g
                .output_bytes_written_at_producer_done
                .unwrap_or(0),
            output_debt_bytes_at_producer_done: g.output_debt_bytes_at_producer_done.unwrap_or(0),
            output_debt_batches_at_producer_done: g
                .output_debt_batches_at_producer_done
                .unwrap_or(0),
            ordered_pending_batches_at_producer_done: g
                .ordered_pending_batches_at_producer_done
                .unwrap_or(0),
            ordered_pending_bytes_at_producer_done: g
                .ordered_pending_bytes_at_producer_done
                .unwrap_or(0),
            next_expected_batch_id_at_producer_done: g
                .next_expected_batch_id_at_producer_done
                .unwrap_or(0),
            largest_completed_batch_id_at_producer_done: g
                .largest_completed_batch_id_at_producer_done
                .unwrap_or(0),
            completed_ahead_gap_at_producer_done: g
                .completed_ahead_gap_at_producer_done
                .unwrap_or(0),
            input_wait_seconds_total: g.wait_diagnostics.input_wait_seconds_total,
            input_wait_events: g.wait_diagnostics.input_wait_events,
            input_wait_max_seconds: g.wait_diagnostics.input_wait_max_seconds,
            compute_start_wait_seconds_total: g.wait_diagnostics.compute_start_wait_seconds_total,
            compute_start_wait_events: g.wait_diagnostics.compute_start_wait_events,
            compute_start_wait_max_seconds: g.wait_diagnostics.compute_start_wait_max_seconds,
            output_submit_wait_seconds_total: g.wait_diagnostics.output_submit_wait_seconds_total,
            output_submit_wait_events: g.wait_diagnostics.output_submit_wait_events,
            output_submit_wait_max_seconds: g.wait_diagnostics.output_submit_wait_max_seconds,
        }
    }
}

fn direct_writer_thread(
    batch_receiver: Receiver<DirectOutputBatch>,
    mut output: Writer,
    output_path: PathBuf,
    output_queue_received: Arc<AtomicU64>,
    output_bytes_submitted: Arc<AtomicU64>,
    output_bytes_written: Arc<AtomicU64>,
    writer_bytes_per_second_estimate: Arc<AtomicU64>,
    writer_last_progress_micros: Arc<AtomicU64>,
    ordered_writer_next_expected_batch_id: Arc<AtomicU64>,
    flow_controller: Arc<OutputFlowController>,
    runtime_config: DirectWriterRuntimeConfig,
    pipeline_start: Instant,
    lifecycle: Arc<PipelineLifecycleMarkers>,
) -> Result<DirectWorkerStats> {
    mark_elapsed_once(&lifecycle.writer_thread_started_micros, pipeline_start);
    let writer_loop_start = Instant::now();
    writer_last_progress_micros.store(0, Ordering::Relaxed);
    let mut worker_stats = DirectWorkerStats::default();
    let mut next_batch_id = 0u64;
    let mut pending: BTreeMap<u64, DirectOutputBatch> = BTreeMap::new();
    let mut waiting_for_batch_counts: HashMap<u64, u64> = HashMap::new();
    let mut drain_controller = OutputDrainController::new(runtime_config.drain_min_base_bytes);
    loop {
        let recv_wait_start = Instant::now();
        let output_batch = match batch_receiver.recv() {
            Ok(batch) => batch,
            Err(_) => break,
        };
        mark_elapsed_once(
            &lifecycle.first_writer_batch_received_micros,
            pipeline_start,
        );
        lifecycle.writer_last_batch_received_micros.store(
            pipeline_start
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let output_batch_bytes_estimate = estimate_output_batch_bytes(&output_batch);
        flow_controller.on_writer_received(output_batch.batch_id);
        output_queue_received.fetch_add(1, Ordering::Relaxed);
        worker_stats.writer_recv_wait_seconds += recv_wait_start.elapsed().as_secs_f64();
        worker_stats.ordered_writer_largest_received_batch_id = worker_stats
            .ordered_writer_largest_received_batch_id
            .max(output_batch.batch_id);
        if output_batch.batch_id > next_batch_id {
            worker_stats.ordered_writer_missing_batch_wait_events += 1;
            worker_stats.ordered_writer_max_gap = worker_stats
                .ordered_writer_max_gap
                .max(output_batch.batch_id - next_batch_id);
            *waiting_for_batch_counts.entry(next_batch_id).or_insert(0) += 1;
        }
        worker_stats.max_completed_batch_gap_at_writer = worker_stats
            .max_completed_batch_gap_at_writer
            .max(output_batch.batch_id.saturating_sub(next_batch_id));
        pending.insert(output_batch.batch_id, output_batch);
        flow_controller.on_writer_ordered_pending(output_batch_bytes_estimate);
        worker_stats.ordered_writer_pending_batches_max = worker_stats
            .ordered_writer_pending_batches_max
            .max(pending.len());
        worker_stats.ordered_writer_pending_map_max_size = worker_stats
            .ordered_writer_pending_map_max_size
            .max(pending.len());
        let order_wait_start = Instant::now();
        while let Some(batch) = pending.remove(&next_batch_id) {
            worker_stats.ordered_writer_wait_for_next_batch_seconds +=
                order_wait_start.elapsed().as_secs_f64();
            worker_stats.batches_processed += 1;
            worker_stats.total_batch_size += batch.stats.groups;
            worker_stats.max_batch_size =
                worker_stats.max_batch_size.max(batch.stats.groups as usize);
            worker_stats.pair_stats.groups += batch.stats.groups;
            worker_stats.pair_stats.candidate_groups_fwd += batch.stats.candidate_groups_fwd;
            worker_stats.pair_stats.candidate_groups_rev += batch.stats.candidate_groups_rev;
            worker_stats.pair_stats.candidate_pairs += batch.stats.candidate_pairs;
            worker_stats.pair_stats.missing_candidate += batch.stats.missing_candidate;
            worker_stats.pair_stats.low_mapq += batch.stats.low_mapq;
            worker_stats.pair_stats.final_pairs += batch.stats.final_pairs;
            let write_start = Instant::now();
            let mut batch_bytes_written = 0u64;
            for (f_record, r_record) in &batch.records {
                output
                    .write(f_record)
                    .context("failed to write direct output forward record")?;
                output
                    .write(r_record)
                    .context("failed to write direct output reverse record")?;
                worker_stats.records_written += 2;
                let pair_bytes = (f_record.inner().l_data + r_record.inner().l_data) as u64;
                worker_stats.estimated_uncompressed_bytes_written += pair_bytes;
                batch_bytes_written += pair_bytes;
            }
            let batch_write_seconds = write_start.elapsed().as_secs_f64();
            worker_stats.write_call_seconds += batch_write_seconds;
            output_bytes_written.fetch_add(batch_bytes_written, Ordering::Relaxed);
            flow_controller.on_writer_batch_written(
                batch.batch_id,
                batch_bytes_written,
                batch_write_seconds,
            );
            mark_elapsed_once(&lifecycle.first_writer_batch_written_micros, pipeline_start);
            lifecycle.writer_last_batch_written_micros.store(
                pipeline_start
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            writer_last_progress_micros.store(
                pipeline_start.elapsed().as_micros() as u64,
                Ordering::Relaxed,
            );
            if batch_write_seconds > 0.0 && batch_bytes_written > 0 {
                let instant_bps = (batch_bytes_written as f64 / batch_write_seconds) as u64;
                let previous_bps = writer_bytes_per_second_estimate.load(Ordering::Relaxed);
                let next_bps = if previous_bps == 0 {
                    instant_bps
                } else {
                    ((previous_bps * 7) + instant_bps) / 8
                };
                writer_bytes_per_second_estimate.store(next_bps, Ordering::Relaxed);
            }
            let writer_bps = writer_bytes_per_second_estimate.load(Ordering::Relaxed);
            let submitted_output_bytes = output_bytes_submitted.load(Ordering::Relaxed);
            let written_output_bytes = output_bytes_written.load(Ordering::Relaxed);
            let output_debt_bytes = submitted_output_bytes.saturating_sub(written_output_bytes);
            let output_debt_seconds =
                output_debt_bytes as f64 / writer_bps.max(4 * 1024 * 1024) as f64;
            let last_progress_micros = writer_last_progress_micros.load(Ordering::Relaxed);
            let writer_progress_age_seconds = if last_progress_micros == 0 {
                pipeline_start.elapsed().as_secs_f64()
            } else {
                (pipeline_start.elapsed().as_micros() as u64).saturating_sub(last_progress_micros)
                    as f64
                    / 1e6f64
            };
            if let Some(probe_reason) = drain_controller.should_probe(
                worker_stats.estimated_uncompressed_bytes_written,
                writer_bps,
                pending.len(),
                output_debt_bytes,
                output_debt_seconds,
                writer_progress_age_seconds,
                &runtime_config,
            ) {
                if probe_reason == "pending_batches_pressure" {
                    if let Some(skip_reason) = should_skip_pending_batches_probe(
                        output_debt_seconds,
                        pending.len(),
                        writer_progress_age_seconds,
                        &worker_stats,
                    ) {
                        worker_stats.writer_probe_skipped_count += 1;
                        if worker_stats.writer_probe_skip_reason.is_empty() {
                            worker_stats.writer_probe_skip_reason = skip_reason.to_string();
                        }
                        worker_stats.writer_probe_last_skip_reason = skip_reason.to_string();
                        worker_stats.writer_probe_skipped_output_debt_seconds = output_debt_seconds;
                        worker_stats.writer_probe_skipped_pending_batches = pending.len() as u64;
                        worker_stats.writer_probe_skipped_writer_progress_age_seconds =
                            writer_progress_age_seconds;
                        worker_stats.writer_probe_skipped_estimated_bytes =
                            worker_stats.estimated_uncompressed_bytes_written;
                        drain_controller.on_probe_skipped();
                        next_batch_id += 1;
                        ordered_writer_next_expected_batch_id
                            .store(next_batch_id, Ordering::Relaxed);
                        continue;
                    }
                }
                let checkpoint_start = Instant::now();
                // rust-htslib::bam::Writer does not expose a flush API in rust-htslib 0.51.0.
                // Keep periodic diagnostics by sampling observable on-disk progress instead.
                let periodic_output_bytes_before = fs::metadata(&output_path)
                    .map(|meta| meta.len())
                    .unwrap_or(0);
                let periodic_output_bytes_after = fs::metadata(&output_path)
                    .map(|meta| meta.len())
                    .unwrap_or(periodic_output_bytes_before);
                let checkpoint_micros = checkpoint_start
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
                worker_stats.writer_periodic_flush_seconds += checkpoint_micros as f64 / 1e6f64;
                worker_stats.writer_periodic_flush_count += 1;
                worker_stats.writer_probe_executed_count += 1;
                worker_stats.writer_probe_reason = probe_reason.to_string();
                worker_stats.writer_probe_started_with_output_debt_bytes = output_debt_bytes;
                worker_stats.writer_probe_started_with_output_debt_seconds = output_debt_seconds;
                worker_stats.writer_probe_started_with_pending_batches = pending.len() as u64;
                worker_stats.writer_probe_started_with_writer_progress_age_seconds =
                    writer_progress_age_seconds;
                worker_stats.writer_probe_elapsed_seconds += checkpoint_micros as f64 / 1e6f64;
                worker_stats.writer_probe_changed_output_bytes = worker_stats
                    .writer_probe_changed_output_bytes
                    .saturating_add(
                        periodic_output_bytes_after.saturating_sub(periodic_output_bytes_before),
                    );
                drain_controller.on_probe(
                    worker_stats.estimated_uncompressed_bytes_written,
                    checkpoint_micros,
                    writer_bps,
                    &runtime_config,
                );
            }
            next_batch_id += 1;
            ordered_writer_next_expected_batch_id.store(next_batch_id, Ordering::Relaxed);
        }
        worker_stats.ordered_writer_next_expected_batch_id = next_batch_id;
    }
    if !pending.is_empty() {
        bail!("ordered writer terminated with pending out-of-order batches");
    }
    let mut waiting_for_top: Vec<(u64, u64)> = waiting_for_batch_counts.into_iter().collect();
    waiting_for_top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    worker_stats.ordered_writer_waiting_for_batch_topn = waiting_for_top
        .into_iter()
        .take(8)
        .map(|(batch_id, events)| format!("{batch_id}:{events}"))
        .collect::<Vec<_>>()
        .join("|");

    worker_stats.writer_loop_seconds = writer_loop_start.elapsed().as_secs_f64();
    worker_stats.pending_batches_before_close = pending.len();
    worker_stats.records_written_before_close = worker_stats.records_written;
    worker_stats.output_bytes_before_close = fs::metadata(&output_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let pre_close_checkpoint_start = Instant::now();
    // No explicit flush API is available for bam::Writer here; close/drop handles finalization.
    let _pre_close_output_bytes = fs::metadata(&output_path)
        .map(|meta| meta.len())
        .unwrap_or(worker_stats.output_bytes_before_close);
    worker_stats.writer_pre_close_flush_seconds =
        pre_close_checkpoint_start.elapsed().as_secs_f64();
    mark_elapsed_once(&lifecycle.writer_finalize_start_micros, pipeline_start);
    let output_drop_start = Instant::now();
    drop(output);
    worker_stats.output_drop_close_seconds = output_drop_start.elapsed().as_secs_f64();
    worker_stats.hts_close_seconds = worker_stats.output_drop_close_seconds;
    worker_stats.output_bytes_after_close = fs::metadata(&output_path)
        .map(|meta| meta.len())
        .unwrap_or(worker_stats.output_bytes_before_close);
    let sync_start = Instant::now();
    worker_stats.file_sync_or_drop_seconds = sync_start.elapsed().as_secs_f64();
    mark_elapsed_once(&lifecycle.writer_finalize_done_micros, pipeline_start);
    worker_stats.bgzf_flush_seconds = 0.0;
    worker_stats.output_finalize_non_tail_seconds = worker_stats.writer_periodic_flush_seconds
        + worker_stats.writer_pre_close_flush_seconds
        + worker_stats.hts_close_seconds;
    mark_elapsed_once(&lifecycle.writer_thread_exit_micros, pipeline_start);
    Ok(worker_stats)
}

fn percentile_seconds(samples: &mut [f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(CmpOrdering::Equal));
    let idx = (((samples.len() - 1) as f64) * (percentile / 100.0)).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

fn classify_primary_bottleneck(parts: &[(&'static str, f64)]) -> &'static str {
    let mut sorted = parts.to_vec();
    sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(CmpOrdering::Equal));
    if sorted.is_empty() || sorted[0].1 <= 0.0 {
        return "unknown";
    }
    if sorted.len() > 1 && sorted[1].1 > sorted[0].1 * 0.75 {
        return "mixed";
    }
    sorted[0].0
}

const READER_CHUNK_INTERVAL_SAMPLE_WINDOW: usize = 4096;

fn push_bounded_sample(samples: &mut Vec<f64>, cursor: &mut usize, value: f64, max_len: usize) {
    if max_len == 0 {
        return;
    }
    if samples.len() < max_len {
        samples.push(value);
        return;
    }
    let index = *cursor % max_len;
    samples[index] = value;
    *cursor = (*cursor + 1) % max_len;
}

fn sample_percentile(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut copy = samples.to_vec();
    percentile_seconds(&mut copy, percentile)
}

fn read_max_rss_kb() -> u64 {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("VmHWM:") {
            return value
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    0
}

fn format_u64_slice(values: &[u64]) -> String {
    values
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

fn format_f64_slice(values: &[f64]) -> String {
    values
        .iter()
        .map(|v| format!("{v:.6}"))
        .collect::<Vec<_>>()
        .join("|")
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
        batch_id: batch.batch_id,
        records: selected,
        stats,
        filter_seconds: process_start.elapsed().as_secs_f64(),
    }
}

fn approx_record_payload_bytes(record: &Record) -> u64 {
    let seq_len = record.seq_len() as u64;
    let qual_len = record.qual().len() as u64;
    let qname_len = record.qname().len() as u64;
    let cigar_ops = record.cigar().len() as u64;
    qname_len + seq_len + qual_len + (cigar_ops * 4)
}

fn decrement_depth(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
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
        let hts_read_start = Instant::now();
        let read_result = reader.read(scratch);
        reader_stats.htslib_read_seconds += hts_read_start.elapsed().as_secs_f64();
        match read_result {
            Some(read_result) => {
                read_result.context("failed to read BAM record")?;
                std::mem::replace(scratch, Record::new())
            }
            None => return Ok(None),
        }
    };
    let group_build_start = Instant::now();
    let mut group = DirectRecordGroup::new(first);
    reader_stats.records_decoded += 1;
    loop {
        let hts_read_start = Instant::now();
        let read_result = reader.read(scratch);
        reader_stats.htslib_read_seconds += hts_read_start.elapsed().as_secs_f64();
        match read_result {
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
    reader_stats.group_build_seconds += group_build_start.elapsed().as_secs_f64();
    let decode_elapsed = decode_start.elapsed().as_secs_f64();
    reader_stats.decode_seconds += decode_elapsed;
    reader_stats.decode_only_seconds += decode_elapsed;
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
    let mut compute_workers = if requested < 16 {
        1
    } else if requested < 64 {
        2
    } else if requested < 128 {
        4
    } else {
        8
    };
    if compute_workers >= resolved_total_threads {
        compute_workers = resolved_total_threads.saturating_sub(1).max(1);
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

fn resolve_direct_queue_policy(requested_threads: usize, cli: &Cli) -> DirectQueuePolicy {
    let bounded_threads = requested_threads.max(1);
    let output_queue_capacity = if bounded_threads <= 32 {
        8
    } else if bounded_threads <= 128 {
        16
    } else {
        24
    };
    let thread_capacity_base = (6.0 + (bounded_threads as f64).sqrt()).round() as usize;
    let chunk_group_penalty = if cli.direct_reader_chunk_groups >= 1024 {
        3usize
    } else if cli.direct_reader_chunk_groups >= 768 {
        2usize
    } else {
        0usize
    };
    let reader_queue_capacity = thread_capacity_base
        .saturating_sub(chunk_group_penalty)
        .clamp(4, 16);
    DirectQueuePolicy {
        output_queue_capacity,
        reader_queue_capacity,
        reader_chunk_groups: cli.direct_reader_chunk_groups,
        batch_size: cli.direct_batch_size,
    }
}

fn resolve_direct_writer_runtime_config(
    requested_threads: usize,
    intended_writer_bgzf_workers: usize,
) -> DirectWriterRuntimeConfig {
    let bounded_threads = requested_threads.max(1) as u64;
    let writer_workers = intended_writer_bgzf_workers.max(1) as u64;
    let flow_target_backlog_seconds = if bounded_threads >= 64 { 0.18 } else { 0.24 };
    let flow_min_inflight_bytes = (8 * 1024 * 1024u64).saturating_mul(writer_workers.min(8));
    let flow_max_inflight_bytes = (64 * 1024 * 1024u64)
        .saturating_mul(writer_workers.max(1))
        .min(1024 * 1024 * 1024u64);
    let flow_max_queue_backlog_batches = (writer_workers * 2).clamp(4, 32);
    let flow_stale_progress_micros = 100_000;
    let flow_wait_poll_micros = 75;
    let flow_soft_queue_backlog_batches = (flow_max_queue_backlog_batches / 2).max(2);
    let drain_min_interval_micros = if bounded_threads >= 64 {
        10_000_000
    } else {
        15_000_000
    };
    let drain_min_base_bytes = (1024 * 1024 * 1024u64)
        .saturating_mul(writer_workers.min(4))
        .max(flow_max_inflight_bytes.saturating_mul(2));
    let drain_bytes_per_probe_second =
        (128 * 1024 * 1024u64).saturating_mul(writer_workers.max(1).min(8));
    let drain_expensive_threshold_micros = 1_000_000;
    let drain_backoff_shift_max = 6;
    DirectWriterRuntimeConfig {
        flow_target_backlog_seconds,
        flow_min_inflight_bytes,
        flow_max_inflight_bytes,
        flow_max_queue_backlog_batches,
        flow_stale_progress_micros,
        flow_wait_poll_micros,
        flow_soft_queue_backlog_batches,
        drain_min_interval_micros,
        drain_min_base_bytes,
        drain_bytes_per_probe_second,
        drain_expensive_threshold_micros,
        drain_backoff_shift_max,
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
        DirectReaderMode::ParallelChunked => "parallel_chunked",
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
    fn direct_thread_roles_use_conservative_compute_tiers() {
        let tier_8 = resolve_direct_thread_roles(8);
        assert_eq!(tier_8.compute_workers, 1);

        let tier_32 = resolve_direct_thread_roles(32);
        assert_eq!(
            tier_32.compute_workers,
            if tier_32.resolved_total_threads > 2 {
                2
            } else {
                1
            }
        );

        let tier_96 = resolve_direct_thread_roles(96);
        assert_eq!(
            tier_96.compute_workers,
            if tier_96.resolved_total_threads > 4 {
                4
            } else {
                tier_96.resolved_total_threads.saturating_sub(1).max(1)
            }
        );

        let tier_128 = resolve_direct_thread_roles(128);
        assert_eq!(
            tier_128.compute_workers,
            if tier_128.resolved_total_threads > 8 {
                8
            } else {
                tier_128.resolved_total_threads.saturating_sub(1).max(1)
            }
        );
        assert!(tier_128.htslib_pool_enabled);
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

    #[test]
    fn output_flow_controller_backpressure_and_release() {
        let ctl = Arc::new(OutputFlowController::new(2));
        for id in 0..12 {
            ctl.on_output_submitted(id, 4 * 1024 * 1024);
        }
        let waiter = {
            let c = Arc::clone(&ctl);
            thread::spawn(move || c.wait_before_output_submit())
        };
        thread::sleep(Duration::from_millis(10));
        for id in 0..12 {
            ctl.on_writer_received(id);
            ctl.on_writer_batch_written(id, 4 * 1024 * 1024, 0.002);
        }
        let waited = waiter.join().expect("waiter join");
        assert!(waited >= 0.0);
    }

    #[test]
    fn output_flow_controller_ordered_backlog_and_producer_done() {
        let ctl = Arc::new(OutputFlowController::new(1));
        ctl.on_output_submitted(10, 1024 * 1024);
        ctl.on_writer_received(10);
        ctl.on_writer_ordered_pending(1024 * 1024);
        let waiter = {
            let c = Arc::clone(&ctl);
            thread::spawn(move || c.wait_before_compute_start())
        };
        thread::sleep(Duration::from_millis(5));
        ctl.on_producers_done();
        let waited = waiter.join().expect("waiter join");
        assert!(waited >= 0.0);
    }

    #[test]
    fn output_flow_controller_density_profiles() {
        let high = OutputFlowController::new(2);
        for id in 0..8 {
            high.on_output_submitted(id, 8 * 1024 * 1024);
        }
        let high_snap = high.snapshot();
        assert!(high_snap.output_debt_max_bytes >= 64 * 1024 * 1024);

        let low = OutputFlowController::new(4);
        for id in 0..32 {
            low.on_output_submitted(id, 16 * 1024);
            low.on_writer_received(id);
            low.on_writer_batch_written(id, 16 * 1024, 0.0001);
        }
        let low_snap = low.snapshot();
        assert_eq!(low_snap.written_batches, 32);
        assert!(low_snap.output_debt_mean_seconds >= 0.0);
    }
}
