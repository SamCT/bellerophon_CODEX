#!/usr/bin/env python3
import argparse
import csv
import os
import re


TIME_PATTERNS = {
    'wall_clock': r'Elapsed \(wall clock\) time .*: (.+)',
    'user_seconds': r'User time \(seconds\): ([0-9.]+)',
    'system_seconds': r'System time \(seconds\): ([0-9.]+)',
    'cpu_percent': r'Percent of CPU this job got: ([0-9]+)%',
    'max_rss_kb': r'Maximum resident set size \(kbytes\): ([0-9]+)',
    'fs_inputs': r'File system inputs: ([0-9]+)',
    'fs_outputs': r'File system outputs: ([0-9]+)',
    'exit_status': r'Exit status: ([0-9]+)',
}


def read_text(path):
    if not os.path.exists(path):
        return ''
    with open(path, 'r', encoding='utf-8') as fh:
        return fh.read()


def elapsed_to_seconds(elapsed):
    if not elapsed:
        return ''
    parts = elapsed.split(':')
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return str((int(hours) * 3600) + (int(minutes) * 60) + float(seconds))
    if len(parts) == 2:
        minutes, seconds = parts
        return str((int(minutes) * 60) + float(seconds))
    return str(float(elapsed))


def parse_time_metrics(path):
    text = read_text(path)
    metrics = {}
    for key, pattern in TIME_PATTERNS.items():
        match = re.search(pattern, text)
        metrics[key] = match.group(1) if match else ''
    metrics['wall_seconds'] = elapsed_to_seconds(metrics['wall_clock'])
    return metrics


def parse_stage_metrics(path):
    text = read_text(path)
    row = {}

    filter_matches = re.findall(
        r'STAGE filter input=(\S+) processed=(\d+) written=(\d+) selected_groups=(\d+) placeholder_groups=(\d+) seconds=([0-9.]+) temp=(\S+) temp_mb=([0-9.]+)',
        text,
    )
    if len(filter_matches) >= 2:
        row['filter_fwd_s'] = filter_matches[0][5]
        row['filter_rev_s'] = filter_matches[1][5]
        row['temp_fwd_mb'] = filter_matches[0][7]
        row['temp_rev_mb'] = filter_matches[1][7]
        row['selected_groups_fwd'] = filter_matches[0][3]
        row['selected_groups_rev'] = filter_matches[1][3]
        row['placeholder_groups_fwd'] = filter_matches[0][4]
        row['placeholder_groups_rev'] = filter_matches[1][4]

    pair_filter = re.search(
        r'STAGE pair_filter groups=(\d+) candidate_pairs=(\d+) selected_groups_fwd=(\d+) selected_groups_rev=(\d+) missing_candidate=(\d+) low_mapq=(\d+) mismatched=(\d+) seconds=([0-9.]+)',
        text,
    )
    if pair_filter:
        row['pair_groups'] = pair_filter.group(1)
        row['candidate_pairs'] = pair_filter.group(2)
        row['selected_groups_fwd'] = pair_filter.group(3)
        row['selected_groups_rev'] = pair_filter.group(4)
        row['missing_candidate'] = pair_filter.group(5)
        row['low_mapq_skips'] = pair_filter.group(6)
        row['mismatched_skips'] = pair_filter.group(7)
        row['pair_filter_s'] = pair_filter.group(8)

    pair_temp = re.search(
        r'STAGE pair_temp pairs=(\d+) seconds=([0-9.]+) temp_fwd=(\S+) temp_rev=(\S+) temp_fwd_mb=([0-9.]+) temp_rev_mb=([0-9.]+)',
        text,
    )
    if pair_temp:
        row['final_pairs'] = pair_temp.group(1)
        row['pair_temp_s'] = pair_temp.group(2)
        row['temp_fwd_mb'] = pair_temp.group(5)
        row['temp_rev_mb'] = pair_temp.group(6)

    merge_match = re.search(
        r'STAGE merge pairs=(\d+) mismatched=(\d+) unmapped=(\d+) low_mapq=(\d+) seconds=([0-9.]+) output=(\S+) output_mb=([0-9.]+)',
        text,
    )
    if merge_match:
        row['final_pairs'] = merge_match.group(1)
        row['mismatched_skips'] = merge_match.group(2)
        row['unmapped_skips'] = merge_match.group(3)
        row['low_mapq_skips'] = merge_match.group(4)
        row['merge_s'] = merge_match.group(5)
        row['output_mb'] = merge_match.group(7)

    direct = re.search(
        r'STAGE direct pairs=(\d+) seconds=([0-9.]+) output=(\S+) output_mb=([0-9.]+)',
        text,
    )
    if direct:
        row['final_pairs'] = direct.group(1)
        row['direct_s'] = direct.group(2)
        row['output_mb'] = direct.group(4)

    def parse_stage_kv(stage_name):
        match = re.search(rf'^STAGE {stage_name} (.+)$', text, flags=re.MULTILINE)
        if not match:
            return {}
        values = {}
        for part in match.group(1).split():
            if '=' not in part:
                continue
            key, value = part.split('=', 1)
            values[key] = value
        return values

    row.update(parse_stage_kv('direct_output_flow_summary'))
    row.update(parse_stage_kv('writer_tail_breakdown'))
    row.update(parse_stage_kv('output_flow_controller_summary'))
    row.update(parse_stage_kv('direct_total_summary'))

    legacy_total = parse_stage_kv('direct_summary').get('total_output_drain_seconds')
    if legacy_total and 'writer_tail_seconds' in row:
        row['legacy_total_output_drain_seconds'] = legacy_total
        row['total_output_drain_seconds_rejected'] = 'true'

    return row


def infer_pipeline(runner):
    if runner == 'python':
        return 'legacy-temp'
    if runner == 'rust_legacy_temp':
        return 'legacy-temp'
    if runner == 'rust_pair_temp':
        return 'pair-temp'
    if runner == 'rust_direct':
        return 'direct'
    return ''


def main():
    parser = argparse.ArgumentParser(description='Collect hqsub perf matrix logs into one CSV.')
    parser.add_argument('--matrix-dir', required=True, help='Directory containing per-run subdirectories.')
    parser.add_argument('--output-csv', default='perf_matrix_summary.csv')
    args = parser.parse_args()

    rows = []
    for run_id in sorted(os.listdir(args.matrix_dir)):
        run_dir = os.path.join(args.matrix_dir, run_id)
        if not os.path.isdir(run_dir):
            continue

        # New format: r<runner>_s<strategy>_q<q>_t<t>
        match = re.match(r'r([^_]+(?:_[^_]+)*)_s([^_]+)_q([0-9]+)_t([0-9]+)$', run_id)
        if match:
            runner, strategy, quality, threads = match.groups()
        else:
            # Backward compatibility with old format: s<strategy>_q<q>_t<t>
            old_match = re.match(r's([^_]+)_q([0-9]+)_t([0-9]+)$', run_id)
            if not old_match:
                continue
            strategy, quality, threads = old_match.groups()
            runner = 'python'

        row = {
            'run_id': run_id,
            'runner': runner,
            'pipeline': infer_pipeline(runner),
            'strategy': strategy,
            'quality': quality,
            'threads': threads,
        }
        row.update(parse_time_metrics(os.path.join(run_dir, 'time.txt')))
        row.update(parse_stage_metrics(os.path.join(run_dir, 'run.log')))
        rows.append(row)

    fieldnames = sorted({k for row in rows for k in row.keys()})
    with open(args.output_csv, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print('wrote', args.output_csv, 'rows', len(rows))


if __name__ == '__main__':
    main()
