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


def parse_time_metrics(path):
    text = read_text(path)
    metrics = {}
    for key, pattern in TIME_PATTERNS.items():
        match = re.search(pattern, text)
        metrics[key] = match.group(1) if match else ''
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
    return row


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
        match = re.match(r's([^_]+)_q([0-9]+)_t([0-9]+)$', run_id)
        if not match:
            continue
        strategy, quality, threads = match.groups()
        row = {
            'run_id': run_id,
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
