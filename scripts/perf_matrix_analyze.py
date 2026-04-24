#!/usr/bin/env python3
import argparse
import csv
import statistics
from collections import defaultdict


def load_rows(path):
    with open(path, 'r', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))


def median_by_runner(rows):
    grouped = defaultdict(list)
    for row in rows:
        wall = row.get('wall_seconds')
        if not wall:
            continue
        key = (row.get('runner', ''), row.get('quality', ''), row.get('threads', ''))
        grouped[key].append(float(wall))

    medians = {}
    for key, values in grouped.items():
        medians[key] = statistics.median(values)
    return medians


def main():
    parser = argparse.ArgumentParser(description='Summarize median wall time and speedups from perf matrix CSV.')
    parser.add_argument('--input-csv', default='perf_matrix_summary.csv')
    parser.add_argument('--baseline-runner', default='python')
    args = parser.parse_args()

    rows = load_rows(args.input_csv)
    medians = median_by_runner(rows)

    print('runner,quality,threads,median_wall_s,speedup_vs_{baseline}'.format(baseline=args.baseline_runner))
    for (runner, quality, threads), median_s in sorted(medians.items()):
        baseline = medians.get((args.baseline_runner, quality, threads))
        speedup = ''
        if baseline and median_s > 0:
            speedup = '{:.4f}'.format(baseline / median_s)
        print('{runner},{quality},{threads},{median:.6f},{speedup}'.format(
            runner=runner,
            quality=quality,
            threads=threads,
            median=median_s,
            speedup=speedup,
        ))


if __name__ == '__main__':
    main()
