#!/usr/bin/env python3
import argparse
import itertools
import os
import subprocess
import sys


RUNNER_PIPELINE = {
    'python': None,
    'rust_legacy_temp': 'legacy-temp',
    'rust_pair_temp': 'pair-temp',
    'rust_direct': 'direct',
}


def parse_csv_ints(value):
    return [int(v.strip()) for v in value.split(',') if v.strip()]


def parse_csv_strs(value):
    return [v.strip() for v in value.split(',') if v.strip()]


def run_one(args, runner, strategy, quality, threads, base_dir):
    run_id = f'r{runner}_s{strategy}_q{quality}_t{threads}'
    run_dir = os.path.join(base_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)
    output_bam = os.path.join(run_dir, 'out.bam')
    time_txt = os.path.join(run_dir, 'time.txt')
    log_txt = os.path.join(run_dir, 'run.log')

    env = os.environ.copy()
    env['BELLEROPHON_IO_THREADS_STRATEGY'] = strategy

    if runner == 'python':
        cmd = [
            '/usr/bin/time',
            '-v',
            '-o',
            time_txt,
            sys.executable,
            '-m',
            'bellerophon.cli',
            '--forward',
            args.forward,
            '--reverse',
            args.reverse,
            '--output',
            output_bam,
            '--quality',
            str(quality),
            '--threads',
            str(threads),
            '--log-level',
            'INFO',
        ]
    else:
        cmd = [
            '/usr/bin/time',
            '-v',
            '-o',
            time_txt,
            args.rust_bin,
            '--forward',
            args.forward,
            '--reverse',
            args.reverse,
            '--output',
            output_bam,
            '--quality',
            str(quality),
            '--threads',
            str(threads),
            '--log-level',
            'info',
            '--tmp-dir',
            run_dir,
        ]
        if runner != 'rust_direct':
            cmd.extend(['--pipeline', RUNNER_PIPELINE[runner]])

    with open(log_txt, 'w', encoding='utf-8') as log_fh:
        subprocess.run(cmd, check=True, env=env, stdout=log_fh, stderr=subprocess.STDOUT)


def main():
    parser = argparse.ArgumentParser(description='Run a local performance matrix (smoke scale).')
    parser.add_argument('--forward', required=True)
    parser.add_argument('--reverse', required=True)
    parser.add_argument('--matrix-dir', default='perf_matrix_local')
    parser.add_argument('--threads', default='1,2')
    parser.add_argument('--qualities', default='0,20')
    parser.add_argument('--runners', default='python,rust_legacy_temp,rust_pair_temp,rust_direct')
    parser.add_argument('--strategies', default='legacy,capped')
    parser.add_argument('--rust-strategy', default='na')
    parser.add_argument('--rust-bin', default='rust/bellerophon-rs/target/release/bellerophon-rs')
    args = parser.parse_args()

    threads = parse_csv_ints(args.threads)
    qualities = parse_csv_ints(args.qualities)
    runners = parse_csv_strs(args.runners)
    strategies = parse_csv_strs(args.strategies)

    os.makedirs(args.matrix_dir, exist_ok=True)

    for runner in runners:
        if runner not in RUNNER_PIPELINE:
            raise SystemExit(f'Unknown runner: {runner}')
        runner_strategies = strategies if runner == 'python' else [args.rust_strategy]
        for quality, thread_count, strategy in itertools.product(qualities, threads, runner_strategies):
            print(f'Running {runner} q={quality} t={thread_count} strategy={strategy}')
            run_one(args, runner, strategy, quality, thread_count, args.matrix_dir)

    print(f'Done. Collect with: python scripts/perf_matrix_collect.py --matrix-dir {args.matrix_dir}')


if __name__ == '__main__':
    main()
