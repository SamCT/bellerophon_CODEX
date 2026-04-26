#!/usr/bin/env python3
import argparse
import itertools
import os
import shlex


RUNNER_PIPELINE = {
    'python': '',
    'rust_legacy_temp': 'legacy-temp',
    'rust_pair_temp': 'pair-temp',
    'rust_direct': '',
}


def parse_csv_ints(value):
    return [int(v.strip()) for v in value.split(',') if v.strip()]


def parse_csv_strs(value):
    return [v.strip() for v in value.split(',') if v.strip()]


def shell_join(values):
    return ' '.join(shlex.quote(v) for v in values)


def build_inner_command(args, runner, strategy, quality, thread_count, run_dir, output_bam, time_txt, log_txt):
    if runner == 'python':
        matrix_cmd = [
            'python',
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
            str(thread_count),
            '--log-level',
            'INFO',
        ]
    else:
        matrix_cmd = [
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
            str(thread_count),
            '--log-level',
            'info',
            '--tmp-dir',
            run_dir,
        ]
        if RUNNER_PIPELINE[runner]:
            matrix_cmd.extend(['--pipeline', RUNNER_PIPELINE[runner]])

    return (
        'mkdir -p {run_dir} && '
        'BELLEROPHON_IO_THREADS_STRATEGY={strategy} '
        '/usr/bin/time -v -o {time_txt} {matrix_cmd} '
        '> {log_txt} 2>&1'
    ).format(
        run_dir=shlex.quote(run_dir),
        strategy=shlex.quote(strategy),
        time_txt=shlex.quote(time_txt),
        matrix_cmd=shell_join(matrix_cmd),
        log_txt=shlex.quote(log_txt),
    )


def main():
    parser = argparse.ArgumentParser(
        description='Generate hqsub commands for bellerophon performance matrix runs.'
    )
    parser.add_argument('--forward', required=True, help='Forward input BAM/CRAM/SAM path.')
    parser.add_argument('--reverse', required=True, help='Reverse input BAM/CRAM/SAM path.')
    parser.add_argument('--scratch-dir', default='/scratch', help='Scratch directory for outputs/logs.')
    parser.add_argument('--threads', default='1,2,4,8,16,32', help='Comma-separated thread values.')
    parser.add_argument('--qualities', default='0,20', help='Comma-separated MAPQ thresholds.')
    parser.add_argument(
        '--runners',
        default='python,rust_legacy_temp,rust_pair_temp,rust_direct',
        help='Comma-separated runners: python,rust_legacy_temp,rust_pair_temp,rust_direct.',
    )
    parser.add_argument(
        '--strategies',
        default='legacy,capped',
        help='Comma-separated IO strategies for python; rust runners use strategy=na unless --rust-strategy is set.',
    )
    parser.add_argument('--rust-strategy', default='na', help='Strategy tag for rust runners (for matrix metadata).')
    parser.add_argument('--rust-bin', default='rust/bellerophon-rs/target/release/bellerophon-rs')
    parser.add_argument('--queue', default='boris')
    parser.add_argument('--project-cpus', type=int, default=32, help='Value passed to hqsub -P.')
    parser.add_argument(
        '--project-cpus-mode',
        choices=('fixed', 'thread'),
        default='fixed',
        help='How to set hqsub -P: fixed uses --project-cpus; thread uses current --threads value.',
    )
    parser.add_argument('--resource', default='p1', help='Value passed to hqsub -r when --resource-prefix is not set.')
    parser.add_argument(
        '--resource-prefix',
        default=None,
        help='Optional prefix for per-job resource names; when set, -r becomes <prefix>_<matrix-run-id>.',
    )
    parser.add_argument('--output-prefix', default='perf_matrix')
    args = parser.parse_args()

    threads = parse_csv_ints(args.threads)
    qualities = parse_csv_ints(args.qualities)
    strategies = parse_csv_strs(args.strategies)
    runners = parse_csv_strs(args.runners)
    base_dir = os.path.join(args.scratch_dir, args.output_prefix)

    for runner in runners:
        if runner not in RUNNER_PIPELINE:
            raise SystemExit('Unknown runner: {runner}'.format(runner=runner))

    print('# mkdir once before submitting jobs')
    print('mkdir -p {base_dir}'.format(base_dir=base_dir))
    print('# submit one job per matrix cell')

    for runner, quality, thread_count in itertools.product(runners, qualities, threads):
        runner_strategies = strategies if runner == 'python' else [args.rust_strategy]
        for strategy in runner_strategies:
            run_id = 'r{runner}_s{strategy}_q{quality}_t{thread_count}'.format(
                runner=runner,
                strategy=strategy,
                quality=quality,
                thread_count=thread_count,
            )
            run_dir = os.path.join(base_dir, run_id)
            output_bam = os.path.join(run_dir, 'out.bam')
            time_txt = os.path.join(run_dir, 'time.txt')
            log_txt = os.path.join(run_dir, 'run.log')
            cmd = build_inner_command(
                args,
                runner,
                strategy,
                quality,
                thread_count,
                run_dir,
                output_bam,
                time_txt,
                log_txt,
            )
            project_cpus = thread_count if args.project_cpus_mode == 'thread' else args.project_cpus
            resource = '{prefix}_{run_id}'.format(prefix=args.resource_prefix, run_id=run_id) if args.resource_prefix else args.resource
            print(
                "hqsub -q '{queue}' -P {project_cpus} -r '{resource}' \"{cmd}\"".format(
                    queue=args.queue,
                    project_cpus=project_cpus,
                    resource=resource,
                    cmd=cmd,
                )
            )


if __name__ == '__main__':
    main()
