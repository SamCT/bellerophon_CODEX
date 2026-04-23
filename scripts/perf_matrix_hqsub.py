#!/usr/bin/env python3
import argparse
import itertools
import os


def parse_csv_ints(value):
    return [int(v.strip()) for v in value.split(',') if v.strip()]


def parse_csv_strs(value):
    return [v.strip() for v in value.split(',') if v.strip()]


def main():
    parser = argparse.ArgumentParser(
        description='Generate hqsub commands for bellerophon performance matrix runs.'
    )
    parser.add_argument('--forward', required=True, help='Forward input BAM/CRAM/SAM path.')
    parser.add_argument('--reverse', required=True, help='Reverse input BAM/CRAM/SAM path.')
    parser.add_argument('--scratch-dir', default='/scratch', help='Scratch directory for outputs/logs.')
    parser.add_argument('--threads', default='1,2,4,8,16,32', help='Comma-separated thread values.')
    parser.add_argument('--qualities', default='0,20', help='Comma-separated MAPQ thresholds.')
    parser.add_argument('--strategies', default='legacy,capped', help='Comma-separated IO thread strategies.')
    parser.add_argument('--queue', default='boris')
    parser.add_argument('--project-cpus', type=int, default=32, help='Value passed to hqsub -P.')
    parser.add_argument('--resource', default='p1', help='Value passed to hqsub -r.')
    parser.add_argument('--output-prefix', default='perf_matrix')
    args = parser.parse_args()

    threads = parse_csv_ints(args.threads)
    qualities = parse_csv_ints(args.qualities)
    strategies = parse_csv_strs(args.strategies)
    base_dir = os.path.join(args.scratch_dir, args.output_prefix)

    print('# mkdir once before submitting jobs')
    print('mkdir -p {base_dir}'.format(base_dir=base_dir))
    print('# submit one job per matrix cell')

    for strategy, quality, thread_count in itertools.product(strategies, qualities, threads):
        run_id = 's{strategy}_q{quality}_t{thread_count}'.format(
            strategy=strategy,
            quality=quality,
            thread_count=thread_count,
        )
        run_dir = os.path.join(base_dir, run_id)
        output_bam = os.path.join(run_dir, 'out.bam')
        time_txt = os.path.join(run_dir, 'time.txt')
        log_txt = os.path.join(run_dir, 'run.log')
        cmd = (
            "mkdir -p {run_dir} && "
            "BELLEROPHON_IO_THREADS_STRATEGY={strategy} "
            "/usr/bin/time -v -o {time_txt} "
            "python -m bellerophon.cli "
            "--forward {forward} --reverse {reverse} "
            "--output {output_bam} --quality {quality} --threads {thread_count} --log-level INFO "
            "> {log_txt} 2>&1"
        ).format(
            run_dir=run_dir,
            strategy=strategy,
            time_txt=time_txt,
            forward=args.forward,
            reverse=args.reverse,
            output_bam=output_bam,
            quality=quality,
            thread_count=thread_count,
            log_txt=log_txt,
        )
        print(
            "hqsub -q '{queue}' -P {project_cpus} -r '{resource}' \"{cmd}\"".format(
                queue=args.queue,
                project_cpus=args.project_cpus,
                resource=args.resource,
                cmd=cmd,
            )
        )


if __name__ == '__main__':
    main()
