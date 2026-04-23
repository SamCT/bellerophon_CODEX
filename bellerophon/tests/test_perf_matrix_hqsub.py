import subprocess
import sys


def test_hqsub_matrix_commands_use_unique_resource_names_and_thread_cpu_mode():
    proc = subprocess.run(
        [
            sys.executable,
            'scripts/perf_matrix_hqsub.py',
            '--forward',
            'fwd.bam',
            '--reverse',
            'rev.bam',
            '--threads',
            '1,4',
            '--qualities',
            '0',
            '--strategies',
            'legacy',
            '--project-cpus-mode',
            'thread',
            '--resource-prefix',
            'perf',
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    lines = [line for line in proc.stdout.splitlines() if line.startswith('hqsub ')]
    assert len(lines) == 2
    assert "-P 1" in lines[0]
    assert "-P 4" in lines[1]
    assert "-r 'perf_slegacy_q0_t1'" in lines[0]
    assert "-r 'perf_slegacy_q0_t4'" in lines[1]


def test_hqsub_matrix_supports_legacy_resource_flag_alias():
    proc = subprocess.run(
        [
            sys.executable,
            'scripts/perf_matrix_hqsub.py',
            '--forward',
            'fwd.bam',
            '--reverse',
            'rev.bam',
            '--threads',
            '2',
            '--qualities',
            '0',
            '--strategies',
            'legacy',
            '--resource',
            'oldprefix',
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    lines = [line for line in proc.stdout.splitlines() if line.startswith('hqsub ')]
    assert len(lines) == 1
    assert "-r 'oldprefix_slegacy_q0_t2'" in lines[0]
