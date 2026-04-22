import hashlib
import json
import os
import re
import subprocess
import tempfile
import time

import pytest


def _dataset_paths():
    forward = os.environ.get('BELLEROPHON_PERF_FORWARD')
    reverse = os.environ.get('BELLEROPHON_PERF_REVERSE')
    if not forward or not reverse:
        pytest.skip('Set BELLEROPHON_PERF_FORWARD and BELLEROPHON_PERF_REVERSE to run performance comparison tests.')
    if not os.path.exists(forward) or not os.path.exists(reverse):
        pytest.skip('Performance dataset paths do not exist.')
    return os.path.abspath(forward), os.path.abspath(reverse)


def _run_with_time(forward, reverse, output, strategy, threads=32, quality=0):
    if not os.path.exists('/usr/bin/time'):
        pytest.skip('GNU time utility is required for performance comparison tests.')
    env = os.environ.copy()
    env['BELLEROPHON_IO_THREADS_STRATEGY'] = strategy
    command = [
        'bash',
        '-lc',
        '/usr/bin/time -v python -m bellerophon.cli '
        '--forward "{forward}" --reverse "{reverse}" --threads {threads} '
        '--quality {quality} --output "{output}"'.format(
            forward=forward,
            reverse=reverse,
            threads=threads,
            quality=quality,
            output=output,
        ),
    ]
    start = time.perf_counter()
    proc = subprocess.run(command, capture_output=True, text=True, env=env, check=True)
    wall_clock = time.perf_counter() - start
    return _parse_time_output(proc.stderr, wall_clock)


def _parse_time_output(stderr_text, wall_clock_fallback):
    metrics = {
        'elapsed_seconds': wall_clock_fallback,
        'cpu_percent': None,
        'max_rss_kb': None,
        'file_system_inputs': None,
        'file_system_outputs': None,
    }
    patterns = {
        'elapsed_seconds': r'Elapsed \(wall clock\) time .*: ([0-9:.]+)',
        'cpu_percent': r'Percent of CPU this job got: ([0-9]+)%',
        'max_rss_kb': r'Maximum resident set size \(kbytes\): ([0-9]+)',
        'file_system_inputs': r'File system inputs: ([0-9]+)',
        'file_system_outputs': r'File system outputs: ([0-9]+)',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, stderr_text)
        if not match:
            continue
        value = match.group(1)
        if key == 'elapsed_seconds':
            metrics[key] = _elapsed_to_seconds(value)
        elif key == 'cpu_percent':
            metrics[key] = float(value)
        else:
            metrics[key] = int(value)
    return metrics


def _elapsed_to_seconds(elapsed):
    parts = elapsed.split(':')
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return (int(hours) * 3600) + (int(minutes) * 60) + float(seconds)
    if len(parts) == 2:
        minutes, seconds = parts
        return (int(minutes) * 60) + float(seconds)
    return float(elapsed)


def _sha256(path):
    with open(path, 'rb') as fh:
        return hashlib.sha256(fh.read()).hexdigest()


@pytest.mark.performance
def test_legacy_and_capped_produce_identical_output_and_metrics_report():
    forward, reverse = _dataset_paths()
    with tempfile.TemporaryDirectory(prefix='bellerophon_perf_') as tmpdir:
        legacy_output = os.path.join(tmpdir, 'legacy.bam')
        capped_output = os.path.join(tmpdir, 'capped.bam')

        legacy_metrics = _run_with_time(forward, reverse, legacy_output, strategy='legacy')
        capped_metrics = _run_with_time(forward, reverse, capped_output, strategy='capped')

        assert _sha256(legacy_output) == _sha256(capped_output)

        report = {
            'dataset': {'forward': forward, 'reverse': reverse},
            'legacy': legacy_metrics,
            'capped': capped_metrics,
            'delta_seconds': capped_metrics['elapsed_seconds'] - legacy_metrics['elapsed_seconds'],
            'delta_max_rss_kb': (
                None
                if capped_metrics['max_rss_kb'] is None or legacy_metrics['max_rss_kb'] is None
                else capped_metrics['max_rss_kb'] - legacy_metrics['max_rss_kb']
            ),
        }
        report_path = os.path.join(tmpdir, 'performance_report.json')
        with open(report_path, 'w', encoding='utf-8') as fh:
            json.dump(report, fh, indent=2, sort_keys=True)

        assert os.path.exists(report_path)
        assert report['legacy']['elapsed_seconds'] > 0
        assert report['capped']['elapsed_seconds'] > 0
