#!/usr/bin/env python3
import argparse
import collections
import json

import pysam


def summarize(path):
    total = 0
    groups = 0
    current = None
    current_n = 0
    group_sizes = collections.Counter()
    mapq = collections.Counter()
    flags = collections.Counter()
    cigars = collections.Counter()

    with pysam.AlignmentFile(path, 'r') as bam:
        for read in bam:
            total += 1
            mapq[str(read.mapping_quality)] += 1
            if read.is_unmapped:
                flags['unmapped'] += 1
            if read.is_secondary:
                flags['secondary'] += 1
            if read.is_supplementary:
                flags['supplementary'] += 1
            cigars[str(read.cigarstring)] += 1

            if current is None:
                current = read.query_name
                current_n = 1
            elif read.query_name == current:
                current_n += 1
            else:
                group_sizes[str(current_n)] += 1
                groups += 1
                current = read.query_name
                current_n = 1

    if current is not None:
        group_sizes[str(current_n)] += 1
        groups += 1

    return {
        'records': total,
        'groups': groups,
        'group_sizes': dict(group_sizes),
        'mapq_distribution': dict(mapq),
        'flags': dict(flags),
        'cigar_distribution_top_20': dict(cigars.most_common(20)),
    }


def ordinal_group_name_mismatches(forward, reverse):
    mismatches = 0
    compared = 0
    with pysam.AlignmentFile(forward, 'r') as f_bam, pysam.AlignmentFile(reverse, 'r') as r_bam:
        f_current = None
        r_current = None
        while True:
            if f_current is None:
                try:
                    f_current = next(f_bam).query_name
                except StopIteration:
                    break
            if r_current is None:
                try:
                    r_current = next(r_bam).query_name
                except StopIteration:
                    break
            compared += 1
            if f_current != r_current:
                mismatches += 1
            current_f = f_current
            current_r = r_current
            f_current = None
            r_current = None
            for read in f_bam:
                if read.query_name != current_f:
                    f_current = read.query_name
                    break
            for read in r_bam:
                if read.query_name != current_r:
                    r_current = read.query_name
                    break
    return compared, mismatches


def main():
    parser = argparse.ArgumentParser(description='Audit input-shape assumptions for zip-based merging.')
    parser.add_argument('--forward', required=True)
    parser.add_argument('--reverse', required=True)
    parser.add_argument('--output', default='-')
    args = parser.parse_args()

    forward_summary = summarize(args.forward)
    reverse_summary = summarize(args.reverse)
    compared, mismatches = ordinal_group_name_mismatches(args.forward, args.reverse)

    payload = {
        'forward': forward_summary,
        'reverse': reverse_summary,
        'ordinal_group_name_compared': compared,
        'ordinal_group_name_mismatches': mismatches,
    }
    if args.output == '-':
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        with open(args.output, 'w', encoding='utf-8') as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
