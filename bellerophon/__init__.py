import logging
import os
import pysam
import sys
import tempfile
import time
from collections import OrderedDict


log = logging.getLogger(__name__)
__version__ = '1.0'
__description__ = 'Filter two single-end BAM, SAM, or CRAM files for reads where ' + \
                  'there is high-quality mapping on both sides of a ligation ' + \
                  'junction, retaining the 5´ side of that mapping, then merge ' + \
                  'them into one paired-end BAM file. '
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

MATCH = 0
SOFT_CLIP = 4
HARD_CLIP = 5


def _classify_read(read):
    """Classify a read relative to ligation junction orientation."""
    if read.is_unmapped:
        return 'unmapped'
    cigartuples = read.cigartuples
    if not cigartuples:
        return 'other'

    first_op = cigartuples[0][0]
    last_op = cigartuples[-1][0]
    begins_with_match = first_op == MATCH
    ends_with_match = last_op == MATCH
    has_internal_match = any(op == MATCH for op, _ in cigartuples[1:-1])
    has_terminal_clip = first_op in (SOFT_CLIP, HARD_CLIP) and last_op in (SOFT_CLIP, HARD_CLIP)

    if (read.is_reverse and ends_with_match) or (not read.is_reverse and begins_with_match):
        return 'five'
    if (read.is_reverse and begins_with_match) or (not read.is_reverse and ends_with_match):
        return 'three'
    if has_terminal_clip and has_internal_match:
        return 'mid'
    return 'other'


def _write_filtered_group(output_fh, count, five_read, first_read):
    """Write the best representative read for a query-name group."""
    if count == 0:
        return 0
    if count in (1, 2) and five_read is not None:
        output_fh.write(five_read)
        return 1
    first_read.is_unmapped = 1
    output_fh.write(first_read)
    return 1


def filter_reads(args):
    log.setLevel(args.log_level)
    retval = []
    save = pysam.set_verbosity(0)
    ffh = pysam.AlignmentFile(args.forward, 'r', threads=args.threads)
    rfh = pysam.AlignmentFile(args.reverse, 'r', threads=args.threads)
    pysam.set_verbosity(save)
    if ffh.header.references != rfh.header.references or ffh.header.lengths != rfh.header.lengths:
        log.error('The input files do not have the same sequence names or lengths.')
        return 1
    for handle in [ffh, rfh]:
        filename = os.path.split(os.path.abspath(handle.filename.decode('utf-8')))[-1]
        log.info('Loading reads from %s...' % filename)
        processed_reads = 0
        written_reads = 0
        previous_read = None
        counter = 0
        first_read = None
        five_read = None
        output_tempfile = tempfile.NamedTemporaryFile(prefix='filtered_', suffix='.bam', delete=False, dir=os.getcwd())
        retval.append(output_tempfile.name)
        output_tempfile.close()
        output_fh = pysam.AlignmentFile(output_tempfile.name, 'wb0', header=handle.header)
        starttime = time.time()
        for read in handle:
            processed_reads += 1
            # If this is 1. Not the first read, and 2. Not the previous read again:
            if previous_read is not None and read.query_name != previous_read:
                written_reads += _write_filtered_group(output_fh, counter, five_read, first_read)
                counter = 0
                first_read = None
                five_read = None
            counter += 1
            if first_read is None:
                first_read = read
            previous_read = read.query_name
            if _classify_read(read) == 'five':
                if five_read is None:
                    five_read = read
                else:
                    five_read = None
        written_reads += _write_filtered_group(output_fh, counter, five_read, first_read)
        output_fh.close()
        log.debug('Processed %d reads in %f seconds and output %d.' % (processed_reads, time.time() - starttime, written_reads))
    # Send the filenames of the filtered alignments back to the caller.
    return retval


def merge_bams(args, filtered_forward, filtered_reverse):
    previous = None
    save = pysam.set_verbosity(0)
    forward = pysam.AlignmentFile(filtered_forward, 'r', threads=args.threads)
    reverse = pysam.AlignmentFile(filtered_reverse, 'r', threads=args.threads)
    pysam.set_verbosity(save)
    new_header = OrderedDict(forward.header)
    if 'PG' in new_header:
        last_pg = new_header['PG'][-1]
        previous = last_pg['ID']
    command = 'bellerophon --forward %s --reverse %s --output %s --quality %s' % \
        (os.path.split(args.forward)[-1], os.path.split(args.reverse)[-1], os.path.split(args.output)[-1], args.quality)
    new_pg = dict(ID=__name__, PN=__name__, PP=None, VN=__version__, CL=command, DS=__description__)
    if previous is not None:
        new_pg['PP'] = previous
        new_pg = new_header['PG'] + [OrderedDict(new_pg)]
    else:
        new_pg = new_header['PG'] + [OrderedDict(ID=__name__, PN=__name__, VN=__version__, CL=command, DS=__description__)]
    new_header['PG'] = new_pg
    output_fh = pysam.AlignmentFile(args.output, 'wb', header=pysam.AlignmentHeader.from_dict(new_header), threads=args.threads)
    processed_reads = 0
    mismatched_reads = 0
    unmapped_reads = 0
    low_quality_reads = 0
    starttime = time.time()
    for forward_read, reverse_read in zip(forward, reverse):
        # Skip reads that aren't the same, are unmapped, or are less than --quality
        if forward_read.query_name != reverse_read.query_name:
            mismatched_reads += 1
            continue
        if forward_read.is_unmapped or reverse_read.is_unmapped:
            unmapped_reads += 1
            continue
        if args.quality > 0 and (forward_read.mapping_quality < args.quality or reverse_read.mapping_quality < args.quality):
            low_quality_reads += 1
            continue
        # Get the proper distances and lengths, since they may be off now.
        if forward_read.reference_id == reverse_read.reference_id:
            distance = abs(forward_read.reference_start - reverse_read.reference_start)
            if forward_read.reference_start >= reverse_read.reference_start:
                forward_length = -distance
                reverse_length = distance
            else:
                forward_length = distance
                reverse_length = -distance
        else:
            forward_length = 0
            reverse_length = 0
        # Zero the right flags for the forward and reverse reads.
        forward_read.is_secondary = 0
        reverse_read.is_secondary = 0
        forward_read.is_unmapped = 0
        reverse_read.is_unmapped = 0
        forward_read.is_supplementary = 0
        reverse_read.is_supplementary = 0
        # Make sure each one has the right flag for read number.
        forward_read.is_read1 = 1
        reverse_read.is_read2 = 1
        reverse_read.is_read1 = 0
        forward_read.is_read2 = 0
        # Swap the mapped and reverse attributes between reads.
        reverse_is_reverse = reverse_read.is_reverse
        forward_is_reverse = forward_read.is_reverse
        forward_read.mate_is_unmapped = 0
        reverse_read.mate_is_unmapped = 0
        forward_read.mate_is_reverse = reverse_is_reverse
        reverse_read.mate_is_reverse = forward_is_reverse
        # Set them to paired and properly paired.
        forward_read.is_proper_pair = 1
        reverse_read.is_proper_pair = 1
        forward_read.is_paired = 1
        reverse_read.is_paired = 1
        # Set the next reference for the reads to each other.
        reverse_read.next_reference_id = forward_read.reference_id
        forward_read.next_reference_id = reverse_read.reference_id
        reverse_read.next_reference_start = forward_read.reference_start
        forward_read.next_reference_start = reverse_read.reference_start
        # And update the length that we calculated above.
        forward_read.template_length = forward_length
        reverse_read.template_length = reverse_length
        output_fh.write(forward_read)
        output_fh.write(reverse_read)
        processed_reads += 1
    log.info('Successfully merged %d read pairs in %f seconds.' % (processed_reads, time.time() - starttime))
    log.debug('Skipped %d pairs with mismatched read names, %d unmapped reads, and %d with a mapping quality below %d.' %
              (mismatched_reads, unmapped_reads, low_quality_reads, args.quality))
    output_fh.close()
    forward.close()
    reverse.close()
    for filename in [filtered_forward, filtered_reverse]:
        os.unlink(filename)
    return 0
