use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use rust_htslib::bam;
use rust_htslib::bam::header::HeaderRecord;
use rust_htslib::bam::record::{Cigar, CigarString, Record};
use rust_htslib::bam::Read;
use rust_htslib::bam::Writer;
use tempfile::tempdir;
use wait_timeout::ChildExt;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data")
        .join(name)
}

fn count_bam_records(path: &Path) -> usize {
    let mut reader = bam::Reader::from_path(path).expect("open BAM");
    reader.records().count()
}

fn assert_nonempty_bam(path: &Path) {
    assert!(
        count_bam_records(path) > 0,
        "expected {} to contain records",
        path.display()
    );
}

fn assert_bgzf_eof_block(path: &Path) {
    const BGZF_EOF: [u8; 28] = [
        0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02,
        0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let bytes = fs::read(path).expect("read BAM bytes");
    assert!(
        bytes.ends_with(&BGZF_EOF),
        "expected {} to end with the BGZF EOF block",
        path.display()
    );
}

fn assert_complete_bam(path: &Path) {
    assert_nonempty_bam(path);
    assert_bgzf_eof_block(path);
}

fn run_bellerophon_with_timeout(
    args: &[&str],
    cwd: &Path,
    timeout: Duration,
) -> std::process::Output {
    let binary = assert_cmd::cargo::cargo_bin("bellerophon-rs");
    let mut child = Command::new(binary)
        .current_dir(cwd)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bellerophon-rs");

    if child.wait_timeout(timeout).expect("wait").is_none() {
        let _ = child.kill();
        let output = child.wait_with_output().expect("collect timed out output");
        panic!(
            "bellerophon-rs did not exit within {:?}\nstdout:\n{}\nstderr:\n{}",
            timeout,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    child.wait_with_output().expect("collect output")
}

fn synthetic_header() -> bam::Header {
    let mut header = bam::Header::new();
    header.push_record(
        HeaderRecord::new(b"HD")
            .push_tag(b"VN", "1.6")
            .push_tag(b"SO", "queryname"),
    );
    header.push_record(
        HeaderRecord::new(b"SQ")
            .push_tag(b"SN", "chr1")
            .push_tag(b"LN", 1_000),
    );
    header
}

fn synthetic_record(qname: &str, pos: i64, cigar: CigarString) -> Record {
    let mut record = Record::new();
    let query_len = cigar
        .iter()
        .map(|op| match op {
            Cigar::Match(len)
            | Cigar::Ins(len)
            | Cigar::SoftClip(len)
            | Cigar::Equal(len)
            | Cigar::Diff(len) => *len as usize,
            _ => 0,
        })
        .sum::<usize>();
    let seq = vec![b'A'; query_len];
    let qual = vec![30; query_len];
    record.set(qname.as_bytes(), Some(&cigar), &seq, &qual);
    record.set_tid(0);
    record.set_pos(pos);
    record.set_mtid(0);
    record.set_mpos(pos + 100);
    record.set_mapq(60);
    record
}

fn write_synthetic_bam(path: &Path, groups: Vec<Vec<Record>>) {
    let header = synthetic_header();
    let mut writer = Writer::from_path(path, &header, bam::Format::Bam).expect("create BAM");
    for group in groups {
        for record in group {
            writer.write(&record).expect("write BAM record");
        }
    }
    drop(writer);
}

#[test]
fn accepts_relative_paths_from_arbitrary_working_directory() {
    let temp = tempdir().expect("tempdir");
    fs::copy(fixture("test_1500_forward.bam"), temp.path().join("R1.bam")).expect("copy R1");
    fs::copy(fixture("test_1500_reverse.bam"), temp.path().join("R2.bam")).expect("copy R2");

    let mut cmd = assert_cmd::Command::cargo_bin("bellerophon-rs").expect("binary");
    cmd.current_dir(temp.path())
        .args([
            "--forward",
            "R1.bam",
            "--reverse",
            "R2.bam",
            "--threads",
            "2",
            "--quality",
            "10",
            "--output",
            "out.bam",
            "--log-level",
            "error",
        ])
        .assert()
        .success();

    assert_complete_bam(&temp.path().join("out.bam"));
}

#[test]
fn exits_after_output_completion() {
    let temp = tempdir().expect("tempdir");
    write_synthetic_bam(
        &temp.path().join("R1.bam"),
        vec![
            vec![synthetic_record(
                "q000",
                10,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q001",
                20,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
        ],
    );
    write_synthetic_bam(
        &temp.path().join("R2.bam"),
        vec![
            vec![synthetic_record(
                "q000",
                110,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q001",
                120,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
        ],
    );

    let output = run_bellerophon_with_timeout(
        &[
            "--forward",
            "R1.bam",
            "--reverse",
            "R2.bam",
            "--threads",
            "4",
            "--quality",
            "0",
            "--output",
            "out.bam",
            "--log-level",
            "error",
        ],
        temp.path(),
        Duration::from_secs(30),
    );

    assert!(
        output.status.success(),
        "bellerophon-rs exited with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined_log = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined_log.contains("STAGE direct_done exit_code=0"),
        "missing direct_done log\n{combined_log}"
    );
    assert_complete_bam(&temp.path().join("out.bam"));
}

#[test]
fn parallel_chunked_eof_edge_exits_and_writes_complete_bam() {
    let temp = tempdir().expect("tempdir");
    write_synthetic_bam(
        &temp.path().join("R1.bam"),
        vec![
            vec![synthetic_record(
                "q000",
                10,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q001",
                20,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q002",
                30,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q999",
                40,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
        ],
    );
    write_synthetic_bam(
        &temp.path().join("R2.bam"),
        vec![
            vec![synthetic_record(
                "q000",
                110,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q001",
                120,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![synthetic_record(
                "q002",
                130,
                CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
            )],
            vec![
                synthetic_record(
                    "q999",
                    140,
                    CigarString(vec![Cigar::Match(10), Cigar::SoftClip(2)]),
                ),
                synthetic_record(
                    "q999",
                    141,
                    CigarString(vec![Cigar::SoftClip(2), Cigar::Match(10)]),
                ),
            ],
        ],
    );

    let output = run_bellerophon_with_timeout(
        &[
            "--forward",
            "R1.bam",
            "--reverse",
            "R2.bam",
            "--threads",
            "4",
            "--quality",
            "0",
            "--output",
            "out.bam",
            "--direct-reader-mode",
            "parallel-chunked",
            "--direct-reader-chunk-groups",
            "1",
            "--direct-batch-size",
            "2",
            "--log-level",
            "error",
        ],
        temp.path(),
        Duration::from_secs(30),
    );

    assert!(
        output.status.success(),
        "bellerophon-rs exited with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let combined_log = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined_log.contains("STAGE direct_reader_eof side=forward"));
    assert!(combined_log.contains("STAGE direct_reader_eof side=reverse"));
    assert!(combined_log.contains("STAGE direct_writer_flushed"));
    assert!(combined_log.contains("STAGE direct_done exit_code=0"));
    assert_complete_bam(&temp.path().join("out.bam"));
}
