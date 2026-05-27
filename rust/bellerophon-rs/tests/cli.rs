use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use rust_htslib::bam;
use rust_htslib::bam::Read;
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

    assert_nonempty_bam(&temp.path().join("out.bam"));
}

#[test]
fn exits_after_output_completion() {
    let temp = tempdir().expect("tempdir");
    fs::copy(fixture("test_1500_forward.bam"), temp.path().join("R1.bam")).expect("copy R1");
    fs::copy(fixture("test_1500_reverse.bam"), temp.path().join("R2.bam")).expect("copy R2");

    let binary = assert_cmd::cargo::cargo_bin("bellerophon-rs");
    let mut child = Command::new(binary)
        .current_dir(temp.path())
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
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bellerophon-rs");

    let status = child
        .wait_timeout(Duration::from_secs(20))
        .expect("wait")
        .unwrap_or_else(|| {
            let _ = child.kill();
            panic!("bellerophon-rs did not exit within timeout");
        });

    assert!(status.success(), "bellerophon-rs exited with {status}");
    assert_nonempty_bam(&temp.path().join("out.bam"));
}
