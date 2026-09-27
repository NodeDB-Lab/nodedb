// SPDX-License-Identifier: Apache-2.0

//! A mid-file corruption detected during recovery files a black-box report
//! that carries the damaged segment with it.
//!
//! The point of preserving the segment is that the failure is not
//! reproducible: the bytes that prove what went wrong exist only on the machine
//! that hit it, and only until something repairs or rolls the log. The report
//! has to hold a copy or the evidence is gone.

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use nodedb_wal::record::{HEADER_SIZE, RecordType, WAL_MAGIC};
use nodedb_wal::writer::WalWriter;
use nodedb_wal::{WalError, recover};

/// The recorder is process-wide and `init` may only be called once, so the
/// reports directory is created once per test binary and every test in this
/// file shares it. It must outlive the process, hence the leak-by-`OnceLock`.
static REPORTS: OnceLock<tempfile::TempDir> = OnceLock::new();

fn reports_dir() -> &'static Path {
    REPORTS
        .get_or_init(|| {
            let dir = crate::tempdir().expect("reports temp dir");
            faultbox::init(
                faultbox::Config::new("nodedb-wal-test", "0.0.0", dir.path())
                    // The test harness owns panic reporting; a chained hook here
                    // would only add noise to an already-failing assertion.
                    .install_panic_hook(false),
            );
            dir
        })
        .path()
}

/// Write `count` records, each in its own fsynced batch, so every one of them
/// is a committed record rather than part of one interrupted write.
fn write_segment(path: &Path, count: u64) {
    let mut writer = WalWriter::open_without_direct_io(path).expect("open writer");
    for i in 0..count {
        writer
            .append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("row-{i}").as_bytes(),
            )
            .expect("append");
        writer.sync().expect("sync");
    }
}

/// Overwrite `len` bytes at `offset` with a pattern that is not the WAL magic,
/// standing in for a bad block.
fn smash(path: &Path, offset: u64, len: usize) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for smashing");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(&vec![0xA5u8; len]).expect("write");
    file.sync_all().expect("sync");
}

/// Byte offset of the `n`th (0-based) record header, found by scanning for the
/// record magic rather than assuming a fixed on-disk layout.
fn record_offset(path: &Path, n: usize) -> u64 {
    let bytes = std::fs::read(path).expect("read segment");
    let magic = WAL_MAGIC.to_le_bytes();
    bytes
        .windows(magic.len())
        .enumerate()
        .filter(|(_, window)| *window == magic)
        .map(|(i, _)| i as u64)
        .nth(n)
        .expect("the segment holds that many records")
}

/// Build a segment with a hole in the middle and committed records behind it.
fn corrupt_segment(dir: &Path, name: &str) -> (PathBuf, u64) {
    let path = dir.join(name);
    write_segment(&path, 6);
    // Smash the second record's header; records 3..6 stay intact and carry
    // higher LSNs, which is what makes this a hole and not a torn tail.
    let hole = record_offset(&path, 1);
    smash(&path, hole, HEADER_SIZE);
    (path, hole)
}

fn group_for(kind: &str) -> faultbox::reader::Group {
    let groups = faultbox::reader::list(reports_dir()).expect("reports are listable");
    groups
        .into_iter()
        .find(|g| g.first.domain_kind.as_deref() == Some(kind))
        .unwrap_or_else(|| panic!("no report was filed with domain kind {kind}"))
}

#[test]
fn recovery_reports_mid_file_corruption_and_preserves_the_segment() {
    let _ = reports_dir();
    let dir = crate::tempdir().expect("wal temp dir");
    let (path, hole) = corrupt_segment(dir.path(), "hole.wal");

    let err = recover(&path).expect_err("a hole must fail recovery");
    assert!(
        matches!(err, WalError::MidFileCorruption { .. }),
        "expected mid-file corruption, got {err}"
    );

    let group = group_for("nodedb_wal.mid_file_corruption");
    let report = group.most_recent();

    // The forensics an operator has to work from when the report is all they
    // have: which file, where the damage starts, and which LSNs it hides.
    let domain = &report.domain;
    assert_eq!(domain["segment_file"], "hole.wal");
    let damage_offset = domain["damage_offset"].as_u64().expect("damage offset");
    assert!(
        damage_offset <= hole,
        "the reader must stop at or before the smashed header at {hole}, got {damage_offset}"
    );
    assert!(
        domain["resync_offset"].as_u64().expect("resync offset") > damage_offset,
        "the resync point must lie past the damage: {domain}"
    );
    assert!(
        domain["resync_lsn"].as_u64().expect("resync lsn")
            > domain["last_lsn_before_damage"]
                .as_u64()
                .expect("last lsn read"),
        "the record behind the hole must carry a higher LSN: {domain}"
    );
    assert!(
        !report.error_chain.is_empty(),
        "the report must carry the error it was filed for"
    );

    // The damaged segment itself, byte for byte.
    let artifact = report
        .artifacts
        .iter()
        .find(|a| a.kind == "wal-segment")
        .expect("the damaged segment must be preserved with the report");
    assert!(
        !artifact.rel_path.is_empty(),
        "the segment was not preserved: {:?}",
        artifact.note
    );
    let preserved = group.dir.join(&artifact.rel_path);
    assert_eq!(
        std::fs::read(&preserved).expect("preserved segment is readable"),
        std::fs::read(&path).expect("source segment is readable"),
        "the preserved artifact must be a verbatim copy of the damaged segment"
    );
}
