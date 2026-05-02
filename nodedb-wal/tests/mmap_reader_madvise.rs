//! Spec: mmap WAL readers must advise MADV_SEQUENTIAL, and replay
//! helpers must filter segments by SegmentMeta.first_lsn before opening.
//!
//! The Event-Plane WAL catchup path scans segments forward. The kernel
//! default (MADV_NORMAL) underreads for sequential workloads and keeps
//! already-consumed pages resident. MADV_SEQUENTIAL doubles readahead
//! and drops consumed pages eagerly.
//!
//! Separately, `replay_segments_mmap*` currently opens every segment
//! file before checking whether its LSN range overlaps the requested
//! `from_lsn..`. Opens burn fds, map 64 MiB regions, and page in
//! headers for segments that will be entirely skipped. The filter
//! should use SegmentMeta.first_lsn of segment N+1 to decide whether
//! segment N can be skipped without opening it.

use nodedb_wal::mmap_reader::{
    self, MmapWalReader, replay_segments_mmap, replay_segments_mmap_limit,
};
use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};
use tempfile::tempdir;

fn tiny_segment_wal(wal_dir: &std::path::Path) -> SegmentedWal {
    let mut cfg = SegmentedWalConfig::for_testing(wal_dir.to_path_buf());
    // Tiny segments so a handful of appends forces rollover.
    cfg.segment_target_size = 128;
    SegmentedWal::open(cfg).unwrap()
}

/// Write `records` records across at least `min_segments` segment files.
/// Each payload is large enough vs the tiny segment_target_size to force
/// rollover between every append.
fn write_n_records(wal_dir: &std::path::Path, records: usize) -> Vec<u64> {
    let mut wal = tiny_segment_wal(wal_dir);
    let mut lsns = Vec::new();
    for i in 0..records {
        let payload = vec![b'a' + (i as u8 % 26); 256];
        lsns.push(wal.append(RecordType::Put as u32, 1, 0, &payload).unwrap());
        wal.sync().unwrap();
    }
    lsns
}

#[test]
fn reader_open_advises_sequential() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path();
    write_n_records(wal_dir, 1);

    let segs = nodedb_wal::segment::discover_segments(wal_dir).unwrap();
    let reader = MmapWalReader::open(&segs[0].path).unwrap();

    assert_eq!(
        reader.madvise_state(),
        Some(libc::MADV_SEQUENTIAL),
        "MmapWalReader::open must advise MADV_SEQUENTIAL"
    );
}

#[test]
fn replay_limit_from_last_segment_opens_only_last_segment() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path();
    write_n_records(wal_dir, 5);

    let segs = nodedb_wal::segment::discover_segments(wal_dir).unwrap();
    assert!(segs.len() >= 2, "test needs multiple segments");
    let last_first_lsn = segs.last().unwrap().first_lsn;

    let before = mmap_reader::test_hooks::segments_opened();
    let (_records, _more) = replay_segments_mmap_limit(wal_dir, last_first_lsn, 1024).unwrap();
    let opened = mmap_reader::test_hooks::segments_opened() - before;

    assert_eq!(
        opened,
        1,
        "replay_segments_mmap_limit with from_lsn == last_segment.first_lsn \
         must open exactly one segment; opened {opened} of {}",
        segs.len()
    );
}

#[test]
fn replay_skips_segments_entirely_below_from_lsn() {
    let dir = tempdir().unwrap();
    let wal_dir = dir.path();
    write_n_records(wal_dir, 4);

    let segs = nodedb_wal::segment::discover_segments(wal_dir).unwrap();
    assert!(segs.len() >= 3);
    // Target the last segment only.
    let target = segs.last().unwrap().first_lsn;

    let before = mmap_reader::test_hooks::segments_opened();
    let _records = replay_segments_mmap(wal_dir, target).unwrap();
    let opened = mmap_reader::test_hooks::segments_opened() - before;

    assert_eq!(
        opened, 1,
        "replay_segments_mmap must filter by first_lsn before opening; \
         expected 1 open, got {opened}"
    );
}

#[test]
fn replay_limit_returns_only_records_in_range() {
    // Behaviour spec: even with skipping, the set of returned records
    // must equal the set of records with lsn >= from_lsn, in LSN order.
    let dir = tempdir().unwrap();
    let wal_dir = dir.path();
    let lsns = write_n_records(wal_dir, 5);

    let threshold = lsns[3];
    let (records, _) = replay_segments_mmap_limit(wal_dir, threshold, 1024).unwrap();

    let got: Vec<u64> = records.iter().map(|r| r.header.lsn).collect();
    let expected: Vec<u64> = lsns.iter().copied().filter(|&l| l >= threshold).collect();
    assert_eq!(got, expected);
}

#[test]
#[cfg(target_os = "linux")]
fn fadvise_dontneed_called_after_segment_iteration() {
    // Spec: after iterating a segment to end during replay, the fd's pages
    // must be hinted via POSIX_FADV_DONTNEED so replay doesn't retain
    // hot pages from segments that won't be read again.
    let dir = tempdir().unwrap();
    let wal_dir = dir.path();
    write_n_records(wal_dir, 3);

    let before = mmap_reader::test_hooks::fadv_dontneed_count();
    let _ = replay_segments_mmap(wal_dir, 0).unwrap();
    let after = mmap_reader::test_hooks::fadv_dontneed_count();
    assert!(
        after > before,
        "replay must emit POSIX_FADV_DONTNEED after each exhausted segment \
         (before={before}, after={after})"
    );
}
