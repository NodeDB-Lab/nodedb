//! Spec: mmap'd columnar column files must advise MADV_SEQUENTIAL, and
//! their pages must be released via POSIX_FADV_DONTNEED after a scan.
//!
//! `ColumnarSegmentWriter::mmap_column` (nodedb/src/engine/timeseries/
//! columnar_segment.rs) is the L1 entry point for timeseries partition
//! scans. A column read is a forward sweep over compressed bytes — the
//! exact workload MADV_SEQUENTIAL targets. Without it, the kernel underreads
//! and retains consumed pages, evicting hotter engines' working sets.
//!
//! This is the same invariant violated by nodedb-vector/mmap_segment.rs
//! (MADV_RANDOM for HNSW) and nodedb-wal/mmap_reader.rs (MADV_SEQUENTIAL
//! for WAL catchup). Columnar column reads share the design flaw.

use nodedb::engine::timeseries::columnar_segment;
use tempfile::tempdir;

fn make_col_file(dir: &std::path::Path, name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(format!("{name}.col")), bytes).unwrap();
}

#[test]
fn mmap_column_advises_sequential() {
    let dir = tempdir().unwrap();
    let partition = dir.path().join("ts-0_1000");
    make_col_file(&partition, "timestamp", &vec![0u8; 64 * 1024]);

    // Spec: mmap_column must emit MADV_SEQUENTIAL on the mapped region.
    // Observable via a module-level counter incremented by the advise call.
    let before = columnar_segment::test_hooks::madv_sequential_count();
    let _mmap =
        columnar_segment::ColumnarSegmentReader::mmap_column(&partition, "timestamp").unwrap();
    let after = columnar_segment::test_hooks::madv_sequential_count();

    assert_eq!(
        after - before,
        1,
        "mmap_column must advise MADV_SEQUENTIAL on every mapped column \
         (before={before}, after={after})"
    );
}

#[test]
fn column_scan_advises_dontneed_after_release() {
    // Spec: once the mmap'd column is dropped, its pages should be hinted
    // via POSIX_FADV_DONTNEED so cold partition reads don't pin page cache
    // across engine boundaries. Either the mmap wrapper emits fadvise on
    // drop, or scan callers do so explicitly — the counter is the contract.
    let dir = tempdir().unwrap();
    let partition = dir.path().join("ts-0_1000");
    make_col_file(&partition, "value", &vec![0u8; 64 * 1024]);

    let before = columnar_segment::test_hooks::fadv_dontneed_count();
    {
        let _mmap =
            columnar_segment::ColumnarSegmentReader::mmap_column(&partition, "value").unwrap();
    } // drop here — wrapper should fadv_dontneed
    let after = columnar_segment::test_hooks::fadv_dontneed_count();

    assert!(
        after > before,
        "dropping a column mmap must emit POSIX_FADV_DONTNEED \
         (before={before}, after={after})"
    );
}

#[test]
fn empty_column_file_does_not_panic() {
    // Zero-byte columns exist for never-populated optional tags. Advising
    // a zero-length region is EINVAL on Linux — the code must skip advice
    // on empty files rather than swallow the error or panic.
    let dir = tempdir().unwrap();
    let partition = dir.path().join("ts-0_1000");
    make_col_file(&partition, "optional_tag", &[]);

    let result = columnar_segment::ColumnarSegmentReader::mmap_column(&partition, "optional_tag");
    assert!(
        result.is_ok(),
        "empty column mmap must succeed without advising"
    );
}
