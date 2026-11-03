// SPDX-License-Identifier: Apache-2.0

//! Spec: mmap vector segments must advise the kernel of their access pattern.
//!
//! HNSW graph traversal jumps between non-adjacent vector IDs. Default
//! `MADV_NORMAL` triggers ~128 KiB readahead per 4 KiB fault, wasting NVMe
//! bandwidth on neighbouring vectors that are evicted before the graph
//! walk reaches them. After `mmap`, segments must set `MADV_RANDOM`.
//!
//! When a segment is dropped, its pages should be proactively released via
//! `MADV_DONTNEED` so cold segments don't dominate the kernel page cache.

use nodedb_vector::mmap_segment::MmapVectorSegment;
use tempfile::tempdir;

use crate::support::test_memory;

fn make_segment(dir: &std::path::Path, name: &str, dim: usize, n: usize) -> MmapVectorSegment {
    let vecs: Vec<Vec<f32>> = (0..n).map(|i| vec![i as f32; dim]).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();
    MmapVectorSegment::create(dir, name, dim, &refs, &test_memory()).unwrap()
}

#[test]
fn open_advises_random() {
    let dir = tempdir().unwrap();
    let seg = make_segment(dir.path(), "rand.vseg", 64, 32);

    // Spec: open must call madvise(MADV_RANDOM) on the mapped region.
    // Observable via an accessor that records the last advice hint.
    let advice = seg.madvise_state();
    assert_eq!(
        advice,
        Some(libc::MADV_RANDOM),
        "MmapVectorSegment::open must advise MADV_RANDOM; got {advice:?}"
    );
}

#[test]
fn reopen_also_advises_random() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("reopen.vseg");
    make_segment(dir.path(), "reopen.vseg", 16, 8);

    let seg = MmapVectorSegment::open(&path, &test_memory()).unwrap();
    assert_eq!(seg.madvise_state(), Some(libc::MADV_RANDOM));
}

#[test]
fn drop_releases_pages_when_configured() {
    use nodedb_vector::mmap_segment::VectorSegmentDropPolicy;

    let dir = tempdir().unwrap();

    // Default policy releases pages on drop.
    assert!(VectorSegmentDropPolicy::default().dontneed_on_drop());

    // The drop-hook must observably emit MADV_DONTNEED. Expose via a
    // test-only counter on the segment module.
    let before = nodedb_vector::mmap_segment::observability::dontneed_count();
    {
        let _seg = make_segment(dir.path(), "drop.vseg", 8, 4);
    }
    let after = nodedb_vector::mmap_segment::observability::dontneed_count();
    assert_eq!(
        after - before,
        1,
        "drop must call madvise(MADV_DONTNEED) when policy.dontneed_on_drop()=true"
    );
}

#[test]
fn drop_skips_release_when_disabled() {
    use nodedb_vector::mmap_segment::VectorSegmentDropPolicy;

    let dir = tempdir().unwrap();
    let vecs: Vec<Vec<f32>> = (0..4).map(|i| vec![i as f32; 8]).collect();
    let refs: Vec<&[f32]> = vecs.iter().map(|v| v.as_slice()).collect();

    let before = nodedb_vector::mmap_segment::observability::dontneed_count();
    {
        let _seg = MmapVectorSegment::create_with_policy(
            dir.path(),
            "drop_off.vseg",
            8,
            &refs,
            VectorSegmentDropPolicy::keep_resident(),
            &test_memory(),
        )
        .unwrap();
    }
    let after = nodedb_vector::mmap_segment::observability::dontneed_count();
    assert_eq!(
        after, before,
        "keep_resident() policy must suppress MADV_DONTNEED on drop"
    );
}

#[test]
fn empty_segment_does_not_advise() {
    let dir = tempdir().unwrap();

    // Zero data bytes — advising a header-only region is a noop at best,
    // EINVAL at worst on some kernels. The open path must handle this.
    let seg = MmapVectorSegment::create(dir.path(), "empty.vseg", 3, &[], &test_memory()).unwrap();
    assert_eq!(seg.count(), 0);
    // Either None (skipped advise on zero-data) or MADV_RANDOM (advised header
    // page only) are acceptable; a panic or error is not.
    let advice = seg.madvise_state();
    assert!(advice.is_none() || advice == Some(libc::MADV_RANDOM));
}
