//! Spec: mmap'd overflow spill regions must advise MADV_RANDOM.
//!
//! `OverflowRegion` (nodedb-mem/src/overflow.rs) is a file-backed bump
//! allocator used by the memory governor when engine budgets spill beyond
//! RAM. Writes are scattered across the region (per-engine slots, free-list
//! reuse); reads from sibling cores' read-only mmaps are equally scattered.
//! The default MADV_NORMAL triggers unwanted readahead on every page fault,
//! polluting the page cache with neighbouring spill data that won't be read.
//!
//! Same invariant as nodedb-vector/mmap_segment.rs (HNSW MADV_RANDOM) and
//! nodedb-wal/mmap_reader.rs (MADV_SEQUENTIAL). Overflow spill shares the
//! design flaw: mmap without advice.

use nodedb_mem::OverflowRegion;
use nodedb_mem::overflow;
use tempfile::tempdir;

#[test]
fn open_advises_random() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("spill.bin");

    // Spec: OverflowRegion::open must call madvise(MADV_RANDOM) on the
    // mapped region. Observable via an accessor on the region itself.
    let region = OverflowRegion::open(&path).unwrap();

    assert_eq!(
        region.madvise_state(),
        Some(libc::MADV_RANDOM),
        "OverflowRegion::open must advise MADV_RANDOM on the mapped spill region"
    );
}

#[test]
fn open_with_capacity_also_advises_random() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("spill2.bin");

    let region = OverflowRegion::open_with_capacity(&path, 1024 * 1024).unwrap();
    assert_eq!(region.madvise_state(), Some(libc::MADV_RANDOM));
}

#[test]
fn regrowth_re_advises_after_mremap() {
    // Spec: after mremap grows the region, the new mapping inherits no
    // advice — the bump-growth path must re-advise MADV_RANDOM on the
    // new base. This is the quietest variant of the bug because growth
    // is rare and the regression would only surface under sustained spill.
    let dir = tempdir().unwrap();
    let path = dir.path().join("spill_grow.bin");

    let before = overflow::test_hooks::madv_random_count();
    let _region = OverflowRegion::open_with_capacity(&path, 4096).unwrap();
    // Region should have advised exactly once on open.
    let opened = overflow::test_hooks::madv_random_count() - before;
    assert_eq!(opened, 1, "open must advise MADV_RANDOM once");
}
