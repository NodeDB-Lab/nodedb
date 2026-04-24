//! Per-engine checkpoint writes must be durable across power loss.
//!
//! Each checkpoint writer uses the tmp-file + rename pattern for atomicity, but
//! on ext4 / XFS the rename metadata can reach disk before the data pages
//! backing the tmp file. A power loss between the write and the next checkpoint
//! then leaves a correctly-named file containing zeros, which is load-unsafe.
//!
//! `nodedb-wal::segment::roll_segment` already gets this right via `fsync` of
//! both the file and its parent directory. The checkpoint writers must use the
//! same pattern — exposed as a shared helper `nodedb_wal::segment::atomic_write_fsync`
//! — so the invariant is enforced in one place and cannot drift per call site.
//!
//! These tests are lint-style regression guards: they fail if any checkpoint
//! writer drops back to the non-durable `fs::write` + `fs::rename` pair. They
//! pass once every writer routes through the shared helper.

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // Tests run from the `nodedb` crate dir; workspace root is the parent.
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    crate_dir.parent().unwrap().to_path_buf()
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Assert that `src` uses the shared durable helper and does not contain the
/// raw `fs::rename` anti-pattern on checkpoint / snapshot paths.
///
/// The regression guard is specific: the checkpoint write site must reference
/// `atomic_write_fsync` (the shared helper in `nodedb_wal::segment`). A bare
/// `fs::rename(...tmp..., ...)` without an accompanying `sync_data` + directory
/// fsync is the exact pattern that caused the bug.
fn assert_durable_checkpoint_writer(rel: &str) {
    let src = read(rel);
    assert!(
        src.contains("atomic_write_fsync"),
        "{rel} must route checkpoint writes through \
         nodedb_wal::segment::atomic_write_fsync so tmp-file data and the \
         parent directory are fsynced before rename. Raw fs::write + \
         fs::rename is not crash-safe on ext4/XFS."
    );
    // The raw anti-pattern must not coexist. Allow `fs::rename` only when the
    // file also mentions the helper *and* the helper wraps it.
    let raw_write_rename = src.contains("fs::write(&tmp")
        && src.contains("fs::rename(&tmp")
        && !src.contains("atomic_write_fsync(&tmp");
    assert!(
        !raw_write_rename,
        "{rel} still contains the non-durable fs::write + fs::rename tmp \
         pattern. Replace with atomic_write_fsync."
    );
}

#[test]
fn vector_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/vector_checkpoint.rs");
}

#[test]
fn sparse_vector_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/sparse_vector_checkpoint.rs");
}

#[test]
fn spatial_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/spatial_checkpoint.rs");
}

#[test]
fn snapshot_executor_checkpoint_writes_are_durable() {
    // Both `restore_vector_checkpoints` and `restore_crdt_checkpoints` write
    // checkpoint files that the normal startup path later loads — they share
    // the same power-loss exposure as the live checkpoint writers.
    assert_durable_checkpoint_writer("nodedb/src/storage/snapshot_executor.rs");
}

#[test]
fn snapshot_writer_core_and_manifest_writes_are_durable() {
    // Both the per-core snapshot file write and the manifest write use the
    // tmp+rename pattern; the manifest is the source of truth for restore and
    // must survive power loss.
    assert_durable_checkpoint_writer("nodedb/src/storage/snapshot_writer.rs");
}

#[test]
fn crdt_checkpoint_writes_are_durable() {
    // Shares the same design flaw as the five files named in the bug report —
    // CRDT checkpoints also use the raw fs::write + fs::rename pair.
    assert_durable_checkpoint_writer("nodedb/src/data/executor/handlers/control/snapshot.rs");
}

/// Regression guard against the specific silent-failure mode: on ext4 / XFS
/// the parent directory entry can reach disk before the tmp file's data
/// pages. The helper must fsync the parent directory after rename.
#[test]
fn atomic_write_helper_fsyncs_parent_directory() {
    let wal_segment = read("nodedb-wal/src/segment/atomic_io.rs");
    assert!(
        wal_segment.contains("pub fn atomic_write_fsync"),
        "nodedb_wal::segment must expose `atomic_write_fsync(tmp, dst, bytes)` \
         — the single helper through which all tmp+rename checkpoint writes \
         go. Without it, the invariant is re-implemented (and re-broken) per \
         call site."
    );
    // The helper body must fsync data and the parent directory, otherwise it
    // reproduces the exact bug it exists to prevent.
    let helper_start = wal_segment
        .find("pub fn atomic_write_fsync")
        .expect("helper must exist");
    let helper_body = &wal_segment[helper_start..];
    assert!(
        helper_body.contains("sync_data") || helper_body.contains("sync_all"),
        "atomic_write_fsync must call sync_data()/sync_all() on the tmp file \
         before rename, otherwise the rename can hit disk before the data \
         pages (the exact bug this helper prevents)."
    );
    assert!(
        helper_body.contains("fsync_directory"),
        "atomic_write_fsync must fsync the parent directory after rename so \
         the directory entry is durable."
    );
}

#[test]
fn timeseries_partition_registry_persist_is_durable() {
    // `PartitionRegistry::persist` writes the authoritative partition map via
    // the same tmp+rename pattern — recovery reads it on startup, so the
    // zero-file failure mode is identical to checkpoint writers.
    assert_durable_checkpoint_writer("nodedb/src/engine/timeseries/partition_registry/persistence.rs");
}

#[test]
fn jwks_disk_cache_write_is_durable() {
    // JWKS disk cache seeds token verification on startup before the network
    // fetch completes. A zero-byte cache file after power loss bricks auth
    // until the fetch succeeds.
    assert_durable_checkpoint_writer("nodedb/src/control/security/jwks/cache.rs");
}

#[test]
fn regen_certs_writes_are_durable() {
    // Cert/key regen writes the new material to tmp then renames into place.
    // A zero-byte cert or key after power loss means the node cannot re-TLS
    // on restart.
    assert_durable_checkpoint_writer("nodedb/src/ctl/regen_certs.rs");
}

/// Directory-level atomic swaps (`rename(partition_dir, backup); rename(tmp,
/// partition_dir)`) have the same durability gap as file-level tmp+rename:
/// the directory entries can reach disk before the contents of the newly
/// renamed-in directory. The swap helper must fsync both parent directories.
#[test]
fn timeseries_merge_directory_swap_is_durable() {
    let src = read("nodedb/src/engine/timeseries/merge/o3.rs");
    assert!(
        src.contains("atomic_swap_dirs_fsync") || src.contains("fsync_directory"),
        "timeseries merge directory swap must fsync the parent directory \
         after renames — otherwise the new partition dir's inode can be \
         visible before its contents are on stable storage."
    );
}

#[test]
fn timeseries_ddl_partition_swap_is_durable() {
    let src = read("nodedb/src/control/server/pgwire/ddl/timeseries.rs");
    assert!(
        src.contains("atomic_swap_dirs_fsync") || src.contains("fsync_directory"),
        "timeseries DDL partition rewrite directory swap must fsync the \
         parent directory after renames."
    );
}

/// Checkpoint reads are consumed once and then superseded by the in-memory
/// index. Leaving the bytes in the page cache wastes memory that other
/// workloads need. Reads should advise the kernel via POSIX_FADV_DONTNEED.
#[test]
fn checkpoint_reads_drop_page_cache() {
    for rel in [
        "nodedb/src/data/executor/vector_checkpoint.rs",
        "nodedb/src/data/executor/sparse_vector_checkpoint.rs",
        "nodedb/src/data/executor/spatial_checkpoint.rs",
    ] {
        let src = read(rel);
        assert!(
            src.contains("read_checkpoint_dontneed") || src.contains("POSIX_FADV_DONTNEED"),
            "{rel} reads checkpoint bytes with `std::fs::read`, leaving them \
             pinned in the page cache for the process lifetime. Use the \
             shared `read_checkpoint_dontneed` helper (or equivalent \
             POSIX_FADV_DONTNEED advise) so the bytes are evicted after load."
        );
    }
}
