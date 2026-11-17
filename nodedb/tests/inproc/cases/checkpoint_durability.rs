// SPDX-License-Identifier: BUSL-1.1

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
        src.contains("atomic_write_fsync")
            || src.contains("write_checkpoint_framed")
            || src.contains("write_keyed_checkpoint"),
        "{rel} must route checkpoint writes through \
         nodedb_wal::segment::atomic_write_fsync — directly, or via the \
         write_checkpoint_framed / write_keyed_checkpoint CRC wrappers that \
         delegate to it — so tmp-file \
         data and the parent directory are fsynced before rename. Raw fs::write + \
         fs::rename is not crash-safe on ext4/XFS."
    );
    // The raw anti-pattern must not coexist with the helper.
    let raw_write_rename = src.contains("fs::write(&tmp") && src.contains("fs::rename(&tmp");
    assert!(
        !raw_write_rename,
        "{rel} still contains the non-durable fs::write + fs::rename tmp \
         pattern. Replace with atomic_write_fsync."
    );
}

#[test]
fn vector_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/vector_checkpoint/write.rs");
    assert_durable_checkpoint_writer("nodedb/src/data/executor/vector_checkpoint/publish.rs");
}

#[test]
fn sparse_vector_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/sparse_vector_checkpoint/write.rs");
}

#[test]
fn kv_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/kv_checkpoint/write.rs");
}

#[test]
fn sync_hwm_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/sync_hwm_checkpoint/write.rs");
}

#[test]
fn columnar_checkpoint_writes_are_durable() {
    // Columnar is memory-only on both halves: the live memtables AND the
    // encoded flushed-segment bytes. Its checkpoint files are the only non-WAL
    // copy of either, so a zero-filled file after power loss is data loss.
    assert_durable_checkpoint_writer("nodedb/src/data/executor/columnar_checkpoint/write.rs");
}

#[test]
fn graph_label_checkpoint_writes_are_durable() {
    // The CSR node-label bitset has no redb store behind it — unlike the edges,
    // which are rebuilt from the `EdgeStore` at boot. This file is the labels'
    // only non-WAL copy, so a zero-filled file after power loss silently
    // unlabels every node while its edges come back intact.
    assert_durable_checkpoint_writer("nodedb/src/data/executor/graph_label_checkpoint/write.rs");
}

#[test]
fn array_segment_and_manifest_writes_are_durable() {
    // The array engine writes no checkpoint blob: the coordinated checkpoint
    // reports its on-disk tile SEGMENTS as its durability, so those writes are
    // checkpoint-class and carry the same power-loss exposure. The manifest
    // write is the commit point that makes a segment reachable at all.
    assert_durable_checkpoint_writer("nodedb/src/engine/array/flush.rs");
    assert_durable_checkpoint_writer("nodedb/src/engine/array/store/manifest.rs");
}

#[test]
fn timeseries_partition_writes_are_durable() {
    // Timeseries writes no checkpoint blob: the coordinated checkpoint reports
    // its on-disk L1 PARTITIONS as its durability, so every file the segment
    // writer emits is checkpoint-class and carries the same power-loss
    // exposure. `partition.meta` is written last and is the commit point that
    // makes the partition reachable at all.
    assert_durable_checkpoint_writer("nodedb/src/engine/timeseries/columnar_segment/writer.rs");
}

#[test]
fn spatial_checkpoint_writes_are_durable() {
    assert_durable_checkpoint_writer("nodedb/src/data/executor/spatial_checkpoint/write.rs");
}

#[test]
fn snapshot_executor_checkpoint_writes_are_durable() {
    // Snapshot restore writes every captured checkpoint file that the normal
    // startup path later loads, so it shares the power-loss exposure of the
    // live checkpoint writers. `RestoreWriter` in `snapshot_files.rs` is the
    // one write site: it calls the durable helper. Every restore module
    // writes through it and never touches a file directly.
    assert_durable_checkpoint_writer("nodedb/src/storage/snapshot_files.rs");
    for rel in [
        "nodedb/src/storage/snapshot_executor.rs",
        "nodedb/src/ctl/restore/execute.rs",
    ] {
        let full = read(rel);
        // Test fixtures below `#[cfg(test)]` stage files on purpose.
        let src = full.split("#[cfg(test)]").next().unwrap_or_default();
        assert!(
            src.contains("RestoreWriter"),
            "{rel} must write restored files through RestoreWriter, whose \
             writes go through atomic_write_fsync."
        );
        assert!(
            !src.contains("fs::write(") && !src.contains("fs::rename("),
            "{rel} writes a restored file directly. Route it through \
             RestoreWriter so the file and its directory are fsynced."
        );
    }
}

#[test]
fn snapshot_writer_core_and_manifest_writes_are_durable() {
    // The snapshot writer routes all writes through an `ObjectStore` backend.
    // `ObjectStore::put` is atomic and durable on all supported backends
    // (LocalFileSystem uses a temp-file+rename internally; S3-compatible stores
    // use upload semantics with no partial-write exposure). The old
    // `atomic_write_fsync` helper is no longer needed in this file.
    let src = read("nodedb/src/storage/snapshot_writer.rs");
    assert!(
        src.contains("ObjectStore") || src.contains("object_store"),
        "snapshot_writer.rs must route writes through an ObjectStore backend \
         for durable, atomic writes across local and remote storage tiers."
    );
    // The raw non-durable pattern must not be present.
    let raw_write_rename = src.contains("fs::write(&tmp") && src.contains("fs::rename(&tmp");
    assert!(
        !raw_write_rename,
        "snapshot_writer.rs still contains the non-durable fs::write + \
         fs::rename tmp pattern. Use ObjectStore::put instead."
    );
}

#[test]
fn crdt_checkpoint_writes_are_durable() {
    // Shares the same design flaw as the five files named in the bug report —
    // CRDT checkpoints also use the raw fs::write + fs::rename pair.
    assert_durable_checkpoint_writer(
        "nodedb/src/data/executor/handlers/control/checkpoint_crdt.rs",
    );
    assert_durable_checkpoint_writer("nodedb/src/data/executor/crdt_checkpoint/publish.rs");
}

/// The keyed checkpoint frame and the helper every per-collection checkpoint
/// writes and reads through must stay on the durable write path and the
/// page-cache-drop read path.
#[test]
fn keyed_checkpoint_frame_wraps_durable_helpers() {
    let frame = read("nodedb-wal/src/segment/checkpoint_keyed_frame.rs");
    assert!(
        frame.contains("atomic_write_fsync") && frame.contains("read_checkpoint_dontneed"),
        "write_checkpoint_keyed / read_checkpoint_keyed must delegate to \
         atomic_write_fsync and read_checkpoint_dontneed."
    );
    let helper = read("nodedb/src/data/executor/checkpoint_key/file.rs");
    assert!(
        helper.contains("write_checkpoint_keyed") && helper.contains("read_checkpoint_keyed"),
        "the keyed checkpoint helper must write and read through the keyed WAL frame."
    );
}

/// The CRC-framing wrapper must not bypass the durable write path: it has to
/// delegate to `atomic_write_fsync` (tmp write + data fsync + rename + parent
/// directory fsync), not hand-roll its own `fs::write` + `fs::rename`.
#[test]
fn framed_checkpoint_writer_wraps_durable_helper() {
    let src = read("nodedb-wal/src/segment/checkpoint_frame.rs");
    assert!(
        src.contains("atomic_write_fsync"),
        "write_checkpoint_framed must delegate to atomic_write_fsync so the CRC \
         frame is written through the same durable tmp+fsync+rename+dir-fsync path."
    );
}

/// Regression guard against the specific silent-failure mode: on ext4 / XFS
/// the parent directory entry can reach disk before the tmp file's data
/// pages. The helper must fsync the parent directory after rename.
#[test]
fn atomic_write_helper_fsyncs_parent_directory() {
    let wal_segment = read("nodedb-wal/src/segment/atomic_io.rs");
    assert!(
        wal_segment.contains("pub fn atomic_write_fsync"),
        "nodedb_wal::segment must expose `atomic_write_fsync(dir, name, bytes)` \
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
    assert_durable_checkpoint_writer(
        "nodedb/src/engine/timeseries/partition_registry/persistence.rs",
    );
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
///
/// A timeseries partition merge renames its staged partition into place and
/// removes its sources. The commit manifest that makes it durable is one
/// atomic file write, and so is the merged partition's rewritten meta.
#[test]
fn timeseries_merge_directory_swap_is_durable() {
    let src = read("nodedb/src/data/executor/handlers/timeseries/merge_commit.rs");
    assert!(
        src.contains("atomic_swap_dirs_fsync") || src.contains("fsync_directory"),
        "timeseries merge directory swap must fsync the parent directory \
         after renames — otherwise the new partition dir's inode can be \
         visible before its contents are on stable storage."
    );
    for rel in [
        "nodedb/src/data/executor/handlers/timeseries/merge_commit.rs",
        "nodedb/src/data/executor/handlers/timeseries/partition_merge.rs",
    ] {
        assert_durable_checkpoint_writer(rel);
    }
}

#[test]
fn timeseries_ddl_partition_swap_is_durable() {
    // The owning Data Plane core performs the `REWRITE PARTITIONS` swap, and
    // boot recovery renames an interrupted swap's backup back into place.
    for rel in [
        "nodedb/src/data/executor/handlers/timeseries/partition_rewrite.rs",
        "nodedb/src/data/executor/timeseries_checkpoint/rewrite_recovery.rs",
    ] {
        let src = read(rel);
        assert!(
            src.contains("atomic_swap_dirs_fsync") || src.contains("fsync_directory"),
            "{rel}: a timeseries partition rewrite directory swap must fsync \
             the parent directory after renames."
        );
    }
}

/// Checkpoint reads are consumed once and then superseded by the in-memory
/// index. Leaving the bytes in the page cache wastes memory that other
/// workloads need. Reads should advise the kernel via POSIX_FADV_DONTNEED.
#[test]
fn checkpoint_reads_drop_page_cache() {
    for rel in [
        "nodedb/src/data/executor/vector_checkpoint/load.rs",
        "nodedb/src/data/executor/sparse_vector_checkpoint/load.rs",
        "nodedb/src/data/executor/spatial_checkpoint/load.rs",
        "nodedb/src/data/executor/crdt_checkpoint/load.rs",
        "nodedb/src/data/executor/kv_checkpoint/load.rs",
        "nodedb/src/data/executor/sync_hwm_checkpoint/load.rs",
        // The columnar load path reads its bytes in the manifest and
        // generation-restore readers, not in `load.rs`.
        "nodedb/src/data/executor/columnar_checkpoint/manifest.rs",
        "nodedb/src/data/executor/columnar_checkpoint/restore.rs",
        "nodedb/src/data/executor/graph_label_checkpoint/load.rs",
        // The timeseries loader reads one `partition.meta` per partition to
        // rebuild the registry at boot. They are consumed once and superseded
        // by the in-memory registry, exactly like every checkpoint blob above.
        "nodedb/src/data/executor/timeseries_checkpoint/load.rs",
        "nodedb/src/data/executor/timeseries_checkpoint/stamp.rs",
        "nodedb/src/data/executor/timeseries_checkpoint/series_catalog.rs",
        "nodedb/src/data/executor/timeseries_checkpoint/schema.rs",
        // Durable timeseries sidecars and the sequencer snapshot are read
        // once at boot or recovery, then superseded by in-memory state.
        "nodedb/src/data/executor/handlers/timeseries/cagg_persist.rs",
        "nodedb/src/data/executor/handlers/timeseries/merge_commit.rs",
        "nodedb/src/data/executor/handlers/timeseries/flush_commit.rs",
        "nodedb/src/control/cluster/sequencer_snapshot.rs",
    ] {
        let src = read(rel);
        assert!(
            src.contains("read_checkpoint_dontneed")
                || src.contains("read_checkpoint_framed")
                || src.contains("read_keyed_checkpoint")
                || src.contains("POSIX_FADV_DONTNEED"),
            "{rel} reads checkpoint bytes with `std::fs::read`, leaving them \
             pinned in the page cache for the process lifetime. Use the \
             shared `read_checkpoint_dontneed` helper — directly or via the \
             `read_checkpoint_framed` CRC wrapper that delegates to it (or an \
             equivalent POSIX_FADV_DONTNEED advise) — so the bytes are evicted \
             after load."
        );
    }
}

/// The CRC-framing read wrapper must not bypass the page-cache-drop path: it
/// has to delegate to `read_checkpoint_dontneed`, not hand-roll `std::fs::read`.
#[test]
fn framed_checkpoint_reader_wraps_dontneed_helper() {
    let src = read("nodedb-wal/src/segment/checkpoint_frame.rs");
    assert!(
        src.contains("read_checkpoint_dontneed"),
        "read_checkpoint_framed must delegate to read_checkpoint_dontneed so \
         framed checkpoint bytes are still evicted from the page cache after load."
    );
}
