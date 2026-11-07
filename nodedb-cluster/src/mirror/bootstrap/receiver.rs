// SPDX-License-Identifier: BUSL-1.1

//! Mirror-side cross-cluster snapshot receiver.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crc32c::{crc32c, crc32c_append};
use nodedb_types::{Lsn, MirrorStatus};
use tracing::{debug, info};

use super::envelope::{
    BootstrapChunkOutcome, CrossClusterSnapshotEnvelope, PROGRESS_REPORT_CHUNK_BYTES,
    ProgressCallback,
};
use crate::mirror::error::MirrorError;

/// Accumulated state for an in-progress cross-cluster snapshot receive.
struct PartialState {
    source_cluster_id: String,
    database_id: String,
    snapshot_lsn: u64,
    total_bytes: u64,
    /// CRC32C declared by the source for the full snapshot.  Compared against
    /// `running_crc` when the final chunk arrives.
    declared_crc32c: u32,
    bytes_done: u64,
    next_expected_offset: u64,
    running_crc: u32,
    crc_initialized: bool,
    partial_file: Option<std::fs::File>,
    partial_path: PathBuf,
    /// Bytes received since the last progress report.
    since_last_report: u64,
}

/// Mirror-side cross-cluster snapshot receiver.
///
/// Thread-safe; wrap in `Arc` for shared use.
pub struct MirrorBootstrapReceiver {
    state: Mutex<Option<PartialState>>,
    data_dir: PathBuf,
    /// Invoked every `PROGRESS_REPORT_CHUNK_BYTES` with the current status.
    on_progress: ProgressCallback,
}

impl MirrorBootstrapReceiver {
    /// Create a new receiver.  `data_dir` is the directory where the
    /// `.partial` file is written (same convention as the in-cluster
    /// snapshot receiver — `<data_dir>/recv_snapshots/<database_id>.partial`).
    pub fn new(data_dir: PathBuf, on_progress: ProgressCallback) -> Self {
        Self {
            state: Mutex::new(None),
            data_dir,
            on_progress,
        }
    }

    /// Acquire the state mutex, surfacing poison as a typed error rather than
    /// silently recovering with `into_inner()`.
    ///
    /// A poisoned mutex means a previous chunk handler panicked while holding
    /// the lock; the partial state is potentially inconsistent (lost CRC,
    /// stale offsets).  Returning `Transport` causes the link to drop and
    /// reconnect, which restarts the snapshot from offset 0 — the only safe
    /// recovery.
    fn lock_state(&self) -> Result<MutexGuard<'_, Option<PartialState>>, MirrorError> {
        self.state
            .lock()
            .map_err(|e: PoisonError<_>| MirrorError::Transport {
                detail: format!(
                    "mirror bootstrap state poisoned (panic in previous chunk handler): {e}"
                ),
            })
    }

    /// Process one incoming [`CrossClusterSnapshotEnvelope`].
    ///
    /// Offset 0 always (re)starts a fresh receive, discarding any existing
    /// partial file.  This implements the resume semantic: on mirror restart
    /// the source will resend from offset 0.
    pub async fn handle_chunk(
        &self,
        envelope: CrossClusterSnapshotEnvelope,
    ) -> Result<BootstrapChunkOutcome, MirrorError> {
        let database_id = envelope.source_database_id.clone();
        let recv_dir = self.data_dir.join("recv_snapshots");

        spawn_blocking_io(
            {
                let d = recv_dir.clone();
                move || std::fs::create_dir_all(&d)
            },
            "create recv_snapshots dir",
        )
        .await?;

        if envelope.offset == 0 {
            // Start or restart: open fresh partial file.
            let partial_path = partial_path_for(&recv_dir, &database_id);
            let file = spawn_blocking_io(
                {
                    let p = partial_path.clone();
                    move || {
                        std::fs::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&p)
                    }
                },
                "open partial file",
            )
            .await?;

            let ps = PartialState {
                source_cluster_id: envelope.source_cluster_id.clone(),
                database_id: database_id.clone(),
                snapshot_lsn: envelope.snapshot_lsn,
                total_bytes: envelope.total_bytes,
                declared_crc32c: envelope.total_crc32c,
                bytes_done: 0,
                next_expected_offset: 0,
                running_crc: 0,
                crc_initialized: false,
                partial_file: Some(file),
                partial_path,
                since_last_report: 0,
            };

            let mut guard = self.lock_state()?;
            *guard = Some(ps);
        } else {
            // Validate continuation offset.
            let guard = self.lock_state()?;
            match guard.as_ref() {
                None => {
                    return Err(MirrorError::SnapshotOffsetRegression {
                        database_id,
                        expected: 0,
                        actual: envelope.offset,
                    });
                }
                Some(ps) if ps.next_expected_offset != envelope.offset => {
                    let expected = ps.next_expected_offset;
                    drop(guard);
                    return Err(MirrorError::SnapshotOffsetRegression {
                        database_id,
                        expected,
                        actual: envelope.offset,
                    });
                }
                Some(_) => {}
            }
        }

        let chunk_bytes = envelope.data.clone();
        let written_len = chunk_bytes.len() as u64;

        // Take the file out, write via spawn_blocking, then restore.
        let file = {
            let file = {
                let mut guard = self.lock_state()?;
                let ps = guard.as_mut().ok_or_else(|| MirrorError::Transport {
                    detail: "partial state disappeared during write".into(),
                })?;
                ps.partial_file
                    .take()
                    .ok_or_else(|| MirrorError::Transport {
                        detail: "partial file already taken".into(),
                    })?
            };
            spawn_blocking_io(
                {
                    let bytes = chunk_bytes.clone();
                    move || -> std::io::Result<std::fs::File> {
                        let mut f = file;
                        f.write_all(&bytes)?;
                        f.flush()?;
                        Ok(f)
                    }
                },
                "write partial chunk",
            )
            .await?
        };

        // Update CRC + offsets, then decide whether a progress report is due.
        // The since_last_report reset happens inside this same locked region
        // so the read-and-reset is atomic w.r.t. concurrent chunk handlers.
        let (bytes_done, total_bytes, should_report) = {
            let mut guard = self.lock_state()?;
            let ps = guard.as_mut().ok_or_else(|| MirrorError::Transport {
                detail: "partial state disappeared after write".into(),
            })?;
            if written_len > 0 {
                if !ps.crc_initialized {
                    ps.running_crc = crc32c(&chunk_bytes);
                    ps.crc_initialized = true;
                } else {
                    ps.running_crc = crc32c_append(ps.running_crc, &chunk_bytes);
                }
            }
            ps.next_expected_offset += written_len;
            ps.bytes_done += written_len;
            ps.since_last_report += written_len;
            ps.partial_file = Some(file);
            let should_report = ps.since_last_report >= PROGRESS_REPORT_CHUNK_BYTES;
            if should_report {
                ps.since_last_report = 0;
            }
            (ps.bytes_done, ps.total_bytes, should_report)
        };

        if should_report {
            (self.on_progress)(MirrorStatus::Bootstrapping {
                bytes_done,
                bytes_total: total_bytes,
            });
        }

        if !envelope.done {
            return Ok(BootstrapChunkOutcome::Pending { bytes_done });
        }

        // Final chunk: validate CRC, then commit.
        let ps = {
            let mut guard = self.lock_state()?;
            guard.take().ok_or_else(|| MirrorError::Transport {
                detail: "partial state disappeared before finalization".into(),
            })?
        };

        // CRC validation.  An empty snapshot (no payload bytes ever written)
        // has running_crc = 0 by construction; the source must declare 0 in
        // that case for the check to pass.
        let computed_crc = if ps.crc_initialized {
            ps.running_crc
        } else {
            0
        };
        if computed_crc != ps.declared_crc32c {
            // Drop the partial file before returning so the next attempt
            // starts fresh.
            drop(ps.partial_file);
            let path = ps.partial_path.clone();
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(&path)).await;
            return Err(MirrorError::SnapshotCrcMismatch {
                database_id: ps.database_id,
                stored: ps.declared_crc32c,
                computed: computed_crc,
            });
        }

        // Close the file by dropping it.
        drop(ps.partial_file);

        let snapshot_lsn = Lsn::new(ps.snapshot_lsn);
        let snapshot_path = ps.partial_path.clone();

        // Rename .partial → .snapshot to signal completion.
        let final_path = snapshot_path.with_extension("snapshot");
        spawn_blocking_io(
            {
                let src = snapshot_path.clone();
                let dst = final_path.clone();
                move || std::fs::rename(&src, &dst)
            },
            "rename partial to snapshot",
        )
        .await?;

        info!(
            database_id = %ps.database_id,
            source_cluster = %ps.source_cluster_id,
            snapshot_lsn = ps.snapshot_lsn,
            total_bytes = ps.bytes_done,
            crc32c = format!("{:#010x}", computed_crc),
            "cross-cluster snapshot transfer complete"
        );

        // The file landed, and the mirror holds no row of it yet. The caller
        // reports `Following` once the re-issue finished.
        (self.on_progress)(MirrorStatus::Bootstrapping {
            bytes_done: ps.bytes_done,
            bytes_total: ps.total_bytes,
        });

        Ok(BootstrapChunkOutcome::Committed {
            snapshot_lsn,
            snapshot_path: final_path,
        })
    }

    /// Abort any in-progress bootstrap, removing the partial file if present.
    ///
    /// Called when the link disconnects mid-bootstrap so the next reconnect
    /// starts fresh.  If the lock is poisoned this still tries to clear what
    /// it can (this path is best-effort cleanup, not correctness-critical).
    pub async fn abort(&self) {
        let ps = match self.state.lock() {
            Ok(mut g) => g.take(),
            Err(p) => p.into_inner().take(),
        };
        if let Some(mut ps) = ps {
            drop(ps.partial_file.take());
            let path = ps.partial_path.clone();
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(&path)).await;
            debug!(database_id = %ps.database_id, "aborted in-progress bootstrap");
        }
    }
}

fn partial_path_for(recv_dir: &Path, database_id: &str) -> PathBuf {
    recv_dir.join(format!("{database_id}.partial"))
}

/// Run a blocking I/O closure on the tokio blocking pool and flatten both the
/// `JoinError` and the inner `io::Error` into a single
/// [`MirrorError::Transport`] tagged with `op` for diagnostics.
async fn spawn_blocking_io<F, T>(f: F, op: &'static str) -> Result<T, MirrorError>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(MirrorError::Transport {
            detail: format!("{op}: {e}"),
        }),
        Err(join_err) => Err(MirrorError::Transport {
            detail: format!("{op}: blocking task panicked or was cancelled: {join_err}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;

    fn crc_of_chunks(chunks: &[&[u8]]) -> u32 {
        let mut state: Option<u32> = None;
        for c in chunks {
            state = Some(match state {
                None => crc32c(c),
                Some(prev) => crc32c_append(prev, c),
            });
        }
        state.unwrap_or(0)
    }

    fn make_envelope(
        offset: u64,
        data: Vec<u8>,
        done: bool,
        total_bytes: u64,
        total_crc32c: u32,
    ) -> CrossClusterSnapshotEnvelope {
        CrossClusterSnapshotEnvelope {
            source_cluster_id: "prod-us".into(),
            source_database_id: "db_01TEST".into(),
            snapshot_lsn: 42,
            total_bytes,
            total_crc32c,
            offset,
            data,
            done,
        }
    }

    #[tokio::test]
    async fn bootstrap_streams_full_snapshot_and_transitions() {
        let tmp = TempDir::new().unwrap();
        let status_log: Arc<Mutex<Vec<MirrorStatus>>> = Arc::new(Mutex::new(Vec::new()));
        let log2 = Arc::clone(&status_log);
        let cb: ProgressCallback = Arc::new(move |s| {
            log2.lock().unwrap().push(s);
        });

        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        let crc = crc_of_chunks(&[b"hel", b"lo!"]);
        let c1 = make_envelope(0, b"hel".to_vec(), false, 6, crc);
        let c2 = make_envelope(3, b"lo!".to_vec(), true, 6, crc);

        let r1 = receiver.handle_chunk(c1).await.unwrap();
        assert!(matches!(
            r1,
            BootstrapChunkOutcome::Pending { bytes_done: 3 }
        ));

        let r2 = receiver.handle_chunk(c2).await.unwrap();
        assert!(
            matches!(r2, BootstrapChunkOutcome::Committed { snapshot_lsn, .. }
                if snapshot_lsn == Lsn::new(42)),
            "unexpected outcome: {r2:?}"
        );

        // The file landed. The mirror holds no row until its caller re-issues
        // it, so the receiver never reports `Following`.
        let log = status_log.lock().unwrap();
        assert!(!log.contains(&MirrorStatus::Following), "log: {log:?}");
        assert_eq!(
            log.last(),
            Some(&MirrorStatus::Bootstrapping {
                bytes_done: 6,
                bytes_total: 6,
            }),
            "log: {log:?}"
        );
    }

    #[tokio::test]
    async fn crc_mismatch_rejects_final_chunk() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        // Source declares a wrong CRC.
        let bad_crc = crc_of_chunks(&[b"hel", b"lo!"]).wrapping_add(1);
        let c1 = make_envelope(0, b"hel".to_vec(), false, 6, bad_crc);
        let c2 = make_envelope(3, b"lo!".to_vec(), true, 6, bad_crc);

        receiver.handle_chunk(c1).await.unwrap();
        let err = receiver.handle_chunk(c2).await.unwrap_err();
        assert!(
            matches!(err, MirrorError::SnapshotCrcMismatch { stored, computed, .. }
                if stored == bad_crc && computed == crc_of_chunks(&[b"hel", b"lo!"])),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn crc_mismatch_removes_partial_file() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        let bad_crc = 0xDEAD_BEEF;
        let c1 = make_envelope(0, b"hello".to_vec(), true, 5, bad_crc);
        let _ = receiver.handle_chunk(c1).await;

        let partial = tmp.path().join("recv_snapshots").join("db_01TEST.partial");
        assert!(
            !partial.exists(),
            "partial file should be removed after CRC mismatch"
        );
    }

    #[tokio::test]
    async fn empty_snapshot_with_zero_crc_commits() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        // A zero-byte snapshot: source declares CRC = 0 (matching the
        // receiver's "no chunks ever observed" baseline).
        let c1 = make_envelope(0, vec![], true, 0, 0);
        let r = receiver.handle_chunk(c1).await.unwrap();
        assert!(matches!(r, BootstrapChunkOutcome::Committed { .. }));
    }

    #[tokio::test]
    async fn progress_reported_every_mib() {
        let tmp = TempDir::new().unwrap();
        let report_count = Arc::new(AtomicU64::new(0));
        let count2 = Arc::clone(&report_count);
        let cb: ProgressCallback = Arc::new(move |s| {
            if matches!(s, MirrorStatus::Bootstrapping { .. }) {
                count2.fetch_add(1, Ordering::Relaxed);
            }
        });

        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);
        let mib = PROGRESS_REPORT_CHUNK_BYTES as usize;
        let total = (mib * 3) as u64;
        let chunks: Vec<Vec<u8>> = (0..3).map(|_| vec![0u8; mib]).collect();
        let crc = crc_of_chunks(&chunks.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
        let c1 = make_envelope(0, chunks[0].clone(), false, total, crc);
        let c2 = make_envelope(mib as u64, chunks[1].clone(), false, total, crc);
        let c3 = make_envelope((mib * 2) as u64, chunks[2].clone(), true, total, crc);
        receiver.handle_chunk(c1).await.unwrap();
        receiver.handle_chunk(c2).await.unwrap();
        receiver.handle_chunk(c3).await.unwrap();

        let count = report_count.load(Ordering::Relaxed);
        assert!(count >= 2, "expected ≥2 Bootstrapping reports, got {count}");
    }

    #[tokio::test]
    async fn offset_regression_returns_error() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        let crc = crc_of_chunks(&[b"abc"]);
        let c1 = make_envelope(0, b"abc".to_vec(), false, 6, crc);
        receiver.handle_chunk(c1).await.unwrap();

        let bad = make_envelope(5, b"xx".to_vec(), false, 6, crc);
        let err = receiver.handle_chunk(bad).await.unwrap_err();
        assert!(
            matches!(
                err,
                MirrorError::SnapshotOffsetRegression {
                    expected: 3,
                    actual: 5,
                    ..
                }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn bytes_done_is_monotonic() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb);

        let chunks: Vec<Vec<u8>> = (0u64..4).map(|i| vec![i as u8; 16]).collect();
        let crc = crc_of_chunks(&chunks.iter().map(|v| v.as_slice()).collect::<Vec<_>>());

        let mut prev = 0u64;
        for (i, c) in chunks.into_iter().enumerate() {
            let offset = (i as u64) * 16;
            let done = i == 3;
            let chunk = make_envelope(offset, c, done, 64, crc);
            match receiver.handle_chunk(chunk).await.unwrap() {
                BootstrapChunkOutcome::Pending { bytes_done } => {
                    assert!(bytes_done > prev, "not monotonic at step {i}");
                    prev = bytes_done;
                }
                BootstrapChunkOutcome::Committed { .. } => {}
            }
        }
    }

    #[tokio::test]
    async fn poisoned_state_returns_transport_error() {
        let tmp = TempDir::new().unwrap();
        let cb: ProgressCallback = Arc::new(|_| {});
        let receiver = Arc::new(MirrorBootstrapReceiver::new(tmp.path().to_path_buf(), cb));

        // Poison the mutex via a panicking closure holding the guard.
        let r2 = Arc::clone(&receiver);
        let _ = std::thread::spawn(move || {
            let _g = r2.state.lock().unwrap();
            panic!("intentional panic to poison the mutex");
        })
        .join();

        let crc = crc_of_chunks(&[b"x"]);
        let c1 = make_envelope(0, b"x".to_vec(), true, 1, crc);
        let err = receiver.handle_chunk(c1).await.unwrap_err();
        assert!(
            matches!(&err, MirrorError::Transport { detail } if detail.contains("poisoned")),
            "expected Transport(poisoned), got: {err:?}"
        );
    }
}
