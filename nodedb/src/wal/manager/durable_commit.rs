// SPDX-License-Identifier: BUSL-1.1

//! Async group-commit durability barrier.
//!
//! WAL appends only buffer a record and return its [`Lsn`]; the fsync that makes
//! the record durable is deferred. [`WalManager::wait_durable`] is the barrier
//! the Control-Plane write path awaits after appending (and after the Data-Plane
//! apply) and before returning the client ack, so an acknowledged write is
//! guaranteed WAL-fsync-durable.
//!
//! Concurrent writers coalesce into one fsync: the first waiter becomes the
//! leader, fsyncs the shared WAL once on a blocking thread, advances
//! `durable_lsn`, and wakes every follower whose target the fsync covered. This
//! is entirely Control-Plane (Tokio) work — leadership is awaited on an async
//! mutex and the only blocking syscall runs inside `spawn_blocking`.

use std::sync::atomic::Ordering::{AcqRel, Acquire};

use super::core::WalManager;
use crate::types::Lsn;

impl WalManager {
    /// Ensure the WAL is fsync-durable through `lsn` before returning.
    ///
    /// Coalesces concurrent callers into a single group-commit fsync: the first
    /// waiter becomes the leader, fsyncs the shared WAL once, advances
    /// `durable_lsn`, and wakes all followers; followers whose `lsn` the leader's
    /// fsync covered return without their own syscall.
    ///
    /// On fsync failure `durable_lsn` is left unchanged and the error is
    /// propagated so the caller's ack fails rather than falsely reporting
    /// durability; followers are still woken so they re-attempt leadership and
    /// observe the same failure.
    pub async fn wait_durable(&self, lsn: Lsn) -> crate::Result<()> {
        let target = lsn.as_u64();

        // Fast path: already durable.
        if self.durable_lsn.load(Acquire) >= target {
            return Ok(());
        }

        loop {
            // Register for notification BEFORE re-checking `durable_lsn`. The
            // `Notified` future captures the notify state at creation, so a
            // `notify_waiters()` racing between this check and the `.await`
            // below is not lost.
            let notified = self.durable_notify.notified();
            if self.durable_lsn.load(Acquire) >= target {
                return Ok(());
            }

            match self.commit_lock.try_lock() {
                Ok(_guard) => {
                    // Re-check under leadership: a prior leader's fsync may have
                    // already covered this target.
                    if self.durable_lsn.load(Acquire) >= target {
                        return Ok(());
                    }

                    // Fsync the shared WAL on a blocking thread — the O_DIRECT
                    // `sync()` must not run inline on a Tokio worker. Read the
                    // head LSN under the same lock the sync holds so `durable_lsn`
                    // advances to exactly what the fsync made durable, never past
                    // it.
                    let wal = std::sync::Arc::clone(&self.wal);
                    let join = tokio::task::spawn_blocking(move || -> crate::Result<u64> {
                        let mut guard = wal.lock().unwrap_or_else(|p| p.into_inner());
                        guard.sync().map_err(crate::Error::Wal)?;
                        // `next_lsn()` is the next LSN to assign; the highest LSN
                        // this sync made durable is one below it.
                        Ok(guard.next_lsn().saturating_sub(1))
                    })
                    .await;

                    let outcome = match join {
                        Ok(Ok(durable_through)) => {
                            self.durable_lsn.fetch_max(durable_through, AcqRel);
                            Ok(())
                        }
                        // fsync error: do NOT advance `durable_lsn`.
                        Ok(Err(e)) => Err(e),
                        Err(join_err) => Err(crate::Error::Internal {
                            detail: format!(
                                "WAL group-commit fsync task failed to join: {join_err}"
                            ),
                        }),
                    };

                    // Wake followers on every leader-exit path so a failed or
                    // panicked fsync never strands them on `notified.await`. On
                    // success `durable_lsn` was advanced first, so they observe
                    // durability; on failure they re-attempt leadership and hit
                    // the same error, failing their acks too.
                    self.durable_notify.notify_waiters();
                    return outcome;
                }
                Err(_) => {
                    // Another writer is the leader; wait for their fsync to land.
                    notified.await;
                    if self.durable_lsn.load(Acquire) >= target {
                        return Ok(());
                    }
                    // Their batch did not cover us (or their fsync failed): loop
                    // and try to lead next.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    #[tokio::test]
    async fn wait_durable_makes_appended_record_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let lsn = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"payload",
            )
            .expect("append");
        wal.wait_durable(lsn).await.expect("wait_durable");
        assert!(wal.durable_lsn.load(Acquire) >= lsn.as_u64());
    }

    #[tokio::test]
    async fn wait_durable_fast_path_when_already_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let lsn = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"payload",
            )
            .expect("append");
        wal.wait_durable(lsn).await.expect("first");
        // Second call is the fast path — no further fsync required.
        wal.wait_durable(lsn).await.expect("second");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_waiters_coalesce() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(open_wal(dir.path()));
        let mut lsns = Vec::new();
        for _ in 0..16 {
            lsns.push(
                wal.appender(NO_APPLY_KEY)
                    .with_event_source(crate::event::EventSource::User)
                    .append_put(
                        TenantId::new(1),
                        VShardId::new(0),
                        DatabaseId::DEFAULT,
                        b"payload",
                    )
                    .expect("append"),
            );
        }
        let max = *lsns.iter().max().expect("nonempty");
        let mut handles = Vec::new();
        for lsn in lsns {
            let wal = std::sync::Arc::clone(&wal);
            handles.push(tokio::spawn(async move {
                wal.wait_durable(lsn).await.expect("wait_durable");
            }));
        }
        for h in handles {
            h.await.expect("join");
        }
        assert!(wal.durable_lsn.load(Acquire) >= max.as_u64());
    }
}
