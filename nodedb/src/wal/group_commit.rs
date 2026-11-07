// SPDX-License-Identifier: BUSL-1.1

//! Leader/follower group commit for a WAL whose appends and syncs are separate steps.
//!
//! A caller appends its records first and notes the highest LSN it appended.
//! It then calls [`GroupCommit::sync_through`] with that LSN.
//! With no sync in flight, the caller becomes the leader and syncs everything appended so far.
//! With a sync in flight, the caller waits until a sync covers its LSN.
//! So concurrent callers share one sync.
//!
//! A failed sync covers a range of LSNs. Every caller whose LSN is in that range gets an error.
//! None of them reports success, even when a later sync succeeds.
//!
//! A failure stays on record only while a caller can still ask about an LSN it covers.
//! An appender notes its LSN before the WAL mutex releases, and the note ends when its sync call returns.
//! A failure below the lowest noted LSN has no one left to tell, so it is pruned.
//! Each caller asks once. A second call for a pruned LSN reads the current durable mark.

use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex, MutexGuard};

/// One failed sync: the LSNs it covered and what failed.
struct FailedSync {
    from: u64,
    through: u64,
    detail: String,
}

struct GroupState {
    /// Every LSN at or below this is durable.
    synced: u64,
    /// Whether a leader is running a sync now.
    syncing: bool,
    /// The failed syncs some noted LSN can still ask about, by ascending `through`.
    failed: Vec<FailedSync>,
    /// LSNs appended and not yet answered by a sync call, with their callers.
    noted: BTreeMap<u64, u32>,
}

impl GroupState {
    /// Add a failure, merged into the last one when their ranges touch.
    ///
    /// Until a sync succeeds every failure starts at the same `from`, so a retry loop keeps one entry.
    fn push_failure(&mut self, failure: FailedSync) {
        match self.failed.last_mut() {
            Some(last) if last.through.saturating_add(1) >= failure.from => {
                last.through = last.through.max(failure.through);
                last.detail = failure.detail;
            }
            Some(_) | None => self.failed.push(failure),
        }
    }

    /// Forget every failure below the lowest noted LSN.
    fn prune_failures(&mut self) {
        let floor = self.noted.keys().next().copied().unwrap_or(u64::MAX);
        self.failed.retain(|failure| failure.through >= floor);
    }

    /// End one caller's note of `lsn`.
    fn end_note(&mut self, lsn: u64) {
        if let Some(callers) = self.noted.get_mut(&lsn) {
            *callers -= 1;
            if *callers == 0 {
                self.noted.remove(&lsn);
            }
        }
        self.prune_failures();
    }
}

/// Shared group-commit state for one WAL.
pub struct GroupCommit {
    /// The WAL's name in an error.
    engine: &'static str,
    state: Mutex<GroupState>,
    changed: Condvar,
}

/// Ends a caller's note on every exit, a panic included.
struct NotedCaller<'a> {
    commit: &'a GroupCommit,
    lsn: u64,
}

impl Drop for NotedCaller<'_> {
    fn drop(&mut self) {
        self.commit.lock().end_note(self.lsn);
    }
}

/// Clears the leader flag on every exit, a panic included, and wakes the followers.
struct LeaderTurn<'a> {
    commit: &'a GroupCommit,
}

impl Drop for LeaderTurn<'_> {
    fn drop(&mut self) {
        self.commit.lock().syncing = false;
        self.commit.changed.notify_all();
    }
}

impl GroupCommit {
    /// Start with every LSN at or below `synced` durable. `engine` names the WAL in an error.
    pub fn new(engine: &'static str, synced: u64) -> Self {
        Self {
            engine,
            state: Mutex::new(GroupState {
                synced,
                syncing: false,
                failed: Vec::new(),
                noted: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        }
    }

    /// Note that `lsn` was appended and its caller will call [`Self::sync_through`] for it.
    ///
    /// The appender must call this before it releases the WAL mutex.
    /// So a failed sync that covers `lsn` stays on record until that call returns.
    pub fn note_appended(&self, lsn: u64) {
        *self.lock().noted.entry(lsn).or_insert(0) += 1;
    }

    fn lock(&self) -> MutexGuard<'_, GroupState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Wait until `lsn` is durable.
    ///
    /// `sync` makes every appended record durable.
    /// It returns the highest LSN appended before it synced, and the sync result.
    /// The caller must hold no lock that an appender or `sync` needs.
    pub fn sync_through(
        &self,
        lsn: u64,
        sync: impl Fn() -> (u64, crate::Result<()>),
    ) -> crate::Result<()> {
        let _caller = NotedCaller { commit: self, lsn };
        let mut state = self.lock();
        // A caller with no earlier note counts as noted while it waits.
        state.noted.entry(lsn).or_insert(1);
        loop {
            if let Some(failure) = state
                .failed
                .iter()
                .find(|failure| (failure.from..=failure.through).contains(&lsn))
            {
                return Err(crate::Error::Storage {
                    engine: self.engine.into(),
                    detail: format!(
                        "{} sync failed for LSN {lsn} (failed range {}..={}): {}; check the WAL \
                         directory for a full disk or an I/O error",
                        self.engine, failure.from, failure.through, failure.detail
                    ),
                });
            }
            if state.synced >= lsn {
                return Ok(());
            }
            if state.syncing {
                state = match self.changed.wait(state) {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                continue;
            }

            // This caller leads: it syncs everything appended so far, with no group lock held.
            state.syncing = true;
            let from = state.synced.saturating_add(1);
            drop(state);
            let turn = LeaderTurn { commit: self };
            let (through, result) = sync();
            {
                let mut state = self.lock();
                match result {
                    Ok(()) => state.synced = state.synced.max(through),
                    Err(e) => {
                        tracing::error!(error = %e, engine = self.engine, from, through, "WAL sync failed");
                        state.push_failure(FailedSync {
                            from,
                            through: through.max(from),
                            detail: e.to_string(),
                        });
                    }
                }
            }
            drop(turn);
            state = self.lock();
        }
    }

    /// The highest durable LSN.
    pub fn synced(&self) -> u64 {
        self.lock().synced
    }

    /// Test support: failed sync ranges on record.
    #[cfg(test)]
    fn failures_on_record(&self) -> usize {
        self.lock().failed.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::*;

    #[test]
    fn concurrent_callers_share_syncs() {
        let commit = Arc::new(GroupCommit::new("test_wal", 0));
        let appended = Arc::new(AtomicU64::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(32));

        let threads: Vec<_> = (0..32)
            .map(|_| {
                let (commit, appended, syncs, start) = (
                    Arc::clone(&commit),
                    Arc::clone(&appended),
                    Arc::clone(&syncs),
                    Arc::clone(&start),
                );
                std::thread::spawn(move || {
                    start.wait();
                    let lsn = appended.fetch_add(1, Ordering::SeqCst) + 1;
                    commit.sync_through(lsn, || {
                        let through = appended.load(Ordering::SeqCst);
                        // A slow disk: every other caller appends while this sync runs.
                        std::thread::sleep(Duration::from_millis(30));
                        syncs.fetch_add(1, Ordering::SeqCst);
                        (through, Ok(()))
                    })
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("caller thread").expect("durable");
        }
        let syncs = syncs.load(Ordering::SeqCst);
        assert!(syncs <= 3, "32 callers must share syncs, saw {syncs}");
        assert_eq!(commit.synced(), 32);
    }

    #[test]
    fn a_failed_sync_reaches_every_covered_waiter() {
        let commit = Arc::new(GroupCommit::new("test_wal", 0));
        let appended = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(8));

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (commit, appended, start) = (
                    Arc::clone(&commit),
                    Arc::clone(&appended),
                    Arc::clone(&start),
                );
                std::thread::spawn(move || {
                    start.wait();
                    let lsn = appended.fetch_add(1, Ordering::SeqCst) + 1;
                    let outcome = commit.sync_through(lsn, || {
                        let through = appended.load(Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(30));
                        (
                            through,
                            Err(crate::Error::Internal {
                                detail: "injected fsync error".into(),
                            }),
                        )
                    });
                    (lsn, outcome)
                })
            })
            .collect();
        for thread in threads {
            let (lsn, outcome) = thread.join().expect("caller thread");
            assert!(outcome.is_err(), "LSN {lsn} must see the sync error");
        }
        assert_eq!(commit.synced(), 0, "a failed sync makes nothing durable");
    }

    #[test]
    fn a_later_success_does_not_clear_an_earlier_failure() {
        let commit = GroupCommit::new("test_wal", 0);
        commit.note_appended(1);
        commit.note_appended(2);
        let failed = commit.sync_through(2, || {
            (
                2,
                Err(crate::Error::Internal {
                    detail: "injected fsync error".into(),
                }),
            )
        });
        assert!(failed.is_err());
        commit.note_appended(3);
        assert!(commit.sync_through(3, || (3, Ok(()))).is_ok());
        assert!(
            commit.sync_through(1, || (3, Ok(()))).is_err(),
            "the caller of LSN 1 must see the sync that covered it"
        );
    }

    #[test]
    fn failures_are_pruned_once_no_caller_can_ask_about_them() {
        let commit = GroupCommit::new("test_wal", 0);
        let failing = || {
            (
                1,
                Err(crate::Error::Internal {
                    detail: "injected fsync error".into(),
                }),
            )
        };
        commit.note_appended(1);
        assert!(commit.sync_through(1, failing).is_err());
        assert_eq!(commit.failures_on_record(), 0, "no caller is left to tell");

        // Thousands of failures and successes in turn leave nothing behind.
        for lsn in 2..2_000u64 {
            commit.note_appended(lsn);
            let outcome = if lsn % 2 == 0 {
                commit.sync_through(lsn, || {
                    (
                        lsn,
                        Err(crate::Error::Internal {
                            detail: "injected fsync error".into(),
                        }),
                    )
                })
            } else {
                commit.sync_through(lsn, || (lsn, Ok(())))
            };
            assert_eq!(outcome.is_err(), lsn % 2 == 0);
            assert_eq!(commit.failures_on_record(), 0);
        }
    }

    #[test]
    fn a_failure_stays_for_an_appender_that_has_not_called_sync_yet() {
        let commit = GroupCommit::new("test_wal", 0);
        commit.note_appended(1);
        commit.note_appended(2);
        let leader = commit.sync_through(2, || {
            (
                2,
                Err(crate::Error::Internal {
                    detail: "injected fsync error".into(),
                }),
            )
        });
        assert!(leader.is_err());
        assert_eq!(commit.failures_on_record(), 1);
        assert!(commit.sync_through(1, || (2, Ok(()))).is_err());
        assert_eq!(commit.failures_on_record(), 0);
    }

    #[test]
    fn repeated_failures_of_one_range_keep_one_entry() {
        let commit = GroupCommit::new("test_wal", 0);
        commit.note_appended(1);
        for lsn in 2..50u64 {
            commit.note_appended(lsn);
            assert!(
                commit
                    .sync_through(lsn, || {
                        (
                            lsn,
                            Err(crate::Error::Internal {
                                detail: "injected fsync error".into(),
                            }),
                        )
                    })
                    .is_err()
            );
            assert_eq!(commit.failures_on_record(), 1);
        }
    }

    #[test]
    fn the_error_names_the_engine_it_was_built_with() {
        let commit = GroupCommit::new("other_wal", 0);
        let error = commit
            .sync_through(1, || {
                (
                    1,
                    Err(crate::Error::Internal {
                        detail: "injected fsync error".into(),
                    }),
                )
            })
            .expect_err("the sync failed");
        match error {
            crate::Error::Storage { engine, detail } => {
                assert_eq!(engine, "other_wal");
                assert!(detail.contains("other_wal sync failed"), "{detail}");
            }
            other => panic!("expected a storage error: {other:?}"),
        }
    }

    #[test]
    fn a_leader_panic_releases_the_followers() {
        let commit = GroupCommit::new("test_wal", 0);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            commit.sync_through(1, || panic!("injected sync panic"))
        }));
        assert!(outcome.is_err());
        assert!(
            commit.sync_through(1, || (1, Ok(()))).is_ok(),
            "the next caller must become leader"
        );
    }
}
