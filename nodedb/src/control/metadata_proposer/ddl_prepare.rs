// SPDX-License-Identifier: BUSL-1.1

//! The DDL preparation lease: the local lock and the replicated lease that
//! serialize descriptor preparation across the cluster.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::RuntimeFlavor;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, WaitOutcome, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

use super::handle::MetadataRaftHandle;
use super::timeouts::DEFAULT_PROPOSE_TIMEOUT;

const DDL_PREPARE_LEASE: Duration = Duration::from_secs(60);
const DDL_PREPARE_WAIT: Duration = Duration::from_secs(70);

fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn propose_metadata_and_wait(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    entry: &MetadataEntry,
    timeout: Duration,
) -> Result<u64, Error> {
    let raw = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })?;
    let index = handle.propose(raw)?;
    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = tokio::task::block_in_place(|| watcher.wait_for(index, timeout));
    match outcome {
        WaitOutcome::Reached => Ok(index),
        WaitOutcome::TimedOut => Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {timeout:?} waiting for log index {index} (current: {})",
                watcher.current()
            ),
        }),
        WaitOutcome::GroupGone => Err(Error::Config {
            detail: "metadata group no longer hosted on this node".into(),
        }),
    }
}

/// RAII ownership of the metadata-Raft-serialized descriptor preparation lease.
/// The matching release is itself replicated, so another node cannot stamp from
/// the same prior catalog version until this guard is dropped and that release
/// has applied.
pub(crate) struct DdlPrepareGuard<'a> {
    shared: &'a SharedState,
    handle: &'a dyn MetadataRaftHandle,
    token: u64,
}

impl DdlPrepareGuard<'_> {
    pub(crate) fn token(&self) -> u64 {
        self.token
    }
}

impl Drop for DdlPrepareGuard<'_> {
    fn drop(&mut self) {
        if let Err(error) = propose_metadata_and_wait(
            self.shared,
            self.handle,
            &MetadataEntry::DdlPrepareRelease { token: self.token },
            DEFAULT_PROPOSE_TIMEOUT,
        ) {
            tracing::error!(token = self.token, %error, "metadata DDL lease release failed");
        }
    }
}

pub(crate) fn acquire_ddl_prepare_lease<'a>(
    shared: &'a SharedState,
    handle: &'a dyn MetadataRaftHandle,
) -> Result<DdlPrepareGuard<'a>, Error> {
    let sequence = shared
        .metadata_ddl_token_seq
        .fetch_add(1, Ordering::Relaxed);
    let token = shared.node_id.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ wall_now_ns().rotate_left(17)
        ^ sequence;
    let deadline = Instant::now() + DDL_PREPARE_WAIT;

    loop {
        propose_metadata_and_wait(
            shared,
            handle,
            &MetadataEntry::DdlPrepareAcquire { token },
            DEFAULT_PROPOSE_TIMEOUT,
        )?;

        loop {
            let owner = *shared
                .metadata_ddl_owner
                .lock()
                .map_err(|_| Error::Config {
                    detail: "metadata DDL owner lock poisoned".into(),
                })?;
            match owner {
                Some((current, _)) if current == token => {
                    return Ok(DdlPrepareGuard {
                        shared,
                        handle,
                        token,
                    });
                }
                Some((current, acquired_at))
                    if shared.is_metadata_leader()
                        && acquired_at.elapsed() >= DDL_PREPARE_LEASE =>
                {
                    // Cancel the dead owner's pending record before releasing its
                    // lease, so it never lingers visible-but-unresolved past the lease.
                    if shared.pending_ddl.contains(current) {
                        propose_metadata_and_wait(
                            shared,
                            handle,
                            &MetadataEntry::DdlPendingCancel { token: current },
                            DEFAULT_PROPOSE_TIMEOUT,
                        )?;
                    }
                    propose_metadata_and_wait(
                        shared,
                        handle,
                        &MetadataEntry::DdlPrepareRelease { token: current },
                        DEFAULT_PROPOSE_TIMEOUT,
                    )?;
                    break;
                }
                None => break,
                Some(_) if Instant::now() < deadline => {
                    // Reached from async tasks (ILP batch flush ->
                    // `propose_catalog_entry`), so hand the worker back to
                    // tokio rather than parking it: the lease owner this
                    // polls for is released by a raft apply that needs a
                    // worker to make progress.
                    tokio::task::block_in_place(|| {
                        std::thread::sleep(Duration::from_millis(10));
                    });
                }
                Some(_) => {
                    return Err(Error::Config {
                        detail: "metadata DDL preparation lease timed out".into(),
                    });
                }
            }
        }
    }
}

/// Take the local DDL preparation lock, handing the wait back to tokio when
/// the caller is on a multi-thread worker.
///
/// The holder keeps this lock across the distributed preparation lease, the
/// descriptor drain and the local apply wait — each already wrapped in
/// `block_in_place`, but that only tells tokio about the waits *inside* the
/// lock, never about the wait *for* it. A bare `lock()` on a worker therefore
/// removes that worker from the runtime silently, including from the raft
/// apply work the current holder needs in order to finish, which turns
/// contention into a self-sustaining stall.
///
/// `block_in_place` is a passthrough outside a multi-thread worker (plain sync
/// callers, blocking-pool threads) and panics on the current-thread runtime,
/// so it is applied only where it is both legal and meaningful — mirroring
/// `lease::drain_propose::poll_leases_drained`.
pub(super) fn lock_ddl_preparation(
    shared: &SharedState,
) -> Result<std::sync::MutexGuard<'_, ()>, Error> {
    let acquire = || {
        shared.metadata_ddl_lock.lock().map_err(|_| Error::Config {
            detail: "metadata DDL preparation lock poisoned".into(),
        })
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(acquire)
        }
        _ => acquire(),
    }
}
