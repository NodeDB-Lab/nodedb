// SPDX-License-Identifier: BUSL-1.1

//! Transaction lifecycle methods on SessionStore.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::{Lsn, TxnId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

use super::read_set::ReadSetEntry;
use super::state::{SavepointEntry, TransactionState};
use super::store::SessionStore;

/// Process-local monotonic counter forming the low 48 bits of a `TxnId`.
/// The high 16 bits carry the node id (see [`SessionStore::begin`]), so the
/// full id is globally unique across the cluster — a transaction's staging
/// overlay lives on whichever shard leader owns the write, which may be a
/// different node than the coordinator, so two coordinators must never mint
/// the same id.
static NEXT_TXN_ID: AtomicU64 = AtomicU64::new(1);

impl SessionStore {
    /// Get transaction state for a connection.
    pub fn transaction_state(&self, addr: &SocketAddr) -> TransactionState {
        self.read_session(addr, |s| s.tx_state)
            .unwrap_or(TransactionState::Idle)
    }

    /// BEGIN — enter transaction block with snapshot isolation.
    ///
    /// Captures the current WAL LSN as the local snapshot point (single-shard
    /// fast path) and the last globally-applied Calvin `snapshot_epoch` as the
    /// cross-shard-valid version anchor. All reads within this transaction see
    /// data as of this LSN.
    pub fn begin(
        &self,
        addr: &SocketAddr,
        current_lsn: Lsn,
        snapshot_epoch: u64,
        node_id: u64,
    ) -> Result<(), &'static str> {
        self.write_session(addr, |session| match session.tx_state {
            TransactionState::Idle => {
                session.tx_state = TransactionState::InBlock;
                session.tx_snapshot_lsn = Some(current_lsn);
                session.tx_snapshot_epoch = Some(snapshot_epoch);
                session.tx_read_set.clear();
                // Transaction ids travel cross-node (staging overlays live on
                // the OWNING shard), so they must be globally unique: node id in
                // the high 16 bits, process-local counter in the low 48. Two
                // coordinators can never mint the same id, so their overlays
                // never collide on a shard that hosts both transactions.
                session.tx_id = Some(TxnId::new(
                    (node_id << 48) | (NEXT_TXN_ID.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF_FFFF),
                ));
                session.tx_vshards.clear();
                Ok(())
            }
            TransactionState::InBlock => {
                // PostgreSQL issues a WARNING here, not an error.
                Ok(())
            }
            TransactionState::Failed => Err(
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
        })
        .unwrap_or(Ok(()))
    }

    /// Append captured read-set entries for write conflict detection.
    ///
    /// The single write path behind [`super::read_set::record_read_set`]: the
    /// neutral capture helper builds one [`ReadSetEntry`] per observed shard and
    /// hands them here. Guarded on the connection being inside a transaction
    /// block — outside one, the entries are dropped (autocommit reads never
    /// enter validation).
    pub fn record_read_entries(&self, addr: &SocketAddr, entries: Vec<ReadSetEntry>) {
        if entries.is_empty() {
            return;
        }
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_read_set.extend(entries);
            }
        });
    }

    /// Get the snapshot LSN for the current transaction.
    pub fn snapshot_lsn(&self, addr: &SocketAddr) -> Option<Lsn> {
        self.read_session(addr, |s| s.tx_snapshot_lsn)?
    }

    /// Get the cross-shard snapshot epoch for the current transaction.
    pub fn snapshot_epoch(&self, addr: &SocketAddr) -> Option<u64> {
        self.read_session(addr, |s| s.tx_snapshot_epoch)?
    }

    /// Current transaction's overlay id, for stamping a `StageWrite` task
    /// before it is dispatched. `None` outside a transaction block.
    pub fn tx_id(&self, addr: &SocketAddr) -> Option<TxnId> {
        self.read_session(addr, |s| s.tx_id).flatten()
    }

    /// Snapshot the current transaction's overlay identity (id + the SET of
    /// vShards it has staged writes to) WITHOUT clearing it. Called before
    /// `rollback()` releases session state so the caller can dispatch
    /// `MetaOp::DropTxnOverlay` to EVERY vShard hosting a staging overlay, and by
    /// savepoint mark/rewind to fan the overlay meta-op over all staged vShards.
    /// The returned Vec is empty when no write has staged yet.
    pub fn txn_identity(&self, addr: &SocketAddr) -> (Option<TxnId>, Vec<VShardId>) {
        self.read_session(addr, |s| (s.tx_id, s.tx_vshards.iter().copied().collect()))
            .unwrap_or((None, Vec::new()))
    }

    /// Collect a value from each buffered write task's plan. Used at commit to
    /// gather the collections this transaction wrote, so its own reads of those
    /// collections are excluded from snapshot-isolation conflict detection
    /// (a read-your-own-write is not a serialization conflict).
    pub fn buffered_collections<F>(
        &self,
        addr: &SocketAddr,
        extract: F,
    ) -> std::collections::HashSet<String>
    where
        F: Fn(&nodedb_physical::physical_plan::PhysicalPlan) -> Option<String>,
    {
        self.read_session(addr, |s| {
            s.tx_buffer
                .iter()
                .filter_map(|task| extract(&task.plan))
                .collect()
        })
        .unwrap_or_default()
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: &SocketAddr) -> Vec<ReadSetEntry> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
    }

    /// COMMIT — drain the write buffer and pending offset commits, return to idle.
    ///
    /// Returns the buffered write tasks for atomic dispatch.
    pub fn commit(&self, addr: &SocketAddr) -> Result<Vec<PhysicalTask>, &'static str> {
        self.write_session(addr, |session| {
            let buffer = std::mem::take(&mut session.tx_buffer);
            session.tx_state = TransactionState::Idle;
            session.tx_snapshot_lsn = None;
            session.tx_snapshot_epoch = None;
            session.tx_id = None;
            session.tx_vshards.clear();
            session.savepoints.clear();
            // Note: pending_sequence_reservations are taken separately via
            // take_pending_reservations() so the caller can finalize them
            // with the GAP_FREE manager (which requires Arc<SequenceRegistry>).
            Ok(buffer)
        })
        .unwrap_or(Ok(Vec::new()))
    }

    /// Take pending GAP_FREE sequence reservations (called after successful COMMIT).
    pub fn take_pending_reservations(
        &self,
        addr: &SocketAddr,
    ) -> Vec<crate::control::sequence::gap_free::ReservationHandle> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_sequence_reservations)
        })
        .unwrap_or_default()
    }

    /// Take pending offset commits (called after successful COMMIT dispatch).
    pub fn take_pending_offsets(&self, addr: &SocketAddr) -> Vec<(u64, String, String, u32, u64)> {
        self.write_session(addr, |session| {
            std::mem::take(&mut session.pending_offset_commits)
        })
        .unwrap_or_default()
    }

    /// Defer an offset commit until the current transaction commits.
    ///
    /// Returns `true` if deferred (in transaction), `false` if not (commit immediately).
    pub fn defer_offset_commit(
        &self,
        addr: &SocketAddr,
        tenant_id: u64,
        stream: String,
        group: String,
        partition_id: u32,
        lsn: u64,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session
                    .pending_offset_commits
                    .push((tenant_id, stream, group, partition_id, lsn));
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// Buffer a write task during a transaction block.
    ///
    /// Stamps the task's `txn_id` from the session's active transaction
    /// identity before buffering, inside the same session-lock scope, so
    /// there is no separate lock acquisition that could race or deadlock
    /// against `buffer_write`'s own lock.
    ///
    /// Returns `true` if buffered (in transaction), `false` if not (dispatch immediately).
    pub fn buffer_write(&self, addr: &SocketAddr, mut task: PhysicalTask) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                task.txn_id = session.tx_id;
                session.tx_vshards.insert(task.vshard_id);
                session.tx_buffer.push(task);
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// ROLLBACK — discard the write buffer and return to idle.
    /// Returns any pending GAP_FREE reservations that need to be rolled back.
    pub fn rollback(
        &self,
        addr: &SocketAddr,
    ) -> Result<Vec<crate::control::sequence::gap_free::ReservationHandle>, &'static str> {
        let reservations = self
            .write_session(addr, |session| {
                session.tx_buffer.clear();
                session.tx_state = TransactionState::Idle;
                session.tx_snapshot_lsn = None;
                session.tx_snapshot_epoch = None;
                session.tx_id = None;
                session.tx_vshards.clear();
                session.tx_read_set.clear();
                session.savepoints.clear();
                session.pending_offset_commits.clear();
                std::mem::take(&mut session.pending_sequence_reservations)
            })
            .unwrap_or_default();
        Ok(reservations)
    }

    /// Mark the current transaction as failed (after a query error inside BEGIN).
    pub fn fail_transaction(&self, addr: &SocketAddr) {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_state = TransactionState::Failed;
            }
        });
    }

    /// Create a savepoint at the current tx_buffer position.
    ///
    /// `markers` maps each vShard that had staged writes at savepoint time to its
    /// Data-Plane value/TTL and GRAPH overlay undo-journal lengths (captured via
    /// `MetaOp::MarkSavepoint`), so a later ROLLBACK TO can rewind every staging
    /// overlay to exactly this point.
    pub fn create_savepoint(
        &self,
        addr: &SocketAddr,
        name: String,
        markers: BTreeMap<VShardId, (usize, usize)>,
    ) {
        self.write_session(addr, |session| {
            let buffer_len = session.tx_buffer.len();
            session.savepoints.push(SavepointEntry {
                name,
                buffer_len,
                markers,
            });
        });
    }

    /// Release a savepoint: destroy the named savepoint and every savepoint
    /// established after it, keeping their buffered/staged effects (PostgreSQL
    /// semantics). Returns `Err` (SQLSTATE 3B001) if the name does not exist.
    pub fn release_savepoint(&self, addr: &SocketAddr, name: &str) -> crate::Result<()> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            session.savepoints.truncate(pos);
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }

    /// Rollback to a savepoint: truncate tx_buffer to the saved position and
    /// return the per-vShard `(value_marker, graph_marker)` overlay journal
    /// markers the caller must rewind each staged vShard's Data-Plane staging
    /// overlays to. A vShard first staged AFTER the savepoint is absent from the
    /// returned map; the caller rewinds it to `(0, 0)`.
    ///
    /// Returns `Err` if the savepoint does not exist (matches PostgreSQL behavior).
    pub fn rollback_to_savepoint(
        &self,
        addr: &SocketAddr,
        name: &str,
    ) -> crate::Result<BTreeMap<VShardId, (usize, usize)>> {
        self.write_session(addr, |session| {
            let pos = session
                .savepoints
                .iter()
                .rposition(|e| e.name == name)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("savepoint \"{name}\" does not exist"),
                })?;
            let buffer_len = session.savepoints[pos].buffer_len;
            let markers = session.savepoints[pos].markers.clone();
            session.tx_buffer.truncate(buffer_len);
            session.savepoints.truncate(pos + 1);
            Ok(markers)
        })
        .unwrap_or_else(|| {
            Err(crate::Error::BadRequest {
                detail: "no active session".to_string(),
            })
        })
    }
}
