// SPDX-License-Identifier: BUSL-1.1

//! The transaction's buffered write tasks, read-set, and descriptor leases.

use std::sync::Arc;

use nodedb_cluster::DescriptorId;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::lease::QueryLeaseScope;

use super::super::connection::SessionId;
use super::super::read_set::ReadSetEntry;
use super::super::state::TransactionState;
use super::super::store::SessionStore;

impl SessionStore {
    /// Append captured read-set entries for write conflict detection.
    ///
    /// The single write path behind [`super::super::read_set::record_read_set`]: the
    /// neutral capture helper builds one [`ReadSetEntry`] per observed shard and
    /// hands them here. Guarded on the connection being inside a transaction
    /// block — outside one, the entries are dropped (autocommit reads never
    /// enter validation).
    pub fn record_read_entries(&self, addr: impl Into<SessionId>, entries: Vec<ReadSetEntry>) {
        if entries.is_empty() {
            return;
        }
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                session.tx_read_set.extend(entries);
            }
        });
    }

    /// Clone the read-set without draining it, so a caller can classify the
    /// commit's dispatch before COMMIT runs.
    pub fn read_set(&self, addr: impl Into<SessionId>) -> Vec<ReadSetEntry> {
        self.read_session(addr, |session| session.tx_read_set.clone())
            .unwrap_or_default()
    }

    /// Drain the read-set for conflict checking at COMMIT time.
    pub fn take_read_set(&self, addr: impl Into<SessionId>) -> Vec<ReadSetEntry> {
        self.write_session(addr, |session| std::mem::take(&mut session.tx_read_set))
            .unwrap_or_default()
    }

    /// Collect a value from each buffered write task's plan. Used at commit to
    /// gather the collections this transaction wrote, so its own reads of those
    /// collections are excluded from snapshot-isolation conflict detection
    /// (a read-your-own-write is not a serialization conflict).
    pub fn buffered_collections<F>(
        &self,
        addr: impl Into<SessionId>,
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

    /// Clone the current transaction's buffered write tasks WITHOUT consuming
    /// them or transitioning session state, so COMMIT can classify dispatch off
    /// the buffered writes while still holding the option to `rollback` on a
    /// conflict. `commit()` remains the consuming drain.
    pub fn buffered_tasks(&self, addr: impl Into<SessionId>) -> Vec<PhysicalTask> {
        self.read_session(addr, |s| s.tx_buffer.clone())
            .unwrap_or_default()
    }

    /// Buffer a write task during a transaction block.
    ///
    /// Stamps the task's `txn_id` from the session's active transaction
    /// identity before buffering, inside the same session-lock scope, so
    /// there is no separate lock acquisition that can race or deadlock
    /// against `buffer_write`'s own lock.
    ///
    /// Returns `true` if buffered (in transaction), `false` if not (dispatch immediately).
    pub fn buffer_write(&self, addr: impl Into<SessionId>, mut task: PhysicalTask) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state == TransactionState::InBlock {
                task.txn_id = session.tx_id;
                session.tx_vshards.insert(task.vshard_id);
                session.tx_buffer.push(task);
                session.tx_lease_scopes.push(None);
                debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
                true
            } else {
                false
            }
        })
        .unwrap_or(false)
    }

    /// Mark every task buffered since `start` as a trigger body's.
    pub fn mark_body_tasks_since(&self, addr: impl Into<SessionId>, start: usize) {
        self.write_session(addr, |session| {
            let end = session.tx_buffer.len();
            session.tx_body_tasks.extend(start.min(end)..end);
        });
    }

    /// Record that the stage-time preview of the most recently buffered task,
    /// a timeseries ingest, rejected `rejected` lines.
    pub fn note_ts_preview_rejected(&self, addr: impl Into<SessionId>, rejected: u64) {
        if rejected == 0 {
            return;
        }
        self.write_session(addr, |session| {
            if let Some(index) = session.tx_buffer.len().checked_sub(1) {
                session.tx_ts_preview_rejected.insert(index, rejected);
            }
        });
    }

    /// Lines each buffered timeseries ingest's stage-time preview rejected,
    /// by index into the buffered tasks.
    pub fn ts_preview_rejected(
        &self,
        addr: impl Into<SessionId>,
    ) -> std::collections::BTreeMap<usize, u64> {
        self.read_session(addr, |session| session.tx_ts_preview_rejected.clone())
            .unwrap_or_default()
    }

    /// Indexes into the buffered tasks of the tasks a trigger body buffered.
    pub fn body_tasks(&self, addr: impl Into<SessionId>) -> std::collections::BTreeSet<usize> {
        self.read_session(addr, |session| session.tx_body_tasks.clone())
            .unwrap_or_default()
    }

    /// Number of tasks currently buffered for this transaction.
    pub fn buffered_task_count(&self, addr: impl Into<SessionId>) -> usize {
        self.read_session(addr, |session| {
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.tx_buffer.len()
        })
        .unwrap_or(0)
    }

    /// Every distinct `(descriptor, version)` pair this transaction's
    /// buffered tasks were planned against.
    ///
    /// Scopes are deduplicated by identity: one statement attaches the same
    /// `Arc<QueryLeaseScope>` to every task it buffered, so walking the
    /// holders without deduplicating repeats one statement's holds once per
    /// task it produced.
    pub fn tx_descriptor_versions(&self, addr: impl Into<SessionId>) -> Vec<(DescriptorId, u64)> {
        self.read_session(addr, |session| {
            let mut scopes: Vec<&Arc<QueryLeaseScope>> = Vec::new();
            for scope in session.tx_lease_scopes.iter().flatten() {
                if !scopes.iter().any(|seen| Arc::ptr_eq(seen, scope)) {
                    scopes.push(scope);
                }
            }
            scopes
                .into_iter()
                .flat_map(|scope| scope.descriptor_versions().iter().cloned())
                .collect()
        })
        .unwrap_or_default()
    }

    /// The retryable error if this node lost a lease any of this
    /// transaction's buffered statements holds, else `None`.
    pub fn tx_lease_revoked(&self, addr: impl Into<SessionId>) -> Option<crate::Error> {
        self.read_session(addr, |session| {
            session
                .tx_lease_scopes
                .iter()
                .flatten()
                .find_map(|scope| scope.check_not_revoked().err())
        })
        .flatten()
    }

    /// Retain a statement's descriptor lease scope for every task buffered
    /// since `start`. Fails closed when the transaction state or the aligned
    /// holders are invalid, or when a different statement already owns one.
    pub fn attach_tx_lease_scope_since(
        &self,
        addr: impl Into<SessionId>,
        start: usize,
        scope: Arc<QueryLeaseScope>,
    ) -> bool {
        self.write_session(addr, |session| {
            if session.tx_state != TransactionState::InBlock
                || session.tx_buffer.len() != session.tx_lease_scopes.len()
                || start > session.tx_buffer.len()
            {
                return false;
            }
            for holder in &mut session.tx_lease_scopes[start..] {
                if let Some(existing) = holder
                    && !Arc::ptr_eq(existing, &scope)
                {
                    return false;
                }
            }
            for holder in &mut session.tx_lease_scopes[start..] {
                if holder.is_none() {
                    *holder = Some(Arc::clone(&scope));
                }
            }
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            true
        })
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
    use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

    use super::super::super::store::SessionStore;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn ingest_task() -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"metrics value=1".to_vec(),
                format: "ilp".to_owned(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }
    }

    /// A staged ingest's preview count is kept against its buffer index
    /// until the transaction ends. A preview that rejected nothing keeps no
    /// entry.
    #[test]
    fn a_preview_rejection_is_kept_until_the_transaction_ends() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5311".parse().expect("addr");
        store.ensure_session(addr);
        store.begin(addr, 0).expect("begin");

        assert!(store.buffer_write(addr, ingest_task()));
        store.note_ts_preview_rejected(addr, 0);
        assert!(store.buffer_write(addr, ingest_task()));
        store.note_ts_preview_rejected(addr, 2);
        assert_eq!(
            store.ts_preview_rejected(addr),
            std::collections::BTreeMap::from([(1usize, 2u64)])
        );

        store.rollback(addr).expect("rollback");
        assert!(store.ts_preview_rejected(addr).is_empty());
    }
}
