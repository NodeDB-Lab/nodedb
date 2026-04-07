//! Transaction command handlers: BEGIN, COMMIT, ROLLBACK, SAVEPOINT.
//!
//! Extracted from `sql_exec.rs` — handles all transactional state management
//! including snapshot isolation conflict detection, WAL transaction batching,
//! GAP_FREE sequence reservation lifecycle, and deferred offset commits.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle BEGIN / START TRANSACTION.
    pub(super) fn handle_begin(&self, addr: &std::net::SocketAddr) -> PgWireResult<Vec<Response>> {
        let snapshot_lsn = {
            let next = self.state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        self.sessions.begin(addr, snapshot_lsn).map_err(|msg| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25P02".to_owned(),
                msg.to_owned(),
            )))
        })?;
        Ok(vec![Response::Execution(Tag::new("BEGIN"))])
    }

    /// Handle COMMIT / END / END TRANSACTION.
    ///
    /// Performs snapshot isolation conflict detection, WAL transaction batching,
    /// GAP_FREE sequence finalization, and deferred offset commits.
    pub(super) async fn handle_commit(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        // Snapshot isolation: check for write conflicts before committing.
        let read_set = self.sessions.take_read_set(addr);
        if let Some(snapshot_lsn) = self.sessions.snapshot_lsn(addr) {
            let current_lsn = self.state.wal.next_lsn();
            let current = crate::types::Lsn::new(current_lsn.as_u64().saturating_sub(1));
            for (_collection, _doc_id, read_lsn) in &read_set {
                if current > *read_lsn && current > snapshot_lsn {
                    // WAL advanced past what we read — concurrent write detected.
                    if let Ok(reservations) = self.sessions.rollback(addr) {
                        for handle in &reservations {
                            let key = handle.sequence_key.clone();
                            let registry = &self.state.sequence_registry;
                            registry.gap_free_manager().rollback(handle, || {
                                let map = registry.sequences_read();
                                if let Some(h) = map.get(&key) {
                                    h.rollback_one();
                                }
                            });
                        }
                    }
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "40001".to_owned(),
                        "could not serialize access due to concurrent update".to_owned(),
                    ))));
                }
            }
        }

        let buffered = self.sessions.commit(addr).map_err(|msg| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "25000".to_owned(),
                msg.to_owned(),
            )))
        })?;

        if !buffered.is_empty() {
            let tenant_id = identity.tenant_id;
            let vshard_id = buffered[0].vshard_id;

            let mut sub_records: Vec<(u16, Vec<u8>)> = Vec::with_capacity(buffered.len());
            for task in &buffered {
                if let Some(entry) = crate::control::wal_replication::to_replicated_entry(
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                ) {
                    let bytes = entry.to_bytes();
                    sub_records.push((nodedb_wal::record::RecordType::Put as u16, bytes));
                }
            }

            if !sub_records.is_empty() {
                let tx_payload = zerompk::to_msgpack_vec(&sub_records).map_err(|e| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("transaction WAL serialization failed: {e}"),
                    )))
                })?;
                self.state
                    .wal
                    .append_transaction(tenant_id, vshard_id, &tx_payload)
                    .map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("transaction WAL append failed: {e}"),
                        )))
                    })?;
            }

            // Dispatch all writes as a single TransactionBatch for
            // atomic execution on the Data Plane.
            let plans: Vec<crate::bridge::envelope::PhysicalPlan> =
                buffered.iter().map(|t| t.plan.clone()).collect();
            let batch_task = crate::control::planner::physical::PhysicalTask {
                tenant_id,
                vshard_id,
                plan: crate::bridge::envelope::PhysicalPlan::Meta(
                    crate::bridge::physical_plan::MetaOp::TransactionBatch { plans },
                ),
                post_dedup: false,
            };
            if let Err(e) = self.dispatch_task_no_wal(batch_task).await {
                tracing::warn!(error = %e, "transaction batch dispatch failed");
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "40001".to_owned(),
                    format!("transaction commit failed: {e}"),
                ))));
            }
        }

        // Flush pending offset commits (deferred from COMMIT OFFSET inside transaction).
        let pending_offsets = self.sessions.take_pending_offsets(addr);
        for (tid, stream, group, partition_id, lsn) in pending_offsets {
            if let Err(e) =
                self.state
                    .offset_store
                    .commit_offset(tid, &stream, &group, partition_id, lsn)
            {
                tracing::warn!(
                    stream = %stream,
                    group = %group,
                    partition = partition_id,
                    error = %e,
                    "failed to commit deferred offset"
                );
            }
        }

        // Finalize GAP_FREE reservations (numbers become permanent).
        let reservations = self.sessions.take_pending_reservations(addr);
        for handle in &reservations {
            self.state
                .sequence_registry
                .gap_free_manager()
                .commit(handle);
            // Log to _system.sequence_log.
            if let Some(catalog) = self.state.credentials.catalog() {
                crate::control::sequence::log::log_reservation(
                    catalog,
                    &crate::control::sequence::log::committed(
                        &handle.sequence_key,
                        handle.value,
                        &identity.username,
                        identity.tenant_id.as_u32(),
                    ),
                );
            }
        }

        // Close non-WITH-HOLD cursors on transaction end.
        self.sessions.close_non_hold_cursors(addr);
        Ok(vec![Response::Execution(Tag::new("COMMIT"))])
    }

    /// Handle ROLLBACK / ABORT.
    pub(super) fn handle_rollback(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let reservations = self.sessions.rollback(addr).unwrap_or_default();
        for handle in &reservations {
            let key = &handle.sequence_key;
            let registry = &self.state.sequence_registry;
            registry.gap_free_manager().rollback(handle, || {
                let map = registry.sequences_read();
                if let Some(h) = map.get(key.as_str()) {
                    h.rollback_one();
                }
            });
            if let Some(catalog) = self.state.credentials.catalog() {
                crate::control::sequence::log::log_reservation(
                    catalog,
                    &crate::control::sequence::log::rolled_back(
                        key,
                        handle.value,
                        &identity.username,
                        identity.tenant_id.as_u32(),
                    ),
                );
            }
        }
        self.sessions.close_non_hold_cursors(addr);
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }

    /// Handle deferred COMMIT OFFSET inside a transaction block.
    ///
    /// Returns `Some(response)` if handled, `None` if not a deferred offset commit.
    pub(super) fn try_handle_deferred_offset(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
        upper: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        if !(upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ")) {
            return None;
        }
        if self.sessions.transaction_state(addr)
            != crate::control::server::pgwire::session::TransactionState::InBlock
        {
            return None;
        }

        let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
        let tenant_id = identity.tenant_id.as_u32();

        // Single-partition: COMMIT OFFSET PARTITION <p> AT <lsn> ON <stream> CONSUMER GROUP <name>
        if parts.len() >= 11
            && parts[2].eq_ignore_ascii_case("PARTITION")
            && parts[4].eq_ignore_ascii_case("AT")
            && parts[6].eq_ignore_ascii_case("ON")
        {
            let partition_id: u16 = parts[3].parse().unwrap_or(0);
            let lsn: u64 = parts[5].parse().unwrap_or(0);
            let stream_name = parts[7].to_lowercase();
            let group_name = parts[10].to_lowercase();
            self.sessions.defer_offset_commit(
                addr,
                tenant_id,
                stream_name,
                group_name,
                partition_id,
                lsn,
            );
            return Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSET"))]));
        }

        // Batch: COMMIT OFFSETS ON <stream> CONSUMER GROUP <name>
        if parts.len() >= 7
            && parts[1].eq_ignore_ascii_case("OFFSETS")
            && parts[2].eq_ignore_ascii_case("ON")
        {
            let stream_name = parts[3].to_lowercase();
            let group_name = parts[6].to_lowercase();
            if let Some(buffer) = self.state.cdc_router.get_buffer(tenant_id, &stream_name) {
                let events = buffer.read_from_lsn(0, usize::MAX);
                let mut latest: std::collections::HashMap<u16, u64> =
                    std::collections::HashMap::new();
                for e in &events {
                    let entry = latest.entry(e.partition).or_insert(0);
                    if e.lsn > *entry {
                        *entry = e.lsn;
                    }
                }
                for (pid, lsn) in latest {
                    self.sessions.defer_offset_commit(
                        addr,
                        tenant_id,
                        stream_name.clone(),
                        group_name.clone(),
                        pid,
                        lsn,
                    );
                }
            }
            return Some(Ok(vec![Response::Execution(Tag::new("COMMIT OFFSETS"))]));
        }

        None
    }

    /// Handle SAVEPOINT <name>.
    pub(super) fn handle_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .nth(1)
            .unwrap_or("sp")
            .to_string();
        self.sessions.create_savepoint(addr, sp_name);
        Ok(vec![Response::Execution(Tag::new("SAVEPOINT"))])
    }

    /// Handle RELEASE SAVEPOINT <name>.
    pub(super) fn handle_release_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        self.sessions.release_savepoint(addr, &sp_name);
        Ok(vec![Response::Execution(Tag::new("RELEASE"))])
    }

    /// Handle ROLLBACK TO SAVEPOINT <name>.
    pub(super) fn handle_rollback_to_savepoint(
        &self,
        addr: &std::net::SocketAddr,
        sql_trimmed: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sp_name = sql_trimmed
            .split_whitespace()
            .last()
            .unwrap_or("sp")
            .to_string();
        if let Err(msg) = self.sessions.rollback_to_savepoint(addr, &sp_name) {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "3B001".to_owned(),
                msg.to_string(),
            ))));
        }
        Ok(vec![Response::Execution(Tag::new("ROLLBACK"))])
    }
}
