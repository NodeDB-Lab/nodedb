// SPDX-License-Identifier: BUSL-1.1

//! Audit recording and memory-estimate update methods for `SharedState`.

use std::sync::Arc;

use tracing::{error, warn};

use super::SharedState;

impl SharedState {
    /// Reset per-second rate counters. Called by a 1-second timer.
    pub fn reset_tenant_rate_counters(&self) {
        match self.tenants.lock() {
            Ok(mut t) => t.reset_rate_counters(),
            Err(poisoned) => poisoned.into_inner().reset_rate_counters(),
        }
    }

    /// Record an audit event (best-effort) with full database context.
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it.
    pub fn audit_record_with_db(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) {
        self.audit_record_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &crate::control::security::audit::AuditAuth::default(),
        );
    }

    /// Record an audit event (best-effort) that carries the authenticated user and session.
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it.
    pub fn audit_record_with_auth(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) {
        let (durable, seq) = self.audit_record_with_backpressure(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth,
        );
        if let Err(e) = durable {
            error!(error = %e, seq, "audit WAL write failed — entry recorded in-memory only");
            crate::diag::audit_wal_append_failed(&e, seq);
        }
    }

    /// Record an audit event with strict durability and full database context.
    ///
    /// Returns an error if the durable audit WAL write fails.
    /// A buffer past its backpressure point makes this wait for a flush, with a time bound.
    /// The wait holds no lock, and this call never runs a flush itself.
    /// A full buffer refuses the record, so the caller refuses the audited action.
    pub fn audit_record_with_db_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        let auth = crate::control::security::audit::AuditAuth::default();
        self.audit_record_batch_strict([crate::control::security::audit::AuditRecordInput {
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth: &auth,
        }])
    }

    /// Record every input with one sync, and return only when all of them are durable.
    ///
    /// This is the strict path, with the same rules as [`Self::audit_record_with_db_strict`].
    pub fn audit_record_batch_strict<'a>(
        &self,
        inputs: impl IntoIterator<Item = crate::control::security::audit::AuditRecordInput<'a>>,
    ) -> crate::Result<()> {
        if crate::control::security::audit::lock_audit_log(&self.audit).is_full() {
            return Err(crate::Error::Storage {
                engine: "audit_log".into(),
                detail: "audit record refused: the audit buffer is full because the catalog \
                         refuses flushes; fix the catalog under the data directory and retry"
                    .into(),
            });
        }
        crate::control::security::audit::record_batch_concurrent(&self.audit, inputs)?;
        self.await_audit_backpressure();
        Ok(())
    }

    /// Record an audit event, then wait for a flush when the buffer is past its backpressure point.
    ///
    /// Returns the WAL result and the entry's seq.
    fn audit_record_with_backpressure(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) -> (crate::Result<()>, u64) {
        let (durable, seq, backpressure) = self.audit_record_buffered_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth,
        );
        if backpressure {
            self.await_audit_backpressure();
        }
        (durable, seq)
    }

    /// Record an audit event into the buffer and the audit WAL, with no backpressure wait.
    ///
    /// Returns the WAL result, the entry's seq, and whether the buffer is past its backpressure point.
    /// The flush path uses this directly, because a flush must never wait for a flush.
    pub(super) fn audit_record_buffered(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) -> (crate::Result<()>, u64, bool) {
        self.audit_record_buffered_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &crate::control::security::audit::AuditAuth::default(),
        )
    }

    /// [`Self::audit_record_buffered`] for an entry that carries the authenticated user and session.
    fn audit_record_buffered_with_auth(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) -> (crate::Result<()>, u64, bool) {
        // The sync wait runs with the audit lock released, so concurrent recorders share one fsync.
        // Each guard drop exports the published entries to SIEM after it unlocks.
        let outcome = crate::control::security::audit::record_batch_concurrent(
            &self.audit,
            [crate::control::security::audit::AuditRecordInput {
                event,
                tenant_id,
                database_id,
                source,
                detail,
                auth,
            }],
        );
        let backpressure =
            crate::control::security::audit::lock_audit_log(&self.audit).needs_backpressure();
        match outcome {
            Ok(seqs) => (Ok(()), seqs.first().copied().unwrap_or(0), backpressure),
            // A full buffer whose WAL refused the entry gives the entry no seq.
            Err(e) => {
                let seq = crate::control::security::audit::lock_audit_log(&self.audit).last_seq();
                (Err(e), seq, backpressure)
            }
        }
    }

    /// Record a batch of Event Plane audit records with one WAL sync, with no backpressure wait.
    ///
    /// Runs on the recorder's blocking task, never on the Event Plane.
    /// `commit` is marked staged under the audit lock, once the batch is in the log.
    /// The sync wait then runs with the lock released, and the batch is published after it.
    /// `commit` is marked durable only when the sync covered the batch.
    ///
    /// A full buffer refuses the batch and leaves `commit` unmarked.
    /// A panic before staging leaves it unmarked too, so the sender records the batch again.
    /// A failed append or sync leaves it staged, so the sender waits for a flush to commit the batch.
    pub fn record_pending_batch(
        &self,
        records: &[crate::control::security::audit::PendingAudit],
        commit: &crate::control::security::audit::recorder::BatchCommit,
    ) {
        let auth = crate::control::security::audit::AuditAuth::default();
        let staged = {
            let mut log = crate::control::security::audit::lock_audit_log(&self.audit);
            if log.is_full() {
                warn!(
                    count = records.len(),
                    "audit buffer is full, the Event Plane batch stays unrecorded"
                );
                log.flush_control().request_flush();
                return;
            }
            let staged = log.stage_batch(records.iter().map(|record| {
                crate::control::security::audit::AuditRecordInput {
                    event: record.event.clone(),
                    tenant_id: record.tenant_id,
                    database_id: record.database_id,
                    source: &record.source,
                    detail: &record.detail,
                    auth: &auth,
                }
            }));
            if let Some(last) = staged.last_seq() {
                commit.mark_staged(last);
            }
            staged
        };
        let last = staged.last_seq();
        let (_, durable) = staged.wait_durable();
        let seq = last.unwrap_or(0);
        if let Some(last) = last {
            crate::control::security::audit::lock_audit_log(&self.audit).publish_through(last);
        }
        match durable {
            Ok(()) => commit.mark_durable(),
            Err(e) => {
                error!(
                    error = %e,
                    seq,
                    count = records.len(),
                    "audit WAL write failed, the Event Plane holds its watermark until a flush \
                     commits the batch"
                );
                crate::diag::audit_wal_append_failed(&e, seq);
            }
        }
    }

    /// [`Self::record_pending_batch`] for a batch the recorder task did not record, with the
    /// backpressure wait after it.
    pub fn record_pending_audits(
        &self,
        records: &[crate::control::security::audit::PendingAudit],
        commit: &crate::control::security::audit::recorder::BatchCommit,
    ) {
        self.record_pending_batch(records, commit);
        self.await_audit_backpressure();
    }

    /// Commit every published entry through `through_seq` to the catalog.
    ///
    /// Returns whether the catalog holds the entry at `through_seq`.
    /// This is the durable path for a batch whose WAL append or sync failed.
    /// Publishing first covers a batch whose recorder panicked in its sync wait.
    pub fn settle_audit_through(&self, through_seq: u64) -> bool {
        crate::control::security::audit::lock_audit_log(&self.audit).publish_through(through_seq);
        self.flush_audit_log();
        crate::control::security::audit::lock_audit_log(&self.audit).is_persisted(through_seq)
    }

    /// Wait for a flush when the buffer is past its backpressure point, with a time bound.
    ///
    /// The wait holds no lock. With no flush executor running it returns at once.
    /// An async caller's worker keeps running other tasks during the wait.
    fn await_audit_backpressure(&self) {
        let control = {
            let log = crate::control::security::audit::lock_audit_log(&self.audit);
            if !log.needs_backpressure() {
                return;
            }
            log.flush_control()
        };
        if control.executor_attached()
            && !crate::control::security::audit::log_commit::hand_off_worker(|| {
                control.await_flush()
            })
        {
            warn!("no audit flush committed within the backpressure wait, entries stay recorded");
        }
    }

    /// Close the bearer refusal window: write one `AuthFailure` row per peer past its row limit.
    pub fn close_bearer_refusal_window(&self) {
        let summaries = self.bearer_refusal_throttle.close_window();
        if summaries.is_empty() {
            return;
        }
        let details: Vec<String> = summaries.iter().map(|summary| summary.detail()).collect();
        let auth = crate::control::security::audit::AuditAuth::default();
        crate::control::security::audit::record_batch_best_effort(
            &self.audit,
            summaries.iter().zip(&details).map(|(summary, detail)| {
                crate::control::security::audit::AuditRecordInput {
                    event: crate::control::security::audit::AuditEvent::AuthFailure,
                    tenant_id: None,
                    database_id: None,
                    source: &summary.source,
                    detail,
                    auth: &auth,
                }
            }),
        );
    }

    /// The Control Plane recorder for Event Plane audit records. The first call spawns it.
    ///
    /// The task holds weak references, so it never keeps this state alive.
    /// It records each batch with one WAL sync, and commits per batch.
    pub fn audit_recorder(self: &Arc<Self>) -> &crate::control::security::audit::AuditRecorder {
        use crate::control::security::audit::PendingAudit;
        use crate::control::security::audit::recorder::BatchCommit;

        self.audit_recorder.get_or_init(|| {
            let for_record = Arc::downgrade(self);
            let for_wait = Arc::downgrade(self);
            crate::control::security::audit::AuditRecorder::spawn(
                crate::control::security::audit::recorder::RECORDER_CHANNEL_CAPACITY,
                Arc::new(move |records: &[PendingAudit], commit: &BatchCommit| {
                    match for_record.upgrade() {
                        Some(state) => {
                            state.record_pending_batch(records, commit);
                            true
                        }
                        None => false,
                    }
                }),
                Arc::new(move || {
                    if let Some(state) = for_wait.upgrade() {
                        state.await_audit_backpressure();
                    }
                }),
            )
        })
    }

    /// Record an audit event (best-effort).
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it. Use
    /// [`audit_record_strict`] when the caller must abort on audit failure
    /// (e.g. data-modifying DDL where the accounting standard requires atomic
    /// audit + data durability).
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        self.audit_record_with_db(event, tenant_id, None, source, detail);
    }

    /// Record an audit event with strict durability.
    ///
    /// Returns an error if the durable audit WAL write fails.
    /// A caller that guards a mutation records first, and refuses the action on the error.
    /// So a failed audit write fails the data write, and a durable row precedes the commit.
    ///
    /// What each path guarantees:
    /// - Catalog DDL from a statement, credentials and grants included: a durable `ActionIntent`
    ///   row before the proposal, at the three chokepoints in `metadata_proposer` and `ddl_flush`.
    ///   Its `DdlChange` row follows the apply on every replica, and that row is best-effort.
    /// - Store and engine actions outside the catalog: a durable `ActionIntent` row before the
    ///   mutation, through `neutral::audit_intent`. Their result row is best-effort.
    /// - Statements whose first effect precedes their catalog entry: the same row, before that
    ///   effect. These are a row copy, a move, a backup, an engine index change, and a purge.
    /// - The JWT key exchange, the session tenant override, and a scheduled backup: a durable
    ///   `ActionIntent` row from this call, before the mutation.
    /// - A restore of a hard-deleted collection: one `ActionIntent` row per collection, before
    ///   the restore's first write.
    /// - A DEFINE EVENT action: a durable `ActionIntent` row through the Control Plane recorder,
    ///   because the Event Plane does no storage I/O. A failed record queues the action's retry.
    /// - Internal catalog proposals with no statement: a `DdlChange` row after the apply only.
    /// - Row DML: its row follows the commit. The Event Plane watermark holds until the row is
    ///   durable, and WAL catch-up delivers it again after a crash.
    /// - Actions that remove access: best-effort, because an audit outage must not block a
    ///   security response. These are a blacklist entry, a kill, a lockdown, a deactivated user,
    ///   a non-Active status, a stopped impersonation, and a revoked delegation.
    /// - A TLS reload: best-effort too. An audit outage must not let a certificate expire.
    pub fn audit_record_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        self.audit_record_with_db_strict(event, tenant_id, None, source, detail)
    }

    /// Update per-tenant memory estimates.
    pub fn update_tenant_memory_estimates(&self) {
        let total_allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64;

        let mut tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        let tenant_requests: Vec<(crate::types::TenantId, u64)> = {
            let users = self.credentials.list_user_details();
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            for user in &users {
                if seen.insert(user.tenant_id) {
                    let total = tenants
                        .usage(user.tenant_id)
                        .map_or(0, |u| u.total_requests);
                    result.push((user.tenant_id, total));
                }
            }
            result
        };

        let total_reqs: u64 = tenant_requests.iter().map(|(_, r)| *r).sum();
        if total_reqs == 0 {
            return;
        }

        for (tid, reqs) in &tenant_requests {
            let proportion = *reqs as f64 / total_reqs as f64;
            let estimated_bytes = (total_allocated as f64 * proportion) as u64;
            tenants.update_memory(*tid, estimated_bytes);
        }
    }
}
