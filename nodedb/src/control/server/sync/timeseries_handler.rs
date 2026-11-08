// SPDX-License-Identifier: BUSL-1.1

//! Timeseries push handler for sync sessions.
//!
//! Decodes Gorilla-encoded metric blocks from Lite, builds ILP-format
//! payloads with `__source` tag, and dispatches to the Data Plane via
//! a [`TimeseriesDispatcher`] implementation supplied by the caller.
//!
//! The leaky `(ack, ingest_data)` tuple is intentionally absent — the
//! handler owns both decode and dispatch so that an ACK can never be
//! returned without the corresponding ingest being attempted.

use async_trait::async_trait;
use tracing::{debug, error};

use super::session::SyncSession;
use super::wire::*;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates the async Data Plane dispatch for a decoded timeseries push.
///
/// Callers supply a concrete implementation so that the handler can complete
/// ingest atomically with ACK generation. This makes it structurally
/// impossible to ACK a push without attempting dispatch.
///
/// Returns the raw `Response.payload` bytes from the Data Plane so that the
/// handler can decode the [`SyncAckResult`] for gate status propagation, and
/// the number of samples the resolve rejected.
#[async_trait]
pub trait TimeseriesDispatcher: Send + Sync {
    /// Immutable database scope selected for this connection.
    ///
    /// The default retains compatibility for external dispatcher
    /// implementations; production overrides it with the session-bound value.
    fn database_id(&self) -> DatabaseId {
        DatabaseId::DEFAULT
    }

    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<TimeseriesIngestResult>;
}

/// What one dispatched timeseries push produced.
#[derive(Debug, Default)]
pub struct TimeseriesIngestResult {
    /// The Data Plane response payload: a [`SyncAckResult`].
    pub payload: Vec<u8>,
    /// The samples the resolve or the install rejected. They never land, so
    /// the ACK does not count them as accepted.
    pub rejected: u64,
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes the ingest to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` so that AFTER triggers are not
/// re-fired on synced data.
pub struct SharedStateTimeseriesDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
}

#[async_trait]
impl<'a> TimeseriesDispatcher for SharedStateTimeseriesDispatcher<'a> {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    async fn dispatch_ingest(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        ilp_payload: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<TimeseriesIngestResult> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::TimeseriesOp;

        let database_id = self.database_id;

        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &collection,
        )?;
        let unresolved = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            payload: ilp_payload.into_bytes(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: Some(provenance),
            // Edge-to-origin sync replays rows already decided by the policy
            // where they were written; the writing device's session is not
            // present here to resolve `$auth.*` against.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });
        // The entry carries the rows the ingest resolves to, and every replica
        // stores exactly those rows: the push resolves before its entry
        // exists.
        let resolved = crate::control::write_resolve::resolve_for_log(
            self.shared,
            crate::control::write_resolve::WriteResolveContext {
                tenant_id,
                database_id,
            },
            vshard,
            &unresolved,
        )
        .await?;
        let plan = resolved.unwrap_or(unresolved);
        let rejected = crate::control::write_resolve::rejected_lines(&plan)?;

        let authorized = super::raft_dispatch::authorize_sync_task(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            vshard,
            plan,
        )?;
        // The replicated apply journals the write on every replica.
        let payload = super::raft_dispatch::dispatch_write_replicated(
            self.shared,
            &collection,
            authorized,
            crate::control::server::shared::session::StatementDeadline::of_statement(self.shared),
            crate::event::EventSource::CrdtSync,
        )
        .await?;
        // The install's count covers the resolve's and adds the rows that
        // conflict with the schema at the record's log position.
        let applied =
            crate::control::server::shared::sql::staging_predicates::extract_rejected_count(
                &payload,
            );
        let rejected = rejected.max(applied);
        Ok(TimeseriesIngestResult { payload, rejected })
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable at a call site.
///
/// Returns a loud `Internal` error — this is intentionally NOT a silent
/// no-op. If this path is reached it means the listener wiring is wrong
/// and the push will otherwise be silently dropped after being ACKed.
pub struct NoOpTimeseriesDispatcher;

#[async_trait]
impl TimeseriesDispatcher for NoOpTimeseriesDispatcher {
    async fn dispatch_ingest(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _ilp_payload: String,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<TimeseriesIngestResult> {
        Err(super::raft_dispatch::noop_dispatch_error("timeseries push"))
    }
}

// ── Handler ──────────────────────────────────────────────────────────────────

impl SyncSession {
    /// Process a timeseries push: decode Gorilla blocks, dispatch to the Data
    /// Plane, and return an ACK frame.
    ///
    /// Typed outcomes preserve the durable frontier. Unknown outcomes close the session.
    /// Authentication errors return a fence without dispatch.
    pub async fn handle_timeseries_push<D: TimeseriesDispatcher>(
        &mut self,
        msg: &TimeseriesPushMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if !self.authenticated {
            return fenced_timeseries_ack(msg, "session is not authenticated");
        }
        let Some(identity) = self.identity.as_ref() else {
            // `authenticated` is never a substitute for a handshake-bound
            // identity: it can otherwise write under a fabricated tenant.
            return fenced_timeseries_ack(msg, "session has no handshake-bound identity");
        };
        let tenant_id = identity.tenant_id;
        let database_id = dispatcher.database_id();
        if !identity.can_access_database(database_id) {
            return fenced_timeseries_ack(msg, "identity may not access the target database");
        }
        if let Some(reason) = self.entry_refusal(&msg.collection) {
            return fenced_timeseries_ack(msg, reason);
        }

        // Decode Gorilla blocks to verify integrity.
        let timestamps = nodedb_codec::GorillaDecoder::new(&msg.ts_block).decode_all();
        let values = nodedb_codec::GorillaDecoder::new(&msg.val_block).decode_all();

        let decoded_count = timestamps.len();
        if decoded_count == 0
            || decoded_count != values.len()
            || decoded_count as u64 != msg.sample_count
        {
            error!(collection = %msg.collection, batch_id = msg.batch_id,
                "timeseries sample blocks contradict their declared count; closing without a verdict");
            return None;
        }

        // Build ILP-format payload for Data Plane ingest.
        let mut ilp_lines = String::with_capacity(decoded_count * 80);
        for i in 0..decoded_count {
            let (ts, _) = timestamps[i];
            let (_, val) = values[i];
            // ILP format: measurement,__source=lite_id value=X timestamp_ns
            ilp_lines.push_str(&msg.collection);
            ilp_lines.push_str(",__source=");
            ilp_lines.push_str(&msg.lite_id);
            ilp_lines.push_str(" value=");
            ilp_lines.push_str(&val.to_string());
            ilp_lines.push(' ');
            // Convert ms to ns for ILP.
            let timestamp_ns = ts.checked_mul(1_000_000)?;
            ilp_lines.push_str(&timestamp_ns.to_string());
            ilp_lines.push('\n');
        }

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            decoded = decoded_count,
            lite_id = %msg.lite_id,
            "timeseries push decoded, dispatching to Data Plane"
        );

        let vshard = nodedb_types::CollectionKey::from_bare(database_id, &msg.collection).vshard();

        match dispatcher
            .dispatch_ingest(
                tenant_id,
                vshard,
                msg.collection.clone(),
                ilp_lines,
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Timeseries,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(result) => {
                let wire = super::ack_decode::decode_core_ack(&result.payload)?;

                // A terminally refused batch ingested no samples, so none of it
                // can be reported as accepted. A sample the resolve rejected
                // never landed either.
                let accepted = if wire.accepted
                    && matches!(wire.status, AckStatus::Applied | AckStatus::Duplicate)
                {
                    (decoded_count as u64).saturating_sub(result.rejected)
                } else {
                    0
                };
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted,
                    rejected: msg.sample_count.saturating_sub(accepted),
                    // WAL LSN is not surfaced by the dispatch helper (returns
                    // payload bytes only); `applied_seq` is the real producer
                    // frontier. Don't conflate the two — leave `lsn` 0 rather
                    // than report a sequence number as a WAL LSN.
                    lsn: 0,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
            Err(e) => {
                error!(session = %self.session_id, collection = %msg.collection,
                    batch_id = msg.batch_id, error = %e, "timeseries dispatch returned an error");
                let wire = super::ack_decode::decode_core_error(&e)?;
                let ack = TimeseriesAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: 0,
                    rejected: msg.sample_count,
                    lsn: 0,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
            }
        }
    }
}

/// Fence session admission without consuming a producer sequence.
fn fenced_timeseries_ack(msg: &TimeseriesPushMsg, reason: impl Into<String>) -> Option<SyncFrame> {
    debug!(reason = %reason.into(), "timeseries session admission fenced");
    let ack = TimeseriesAckMsg {
        collection: msg.collection.clone(),
        batch_id: msg.batch_id,
        accepted: 0,
        rejected: msg.sample_count,
        lsn: 0,
        applied_seq: 0,
        status: AckStatus::Fenced,
    };
    SyncFrame::try_encode(SyncMessageType::TimeseriesAck, &ack)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "timeseries_handler_tests.rs"]
mod tests;
