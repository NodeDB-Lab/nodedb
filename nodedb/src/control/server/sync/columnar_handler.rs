// SPDX-License-Identifier: BUSL-1.1

//! Columnar insert handler for sync sessions.
//!
//! Decodes a `ColumnarInsertMsg` from a Lite client, deserializes the
//! row payloads (MessagePack `Vec<Value>` per row), converts positional
//! rows to named `Value::Object` rows using the schema carried in
//! `schema_bytes`, and dispatches to the Data Plane via a [`ColumnarDispatcher`].
//!
//! The handler follows the same structural pattern as `timeseries_handler`:
//! a dispatcher trait keeps the ingest and the ACK generation coupled so
//! an ACK cannot be returned without at least attempting dispatch.

use async_trait::async_trait;
use tracing::{debug, error};

use nodedb_types::value::Value;

use super::session::SyncSession;
use super::wire::*;
use crate::control::server::dispatch_utils::RecordOwner;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── PK extraction helper ─────────────────────────────────────────────────────

/// Extract the primary-key bytes for a decoded columnar row, mirroring the
/// non-sync planner's key precedence (`id`, `document_id`, `key`). Returns an
/// empty `Vec` for headless rows (no PK column found, or PK is null/empty),
/// which the caller binds to a fresh anonymous surrogate.
///
/// String rendering matches `sql_value_to_string` in the non-sync path so
/// that a numeric id such as `Value::Integer(5)` produces `b"5"`, not
/// `b"Int(5)"`, giving identical surrogate assignments across both paths.
fn columnar_row_pk_bytes(row: &Value) -> Vec<u8> {
    let map = match row {
        Value::Object(m) => m,
        _ => return Vec::new(),
    };
    let pk_str = ["id", "document_id", "key"]
        .iter()
        .find_map(|k| map.get(*k))
        .map(|v| match v {
            Value::String(s) => s.clone(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => String::new(),
        })
        .unwrap_or_default();
    pk_str.into_bytes()
}

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates async Data Plane dispatch for a decoded columnar insert.
///
/// Returns the raw `Response.payload` bytes from the Data Plane so that the
/// handler can decode the [`SyncAckResult`] for gate status propagation.
#[async_trait]
pub trait ColumnarDispatcher: Send + Sync {
    /// Dispatch a batch of rows to the Data Plane for columnar ingest.
    ///
    /// `rows` contains one element per accepted row, each a `Vec<Value>`
    /// in schema column order. `schema_bytes` is the MessagePack-encoded
    /// `ColumnarSchema` hint from the wire message (can be empty).
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        rows: Vec<Vec<Value>>,
        schema_bytes: Vec<u8>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes the insert to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` so that AFTER triggers are not
/// re-fired on synced data.
pub struct SharedStateColumnarDispatcher<'a> {
    shared: &'a crate::control::state::SharedState,
    identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    database_id: DatabaseId,
}

impl<'a> SharedStateColumnarDispatcher<'a> {
    /// Construct an externally admitted sync dispatcher bound to the
    /// handshake-authenticated identity and database scope.
    pub fn new(
        shared: &'a crate::control::state::SharedState,
        identity: &'a crate::control::security::identity::AuthenticatedIdentity,
        database_id: DatabaseId,
    ) -> Self {
        Self::from_session(shared, Some(identity), database_id)
    }

    pub(crate) fn from_session(
        shared: &'a crate::control::state::SharedState,
        identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            shared,
            identity,
            database_id,
        }
    }
}

#[async_trait]
impl<'a> ColumnarDispatcher for SharedStateColumnarDispatcher<'a> {
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        rows: Vec<Vec<Value>>,
        schema_bytes: Vec<u8>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::columnar::{ColumnarInsertIntent, ColumnarOp};
        use nodedb_types::columnar::ColumnarSchema;
        use std::collections::HashMap;

        let prov = provenance;
        let database_id = self.database_id;
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &collection,
        )?;

        // The Data Plane columnar insert handler takes rows as
        // `Value::Object(HashMap<String, Value>)`, not positional arrays. A row
        // names its cells by the schema it carries. A row with no schema lists
        // them in the collection's declared column order, as an INSERT with no
        // column list does.
        let column_names: Vec<String> = if schema_bytes.is_empty() {
            self.shared
                .credentials
                .catalog()
                .get_collection(database_id, tenant_id.as_u64(), &collection)?
                .ok_or_else(|| crate::Error::CollectionNotFound {
                    tenant_id,
                    collection: collection.clone(),
                })?
                .fields
                .into_iter()
                .map(|(name, _)| name)
                .collect()
        } else {
            zerompk::from_msgpack::<ColumnarSchema>(&schema_bytes)
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("columnar sync into '{collection}': the row schema: {e}"),
                })?
                .columns
                .into_iter()
                .map(|c| c.name)
                .collect()
        };

        // Name each positional cell. A row with more cells than the schema
        // has columns is refused whole: a cell with no column has no place.
        let mut object_rows: Vec<Value> = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() > column_names.len() {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "columnar sync into '{collection}': a row carries {} cells, but the \
                         schema names {} columns",
                        row.len(),
                        column_names.len()
                    ),
                });
            }
            let map: HashMap<String, Value> = column_names.iter().cloned().zip(row).collect();
            object_rows.push(Value::Object(map));
        }

        // Assign cross-engine surrogates in row order before WAL append so
        // every replica stores and applies the same surrogate set. The
        // coordinator assigns once, in one batch at the collection's home.
        // Followers replay from the WAL record. A row with no key gets a fresh
        // anonymous surrogate.
        let pks: Vec<Vec<u8>> = object_rows.iter().map(columnar_row_pk_bytes).collect();
        let keyed: Vec<&[u8]> = pks
            .iter()
            .filter(|pk| !pk.is_empty())
            .map(Vec::as_slice)
            .collect();
        let mut bound = crate::control::server::surrogate_exchange::assign_surrogates_routed(
            self.shared,
            nodedb_types::CollectionKey::from_bare(database_id, &collection),
            tenant_id,
            &keyed,
            crate::types::TraceId::ZERO,
        )
        .await?
        .into_iter();
        // Every keyless row draws in one catalog transaction.
        let keyless_count = pks.len() - keyed.len();
        let mut keyless = if keyless_count > 0 {
            self.shared
                .surrogate_assigner
                .assign_anonymous_many(
                    nodedb_types::CollectionKey::from_bare(database_id, &collection),
                    tenant_id,
                    keyless_count,
                )
                .await?
        } else {
            Vec::new()
        }
        .into_iter();
        let mut surrogates: Vec<nodedb_types::Surrogate> = Vec::with_capacity(pks.len());
        for pk in &pks {
            let next = if pk.is_empty() {
                keyless.next()
            } else {
                bound.next()
            };
            surrogates.push(next.ok_or_else(|| crate::Error::Internal {
                detail: format!("columnar sync into '{collection}': a row got no surrogate"),
            })?);
        }

        // Encode as msgpack — the Data Plane handler calls `value_from_msgpack(payload)`
        // and expects Value::Array([Value::Object, ...]).
        let array_value = Value::Array(object_rows);
        let payload =
            nodedb_types::value_to_msgpack(&array_value).map_err(|e| crate::Error::Internal {
                detail: format!("columnar sync: msgpack serialize rows: {e}"),
            })?;

        // The replicated entry carries the surrogates, so followers never mint
        // their own divergent ids.
        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            payload,
            format: "msgpack".to_string(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates,
            schema_bytes,
            provenance: Some(prov),
            // Each replica's apply allocates its own LSN.
            wal_lsn: None,
            system_from: None,
            restored_versions: Vec::new(),
            // Edge-to-origin sync replays rows already decided by the policy
            // where they were written; the writing device's session is not
            // present here to resolve `$auth.*` against.
            rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            // Sync answers with an ack, never a row set, for the same reason:
            // there is no requesting identity here to project or gate rows for.
            returning: None,
            rls_filters: Vec::new(),
        });

        super::raft_dispatch::authorize_and_dispatch(self.shared, self.identity, owner, plan).await
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable.
///
/// Returns a loud `Internal` error — intentionally NOT a silent no-op.
pub struct NoOpColumnarDispatcher;

#[async_trait]
impl ColumnarDispatcher for NoOpColumnarDispatcher {
    async fn dispatch_insert(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _rows: Vec<Vec<Value>>,
        _schema_bytes: Vec<u8>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("columnar insert"))
    }
}

// ── Handler ──────────────────────────────────────────────────────────────────

impl SyncSession {
    /// Process a columnar batch insert: decode rows, dispatch to the Data
    /// Plane, and return an ACK frame.
    ///
    /// Typed outcomes preserve the durable frontier. Unknown outcomes close the session.
    /// Unauthenticated sessions receive a fence without dispatch.
    pub async fn handle_columnar_insert<D: ColumnarDispatcher>(
        &mut self,
        msg: &ColumnarInsertMsg,
        dispatcher: &D,
    ) -> Option<SyncFrame> {
        self.last_activity = std::time::Instant::now();

        if let Some(reason) = self.entry_refusal(&msg.collection) {
            let ack = ColumnarInsertAckMsg {
                collection: msg.collection.clone(),
                batch_id: msg.batch_id,
                accepted: 0,
                rejected: msg.rows.len() as u64,
                reject_reason: Some(reason),
                applied_seq: 0,
                status: AckStatus::Fenced,
            };
            return SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack);
        }

        // Decode every row before dispatch. Undecodable content has no durable verdict.
        let total = msg.rows.len() as u64;
        let mut decoded_rows: Vec<Vec<Value>> = Vec::with_capacity(msg.rows.len());
        for (i, row_bytes) in msg.rows.iter().enumerate() {
            match zerompk::from_msgpack::<Vec<Value>>(row_bytes) {
                Ok(row) => decoded_rows.push(row),
                Err(e) => {
                    error!(
                        session = %self.session_id,
                        collection = %msg.collection,
                        batch_id = msg.batch_id,
                        row_index = i,
                        error = %e,
                        "columnar sync: row decode error; closing without a verdict"
                    );
                    // No durable verdict exists for undecodable content. Retain the producer entry.
                    return None;
                }
            }
        }

        let decoded = decoded_rows.len() as u64;

        let tenant_id = self.tenant_id.unwrap_or(TenantId::new(0));
        let vshard =
            nodedb_types::CollectionKey::from_bare(self.database_id(), &msg.collection).vshard();

        debug!(
            session = %self.session_id,
            collection = %msg.collection,
            batch_id = msg.batch_id,
            rows = decoded,
            lite_id = %msg.lite_id,
            "columnar insert: dispatching to Data Plane"
        );

        match dispatcher
            .dispatch_insert(
                tenant_id,
                vshard,
                msg.collection.clone(),
                decoded_rows,
                msg.schema_bytes.clone(),
                nodedb_types::sync::wire::SyncProvenance {
                    producer_id: self.producer_id,
                    epoch: self.accepted_epoch,
                    stream_id: nodedb_types::sync::wire::stream_id_for(
                        nodedb_types::sync::wire::EngineKind::Columnar,
                        &msg.collection,
                    ),
                    seq: msg.seq,
                },
            )
            .await
        {
            Ok(payload_bytes) => {
                let wire = super::ack_decode::decode_core_ack(&payload_bytes)?;

                // A terminally refused batch landed no rows, so none of it can
                // be counted as processed or reported as accepted.
                let accepted = if wire.accepted
                    && matches!(wire.status, AckStatus::Applied | AckStatus::Duplicate)
                {
                    decoded
                } else {
                    0
                };
                self.mutations_processed += accepted;
                let ack = ColumnarInsertAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted,
                    rejected: total.saturating_sub(accepted),
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack)
            }
            Err(e) => {
                error!(
                    session = %self.session_id,
                    collection = %msg.collection,
                    batch_id = msg.batch_id,
                    error = %e,
                    "columnar insert dispatch returned an error"
                );
                let wire = super::ack_decode::decode_core_error(&e)?;
                let ack = ColumnarInsertAckMsg {
                    collection: msg.collection.clone(),
                    batch_id: msg.batch_id,
                    accepted: 0,
                    rejected: total,
                    reject_reason: wire.reject_reason,
                    applied_seq: wire.applied_seq,
                    status: wire.status,
                };
                SyncFrame::try_encode(SyncMessageType::ColumnarInsertAck, &ack)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "columnar_handler_tests.rs"]
mod tests;
