// SPDX-License-Identifier: BUSL-1.1

//! Shape snapshot production: plan construction, RLS, and predicate filtering.

use tracing::{info, warn};

use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{ShapeSnapshotRefusal, ShapeSnapshotRejectMsg};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::sync::shape::handler::ShapeSnapshotData;
use crate::control::server::sync::wire::{SyncFrame, SyncMessageType};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Why Origin produced no snapshot for an authorized shape. The subscriber
/// receives it as a `ShapeSnapshotReject` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnapshotFailure {
    pub refusal: ShapeSnapshotRefusal,
    /// Detail for logs. Names no stored value.
    pub detail: String,
}

impl SnapshotFailure {
    pub(super) fn new(refusal: ShapeSnapshotRefusal, detail: impl Into<String>) -> Self {
        Self {
            refusal,
            detail: detail.into(),
        }
    }

    /// The `ShapeSnapshotReject` frame that tells the subscriber of `shape_id`
    /// its snapshot failed.
    pub(super) fn reject_frame(self, shape_id: &str) -> Option<SyncFrame> {
        let reject = ShapeSnapshotRejectMsg {
            shape_id: shape_id.to_string(),
            refusal: self.refusal,
            detail: self.detail,
        };
        SyncFrame::try_encode(SyncMessageType::ShapeSnapshotReject, &reject)
    }
}

/// Everything a snapshot needs, resolved from the authorized session.
///
/// Tenant and database come from the handshake identity, never from the shape
/// body — a client cannot point a subscription at another tenant's or another
/// database's copy of a collection name.
pub(super) struct SnapshotRequest<'a> {
    pub shared: &'a SharedState,
    pub session_id: &'a str,
    pub shape: &'a ShapeDefinition,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The sync session's real remote address (`session.device_metadata.remote_addr`),
    /// threaded down to the one `RequestAdmission::NotYetAdmitted`
    /// `dispatch_for_identity` call this snapshot path makes, for the
    /// IP-blacklist half of `check_request_admission`.
    pub peer_addr: &'a str,
}

/// Produce the initial snapshot payload for a shape definition.
///
/// Dispatches into the Data Plane for Document shapes; returns lightweight or
/// empty payloads for Vector / Graph / Array (see inline comments).
///
/// Returns a [`SnapshotFailure`] when the snapshot cannot be produced: a
/// policy refusal, a failed query or rows the read does not produce. The
/// caller answers it with a `ShapeSnapshotReject` frame and sends no
/// `ShapeSnapshot`: an empty snapshot is an assertion that the shape matches
/// nothing, and a client that believes it has a complete empty baseline will
/// never ask again. An intentionally empty snapshot (a Graph shape, an
/// unmatched Array) is still a snapshot, because that answer is real.
pub(super) async fn take_shape_snapshot(
    req: SnapshotRequest<'_>,
) -> Result<ShapeSnapshotData, SnapshotFailure> {
    let SnapshotRequest {
        shared,
        session_id,
        shape,
        identity,
        tenant_id,
        database_id,
        peer_addr,
    } = req;

    let _request = shared.tenant_request_guard(tenant_id);
    match &shape.shape_type {
        ShapeType::Document {
            collection,
            predicate,
        } => {
            document_snapshot(DocumentSnapshot {
                shared,
                shape_id: &shape.shape_id,
                collection,
                predicates: &[predicate.as_slice()],
                identity,
                database_id,
                peer_addr,
            })
            .await
        }
        ShapeType::Vector { collection, .. } => Ok(ShapeSnapshotData {
            data: collection.as_bytes().to_vec(),
            doc_count: 0,
        }),
        ShapeType::Graph { .. } => Ok(ShapeSnapshotData::empty()),
        ShapeType::Array {
            array_name,
            coord_range,
        } => {
            let array_known = shared.array_sync_schemas.schema_hlc(array_name).is_some();
            if !array_known {
                warn!(
                    session = session_id,
                    array = %array_name,
                    "array shape subscribe: array not known to Origin schema registry"
                );
                return Ok(ShapeSnapshotData::empty());
            }
            shared
                .array_subscriber_cursors
                .register(session_id, array_name, coord_range.clone());
            info!(
                session = session_id,
                array = %array_name,
                "array shape subscribed; cursor initialized at HLC::ZERO"
            );
            Ok(ShapeSnapshotData::empty())
        }
        _ => {
            warn!(
                session = session_id,
                "shape subscribe: unknown shape_type variant, sending empty snapshot"
            );
            Ok(ShapeSnapshotData::empty())
        }
    }
}

pub(super) struct DocumentSnapshot<'a> {
    pub shared: &'a SharedState,
    /// The shape the snapshot answers, or the collection a collection resync
    /// covers. Names the snapshot in logs.
    pub shape_id: &'a str,
    pub collection: &'a str,
    /// One predicate per shape the snapshot covers. A row passes when any
    /// matches it.
    pub predicates: &'a [&'a [u8]],
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    pub peer_addr: &'a str,
}

/// Scan a document or key-value collection for the subscription's initial
/// dataset.
///
/// A KV collection's rows live in the KV engine, so it is read with a KV
/// scan, and each row becomes a `{id, data}` envelope keyed by its key (see
/// [`super::kv_rows`]). Every other collection is read with a document
/// range scan. Either way the snapshot holds every row the collection
/// stores, so a client that replaces its rows with it loses none.
///
/// The scan carries row-level security: it is a read on the subscriber's
/// behalf, so the subscriber's policies apply to it exactly as they do to
/// the same rows fetched over SQL. A `RangeScan` has no filter slot, so a
/// collection carrying a read policy refuses here rather than streaming
/// unfiltered rows into a client's local replica — where the policy will have
/// no further chance to apply.
///
/// Column redaction applies for the same reason, and is applied to the
/// delivered payload here rather than left to the SELECT-path shaping core
/// this dispatch never reaches — see [`super::payload`].
pub(super) async fn document_snapshot(
    req: DocumentSnapshot<'_>,
) -> Result<ShapeSnapshotData, SnapshotFailure> {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};

    use super::payload::{
        SnapshotPayload, finalize_snapshot, predicate_redacted_field, snapshot_redaction,
    };

    let is_kv = match req.shared.credentials.catalog().get_collection(
        req.database_id,
        req.identity.tenant_id.as_u64(),
        req.collection,
    ) {
        Ok(stored) => stored.is_some_and(|stored| stored.collection_type.is_kv()),
        Err(error) => {
            return Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::QueryFailed,
                format!("the collection's catalog entry does not read: {error}"),
            ));
        }
    };

    let collection = nodedb_types::QualifiedCollection::new(req.database_id, req.collection);
    // The replica's initial dataset is every row: a cut leaves the
    // client missing rows with nothing saying so. The Data Plane bounds the
    // read by its scan byte budget and fails loudly past it.
    let plan = if is_kv {
        PhysicalPlan::Kv(KvOp::Scan {
            collection,
            cursor: Vec::new(),
            count: usize::MAX,
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            match_pattern: None,
            sort_keys: Vec::new(),
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    } else {
        PhysicalPlan::Document(DocumentOp::RangeScan {
            collection,
            field: String::new(),
            lower: None,
            upper: None,
            limit: usize::MAX,
            rls_filters: Vec::new(),
        })
    };

    // Resolved once for the whole snapshot, before `plan` is moved into the
    // dispatch, and from the subscriber's own identity — every row of this
    // payload is delivered under it.
    let redaction = match snapshot_redaction(req.shared, req.identity, req.database_id, &plan) {
        Ok(redaction) => redaction,
        Err(error) => {
            tracing::error!(
                shape_id = %req.shape_id,
                collection = %req.collection,
                error = %error,
                "shape snapshot refused: the subscriber's request scope could not be resolved"
            );
            return Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::QueryFailed,
                "the subscriber's request scope could not be resolved",
            ));
        }
    };

    // A predicate that probes a redacted column discloses it through row
    // presence no matter how the delivered cell is masked, so the subscription
    // is refused rather than answered.
    if let Some(field) = req.predicates.iter().find_map(|predicate| {
        predicate_redacted_field(predicate, &redaction, &req.shared.redaction)
    }) {
        warn!(
            shape_id = %req.shape_id,
            collection = %req.collection,
            field = %field,
            "shape snapshot refused: the shape predicate filters on a redacted column"
        );
        return Err(SnapshotFailure::new(
            ShapeSnapshotRefusal::PolicyRefused,
            "the shape predicate filters on a redacted column",
        ));
    }

    // The subscriber's own capability, not the system door: the scan is
    // authorized into a task, row-level security is applied to it, and that
    // exact plan is what reaches storage.
    //
    // Unlike the DDL/DSL passthrough handlers, this call is NOT reached
    // through `shared::ddl::dispatch` — `handle_shape_subscribe_async`
    // deliberately runs only blacklist + account status + quota before this
    // point (shape subscription is not the per-query traffic the
    // rate-limiter's cost table models), so this is the one place this
    // request is ever admitted. `NotYetAdmitted` keeps it that way.
    match dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state: req.shared,
            identity: req.identity,
            database_id: req.database_id,
            collection: req.collection,
            plan,
            deadline: crate::control::server::shared::session::StatementDeadline::of_statement(
                req.shared,
            ),
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::NotYetAdmitted {
                peer_addr: req.peer_addr,
            },
            // A sync client's snapshot has no read-consistency setting, so it
            // takes the strong default.
            linearizable: true,
        },
    )
    .await
    {
        Ok(payload) => finalize_snapshot(SnapshotPayload {
            payload: if is_kv {
                super::kv_rows::kv_rows_as_envelopes(&payload)?
            } else {
                payload
            },
            predicates: req.predicates,
            shape_id: req.shape_id,
            redaction: &redaction,
            store: &req.shared.redaction,
        }),
        Err(error) => {
            warn!(
                shape_id = %req.shape_id,
                %error,
                "shape snapshot query failed; sending a snapshot reject"
            );
            Err(SnapshotFailure::new(
                ShapeSnapshotRefusal::QueryFailed,
                format!("the snapshot query failed: {error}"),
            ))
        }
    }
}
