// SPDX-License-Identifier: BUSL-1.1

//! Array-sync frame handling for a Lite WebSocket session.
//!
//! Builds the per-session inbound array engine and routes inbound array
//! frames (deltas, snapshots, schema, acks, catchup) to it. The inbound
//! engine applies them and may return a reject frame to send back to the
//! client.

use std::sync::Arc;

use tracing::warn;

use nodedb_types::sync::wire::array::{
    ArrayAckMsg, ArrayAdvisoryKind, ArrayAdvisoryRefusedMsg, ArrayCatchupRequestMsg,
    ArrayDeltaBatchMsg, ArrayDeltaMsg, ArrayRejectMsg, ArrayRejectReason, ArraySchemaSyncMsg,
    ArraySnapshotChunkMsg, ArraySnapshotMsg,
};

use super::super::wire::{SyncFrame, SyncMessageType};
use crate::control::array_sync::InboundOutcome;
use crate::control::state::SharedState;

/// Build the per-session inbound array engine bound to `tenant_id`, or `None`
/// when `SharedState` is absent (the no-op listener path used in tests).
///
/// `tenant_id` MUST be the session's handshake-authenticated tenant: the
/// inbound engine stamps it onto every replicated array write (Raft-log
/// routing) and the fan-out uses it to match subscriber shapes. The caller
/// therefore builds this lazily, only after authentication — building it under
/// a placeholder tenant would misroute every inbound array delta.
pub(super) fn build_array_inbound(
    shared: &Option<Arc<SharedState>>,
    identity: crate::control::security::identity::AuthenticatedIdentity,
) -> Option<Arc<crate::control::array_sync::OriginArrayInbound>> {
    let tenant_id = identity.tenant_id;
    let database_id = identity
        .default_database
        .unwrap_or(crate::types::DatabaseId::DEFAULT);
    shared.as_ref().map(|s| {
        let engine = Arc::new(crate::control::array_sync::OriginApplyEngine::new(
            Arc::clone(&s.array_sync_schemas),
            Arc::clone(&s.array_sync_op_log),
        ));
        let fanout = Arc::new(crate::control::array_sync::ArrayFanout::new(
            Arc::clone(&s.shape_registry),
            Arc::clone(&s.array_delivery),
            Arc::clone(&s.array_subscriber_cursors),
            Arc::clone(&s.array_snapshot_hlcs),
            Arc::clone(&s.array_merger_registry),
            0,
            crate::control::server::sync::shape::ShapeScope {
                tenant_id: tenant_id.as_u64(),
                database_id,
            },
        ));
        let inbound = crate::control::array_sync::OriginArrayInbound::new(
            engine,
            Arc::clone(&s.array_sync_schemas),
            Arc::clone(s),
            identity,
        )
        .with_observer(fanout);
        Arc::new(inbound)
    })
}

/// True for the array message types this session routes to the inbound engine.
pub(super) fn is_array_frame(msg_type: SyncMessageType) -> bool {
    matches!(
        msg_type,
        SyncMessageType::ArrayDelta
            | SyncMessageType::ArrayDeltaBatch
            | SyncMessageType::ArraySnapshot
            | SyncMessageType::ArraySnapshotChunk
            | SyncMessageType::ArraySchema
            | SyncMessageType::ArrayAck
            | SyncMessageType::ArrayReject
            | SyncMessageType::ArrayCatchupRequest
            | SyncMessageType::ArrayOpResults
            | SyncMessageType::ArrayAdvisoryRefused
    )
}

/// Return a bounded rejection when an array frame lacks the current
/// authenticated handshake binding.
///
/// The caller invokes this before constructing or reusing an inbound engine,
/// so an unauthenticated frame cannot affect catalog, data, or fan-out state.
pub(super) fn unauthenticated_array_reject(
    frame: &SyncFrame,
    authenticated: bool,
    identity_present: bool,
) -> Option<SyncFrame> {
    if !is_array_frame(frame.msg_type) || (authenticated && identity_present) {
        return None;
    }

    // An advisory frame names no local op, so its refusal leaves the push
    // window alone.
    let advisory = match frame.msg_type {
        SyncMessageType::ArrayAck => Some(ArrayAdvisoryKind::Ack),
        SyncMessageType::ArrayCatchupRequest => Some(ArrayAdvisoryKind::CatchupRequest),
        _ => None,
    };
    if let Some(request) = advisory {
        return advisory_refusal_frame(&ArrayAdvisoryRefusedMsg {
            array: String::new(),
            request,
            reason: ArrayRejectReason::Unauthenticated,
            detail: "unauthenticated array frame".into(),
        });
    }

    let reject = ArrayRejectMsg {
        // Do not reflect untrusted frame contents into the response: this
        // keeps the rejection bounded even for a deliberately oversized body.
        array: String::new(),
        op_hlc_bytes: [0; 18],
        reason: ArrayRejectReason::Unauthenticated,
        detail: "unauthenticated array frame".into(),
    };
    SyncFrame::try_encode(SyncMessageType::ArrayReject, &reject)
}

/// The reject for an array frame whose body does not decode. It names no
/// array and no op, so the client holds every op it has in flight.
fn undecodable_frame_reject(msg_type: SyncMessageType) -> Option<SyncFrame> {
    let reject = ArrayRejectMsg {
        array: String::new(),
        op_hlc_bytes: [0; 18],
        reason: ArrayRejectReason::ShapeInvalid,
        detail: format!("array frame 0x{:02x} body did not decode", msg_type as u8),
    };
    SyncFrame::try_encode(SyncMessageType::ArrayReject, &reject)
}

fn reject_frame(reject: &ArrayRejectMsg) -> Option<SyncFrame> {
    SyncFrame::try_encode(SyncMessageType::ArrayReject, reject)
}

fn advisory_refusal_frame(refusal: &ArrayAdvisoryRefusedMsg) -> Option<SyncFrame> {
    SyncFrame::try_encode(SyncMessageType::ArrayAdvisoryRefused, refusal)
}

/// The refusal for an advisory frame whose body does not decode.
fn undecodable_advisory_refusal(request: ArrayAdvisoryKind) -> Option<SyncFrame> {
    advisory_refusal_frame(&ArrayAdvisoryRefusedMsg {
        array: String::new(),
        request,
        reason: ArrayRejectReason::ShapeInvalid,
        detail: "advisory array frame body did not decode".into(),
    })
}

/// The response for a completed inbound snapshot: its per-op results.
fn snapshot_outcome_frame(outcome: InboundOutcome) -> Option<SyncFrame> {
    match outcome {
        InboundOutcome::SnapshotApplied(results) => {
            SyncFrame::try_encode(SyncMessageType::ArrayOpResults, &results)
        }
        InboundOutcome::Applied
        | InboundOutcome::Idempotent
        | InboundOutcome::SnapshotPartial { .. }
        | InboundOutcome::SchemaImported
        | InboundOutcome::AckRecorded
        | InboundOutcome::CatchupRequested => None,
    }
}

/// Route one inbound array frame to the inbound engine, returning the frame
/// to send back: a reject, or the per-op results of a multi-op frame.
pub(super) async fn dispatch_array_frame(
    frame: &SyncFrame,
    inbound: &crate::control::array_sync::OriginArrayInbound,
    session_id: &str,
) -> Option<SyncFrame> {
    match frame.msg_type {
        SyncMessageType::ArrayDelta => match frame.decode_body::<ArrayDeltaMsg>() {
            Some(msg) => match inbound.handle_delta(&msg).await {
                Ok(_) => None,
                Err(reject) => reject_frame(&reject),
            },
            None => undecodable_frame_reject(frame.msg_type),
        },
        SyncMessageType::ArrayDeltaBatch => match frame.decode_body::<ArrayDeltaBatchMsg>() {
            Some(msg) => {
                let results = inbound.handle_delta_batch(&msg).await;
                SyncFrame::try_encode(SyncMessageType::ArrayOpResults, &results)
            }
            None => undecodable_frame_reject(frame.msg_type),
        },
        SyncMessageType::ArraySnapshot => match frame.decode_body::<ArraySnapshotMsg>() {
            Some(msg) => match inbound.handle_snapshot_header(&msg) {
                Ok(_) => None,
                Err(reject) => reject_frame(&reject),
            },
            None => undecodable_frame_reject(frame.msg_type),
        },
        SyncMessageType::ArraySnapshotChunk => match frame.decode_body::<ArraySnapshotChunkMsg>() {
            Some(msg) => match inbound.handle_snapshot_chunk(&msg).await {
                Ok(outcome) => snapshot_outcome_frame(outcome),
                Err(reject) => reject_frame(&reject),
            },
            None => undecodable_frame_reject(frame.msg_type),
        },
        SyncMessageType::ArraySchema => match frame.decode_body::<ArraySchemaSyncMsg>() {
            Some(msg) => match inbound.handle_schema(&msg).await {
                Ok(_) => None,
                Err(reject) => reject_frame(&reject),
            },
            None => undecodable_frame_reject(frame.msg_type),
        },
        SyncMessageType::ArrayAck => match frame.decode_body::<ArrayAckMsg>() {
            Some(msg) => match inbound.handle_ack(&msg) {
                Ok(_) => None,
                Err(refusal) => advisory_refusal_frame(&refusal),
            },
            None => undecodable_advisory_refusal(ArrayAdvisoryKind::Ack),
        },
        SyncMessageType::ArrayCatchupRequest => {
            match frame.decode_body::<ArrayCatchupRequestMsg>() {
                Some(msg) => match inbound.handle_catchup_request(&msg, session_id) {
                    Ok(_) => None,
                    Err(refusal) => advisory_refusal_frame(&refusal),
                },
                None => undecodable_advisory_refusal(ArrayAdvisoryKind::CatchupRequest),
            }
        }
        SyncMessageType::ArrayAdvisoryRefused => {
            warn!(
                session = %session_id,
                "sync: received ArrayAdvisoryRefused (outbound-only); ignoring"
            );
            None
        }
        SyncMessageType::ArrayReject => {
            if let Some(msg) = frame.decode_body::<ArrayRejectMsg>() {
                warn!(
                    session = %session_id,
                    array = %msg.array,
                    reason = ?msg.reason,
                    "sync: received ArrayReject (outbound-only); ignoring"
                );
            }
            None
        }
        SyncMessageType::ArrayOpResults => {
            warn!(
                session = %session_id,
                "sync: received ArrayOpResults (outbound-only); ignoring"
            );
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
    use crate::wal::WalManager;

    /// Regression guard for the tenant-isolation bug: `build_array_inbound`
    /// must bind the inbound array engine to the tenant it is GIVEN, never a
    /// hardcoded placeholder. The engine's tenant flows into every replicated
    /// array write (`ReplicatedEntry`) and into
    /// `ArrayId::in_database(tenant, database, array)`,
    /// so a reverted-to-0 tenant here silently routes one tenant's array
    /// writes under another. Building a real `SharedState` (no cluster) is
    /// enough — the tenant threading is entirely local to this function.
    #[tokio::test]
    async fn build_array_inbound_binds_given_tenant_not_placeholder() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).unwrap();

        // A deliberately non-zero, non-default tenant so a placeholder-0
        // regression is unmistakable.
        let tenant = crate::types::TenantId::new(7);
        let identity = AuthenticatedIdentity::new_regular(
            7,
            "array-writer",
            tenant,
            AuthMethod::ApiKey,
            Vec::new(),
            None,
            AuthenticatedIdentity::default_database_set(false),
        );
        let inbound = build_array_inbound(&Some(shared), identity.clone())
            .expect("SharedState present => engine built");
        assert_eq!(
            inbound.tenant_id(),
            tenant,
            "inbound array engine must bind the session tenant, not a placeholder"
        );
        assert_eq!(
            inbound.database_id(),
            crate::types::DatabaseId::DEFAULT,
            "an identity without an explicit database remains compatible with default"
        );

        // No SharedState (the no-op listener path) => no engine.
        assert!(build_array_inbound(&None, identity).is_none());

        drop(dir);
    }

    #[tokio::test]
    async fn build_array_inbound_binds_non_default_session_database() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("shared state");
        let database_id = crate::types::DatabaseId::new(4_096);
        let identity = AuthenticatedIdentity::new_regular(
            8,
            "non-default-array-writer",
            crate::types::TenantId::new(8),
            AuthMethod::ApiKey,
            Vec::new(),
            Some(database_id),
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![database_id]),
        );

        let inbound = build_array_inbound(&Some(shared), identity)
            .expect("SharedState present => engine built");
        assert_eq!(inbound.database_id(), database_id);
    }

    #[test]
    fn unauthenticated_array_schema_and_delta_are_rejected_before_inbound_dispatch() {
        let delta = SyncFrame::try_encode(
            SyncMessageType::ArrayDelta,
            &ArrayDeltaMsg {
                array: "never-dispatched".into(),
                op_payload: vec![1, 2, 3],
                producer_id: 1,
                epoch: 1,
                seq: 1,
            },
        )
        .expect("encode delta");
        let schema = SyncFrame::try_encode(
            SyncMessageType::ArraySchema,
            &ArraySchemaSyncMsg {
                array: "never-dispatched".into(),
                replica_id: 1,
                schema_hlc_bytes: [1; 18],
                snapshot_payload: vec![1],
            },
        )
        .expect("encode schema");

        for frame in [&delta, &schema] {
            for (authenticated, identity_present) in [(false, false), (true, false)] {
                let reject = unauthenticated_array_reject(frame, authenticated, identity_present)
                    .expect("unauthenticated array frame must be rejected before inbound dispatch");
                assert_eq!(reject.msg_type, SyncMessageType::ArrayReject);
                let body: ArrayRejectMsg = reject.decode_body().expect("decode rejection");
                assert_eq!(body.reason, ArrayRejectReason::Unauthenticated);
                assert_eq!(body.detail, "unauthenticated array frame");
                assert!(body.array.is_empty(), "rejection must remain bounded");
            }
            assert!(
                unauthenticated_array_reject(frame, true, true).is_none(),
                "a current authenticated identity must reach normal array dispatch"
            );
        }
    }

    #[tokio::test]
    async fn an_undecodable_array_frame_draws_an_unattributed_shape_reject() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) = reject_fixture::writer(dir.path(), reject_fixture::refuse_all());
        for msg_type in [
            SyncMessageType::ArrayDelta,
            SyncMessageType::ArrayDeltaBatch,
            SyncMessageType::ArraySchema,
        ] {
            // 0xc1 is a reserved MessagePack byte: the frame parses, its body does not.
            let body = [0xc1_u8];
            let mut wire = vec![SyncFrame::FORMAT_VERSION, msg_type as u8];
            wire.extend_from_slice(&(body.len() as u32).to_le_bytes());
            wire.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
            wire.extend_from_slice(&body);
            let frame = SyncFrame::from_bytes(&wire).expect("a well-formed frame");
            let reply = dispatch_array_frame(&frame, &inbound, "s")
                .await
                .expect("an undecodable frame must be answered");
            assert_eq!(reply.msg_type, SyncMessageType::ArrayReject);
            let body: ArrayRejectMsg = reply.decode_body().expect("decode reject");
            assert_eq!(body.reason, ArrayRejectReason::ShapeInvalid);
            assert_eq!(body.op_hlc_bytes, [0; 18]);
            assert!(body.array.is_empty());
        }
    }

    #[tokio::test]
    async fn a_capacity_refused_delta_is_rejected_as_transient() {
        use nodedb_types::sync::wire::array::ArrayRejectClass;

        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) = reject_fixture::writer(dir.path(), reject_fixture::refuse_all());
        let op = reject_fixture::delete_op(reject_fixture::ARRAY, 1, Hlc::ZERO);
        let frame = SyncFrame::try_encode(
            SyncMessageType::ArrayDelta,
            &ArrayDeltaMsg {
                array: reject_fixture::ARRAY.into(),
                op_payload: op_codec::encode_op(&op).expect("encode op"),
                producer_id: 0,
                epoch: 0,
                seq: 1,
            },
        )
        .expect("encode delta");

        let reply = dispatch_array_frame(&frame, &inbound, "s")
            .await
            .expect("a refused delta must be answered");
        assert_eq!(reply.msg_type, SyncMessageType::ArrayReject);
        let body: ArrayRejectMsg = reply.decode_body().expect("decode reject");
        assert_eq!(body.reason, ArrayRejectReason::EngineUnavailable);
        assert_eq!(body.class(), ArrayRejectClass::Transient);
        assert_eq!(body.op_hlc_bytes, op.header.hlc.to_bytes());
        assert_eq!(body.array, reject_fixture::ARRAY);
    }

    #[tokio::test]
    async fn a_batch_reports_every_op_in_order_and_keeps_applying_after_a_reject() {
        use nodedb_types::sync::wire::array::{ArrayOpOutcome, ArrayOpResultsMsg};

        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) =
            reject_fixture::writer(dir.path(), reject_fixture::admit_first_then_refuse());
        let applied = reject_fixture::delete_op(reject_fixture::ARRAY, 2, Hlc::ZERO);
        let unknown = reject_fixture::delete_op("no-such-array", 3, Hlc::ZERO);
        let refused = reject_fixture::delete_op(reject_fixture::ARRAY, 4, Hlc::ZERO);
        let too_new_schema =
            Hlc::new(nodedb_array::sync::hlc::MAX_PHYSICAL_MS, 0, ReplicaId(9)).expect("max hlc");
        let too_new = reject_fixture::delete_op(reject_fixture::ARRAY, 5, too_new_schema);
        let encode = |op: &ArrayOp| op_codec::encode_op(op).expect("encode op");
        let frame = SyncFrame::try_encode(
            SyncMessageType::ArrayDeltaBatch,
            &ArrayDeltaBatchMsg {
                array: reject_fixture::ARRAY.into(),
                op_payloads: vec![
                    vec![0xc1],
                    encode(&applied),
                    encode(&unknown),
                    encode(&refused),
                    encode(&too_new),
                ],
            },
        )
        .expect("encode batch");

        let reply = dispatch_array_frame(&frame, &inbound, "s")
            .await
            .expect("a batch is always answered");
        assert_eq!(reply.msg_type, SyncMessageType::ArrayOpResults);
        let body: ArrayOpResultsMsg = reply.decode_body().expect("decode results");
        assert_eq!(body.array, reject_fixture::ARRAY);

        let observed: Vec<([u8; 18], Option<ArrayRejectReason>)> = body
            .results
            .iter()
            .map(|result| {
                let reason = match &result.outcome {
                    ArrayOpOutcome::Applied | ArrayOpOutcome::Duplicate => None,
                    ArrayOpOutcome::Rejected { reason, .. } => Some(*reason),
                };
                (result.op_hlc_bytes, reason)
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                ([0; 18], Some(ArrayRejectReason::ShapeInvalid)),
                (applied.header.hlc.to_bytes(), None),
                (
                    unknown.header.hlc.to_bytes(),
                    Some(ArrayRejectReason::ArrayUnknown)
                ),
                (
                    refused.header.hlc.to_bytes(),
                    Some(ArrayRejectReason::EngineUnavailable)
                ),
                (
                    too_new.header.hlc.to_bytes(),
                    Some(ArrayRejectReason::SchemaTooNew)
                ),
            ]
        );
        assert_eq!(body.results[1].outcome, ArrayOpOutcome::Applied);
    }

    #[tokio::test]
    async fn a_denied_ack_and_catch_up_draw_a_typed_advisory_refusal() {
        use nodedb_types::sync::wire::AckStatus;
        use nodedb_types::sync::wire::array::ArrayRejectClass;

        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) = reject_fixture::no_permission(dir.path());
        let ack = SyncFrame::try_encode(
            SyncMessageType::ArrayAck,
            &ArrayAckMsg {
                array: reject_fixture::ARRAY.into(),
                replica_id: 3,
                ack_hlc_bytes: [0; 18],
                applied_seq: 0,
                status: AckStatus::Applied,
            },
        )
        .expect("encode ack");
        let catch_up = SyncFrame::try_encode(
            SyncMessageType::ArrayCatchupRequest,
            &ArrayCatchupRequestMsg {
                array: reject_fixture::ARRAY.into(),
                from_hlc_bytes: [0; 18],
            },
        )
        .expect("encode catch-up");

        for (frame, request) in [
            (&ack, ArrayAdvisoryKind::Ack),
            (&catch_up, ArrayAdvisoryKind::CatchupRequest),
        ] {
            let reply = dispatch_array_frame(frame, &inbound, "s")
                .await
                .expect("a denied advisory request must be answered");
            assert_eq!(
                reply.msg_type,
                SyncMessageType::ArrayAdvisoryRefused,
                "an advisory denial never travels as an ArrayReject"
            );
            let body: ArrayAdvisoryRefusedMsg = reply.decode_body().expect("decode refusal");
            assert_eq!(body.request, request);
            assert_eq!(body.array, reject_fixture::ARRAY);
            assert_eq!(body.reason, ArrayRejectReason::PermissionDenied);
            assert_eq!(body.class(), ArrayRejectClass::Permanent);
        }
    }

    #[tokio::test]
    async fn a_catch_up_for_an_unknown_array_draws_a_schema_refusal() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) = reject_fixture::writer(dir.path(), reject_fixture::refuse_all());
        let frame = SyncFrame::try_encode(
            SyncMessageType::ArrayCatchupRequest,
            &ArrayCatchupRequestMsg {
                array: "no-such-array".into(),
                from_hlc_bytes: [0; 18],
            },
        )
        .expect("encode catch-up");
        let reply = dispatch_array_frame(&frame, &inbound, "s")
            .await
            .expect("an unknown-array catch-up must be answered");
        assert_eq!(reply.msg_type, SyncMessageType::ArrayAdvisoryRefused);
        let body: ArrayAdvisoryRefusedMsg = reply.decode_body().expect("decode refusal");
        assert_eq!(body.request, ArrayAdvisoryKind::CatchupRequest);
        assert_eq!(body.reason, ArrayRejectReason::ArrayUnknown);
    }

    #[test]
    fn an_unauthenticated_advisory_frame_draws_an_advisory_refusal() {
        let ack = SyncFrame::try_encode(
            SyncMessageType::ArrayAck,
            &ArrayAckMsg {
                array: "a".into(),
                replica_id: 1,
                ack_hlc_bytes: [0; 18],
                applied_seq: 0,
                status: nodedb_types::sync::wire::AckStatus::Applied,
            },
        )
        .expect("encode ack");
        let reply = unauthenticated_array_reject(&ack, false, false).expect("answered");
        assert_eq!(reply.msg_type, SyncMessageType::ArrayAdvisoryRefused);
        let body: ArrayAdvisoryRefusedMsg = reply.decode_body().expect("decode refusal");
        assert_eq!(body.reason, ArrayRejectReason::Unauthenticated);
        assert_eq!(body.request, ArrayAdvisoryKind::Ack);
    }

    #[tokio::test]
    async fn a_full_snapshot_buffer_is_rejected_as_transient() {
        use nodedb_array::sync::snapshot::{CoordRange, SnapshotHeader};
        use nodedb_types::sync::wire::array::ArrayRejectClass;

        let dir = tempfile::tempdir().expect("tmpdir");
        let (_shared, inbound) = reject_fixture::writer(dir.path(), reject_fixture::refuse_all());
        let header_frame = |ms: u64| {
            let header = SnapshotHeader {
                array: reject_fixture::ARRAY.into(),
                coord_range: CoordRange {
                    lo: Vec::new(),
                    hi: Vec::new(),
                },
                total_chunks: 1,
                snapshot_hlc: Hlc::new(ms, 0, ReplicaId(3)).expect("hlc"),
                schema_hlc: Hlc::ZERO,
            };
            SyncFrame::try_encode(
                SyncMessageType::ArraySnapshot,
                &ArraySnapshotMsg {
                    array: reject_fixture::ARRAY.into(),
                    header_payload: zerompk::to_msgpack_vec(&header).expect("encode header"),
                },
            )
            .expect("encode snapshot frame")
        };

        let mut ms = 1;
        let reject = loop {
            if let Some(reply) = dispatch_array_frame(&header_frame(ms), &inbound, "s").await {
                break reply;
            }
            ms += 1;
            assert!(ms < 10_000, "the assembly cap must refuse a header");
        };
        let body: ArrayRejectMsg = reject.decode_body().expect("decode reject");
        assert_eq!(body.reason, ArrayRejectReason::SnapshotCapacity);
        assert_eq!(body.class(), ArrayRejectClass::Transient);
    }

    use nodedb_array::sync::hlc::Hlc;
    use nodedb_array::sync::op::ArrayOp;
    use nodedb_array::sync::op_codec;
    use nodedb_array::sync::replica_id::ReplicaId;

    /// An authenticated array writer whose Raft proposer is a test double.
    mod reject_fixture {
        use std::path::Path;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::sync::SchemaDoc;
        use nodedb_array::sync::hlc::{Hlc, HlcGenerator};
        use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
        use nodedb_array::sync::replica_id::ReplicaId;
        use nodedb_array::types::coord::value::CoordValue;
        use nodedb_array::types::domain::{Domain, DomainBound};

        use crate::bridge::dispatch::Dispatcher;
        use crate::control::array_sync::OriginArrayInbound;
        use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
        use crate::control::state::SharedState;
        use crate::control::wal_replication::{AppliedOutput, AsyncRaftProposer};
        use crate::types::{DatabaseId, TenantId};
        use crate::wal::WalManager;

        pub(super) const ARRAY: &str = "reject-class-array";
        const TENANT: u64 = 7;

        fn capacity_refusal() -> crate::Error {
            crate::Error::DispatchCapacity {
                scope: crate::DispatchCapacityScope::QueueFull {
                    core_id: 0,
                    capacity: 8,
                },
            }
        }

        /// A proposer that refuses every write at dispatch capacity.
        pub(super) fn refuse_all() -> Arc<AsyncRaftProposer> {
            Arc::new(|_vshard, _key, _bytes, _deadline| {
                Box::pin(async { Err::<AppliedOutput, _>(capacity_refusal()) })
            })
        }

        /// A proposer that applies the first write and refuses the rest at
        /// dispatch capacity.
        pub(super) fn admit_first_then_refuse() -> Arc<AsyncRaftProposer> {
            let calls = Arc::new(AtomicUsize::new(0));
            Arc::new(move |_vshard, _key, _bytes, _deadline| {
                let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                Box::pin(async move {
                    if first {
                        Ok(Vec::new())
                    } else {
                        Err(capacity_refusal())
                    }
                })
            })
        }

        pub(super) fn delete_op(array: &str, ms: u64, schema_hlc: Hlc) -> ArrayOp {
            ArrayOp {
                header: ArrayOpHeader {
                    array: array.into(),
                    hlc: Hlc::new(ms, 0, ReplicaId::new(3)).expect("hlc"),
                    schema_hlc,
                    valid_from_ms: 0,
                    valid_until_ms: -1,
                    system_from_ms: ms as i64,
                },
                kind: ArrayOpKind::Delete,
                coord: vec![CoordValue::Int64(ms as i64)],
                attrs: None,
            }
        }

        fn import_schema(shared: &SharedState) {
            let schema = ArraySchemaBuilder::new(ARRAY)
                .dim(DimSpec::new(
                    "x",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_023)),
                ))
                .attr(AttrSpec::new("v", AttrType::Int64, true))
                .tile_extents(vec![16])
                .build()
                .expect("schema");
            let hlc_gen = HlcGenerator::new(ReplicaId(7));
            let doc = SchemaDoc::from_schema(ReplicaId(7), &schema, &hlc_gen).expect("doc");
            let snapshot = doc.export_snapshot().expect("snapshot");
            shared
                .array_sync_schemas
                .import_snapshot_in_database(
                    DatabaseId::DEFAULT,
                    TENANT,
                    ARRAY,
                    &snapshot,
                    doc.schema_hlc(),
                )
                .expect("import schema");
            // A schema import also puts the array's row in the catalog, which
            // routes each synced cell to its tile vShard.
            let entry = crate::control::array_sync::catalog_register::catalog_entry_of(
                crate::types::TenantId::new(TENANT),
                DatabaseId::DEFAULT,
                ARRAY,
                &snapshot,
                doc.schema_hlc(),
            )
            .expect("catalog row");
            shared
                .array_catalog
                .write()
                .expect("array catalog lock")
                .register(entry)
                .expect("register array");
        }

        /// A read-write session on `ARRAY` whose proposer is `proposer`.
        pub(super) fn writer(
            dir: &Path,
            proposer: Arc<AsyncRaftProposer>,
        ) -> (Arc<SharedState>, Arc<OriginArrayInbound>) {
            session(dir, proposer, vec![Role::ReadWrite])
        }

        /// A session on `ARRAY` that holds no role, so every request is denied.
        pub(super) fn no_permission(dir: &Path) -> (Arc<SharedState>, Arc<OriginArrayInbound>) {
            session(dir, refuse_all(), Vec::new())
        }

        fn session(
            dir: &Path,
            proposer: Arc<AsyncRaftProposer>,
            roles: Vec<Role>,
        ) -> (Arc<SharedState>, Arc<OriginArrayInbound>) {
            let wal = Arc::new(WalManager::open_for_testing(&dir.join("reject.wal")).expect("wal"));
            let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
            let shared = SharedState::new(dispatcher, wal).expect("shared state");
            crate::control::vshard_admission::install_async_raft_proposer(
                &shared,
                crate::control::vshard_admission::applying_submit(proposer),
            )
            .expect("install proposer");
            import_schema(&shared);
            let identity = AuthenticatedIdentity::new_regular(
                1,
                "array-writer",
                TenantId::new(TENANT),
                AuthMethod::Trust,
                roles,
                Some(DatabaseId::DEFAULT),
                AuthenticatedIdentity::default_database_set(false),
            );
            let inbound = super::super::build_array_inbound(&Some(Arc::clone(&shared)), identity)
                .expect("inbound engine");
            (shared, inbound)
        }
    }
}
