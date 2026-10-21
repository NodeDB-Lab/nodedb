// SPDX-License-Identifier: BUSL-1.1

//! Dispatch a CRDT delta to the Data Plane and hand its outcome to the frame
//! builder.

use tracing::warn;

use nodedb_types::sync::wire::{EngineKind, SyncProvenance, stream_id_for};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::wire::{
    CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};
use super::authorize::{authorize_delta_write, permission_denied_delta_reject};
use super::outcome::frame_for_dispatch;
use super::peer_identity::{
    PeerIdentity, PeerIdentityRequest, admit_peer_identity, peer_collision_reason,
};
use super::signature::delta_signature_valid;

/// What one delta's durable dispatch produced.
///
/// The frame is what the client is told; `trimmed_ops` is what the delta
/// actually carried. They are returned together because the session needs both
/// and neither can be derived from the other: two deltas with opposite
/// consequences for the client's data can produce the same frame.
pub(crate) struct DeltaDispatchOutcome {
    pub(crate) frame: Option<SyncFrame>,
    pub(crate) trimmed_ops: u64,
}

impl DeltaDispatchOutcome {
    /// A refusal decided before any CRDT merge ran, so nothing was trimmed.
    fn refused(frame: Option<SyncFrame>) -> Self {
        Self {
            frame,
            trimmed_ops: 0,
        }
    }
}

/// The handshake-bound session state a delta apply is evaluated against.
///
/// These travel together because they all describe the same authenticated sync
/// session: the identity the delta is authorized as, the key its ops are signed
/// with, its producer/epoch position, and the peer address the blacklist is
/// checked against.
pub(crate) struct DeltaSessionContext<'a> {
    pub(crate) identity: Option<&'a AuthenticatedIdentity>,
    pub(crate) signing_key: Option<&'a [u8; 32]>,
    pub(crate) producer_id: u64,
    pub(crate) epoch: u64,
    pub(crate) peer_addr: &'a str,
}

/// Whether `delta` fails to state `current`, the incarnation of its collection
/// the catalog holds. Every committed collection has a nonzero incarnation and
/// every client the handshake admits states one, so a delta that states none
/// (`Hlc::ZERO`) is refused like one for a replaced incarnation.
fn incarnation_mismatch(delta: &DeltaPushMsg, current: nodedb_types::Hlc) -> bool {
    delta.collection_incarnation == nodedb_types::Hlc::ZERO
        || delta.collection_incarnation != current
}

/// Apply a CRDT delta on the Data Plane, converting the outcome into the final
/// client frame.
///
/// The in-memory session already produced a provisional `DeltaAck`; this step
/// performs the actual durable apply and finalizes the client frame. Which
/// frame that is — a `DeltaAck` (including the retryable `Gap`) or a terminal
/// `DeltaReject` — is decided in exactly one place, [`frame_for_dispatch`], so
/// the sender's obligation always matches what the server actually did.
pub(crate) async fn apply_delta_and_finalize(
    shared: &SharedState,
    delta_msg: &DeltaPushMsg,
    ack_frame: SyncFrame,
    session: DeltaSessionContext<'_>,
) -> DeltaDispatchOutcome {
    let DeltaSessionContext {
        identity,
        signing_key: session_signing_key,
        producer_id: session_producer_id,
        epoch: session_epoch,
        peer_addr,
    } = session;
    // Pinned once: preview, propose and apply all end at this instant.
    let deadline = StatementDeadline::of_statement(shared);
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::CrdtOp;

    // Authorize before quota, surrogate allocation, catalog lookup, plan
    // construction, or dispatch. The tenant is derived only from the
    // handshake-bound identity; an absent identity fails closed without a
    // fallback tenant or reconstructed principal.
    let tenant_id = match authorize_delta_write(shared, identity, &delta_msg.collection) {
        Ok(tenant_id) => tenant_id,
        Err(_) => {
            return DeltaDispatchOutcome::refused(permission_denied_delta_reject(delta_msg));
        }
    };
    let identity = match identity {
        Some(identity) => identity,
        None => {
            return DeltaDispatchOutcome::refused(permission_denied_delta_reject(delta_msg));
        }
    };

    // Same binding the session's reads use: the principal's database, not the
    // built-in default. A delta must land in the database its subscriber will
    // read it back from.
    let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);

    // Blacklist + account status, no rate limit: CRDT delta sync is not the
    // per-query traffic the rate-limiter's cost table models, so charging it
    // against a query rate limit will throttle legitimate offline-first
    // sync traffic. A blacklisted or suspended/banned account must not be
    // able to keep pushing deltas, though — `check_blacklist_and_status`
    // runs that half of `check_request_admission`'s gate (plus the
    // internal-service exemption every other transport gets) using the sync
    // session's real remote address.
    // The scope is resolved against that same address, so `$auth.risk_score`
    // is stamped and an IP-conditional grant is evaluated for this device
    // rather than being withheld.
    let request = match crate::control::security::request_scope::ClientRequestScope::for_database(
        identity,
        shared.auth_stores(),
        database_id,
        peer_addr,
    ) {
        Ok(request) => request,
        Err(e) => {
            warn!(error = %e, "sync: delta request scope could not be resolved");
            let detail = e.to_string();
            return terminal_reject(
                delta_msg,
                detail.clone(),
                CompensationHint::Custom {
                    constraint: "request_scope".into(),
                    detail,
                },
            );
        }
    };
    if let Err(e) =
        crate::control::server::session_auth::check_blacklist_and_status(shared, &request)
    {
        warn!(error = %e, "sync: delta rejected by blacklist or account status");
        return terminal_reject(
            delta_msg,
            "sender is blocked",
            CompensationHint::PermissionDenied,
        );
    }

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&shared.audit));
    // RLS write policies are stored keyed by `db_qualified(database_id,
    // collection)`, so the policy is handed that same key or it silently
    // misses a policy on a non-default database. `delta_msg.collection`
    // itself stays bare everywhere else below: it feeds the catalog lookup,
    // vShard routing, and the dispatched op's own collection field, which
    // must stay mutually consistent.
    let qualified_collection = crate::control::planner::sql_plan_convert::convert::db_qualified(
        database_id,
        &delta_msg.collection,
    );
    let crs_set = crate::control::crs_context::crs_set_for(shared);
    let policy =
        crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
            tenant_id,
            database_id,
            &qualified_collection,
            identity,
            "sync".into(),
            crate::control::crdt_post_image_policy::PolicyServices {
                rls: &shared.rls,
                audit: &audit,
                crs_set: &crs_set,
            },
        )
        .with_type_guards_from(shared, database_id);

    let (constraint_version_required, signing_required) = match shared
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id.as_u64(), &delta_msg.collection)
    {
        // A delta built against an incarnation a drop and a same-name create
        // replaced writes into a collection that no longer exists. Merged
        // into the new collection's document it would bring the dropped rows
        // back, so it is refused, and the client drops its state for it. A
        // delta that states no incarnation cannot be judged, so it is refused
        // the same way.
        Ok(Some(collection)) if incarnation_mismatch(delta_msg, collection.incarnation) => {
            let reason = if delta_msg.collection_incarnation == nodedb_types::Hlc::ZERO {
                format!(
                    "the delta for collection '{}' states no incarnation; drop the local state \
                     of the collection and sync it again",
                    delta_msg.collection
                )
            } else {
                format!(
                    "collection '{}' was dropped and created again: the delta targets an \
                     incarnation it no longer holds; drop the local state of the collection \
                     and sync it again",
                    delta_msg.collection
                )
            };
            return terminal_reject(
                delta_msg,
                reason.clone(),
                CompensationHint::Custom {
                    constraint: "collection_incarnation".into(),
                    detail: reason,
                },
            );
        }
        Ok(Some(collection)) => (
            collection.constraint_version,
            collection.crdt_signing_required,
        ),
        Ok(None) => {
            return terminal_reject(
                delta_msg,
                "collection not found",
                CompensationHint::PermissionDenied,
            );
        }
        Err(error) => {
            warn!(%error, collection = %delta_msg.collection, "sync signing policy lookup failed");
            return terminal_reject(
                delta_msg,
                "collection security policy is temporarily unavailable",
                CompensationHint::PermissionDenied,
            );
        }
    };

    if signing_required && !shared.wal.payloads_authenticated() {
        return terminal_reject(
            delta_msg,
            "SIGNED_DELTAS requires authenticated WAL encryption",
            CompensationHint::PermissionDenied,
        );
    }

    let signing_valid = delta_signature_valid(
        delta_msg,
        identity.user_id,
        session_signing_key,
        session_producer_id,
        session_epoch,
        signing_required,
    );
    if !signing_valid {
        return terminal_reject(
            delta_msg,
            "CRDT delta signature is missing or invalid",
            CompensationHint::PermissionDenied,
        );
    }

    // Quota enforcement — reject before dispatch.
    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(error = %e, "sync: delta validation rejected by quota");
        let detail = e.to_string();
        return terminal_reject(
            delta_msg,
            detail.clone(),
            CompensationHint::Custom {
                constraint: "quota".into(),
                detail,
            },
        );
    }

    // Hold the delta's Loro peer id to this session's producer before a
    // surrogate is assigned or anything is dispatched. A colliding peer id is
    // refused here because it cannot be refused later: past this point the
    // merge absorbs the delta and reports success.
    let peer_identity = admit_peer_identity(
        shared,
        PeerIdentityRequest {
            database_id,
            tenant_id,
            collection: &delta_msg.collection,
            peer_id: delta_msg.peer_id,
            producer_id: session_producer_id,
        },
    )
    .await;
    match peer_identity {
        Ok(PeerIdentity::Owned | PeerIdentity::Unbound) => {}
        Ok(PeerIdentity::Collision { owner_producer_id }) => {
            let reason =
                peer_collision_reason(&delta_msg.collection, delta_msg.peer_id, owner_producer_id);
            return terminal_reject(
                delta_msg,
                reason.clone(),
                CompensationHint::Custom {
                    constraint: CompensationHint::PEER_ID_COLLISION.into(),
                    detail: reason,
                },
            );
        }
        Err(error) => {
            // The binding cannot be established, so whether this peer id is
            // safe to write under is unknown. Admitting the delta will gamble
            // the client's write on it; refusing retryably costs a re-push.
            warn!(
                %error,
                collection = %delta_msg.collection,
                peer_id = delta_msg.peer_id,
                "sync: peer-id binding unavailable; refusing the delta retryably"
            );
            return DeltaDispatchOutcome::refused(retryable_binding_refusal(delta_msg));
        }
    }

    let surrogate = match crate::control::server::surrogate_exchange::assign_surrogate_routed(
        shared,
        nodedb_types::CollectionKey::from_bare(database_id, &delta_msg.collection),
        tenant_id,
        delta_msg.document_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "sync: surrogate assignment failed");
            let detail = e.to_string();
            return terminal_reject(
                delta_msg,
                detail.clone(),
                CompensationHint::Custom {
                    constraint: "surrogate".into(),
                    detail,
                },
            );
        }
    };

    // Server-authoritative provenance: producer_id + epoch come from the
    // session's handshake-assigned identity, never from the wire message — a
    // client cannot spoof another producer's id or replay a fenced epoch. Only
    // `seq` is client-owned (the per-producer monotonic counter the gate validates).
    let prov = SyncProvenance {
        producer_id: session_producer_id,
        epoch: session_epoch,
        stream_id: stream_id_for(EngineKind::Crdt, &delta_msg.collection),
        seq: delta_msg.seq,
    };

    let plan = PhysicalPlan::Crdt(CrdtOp::ApplyAuthenticated {
        collection: nodedb_types::QualifiedCollection::new(database_id, &delta_msg.collection),
        document_id: delta_msg.document_id.clone(),
        delta: delta_msg.delta.clone(),
        peer_id: delta_msg.peer_id,
        mutation_id: delta_msg.mutation_id,
        surrogate,
        provenance: prov,
        constraint_version_required,
        expected_frontier_digest: None,
        auth_user_id: identity.user_id,
        auth_device_id: session_producer_id,
        auth_seq_no: delta_msg.seq,
        delta_signature: delta_msg.delta_signature,
        signing_required,
    });

    // Extracted before `plan` is moved into `authorize_sync_task` below, and
    // only when metering is enabled — the default is disabled, so this is a
    // no-op on the hot path for every deployment that hasn't turned it on.
    let plan_metering_info = shared
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));

    // A spent hard quota refuses the delta before it is applied. The charge
    // below runs only once dispatch succeeded, so it can never be where a cap
    // blocks anything. A refusal is surfaced through the same dispatch-error
    // path an authorization failure takes, so the client sees one shape.
    let quota_refusal = plan_metering_info.as_ref().and_then(|info| {
        crate::control::server::shared::quota_admission::admit_quota_for_dispatch(
            shared,
            request.scope(),
            info,
        )
        .err()
    });

    let vshard_id =
        nodedb_types::CollectionKey::from_bare(database_id, &delta_msg.collection).vshard();
    let authorized = super::super::super::raft_dispatch::authorize_sync_task(
        shared,
        Some(identity),
        tenant_id,
        database_id,
        vshard_id,
        plan,
    );
    let _request = shared.tenant_request_guard(tenant_id);
    let dispatch_result = match quota_refusal {
        Some(refusal) => Err(refusal),
        None => match authorized {
            Ok(authorized) => {
                super::super::super::raft_dispatch::dispatch_sync_bytes(
                    shared,
                    &delta_msg.collection,
                    authorized,
                    deadline,
                    crate::event::EventSource::CrdtSync,
                    &policy,
                )
                .await
            }
            Err(error) => Err(error),
        },
    };

    let trimmed_ops = dispatch_result
        .as_ref()
        .map(|outcome| outcome.trimmed_ops)
        .unwrap_or(0);

    // Metered only on the dispatch success path, with the real ops-written
    // count — a failed dispatch (`dispatch_result.is_err()`) applied nothing
    // durable. The identity here is the tenant's Lite/edge sync client, not
    // an internal-service principal (`meter_dispatch` still gates on that
    // itself as belt-and-suspenders), so this is real, tenant-attributable
    // write work like any other CRDT apply.
    if dispatch_result.is_ok()
        && let Some(info) = &plan_metering_info
    {
        meter_dispatch(shared, request.scope(), info, Some(trimmed_ops));
    }

    DeltaDispatchOutcome {
        frame: frame_for_dispatch(
            delta_msg,
            &ack_frame,
            dispatch_result.map(|outcome| outcome.payload),
        ),
        trimmed_ops,
    }
}

/// Refuse retryably: nothing was applied and the identical delta at the same
/// sequence must be re-pushed once the binding can be established.
fn retryable_binding_refusal(delta_msg: &DeltaPushMsg) -> Option<SyncFrame> {
    use nodedb_types::sync::wire::AckStatus;

    use super::super::super::wire::DeltaAckMsg;

    let ack = DeltaAckMsg {
        mutation_id: delta_msg.mutation_id,
        lsn: 0,
        clock_skew_warning_ms: None,
        applied_seq: delta_msg.seq.saturating_sub(1),
        status: AckStatus::Gap {
            expected: delta_msg.seq,
        },
    };
    SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
}

/// A refusal decided in the Control Plane before the apply was ever attempted.
///
/// Every one of these is permanent for the frame as sent — a missing
/// collection, a revoked grant, an invalid signature, an exhausted quota —
/// so the sender must compensate rather than re-push identical bytes.
fn terminal_reject(
    delta_msg: &DeltaPushMsg,
    reason: impl Into<String>,
    compensation: CompensationHint,
) -> DeltaDispatchOutcome {
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: reason.into(),
        compensation: Some(compensation),
    };
    DeltaDispatchOutcome::refused(SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Permission, Role};
    use crate::control::security::risk::RiskConfig;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    const PEER_ADDR: &str = "10.0.0.7:44321";

    /// Only a delta that states the incarnation the catalog holds is
    /// admitted. A delta for an incarnation a drop and a same-name create
    /// replaced is refused, and so is one that states none.
    #[test]
    fn only_the_current_incarnation_is_admitted() {
        let current = nodedb_types::Hlc::new(2_000, 0);
        let dropped = nodedb_types::Hlc::new(1_000, 0);
        let mut msg = delta();
        msg.collection_incarnation = nodedb_types::Hlc::ZERO;
        assert!(
            incarnation_mismatch(&msg, current),
            "an unstated incarnation"
        );
        msg.collection_incarnation = current;
        assert!(
            !incarnation_mismatch(&msg, current),
            "the current incarnation"
        );
        msg.collection_incarnation = dropped;
        assert!(incarnation_mismatch(&msg, current), "a dropped incarnation");
    }

    fn state_with_risk(risk: RiskConfig) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create delta admission test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("delta-admission.wal"))
                .expect("open delta admission test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_risk_config(dispatcher, wal, risk)
            .expect("construct delta admission state");
        (state, dir)
    }

    fn pusher() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            88,
            "device",
            TenantId::new(3),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            Some(DatabaseId::DEFAULT),
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// The delta must be authorized before it reaches the admission gate, or
    /// the test will be measuring the wrong refusal.
    fn grant_write(shared: &SharedState) {
        shared
            .permissions
            .grant(
                "collection:0:3:notes",
                "user:device",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory write grant succeeds");
    }

    fn delta() -> DeltaPushMsg {
        DeltaPushMsg {
            collection: "notes".into(),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 1,
            epoch: 1,
            seq: 1,
            device_id: 0,
            delta_signature: [0u8; 32],
            collection_incarnation: nodedb_types::Hlc::new(1_000, 0),
        }
    }

    fn session(identity: &AuthenticatedIdentity) -> DeltaSessionContext<'_> {
        DeltaSessionContext {
            identity: Some(identity),
            signing_key: None,
            producer_id: 1,
            epoch: 1,
            peer_addr: PEER_ADDR,
        }
    }

    fn reject_reason(outcome: &DeltaDispatchOutcome) -> String {
        let frame = outcome
            .frame
            .as_ref()
            .expect("a refused delta always answers with a frame");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        frame
            .decode_body::<DeltaRejectMsg>()
            .expect("decode the reject frame")
            .reason
    }

    fn ack() -> SyncFrame {
        SyncFrame::try_encode(
            SyncMessageType::DeltaAck,
            &nodedb_types::sync::wire::DeltaAckMsg {
                mutation_id: 42,
                lsn: 0,
                clock_skew_warning_ms: None,
                applied_seq: 0,
                status: Default::default(),
            },
        )
        .expect("encode provisional ack")
    }

    /// With risk scoring enabled and every score inside the allow band, an
    /// authorized delta must pass the admission gate and be refused only by
    /// the next step (its collection does not exist here).
    ///
    /// The session's address reaches this scope, so the gate has a score to
    /// enforce and does not refuse every delta push when `[auth.risk]` is on.
    #[tokio::test]
    async fn scored_delta_passes_the_admission_gate_instead_of_failing_closed() {
        let (state, _dir) = state_with_risk(RiskConfig {
            enabled: true,
            allow_threshold: 1.0,
            deny_threshold: 2.0,
            ..Default::default()
        });
        grant_write(&state);
        let identity = pusher();

        let outcome = apply_delta_and_finalize(&state, &delta(), ack(), session(&identity)).await;

        assert_eq!(
            reject_reason(&outcome),
            "collection not found",
            "the delta must reach the step after admission, not be refused as unassessed"
        );
    }

    /// The same address that admits an in-band delta refuses an out-of-band
    /// one — proof the score is the session's, not a constant.
    #[tokio::test]
    async fn deny_band_delta_is_refused_by_the_risk_gate() {
        let (state, _dir) = state_with_risk(RiskConfig {
            enabled: true,
            allow_threshold: -1.0,
            deny_threshold: 0.0,
            ..Default::default()
        });
        grant_write(&state);
        let identity = pusher();

        let outcome = apply_delta_and_finalize(&state, &delta(), ack(), session(&identity)).await;

        assert_eq!(reject_reason(&outcome), "sender is blocked");
    }
}
