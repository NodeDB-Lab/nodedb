// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane dispatch for DDL and DSL statements a user issued.
//!
//! These statements have a principal behind them, so they take the authorized
//! door: the plan is authorized into a capability, row-level security is
//! applied, and the capability is what reaches storage. Statement handlers use
//! this instead of the system door, which exists only for work no user asked
//! for.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::server::shared::metering::{
    PlanMeteringInfo, meter_dispatch, operation_for_plan,
};
use crate::control::server::shared::response_payload::payload_or_typed_error;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::sync_dispatch::dispatch_authorized;

/// Whether the caller already ran
/// [`check_request_admission`](crate::control::server::session_auth::check_request_admission)
/// for this request at its own transport entry point.
///
/// Every native/pgwire/HTTP caller of this door reaches it only through
/// `shared::ddl::dispatch`, which every one of those transports calls AFTER
/// its own single per-request admission gate — so those callers must pass
/// [`RequestAdmission::AlreadyAdmitted`] or the request is charged against
/// its rate-limit budget twice. The one caller that reaches this door
/// directly, bypassing `shared::ddl::dispatch` entirely — the CDC-sync
/// shape-subscription snapshot (`sync::async_dispatch::shape::snapshot`) —
/// has no earlier admission call on its path (shape subscribe deliberately
/// runs only blacklist + account status + quota, not the full gate) and must pass
/// [`RequestAdmission::NotYetAdmitted`] so this remains the one place that
/// request is ever admitted.
///
/// The peer address lives on the `NotYetAdmitted` variant rather than beside
/// it, because it is read on exactly that path and nowhere else. A separate
/// field will force every `AlreadyAdmitted` caller to supply an empty
/// string that is never read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestAdmission<'a> {
    /// The caller's own transport entry already ran the full admission gate
    /// for this request; running it again here will double-charge it.
    AlreadyAdmitted,
    /// Nothing upstream of this call has admitted the request yet — this is
    /// the one gate it passes through, against `peer_addr`: the caller's real
    /// remote address, which reaches both the IP blacklist and the risk
    /// scorer.
    NotYetAdmitted { peer_addr: &'a str },
}

/// Parameters for [`dispatch_for_identity`].
///
/// Grouped into a struct rather than passed positionally because the
/// argument count (state, identity, database, collection, plan, timeout,
/// admission) exceeds what a positional call stays readable at.
pub(crate) struct DispatchRequest<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    pub collection: &'a str,
    pub plan: PhysicalPlan,
    pub timeout: Duration,
    /// Whether this request still has to pass the admission gate, and — when
    /// it does — the real remote address it is admitted against.
    pub admission: RequestAdmission<'a>,
    /// A read confirms its group on this node before it reads. Writes ignore
    /// it: Raft orders them.
    pub linearizable: bool,
}

/// Authorize `plan` for `identity`, apply row-level security, and dispatch it.
///
/// Returns the Data-Plane payload. Authorization failures and policy refusals
/// surface as typed errors before anything reaches storage.
pub(crate) async fn dispatch_for_identity(req: DispatchRequest<'_>) -> crate::Result<Vec<u8>> {
    let DispatchRequest {
        state,
        identity,
        database_id,
        collection,
        plan,
        timeout,
        admission,
        linearizable,
    } = req;
    // Extracted before `plan` is moved into `authorize_for_identity` (which
    // consumes it for RLS injection and task construction) — metering needs
    // the collection/engine shape after the dispatch below succeeds, and by
    // then the original plan is long gone. Only the narrow metering shape is
    // captured (see `PlanMeteringInfo`), not a full `plan.clone()`, and only
    // when metering is enabled — the default is disabled, so this is a no-op
    // on the hot path for every caller that hasn't turned it on.
    let plan_metering_info = state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));
    let result =
        match authorize_for_identity(state, identity, database_id, collection, plan, admission)
            .await?
        {
            // A clone hook served the request itself (copy-up/tombstone applied, or
            // a shadowed-clone read merged from its source): its `Response` is the
            // answer, flattened to bytes the same way the Data Plane's own is, so a
            // caller cannot tell the two apart by error shape.
            CloneCheckedOutcome::Handled(response) => payload_or_typed_error(response),
            CloneCheckedOutcome::Proceed(checked) => {
                dispatch_authorized(state, checked, collection, timeout, linearizable).await
            }
        };
    if result.is_ok() {
        // Metered only on the success path above — a denied/errored/timed-out
        // request performed no billable work, while a clone hook that served
        // the request did the work this dispatch was charged for and is metered
        // like any other success. Rebuilt from `state`/`identity`/`database_id`
        // rather than threaded out of `authorize_for_identity`, since that
        // function's scope is local to its own clone-check-and-authorize step;
        // this is the same derivation `resolve_dispatch_scope` already gives
        // every other caller in this file, so it cannot disagree with it.
        //
        // `rows: None` — `dispatch_authorized` returns a raw MessagePack
        // payload, and decoding it here solely to count rows will add real
        // per-request cost on this fan-in path for every one of the ~200
        // handlers that go through this door. `meter_dispatch` charges one
        // unit for `None`, which is correct for the lookup/mutation that
        // happened.
        if let Some(info) = &plan_metering_info {
            let metering_scope = resolve_dispatch_scope(state, identity, database_id, admission);
            meter_dispatch(state, &metering_scope, info, None);
        }
    }
    result
}

/// Resolve the request-scoped auth contract for `identity` against
/// `database_id`, apply row-level security, and authorize the resulting
/// `PhysicalTask`.
///
/// Split out from [`dispatch_for_identity`] so this pre-dispatch step — the
/// part that must never let the task's database and `$auth.database_id`
/// diverge — is directly unit-testable without spinning up the Data Plane
/// dispatch machinery.
///
/// `database_id` flows through [`RequestAuthScope::builder`] as the session
/// database, so `PhysicalTask::database_id` and `$auth.database_id` never
/// resolve separately from `identity`. `scope.database_id()`
/// is what lands on the task, so the two cannot disagree.
///
/// The trailing step is [`intercept_and_authorize`], not a bare
/// `authorize_task_set`: this door hands its task straight to
/// [`dispatch_authorized`], which accepts only a clone-checked capability, so
/// the ~200 handlers behind it inherit clone-write copy-up and clone-read merge
/// without a call-site edit. Order is load-bearing and unchanged ahead of it —
/// admission gate, RLS injection, redaction refusal — so a request is shed, or
/// refused, before any clone work is spent on it.
async fn authorize_for_identity(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
    admission: RequestAdmission<'_>,
) -> crate::Result<CloneCheckedOutcome> {
    let mut plan = plan;

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — before RLS injection and task authorization,
    // so load is shed before it is spent. Skipped when the caller's own
    // transport entry already ran this gate for the request — see
    // [`RequestAdmission`] for why both cases exist.
    let scope = match admission {
        RequestAdmission::AlreadyAdmitted => {
            resolve_dispatch_scope(state, identity, database_id, admission)
        }
        RequestAdmission::NotYetAdmitted { peer_addr } => {
            let request = RequestAuthScope::builder(identity, state.auth_stores())
                .with_session_database(Some(database_id))
                .build_for_client(peer_addr);
            crate::control::server::session_auth::check_request_admission(
                state,
                &request,
                operation_for_plan(&plan),
            )?;
            request.into_resolved_scope()
        }
    };

    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        identity.tenant_id.as_u64(),
        scope.database_id(),
        &mut plan,
        &state.rls,
        state.credentials.catalog(),
        scope.auth(),
    )?;
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        identity.tenant_id,
        scope.database_id(),
        scope.auth(),
        &state.redaction,
    )?;

    let task = PhysicalTask {
        tenant_id: identity.tenant_id,
        vshard_id: nodedb_types::CollectionKey::from_bare(scope.database_id(), collection).vshard(),
        database_id: scope.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task,
        identity,
        tenant_id: identity.tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await
}

/// Resolve the request-scoped auth contract for a user-issued dispatch.
///
/// The single source both [`PhysicalTask::database_id`] (via
/// [`RequestAuthScope::database_id`]) and `$auth.database_id` (via
/// [`RequestAuthScope::auth`]) are read from — split out from
/// [`authorize_for_identity`] so that guarantee is directly unit-testable.
/// Reads the auth stores off `state`.
///
/// A `NotYetAdmitted` request carries its real remote address, so the scope is
/// resolved against it and `$auth.risk_score` plus any IP-conditional grant
/// are live on this path too. An `AlreadyAdmitted` request was admitted at its
/// own transport entry and reaches this fan-in door with no address in hand;
/// its scope is resolved without one rather than against a placeholder that
/// will be scored as if it were a real client.
fn resolve_dispatch_scope<'a>(
    state: &'a SharedState,
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
    admission: RequestAdmission<'_>,
) -> RequestAuthScope<'a> {
    let builder = RequestAuthScope::builder(identity, state.auth_stores())
        .with_session_database(Some(database_id));
    match admission {
        RequestAdmission::AlreadyAdmitted => builder.build(),
        RequestAdmission::NotYetAdmitted { peer_addr } => {
            builder.build_for_client(peer_addr).into_resolved_scope()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
    use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::state::SharedState;
    use crate::types::{Lsn, TenantId};
    use crate::wal::WalManager;
    use nodedb_physical::physical_plan::KvOp;

    use super::*;

    fn trivial_kv_get_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "widgets"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// An identity whose session default database differs from the database
    /// the caller passed in for this dispatch. An RLS policy comparing
    /// `database_id = $auth.database_id` must see the passed-in database.
    /// This test fails if `resolve_dispatch_scope` resolves `$auth.database_id`
    /// from `identity.default_database` instead of the passed-in
    /// `database_id`. It asserts both
    /// `scope.database_id()` (what lands on the task) and
    /// `scope.auth().database_id` (what RLS substitutes for `$auth.*`)
    /// equal the passed-in database, not the identity's default.
    #[test]
    fn scope_database_and_auth_database_track_passed_in_database_not_identity_default() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity_default = DatabaseId::new(7);
        let dispatch_target = DatabaseId::new(99);
        assert_ne!(identity_default, dispatch_target);

        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        identity.default_database = Some(identity_default);

        let scope = resolve_dispatch_scope(
            &state,
            &identity,
            dispatch_target,
            RequestAdmission::NotYetAdmitted {
                peer_addr: "127.0.0.1:5432",
            },
        );

        assert_eq!(scope.database_id(), dispatch_target);
        assert_eq!(scope.auth().database_id, Some(dispatch_target));
    }

    /// End-to-end sanity check that the resolved scope's database is what
    /// actually lands on the authorized `PhysicalTask`, using the same
    /// mismatched-identity setup as the test above.
    ///
    /// Neither database here names a clone, so the gate must hand back the
    /// capability rather than serving the request itself — asserted, because a
    /// `Handled` outcome will carry no task for the database claim to be made
    /// about at all.
    #[tokio::test]
    async fn authorized_task_database_matches_passed_in_database_not_identity_default() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity_default = DatabaseId::new(7);
        let dispatch_target = DatabaseId::new(99);

        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        identity.default_database = Some(identity_default);

        let plan = trivial_kv_get_plan();

        let outcome = authorize_for_identity(
            &state,
            &identity,
            dispatch_target,
            "widgets",
            plan,
            RequestAdmission::NotYetAdmitted {
                peer_addr: "127.0.0.1:9",
            },
        )
        .await
        .expect("authorize task for identity");

        let CloneCheckedOutcome::Proceed(checked) = outcome else {
            panic!("a non-clone collection must not be served by a clone hook");
        };
        assert_eq!(checked.database_id(), dispatch_target);
    }

    /// A caller that already ran the transport's own admission gate is not
    /// charged against the rate-limit budget a second time here.
    /// `check_request_admission` has no test-visible counter, so this test
    /// pins the observable contract: `AlreadyAdmitted` runs no blacklist
    /// check, so a blacklisted identity stays authorized when the caller
    /// asserts it already admitted the request.
    #[tokio::test]
    async fn already_admitted_skips_the_gate_even_for_a_blacklisted_identity() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let identity = AuthenticatedIdentity::new_regular(
            2,
            "blocked-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        // `NotYetAdmitted` still enforces the gate: the blacklist rejects it.
        let denied = authorize_for_identity(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "widgets",
            trivial_kv_get_plan(),
            RequestAdmission::NotYetAdmitted {
                peer_addr: "127.0.0.1:9",
            },
        )
        .await;
        assert!(
            denied.is_err(),
            "NotYetAdmitted must still run the full gate and reject a blacklisted identity"
        );

        // `AlreadyAdmitted` skips it: the same blacklisted identity is
        // authorized, because the caller's own transport entry already
        // admitted (or rejected) this request.
        let allowed = authorize_for_identity(
            &state,
            &identity,
            DatabaseId::DEFAULT,
            "widgets",
            trivial_kv_get_plan(),
            RequestAdmission::AlreadyAdmitted,
        )
        .await;
        assert!(
            allowed.is_ok(),
            "AlreadyAdmitted must skip the gate so an already-admitted request is not \
             double-charged or re-evaluated"
        );
    }

    /// Returns state plus the fake Data-Plane data-side and the backing
    /// `TempDir` guard — the caller must keep the guard alive for as long as
    /// `state` is in use.
    fn metering_fixture() -> (Arc<SharedState>, CoreChannelDataSide, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, mut sides) = Dispatcher::new(1, 64);
        let side = sides.pop().expect("one data side");
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, side, dir)
    }

    /// `metering_config` has no live-mutation path by design — reach in via
    /// `Arc::get_mut` while the test is still the sole owner of the freshly
    /// constructed state, before any clone escapes (e.g. into a spawned
    /// responder task). Same pattern as `metering::tests::enable_metering`.
    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    /// Fake Data-Plane responder: pops the one dispatched request off `side`
    /// and answers it `Ok` with an empty payload, so `dispatch_for_identity`
    /// completes its round trip without a real Data-Plane core.
    async fn respond_ok_once(mut side: CoreChannelDataSide, state: Arc<SharedState>) {
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let mut handled = false;
        while !handled && Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: Response {
                            request_id: request.inner.request_id,
                            status: Status::Ok,
                            attempt: 1,
                            partial: false,
                            payload: Payload::empty(),
                            watermark_lsn: Lsn::ZERO,
                            error_code: None,
                            stage_vote: None,
                            read_versions: crate::types::ReadVersions::new(),
                            write_set: Vec::new(),
                        },
                    })
                    .expect("fake data-plane response queue has capacity");
                handled = true;
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
        assert!(handled, "fake data plane received the dispatched request");
        state.poll_and_route_responses();
    }

    fn regular_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "regular-user",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    /// A successful dispatch through this door records exactly one usage
    /// event, attributed to the dispatched plan's collection and engine.
    #[tokio::test]
    async fn successful_dispatch_records_one_event_with_collection_and_engine() {
        let (mut state, side, _dir) = metering_fixture();
        enable_metering(&mut state);
        let identity = regular_identity(1);

        let responder = tokio::spawn(respond_ok_once(side, Arc::clone(&state)));
        let result = dispatch_for_identity(DispatchRequest {
            state: &state,
            identity: &identity,
            database_id: DatabaseId::DEFAULT,
            collection: "widgets",
            plan: trivial_kv_get_plan(),
            timeout: Duration::from_secs(5),
            admission: RequestAdmission::AlreadyAdmitted,
            linearizable: false,
        })
        .await;
        responder.await.expect("responder completes");

        assert!(result.is_ok(), "dispatch through the door must succeed");
        let events = state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "exactly one usage event per dispatched task"
        );
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    /// A denied dispatch — rejected before it ever reaches the Data Plane —
    /// performed no billable work and must record nothing.
    #[tokio::test]
    async fn denied_dispatch_records_nothing() {
        let (mut state, _side, _dir) = metering_fixture();
        enable_metering(&mut state);
        let identity = regular_identity(2);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let result = dispatch_for_identity(DispatchRequest {
            state: &state,
            identity: &identity,
            database_id: DatabaseId::DEFAULT,
            collection: "widgets",
            plan: trivial_kv_get_plan(),
            timeout: Duration::from_secs(5),
            admission: RequestAdmission::NotYetAdmitted {
                peer_addr: "127.0.0.1:9",
            },
            linearizable: false,
        })
        .await;

        assert!(result.is_err(), "a blacklisted identity must be denied");
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// An internal-service identity's dispatch succeeds but is never metered
    /// — billing a tenant for server-owned work will be wrong.
    #[tokio::test]
    async fn internal_service_identity_records_nothing_on_success() {
        let (mut state, side, _dir) = metering_fixture();
        enable_metering(&mut state);
        let identity = AuthenticatedIdentity::new_internal_service(
            3,
            "internal-service",
            TenantId::new(1),
            Vec::new(),
            true,
            None,
            AuthenticatedIdentity::default_database_set(true),
        );

        let responder = tokio::spawn(respond_ok_once(side, Arc::clone(&state)));
        let result = dispatch_for_identity(DispatchRequest {
            state: &state,
            identity: &identity,
            database_id: DatabaseId::DEFAULT,
            collection: "widgets",
            plan: trivial_kv_get_plan(),
            timeout: Duration::from_secs(5),
            admission: RequestAdmission::AlreadyAdmitted,
            linearizable: false,
        })
        .await;
        responder.await.expect("responder completes");

        assert!(
            result.is_ok(),
            "internal-service dispatch must still succeed"
        );
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// Metering disabled (the default) records nothing on a successful
    /// dispatch — proves this change is inert for every existing caller that
    /// never enables `metering_config`.
    #[tokio::test]
    async fn metering_disabled_by_default_records_nothing_on_success() {
        let (state, side, _dir) = metering_fixture();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = regular_identity(4);

        let responder = tokio::spawn(respond_ok_once(side, Arc::clone(&state)));
        let result = dispatch_for_identity(DispatchRequest {
            state: &state,
            identity: &identity,
            database_id: DatabaseId::DEFAULT,
            collection: "widgets",
            plan: trivial_kv_get_plan(),
            timeout: Duration::from_secs(5),
            admission: RequestAdmission::AlreadyAdmitted,
            linearizable: false,
        })
        .await;
        responder.await.expect("responder completes");

        assert!(
            result.is_ok(),
            "dispatch must still succeed with metering disabled"
        );
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }
}
