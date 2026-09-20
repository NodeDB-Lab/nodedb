// SPDX-License-Identifier: BUSL-1.1

//! RESP gateway dispatch helpers.
//!
//! Routes KV operations through `Gateway::execute` when the gateway is
//! available (cluster-aware routing), falling back to direct local SPSC
//! dispatch on single-node boot.
//!
//! All helpers return `crate::Result<Response>` so the existing sub-handler
//! code (`handler_kv`, `handler_hash`, `handler_sorted`) is unchanged.

use std::sync::Arc;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::dispatch_utils;
use crate::control::server::shared::clone_write::CloneCheckedOutcome;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::session::RespSession;

/// Dispatch a read-only KV operation.
///
/// Routes through the gateway when available (cluster-aware routing), falling
/// back to direct local SPSC dispatch on single-node boot.
///
/// Bridge/dispatch errors are mapped to `Error::Bridge` with a `BUSY` detail
/// so the RESP handler can return `-BUSY` to the Redis client.
pub(super) async fn dispatch_kv(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    // RESP protocol carries no database selector; all ops deliberately target
    // DatabaseId::DEFAULT. `database_id` is the single literal for that
    // decision — `authorize_resp_task` threads it through
    // `RequestAuthScope::builder` so the dispatched task and `$auth.database_id`
    // resolve from the same value and cannot drift apart.
    let database_id = DatabaseId::DEFAULT;
    let vshard = VShardId::from_collection_in_database(database_id, &session.collection);
    // Extracted before `plan` is moved into `authorize_resp_task`, which
    // consumes it for RLS injection and task construction — metering needs
    // the collection/engine shape after dispatch succeeds below, and by then
    // the original plan is gone. Only the narrow metering shape is captured
    // (see `PlanMeteringInfo`), not a full `plan.clone()`, and only when
    // metering is enabled — the default is disabled, so this is a no-op on
    // the hot RESP path for every deployment that hasn't turned it on.
    let plan_metering_info = state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));
    if let Some(info) = &plan_metering_info {
        admit_resp_quota(state, session, database_id, info)?;
    }
    let checked =
        intercept_and_authorize_resp_task(state, session, plan, vshard, database_id, "kv_get")
            .await?;
    let checked = match checked {
        CloneCheckedOutcome::Handled(resp) => return Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    let result = match state.gateway.get() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: TraceId::generate(),
                database_id: checked.database_id(),
                txn_id: None,
            };
            gw.execute(&gw_ctx, checked)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => dispatch_utils::dispatch_authorized_to_data_plane(state, checked, TraceId::ZERO)
            .await
            .map_err(map_busy_error),
    };
    if result.is_ok()
        && let Some(info) = &plan_metering_info
    {
        meter_resp_dispatch(state, session, database_id, info);
    }
    result
}

/// Dispatch a KV write operation through the gateway or the local Data Plane.
///
/// Routes through the gateway when available (cluster-aware routing) — where the
/// gateway owns WAL durability on the target node — falling back to direct local
/// SPSC dispatch on single-node boot. On the local path the WAL append is
/// performed inside the dispatch core, under the write-admission guard and just
/// before the enqueue, so LSN order matches apply order.
pub(super) async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    // See `dispatch_kv` above: RESP carries no database selector, so
    // DatabaseId::DEFAULT is deliberate here, resolved once and threaded
    // through `authorize_resp_task` via `RequestAuthScope::builder`.
    let database_id = DatabaseId::DEFAULT;
    let vshard = VShardId::from_collection_in_database(database_id, &session.collection);
    // See `dispatch_kv` above: extracted before `authorize_resp_task` moves
    // `plan`, since metering needs the plan shape after dispatch succeeds.
    let plan_metering_info = state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));
    if let Some(info) = &plan_metering_info {
        admit_resp_quota(state, session, database_id, info)?;
    }
    // Clone CoW write-path interception, then authorization, run once per
    // task before dispatch — same protocol-neutral gate pgwire and native run.
    let checked =
        intercept_and_authorize_resp_task(state, session, plan, vshard, database_id, "kv_put")
            .await?;
    let checked = match checked {
        CloneCheckedOutcome::Handled(resp) => return Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    let result = match state.gateway.get() {
        Some(gw) => {
            let gw_ctx = QueryContext {
                tenant_id: session.tenant_id,
                trace_id: TraceId::generate(),
                database_id: checked.database_id(),
                txn_id: None,
            };
            gw.execute(&gw_ctx, checked)
                .await
                .map_err(|e| crate::Error::Bridge {
                    detail: GatewayErrorMap::to_resp(&e),
                })
                .map(gateway_payloads_to_response)
        }
        None => dispatch_utils::dispatch_authorized_autocommit_write(state, checked, TraceId::ZERO)
            .await
            .map_err(map_busy_error),
    };
    if result.is_ok()
        && let Some(info) = &plan_metering_info
    {
        meter_resp_dispatch(state, session, database_id, info);
    }
    result
}

/// Refuse the command when a covering scope's hard quota is already spent.
///
/// The sibling of [`meter_resp_dispatch`], run before dispatch rather than
/// after it: charging happens on the success path by design and so can never
/// be where a cap blocks anything.
fn admit_resp_quota(
    state: &SharedState,
    session: &RespSession,
    database_id: DatabaseId,
    info: &PlanMeteringInfo,
) -> crate::Result<()> {
    let Some(identity) = session.identity.as_ref() else {
        return Ok(());
    };
    let scope = resp_auth_scope(
        identity,
        state.auth_stores(),
        database_id,
        &session.peer_addr,
    );
    admit_quota_for_dispatch(state, scope.scope(), info)
}

/// Meter one completed RESP KV dispatch, once dispatch above has already
/// returned success.
///
/// Recomputes the [`RequestAuthScope`] from `session.identity` rather than
/// threading it out of `authorize_resp_task` — that keeps this a pure
/// after-the-fact accounting step with no new state flowing through the
/// dispatch path, and it is the same derivation `resp_auth_scope` already
/// gives every caller in this file, so it cannot disagree with the scope
/// `authorize_resp_task` used to authorize the request. `session.identity`
/// is guaranteed `Some` here: `authorize_resp_task` already returned `Ok`
/// on this call path, and it fails closed on a missing identity before this
/// point is ever reached.
///
/// RESP ops are single-key, so the row count is known structurally without
/// decoding the dispatch payload: `Some(1)` for both a hit and a miss — a
/// miss still performed the lookup, and `meter_dispatch` charges at least
/// one unit regardless, so this keeps that contract explicit rather than
/// relying on the `rows: None` fallback to do it implicitly.
fn meter_resp_dispatch(
    state: &SharedState,
    session: &RespSession,
    database_id: DatabaseId,
    info: &PlanMeteringInfo,
) {
    let Some(identity) = session.identity.as_ref() else {
        return;
    };
    let scope = resp_auth_scope(
        identity,
        state.auth_stores(),
        database_id,
        &session.peer_addr,
    );
    meter_dispatch(state, scope.scope(), info, Some(1));
}

fn authorize_resp_task(
    state: &SharedState,
    session: &RespSession,
    mut plan: PhysicalPlan,
    vshard_id: VShardId,
    database_id: DatabaseId,
    operation: &str,
) -> crate::Result<PhysicalTask> {
    let identity = session
        .identity
        .as_ref()
        .ok_or_else(|| crate::Error::RejectedAuthz {
            tenant_id: session.tenant_id,
            resource: "RESP AUTH required before data access".into(),
        })?;

    let request = resp_auth_scope(
        identity,
        state.auth_stores(),
        database_id,
        &session.peer_addr,
    );

    // Request-admission gate: internal-service exemption, blacklist, account
    // status, then rate limit — before RLS injection and task authorization,
    // so load is shed before it is spent. Per this function's own doc below,
    // every RESP command reaches the Data Plane through here, so this one
    // call covers the whole protocol, including the IP-blacklist half via
    // `session.peer_addr` (set at connection accept).
    crate::control::server::session_auth::check_request_admission(state, &request, operation)?;
    let scope = request.into_resolved_scope();

    // Row-level security is injected here, before the capability is minted, for
    // the same reason the native path injects before dispatch: the plan the
    // capability authorizes must be the plan the Data Plane executes. Every
    // RESP command reaches the Data Plane through this function, so this is the
    // whole protocol's RLS enforcement point.
    //
    // Operations that cannot carry a filter (`BatchGet`, `FieldGet`) fail
    // closed here with a typed error rather than executing unfiltered.
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        session.tenant_id.as_u64(),
        database_id,
        &mut plan,
        &state.rls,
        scope.auth(),
    )?;

    // Reads whose results column redaction cannot rewrite (an aggregate over a
    // redacted column, a graph traversal) are refused on the same seam, so the
    // capability is never minted for a plan that would leak them.
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        session.tenant_id,
        database_id,
        scope.auth(),
        &state.redaction,
    )?;

    Ok(PhysicalTask {
        tenant_id: session.tenant_id,
        vshard_id,
        database_id: scope.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    })
}

/// Build the RESP task, then clone-check and authorize it — the single entry
/// point through which every RESP command reaches a `CloneCheckedOutcome`.
async fn intercept_and_authorize_resp_task(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
    vshard_id: VShardId,
    database_id: DatabaseId,
    operation: &str,
) -> crate::Result<CloneCheckedOutcome> {
    let task = authorize_resp_task(state, session, plan, vshard_id, database_id, operation)?;
    // Checked above: `authorize_resp_task` already fails closed when absent.
    let identity = session
        .identity
        .as_ref()
        .ok_or_else(|| crate::Error::RejectedAuthz {
            tenant_id: session.tenant_id,
            resource: "RESP AUTH required before data access".into(),
        })?;
    let tenant_id = session.tenant_id;
    let emitter = crate::control::security::audit::ArcAuditEmitter(Arc::clone(&state.audit));
    crate::control::server::shared::clone_write::intercept_and_authorize(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state,
            task,
            identity,
            tenant_id,
            permissions: &state.permissions,
            roles: &state.roles,
            emitter: &emitter,
        },
    )
    .await
}

/// Resolve the request-scoped auth contract for a RESP identity.
///
/// RESP carries no session/database selector, so `database_id` is always
/// `DatabaseId::DEFAULT` at every current call site (see `dispatch_kv` /
/// `dispatch_kv_write`) — deliberate, not a fall-through. It is threaded
/// through the scope builder as the session database rather than
/// resolving `$auth.database_id` from `identity` separately, so
/// `scope.database_id()` (used for `PhysicalTask::database_id`) and
/// `scope.auth().database_id` (used for RLS substitution) cannot disagree —
/// split out from `authorize_resp_task` so that guarantee is directly
/// unit-testable.
///
/// `peer_addr` is the connection's accept-time remote address; it reaches
/// the risk scorer so `$auth.risk_score` is stamped for RESP commands too.
fn resp_auth_scope<'a, 'p>(
    identity: &'a AuthenticatedIdentity,
    stores: crate::control::security::request_scope::AuthStores<'a>,
    database_id: DatabaseId,
    peer_addr: &'p str,
) -> ClientRequestScope<'a, 'p> {
    ClientRequestScope::for_database(identity, stores, database_id, peer_addr)
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
///
/// The RESP sub-handlers inspect `resp.status` and `resp.payload`; we
/// synthesise a `Status::Ok` response carrying the first payload so that all
/// existing sub-handler logic continues to work without modification.
fn gateway_payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
    let payload = payloads
        .into_iter()
        .next()
        .map(Payload::from_vec)
        .unwrap_or_else(Payload::empty);
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 0,
        partial: false,
        payload,
        watermark_lsn: Lsn::new(0),
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Map bridge/dispatch errors to a BUSY error for Redis client compatibility.
///
/// When the SPSC ring buffer is full or the Data Plane core is overloaded,
/// the Redis client receives `-BUSY NodeDB is processing requests, retry later`
/// which Redis clients handle with automatic retry (same as Redis Cluster BUSY).
fn map_busy_error(e: crate::Error) -> crate::Error {
    match &e {
        crate::Error::Bridge { .. }
        | crate::Error::Dispatch { .. }
        | crate::Error::DispatchCapacityBusy { .. } => crate::Error::Bridge {
            detail: "BUSY NodeDB is processing requests, retry later".into(),
        },
        _ => e,
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::identity::{AuthMethod, DatabaseSet, Role};
    use crate::control::security::metering::quota::QuotaManager;
    use crate::control::security::request_scope::AuthStores;
    use crate::control::security::scope::grant::ScopeGrantStore;
    use crate::types::TenantId;

    use super::*;

    /// RESP deliberately pins every dispatch to `DatabaseId::DEFAULT` (the
    /// protocol has no session/database selector). `$auth.database_id` must
    /// be pinned to that same `DatabaseId::DEFAULT`, not resolved from
    /// `build_auth_context(identity)`, which stamps `identity.default_database`
    /// — resolving it that way gives a user whose default database is not
    /// DEFAULT a task/RLS database mismatch. This test uses an
    /// identity whose `default_database` is deliberately NOT
    /// `DatabaseId::DEFAULT` and asserts both halves of the resolved scope
    /// still land on DEFAULT and agree with each other. It fails if
    /// `resp_auth_scope` (or its inlined equivalent) goes back to resolving
    /// `$auth.database_id` from `identity.default_database` instead of the
    /// pinned `database_id` argument.
    #[test]
    fn resp_scope_pins_default_database_regardless_of_identity_default() {
        let mut identity = AuthenticatedIdentity::new_regular(
            1,
            "resp-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        identity.default_database = Some(DatabaseId::new(42));
        assert_ne!(identity.default_database, Some(DatabaseId::DEFAULT));

        let grants = ScopeGrantStore::new();
        let quotas = QuotaManager::new();

        let scorer = crate::control::security::risk::RiskScorer::default();
        let scope = resp_auth_scope(
            &identity,
            AuthStores::new(&grants, &quotas, &scorer),
            DatabaseId::DEFAULT,
            "127.0.0.1:6379",
        );

        assert_eq!(scope.scope().database_id(), DatabaseId::DEFAULT);
        assert_eq!(scope.scope().auth().database_id, Some(DatabaseId::DEFAULT));
    }

    /// RESP threads `session.peer_addr` (set at connection accept, see
    /// `listener::handle_connection`) into `check_request_admission`, so a
    /// `BLACKLIST IP` entry that matches the connection's real remote
    /// address must reject the request — this is the regression that a
    /// hardcoded `""` peer address made silently inert.
    #[test]
    fn blacklisted_peer_ip_rejects_resp_dispatch() {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;
        use nodedb_physical::physical_plan::KvOp;

        let dir = tempfile::tempdir().expect("create test directory");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        state
            .blacklist
            .blacklist_ip("10.0.0.0/8", "test ip ban", "admin", 0)
            .expect("blacklist CIDR range");

        let identity = AuthenticatedIdentity::new_regular(
            1,
            "resp-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        );
        let mut session = RespSession {
            peer_addr: "10.1.2.3:54321".into(),
            ..RespSession::default()
        };
        session.identity = Some(identity);

        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                DatabaseId::DEFAULT,
                &session.collection,
            ),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        let vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, &session.collection);

        let result = authorize_resp_task(
            &state,
            &session,
            plan,
            vshard,
            DatabaseId::DEFAULT,
            "kv_get",
        );
        assert!(
            result.is_err(),
            "a RESP session whose real peer address falls inside a blacklisted CIDR range \
             must be rejected"
        );
    }

    /// Returns state plus the fake Data-Plane data-side and the backing
    /// `TempDir` guard — the caller must keep the guard alive for as long as
    /// `state` is in use.
    fn metering_fixture() -> (
        Arc<SharedState>,
        crate::bridge::dispatch::CoreChannelDataSide,
        tempfile::TempDir,
    ) {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

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
    /// constructed state, before any clone escapes into a spawned responder
    /// task. Same pattern as `metering::tests::enable_metering`.
    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    /// Fake Data-Plane responder: pops the one dispatched request off `side`
    /// and answers it `Ok` with an empty payload, so `dispatch_kv` /
    /// `dispatch_kv_write` complete their round trip without a real Data-Plane
    /// core (mirrors the responder in `dispatch_utils::dispatch::tests`).
    async fn respond_ok_once(
        mut side: crate::bridge::dispatch::CoreChannelDataSide,
        state: Arc<SharedState>,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut handled = false;
        while !handled && std::time::Instant::now() < deadline {
            if let Ok(request) = side.request_rx.try_pop() {
                side.response_tx
                    .try_push(crate::bridge::dispatch::BridgeResponse {
                        inner: Response {
                            request_id: request.inner.request_id,
                            status: Status::Ok,
                            attempt: 1,
                            partial: false,
                            payload: Payload::empty(),
                            watermark_lsn: Lsn::new(0),
                            error_code: None,
                            read_set_valid: None,
                            read_version_lsn: Lsn::ZERO,
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

    fn resp_session_with_identity(identity: AuthenticatedIdentity) -> RespSession {
        let mut session = RespSession {
            collection: "widgets".into(),
            ..RespSession::default()
        };
        session.identity = Some(identity);
        session
    }

    fn regular_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "resp-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    fn kv_get_plan(collection: &str) -> PhysicalPlan {
        use nodedb_physical::physical_plan::KvOp;
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// A successful RESP KV dispatch records exactly one usage event,
    /// attributed to the RESP session's selected collection.
    #[tokio::test]
    async fn successful_kv_dispatch_records_one_event_for_session_collection() {
        let (mut state, side, _dir) = metering_fixture();
        enable_metering(&mut state);
        let session = resp_session_with_identity(regular_identity(1));
        let plan = kv_get_plan(&session.collection);

        let responder = tokio::spawn(respond_ok_once(side, Arc::clone(&state)));
        let result = dispatch_kv(&state, &session, plan).await;
        responder.await.expect("responder completes");

        assert!(result.is_ok(), "RESP KV dispatch must succeed");
        let events = state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "exactly one usage event per dispatched task"
        );
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    /// A denied RESP dispatch — rejected before reaching the Data Plane —
    /// performed no billable work and must record nothing.
    #[tokio::test]
    async fn denied_kv_dispatch_records_nothing() {
        let (mut state, _side, _dir) = metering_fixture();
        enable_metering(&mut state);
        state
            .blacklist
            .blacklist_ip("10.0.0.0/8", "test ip ban", "admin", 0)
            .expect("blacklist CIDR range");
        let mut session = resp_session_with_identity(regular_identity(2));
        session.peer_addr = "10.1.2.3:54321".into();
        let plan = kv_get_plan(&session.collection);

        let result = dispatch_kv(&state, &session, plan).await;

        assert!(result.is_err(), "a blacklisted peer must be denied");
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// Metering disabled (the default) records nothing on a successful RESP
    /// dispatch — proves this change is inert for the existing RESP suite.
    #[tokio::test]
    async fn metering_disabled_by_default_records_nothing_on_resp_success() {
        let (state, side, _dir) = metering_fixture();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let session = resp_session_with_identity(regular_identity(3));
        let plan = kv_get_plan(&session.collection);

        let responder = tokio::spawn(respond_ok_once(side, Arc::clone(&state)));
        let result = dispatch_kv(&state, &session, plan).await;
        responder.await.expect("responder completes");

        assert!(
            result.is_ok(),
            "dispatch must still succeed with metering disabled"
        );
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }
}
