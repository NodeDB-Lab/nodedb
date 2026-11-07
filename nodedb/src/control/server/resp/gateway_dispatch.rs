// SPDX-License-Identifier: BUSL-1.1

//! RESP gateway dispatch helpers.
//!
//! Routes KV operations through `Gateway::execute_response`, which sends each
//! one to the node that owns its vShard.
//!
//! All helpers return `crate::Result<Response>` so the existing sub-handler
//! code (`handler_kv`, `handler_hash`, `handler_sorted`) is unchanged.

use std::sync::Arc;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::local_dispatch::reject_data_plane_error;
use crate::control::mirror::source::sync::await_database_sync;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::server::shared::clone_write::CloneCheckedOutcome;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::session::RespSession;

/// Dispatch a read-only KV operation through the gateway.
///
/// A full dispatch queue reaches the Redis client as `-BUSY`
/// (`GatewayErrorMap::to_resp`), which Redis clients retry.
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
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &session.collection).vshard();
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
    let metering_scope = match &plan_metering_info {
        Some(info) => admit_resp_quota(state, session, database_id, info)?,
        None => None,
    };
    let checked =
        intercept_and_authorize_resp_task(state, session, plan, vshard, database_id, "kv_get")
            .await?;
    let checked = match checked {
        CloneCheckedOutcome::Handled(resp) => return Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    let result = execute_through_gateway(state, session, checked).await;
    if result.is_ok()
        && let (Some(info), Some(scope)) = (&plan_metering_info, &metering_scope)
    {
        meter_resp_dispatch(state, scope, info);
    }
    result
}

/// Dispatch a KV write operation through the gateway.
///
/// The gateway routes the write to the node that owns its vShard and owns WAL
/// durability there.
pub(super) async fn dispatch_kv_write(
    state: &SharedState,
    session: &RespSession,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    // See `dispatch_kv` above: RESP carries no database selector, so
    // DatabaseId::DEFAULT is deliberate here, resolved once and threaded
    // through `authorize_resp_task` via `RequestAuthScope::builder`.
    let database_id = DatabaseId::DEFAULT;
    let vshard = nodedb_types::CollectionKey::from_bare(database_id, &session.collection).vshard();
    // See `dispatch_kv` above: extracted before `authorize_resp_task` moves
    // `plan`, since metering needs the plan shape after dispatch succeeds.
    let plan_metering_info = state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));
    let metering_scope = match &plan_metering_info {
        Some(info) => admit_resp_quota(state, session, database_id, info)?,
        None => None,
    };
    // Clone CoW write-path interception, then authorization, run once per
    // task before dispatch — same protocol-neutral gate pgwire and native run.
    let checked =
        intercept_and_authorize_resp_task(state, session, plan, vshard, database_id, "kv_put")
            .await?;
    let checked = match checked {
        CloneCheckedOutcome::Handled(resp) => return Ok(resp),
        CloneCheckedOutcome::Proceed(checked) => checked,
    };
    let result = execute_through_gateway(state, session, checked).await;
    if result.is_ok()
        && let (Some(info), Some(scope)) = (&plan_metering_info, &metering_scope)
    {
        meter_resp_dispatch(state, scope, info);
    }
    let response = result?;
    // A write the Data Plane refused waits for nothing. Its caller reads the
    // refusal from the response.
    if reject_data_plane_error(&response).is_ok() {
        await_database_sync(state, database_id).await?;
    }
    Ok(response)
}

/// Run one checked RESP task through the gateway.
///
/// A gateway error reaches the client as `Error::Bridge` carrying its RESP
/// rendering.
async fn execute_through_gateway(
    state: &SharedState,
    session: &RespSession,
    checked: crate::control::server::shared::clone_write::CloneCheckedTask,
) -> crate::Result<Response> {
    let gateway = state.installed_gateway()?;
    let gw_ctx = QueryContext {
        tenant_id: session.tenant_id,
        trace_id: TraceId::generate(),
        database_id: checked.database_id(),
        txn_id: None,
        linearizable: true,
    };
    gateway
        .execute_response(&gw_ctx, checked)
        .await
        .map_err(|e| crate::Error::Bridge {
            detail: GatewayErrorMap::to_resp(&e),
        })
}

/// Refuse the command when a covering scope's hard quota is already spent.
///
/// The sibling of [`meter_resp_dispatch`], run before dispatch rather than
/// after it: charging happens on the success path by design and so can never
/// be where a cap blocks anything.
///
/// Returns the scope it admitted against, so the metering charge after
/// dispatch reuses it and a scope error refuses the command before any work
/// happens. Returns `None` for a session with no identity.
fn admit_resp_quota<'a>(
    state: &'a SharedState,
    session: &'a RespSession,
    database_id: DatabaseId,
    info: &PlanMeteringInfo,
) -> crate::Result<Option<ClientRequestScope<'a, 'a>>> {
    let Some(identity) = session.identity.as_ref() else {
        return Ok(None);
    };
    let scope = resp_auth_scope(
        identity,
        state.auth_stores(),
        database_id,
        &session.peer_addr,
    )?;
    admit_quota_for_dispatch(state, scope.scope(), info)?;
    Ok(Some(scope))
}

/// Meter one completed RESP KV dispatch, once dispatch above has already
/// returned success.
///
/// Charges against the scope [`admit_resp_quota`] resolved before dispatch,
/// which is the same `resp_auth_scope` derivation `authorize_resp_task` uses,
/// so it cannot disagree with the scope that authorized the request.
///
/// RESP ops are single-key, so the row count is known structurally without
/// decoding the dispatch payload: `Some(1)` for both a hit and a miss — a
/// miss still performed the lookup, and `meter_dispatch` charges at least
/// one unit regardless, so this keeps that contract explicit rather than
/// relying on the `rows: None` fallback to do it implicitly.
fn meter_resp_dispatch(
    state: &SharedState,
    scope: &ClientRequestScope<'_, '_>,
    info: &PlanMeteringInfo,
) {
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
    )?;

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
    //
    // A read of a redacted column outside the projection is refused first,
    // before RLS adds the read policy's own predicates.
    crate::control::planner::redaction_refusal::refuse_redacted_expression_reads(
        &plan,
        session.tenant_id,
        database_id,
        &crate::control::planner::redaction_refusal::RedactionReadCheck {
            auth: scope.auth(),
            store: &state.redaction,
            catalog: state.credentials.catalog(),
        },
    )?;
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        session.tenant_id.as_u64(),
        database_id,
        &mut plan,
        &state.rls,
        state.credentials.catalog(),
        scope.auth(),
    )?;

    // Reads whose results column redaction cannot rewrite (an aggregate over a
    // redacted column, a graph traversal) are refused on the same seam, so the
    // capability is never minted for a plan that will leak them.
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
) -> crate::Result<ClientRequestScope<'a, 'p>> {
    ClientRequestScope::for_database(identity, stores, database_id, peer_addr)
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
        )
        .expect("resolve RESP scope");

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
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        let vshard =
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &session.collection)
                .vshard();

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

    /// A RESP write to a database with a sync mirror answers the typed timeout
    /// once the link confirms nothing. The write stays committed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kv_write_waits_for_a_sync_mirror() {
        use nodedb_physical::physical_plan::KvOp;
        use nodedb_types::{MirrorMode, MirrorPin, MirrorPinState};

        let cluster = crate::control::cluster::test_one_node::boot_with(|state| {
            state.tuning.mirror.sync_timeout_ms = 200;
        })
        .await;
        cluster.state.mirror.pins.apply(&MirrorPin {
            link_id: 1,
            client: "resp-test".to_string(),
            database: DatabaseId::DEFAULT.as_u64(),
            positions: Vec::new(),
            acked_ms: 0,
            state: MirrorPinState::Live,
            mode: MirrorMode::Sync,
            acked_ordinal: 1,
        });
        let session = resp_session_with_identity(regular_identity(4));
        let plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                DatabaseId::DEFAULT,
                &session.collection,
            ),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        });

        let error = dispatch_kv_write(&cluster.state, &session, plan)
            .await
            .expect_err("a stopped sync mirror confirms nothing");

        assert!(
            matches!(error, crate::Error::MirrorSyncTimeout { .. }),
            "{error:?}"
        );
        cluster.shutdown().await;
    }

    /// A booted one-node cluster with metering on. `metering_config` has no
    /// live-mutation path, so it is set before the state is shared.
    async fn metered_cluster() -> crate::control::cluster::test_one_node::OneNodeCluster {
        crate::control::cluster::test_one_node::boot_with(|state| {
            state.metering_config.enabled = true;
        })
        .await
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
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    /// A successful RESP KV dispatch records exactly one usage event,
    /// attributed to the RESP session's selected collection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_kv_dispatch_records_one_event_for_session_collection() {
        let cluster = metered_cluster().await;
        let session = resp_session_with_identity(regular_identity(1));
        let plan = kv_get_plan(&session.collection);

        let result = dispatch_kv(&cluster.state, &session, plan).await;

        assert!(result.is_ok(), "RESP KV dispatch must succeed: {result:?}");
        let events = cluster.state.usage_counter.drain();
        assert_eq!(
            events.len(),
            1,
            "exactly one usage event per dispatched task"
        );
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
        cluster.shutdown().await;
    }

    /// A denied RESP dispatch — rejected before reaching the Data Plane —
    /// performed no billable work and must record nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn denied_kv_dispatch_records_nothing() {
        let cluster = metered_cluster().await;
        cluster
            .state
            .blacklist
            .blacklist_ip("10.0.0.0/8", "test ip ban", "admin", 0)
            .expect("blacklist CIDR range");
        let mut session = resp_session_with_identity(regular_identity(2));
        session.peer_addr = "10.1.2.3:54321".into();
        let plan = kv_get_plan(&session.collection);

        let result = dispatch_kv(&cluster.state, &session, plan).await;

        assert!(result.is_err(), "a blacklisted peer must be denied");
        assert_eq!(cluster.state.usage_counter.total_tokens(), 0);
        cluster.shutdown().await;
    }

    /// Metering disabled (the default) records nothing on a successful RESP
    /// dispatch — proves this change is inert for the existing RESP suite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metering_disabled_by_default_records_nothing_on_resp_success() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        assert!(
            !cluster.state.metering_config.enabled,
            "default config is disabled"
        );
        let session = resp_session_with_identity(regular_identity(3));
        let plan = kv_get_plan(&session.collection);

        let result = dispatch_kv(&cluster.state, &session, plan).await;

        assert!(
            result.is_ok(),
            "dispatch must still succeed with metering disabled: {result:?}"
        );
        assert_eq!(cluster.state.usage_counter.total_tokens(), 0);
        cluster.shutdown().await;
    }
}
