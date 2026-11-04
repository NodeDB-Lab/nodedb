// SPDX-License-Identifier: BUSL-1.1

//! [`check_request_admission`] — the single composed entry point for the
//! blacklist, account-status, and rate-limit guards.
//!
//! `guards.rs` defines the individual primitives; this module composes them
//! in the order every transport needs, so no call site can apply them out of
//! order or forget one.

use nodedb_types::DatabaseId;

use crate::control::security::ratelimit::limiter::RateLimitResult;
use crate::control::security::request_scope::ClientRequestScope;
use crate::control::state::SharedState;

use super::guards::{check_blacklist, check_rate_limit, check_risk};

/// Run the full request-admission gate: internal-service exemption,
/// blacklist, account status, then rate limit.
///
/// Returns `Ok(None)` when the request was exempt (server-owned work) and
/// nothing further was checked, or `Ok(Some(result))` with the rate-limit
/// outcome once every guard passed. HTTP uses the `Some` case to emit
/// `X-RateLimit-*` / `Retry-After` headers; other transports may discard it.
///
/// Order matters:
/// 1. `scope.identity().is_internal_service()` — server-owned work (triggers,
///    Raft apply, CRDT sync, scheduler, replay) must never be blacklisted or
///    rate-limited; doing so could stall replay. This is the cheapest check
///    and short-circuits everything else.
/// 2. [`check_blacklist`] — cheap, identity-shaped rejection before any
///    heavier work.
/// 3. [`AuthContext::check_status`](crate::control::security::auth_context::AuthContext::check_status)
///    — account status (`Suspended` / `Banned`). This is *not* redundant with
///    the blacklist's auth-user status check: the blacklist reads the
///    persistent `state.auth_users` store — where auto-escalation lands its
///    verdicts — whereas `AuthContext.status` carries the status the session
///    was built with, on a possibly-pooled context. Both must be checked.
/// 4. [`check_risk`] — the adaptive-auth risk decision for the score the
///    scope was built with. Off unless the operator enabled `[auth.risk]`.
///    It runs after the cheap identity-shaped rejections and before the rate
///    limiter, because a request that is about to be refused on risk should
///    not consume the caller's rate-limit budget.
/// 5. [`check_rate_limit`] — runs last, and before any planning/catalog work,
///    so load is shed before it is spent.
///
/// The request arrives as a [`ClientRequestScope`], never as a bare
/// [`RequestAuthScope`](crate::control::security::request_scope::RequestAuthScope)
/// plus a separate address argument. Passing those as two independent
/// parameters lets a transport resolve an address-less scope while
/// passing a real address here, looking correct at the call site but leaving
/// `$auth.risk_score` unstamped — refused as unassessed by step 4 the moment
/// risk scoring is enabled, and silently short of every `REQUIRE IP` grant.
/// One value means the address the blacklist parses is provably the address
/// the scope was scored and IP-matched against.
pub fn check_request_admission(
    state: &SharedState,
    request: &ClientRequestScope<'_, '_>,
    operation: &str,
) -> crate::Result<Option<RateLimitResult>> {
    let scope = request.scope();
    let peer_addr = request.peer_addr();
    if scope.identity().is_internal_service() {
        return Ok(None);
    }

    let database_id: DatabaseId = scope.database_id();
    super::admitted_database::note(database_id);
    check_blacklist(state, scope.identity(), peer_addr)?;
    scope.auth().check_status()?;
    check_risk(state, scope.identity(), scope.auth(), peer_addr)?;

    let result = check_rate_limit(
        state,
        scope.identity(),
        scope.auth(),
        operation,
        database_id,
    )?;

    state.database_metrics.record_qps(database_id);
    Ok(Some(result))
}

/// Run the blacklist + account-status guards without rate limiting.
///
/// For admission doors whose traffic is not the per-query traffic the
/// rate-limiter's cost table models — ILP/OTLP ingest, CRDT delta sync,
/// shape subscription/resync, and admin-scoped COPY backup/restore — but
/// which must still refuse a blacklisted or suspended/banned account.
/// Composes the same internal-service exemption, [`check_blacklist`],
/// [`AuthContext::check_status`](crate::control::security::auth_context::AuthContext::check_status),
/// and [`check_risk`] steps [`check_request_admission`] runs, minus
/// [`check_rate_limit`] — see that function's doc for why the order
/// (exemption, then blacklist, then status, then risk) matters. The risk
/// gate is not part of the rate-limiter's cost model, so it belongs on this
/// door too: a request refused by risk policy must be refused on every door,
/// not only the ones that also meter QPS.
///
/// Takes a [`ClientRequestScope`] for the reason [`check_request_admission`]
/// documents — and this door is where that mattered most, since every one of
/// its callers is a transport that builds its own scope by hand.
pub fn check_blacklist_and_status(
    state: &SharedState,
    request: &ClientRequestScope<'_, '_>,
) -> crate::Result<()> {
    let scope = request.scope();
    let peer_addr = request.peer_addr();
    if scope.identity().is_internal_service() {
        return Ok(());
    }

    super::admitted_database::note(scope.database_id());
    check_blacklist(state, scope.identity(), peer_addr)?;
    scope.auth().check_status()?;
    check_risk(state, scope.identity(), scope.auth(), peer_addr)?;

    state.database_metrics.record_qps(scope.database_id());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::auth_context::AuthStatus;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::security::request_scope::RequestAuthScope;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    use super::*;

    /// Returns the state plus the backing `TempDir` guard — the caller must
    /// keep the guard alive for as long as `state` is in use, or the WAL's
    /// backing file is removed out from under it.
    async fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
    }

    /// Same as [`test_state`] but with risk scoring built from `risk_config`
    /// instead of the disabled default.
    async fn test_state_with_risk(
        risk_config: crate::control::security::risk::RiskConfig,
    ) -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_risk_config(dispatcher, wal, risk_config)
            .expect("construct shared state");
        (state, dir)
    }

    fn regular_identity(user_id: u64, auth_method: AuthMethod) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            user_id,
            "regular-user",
            TenantId::new(1),
            auth_method,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    /// `new_internal_service` is crate-private; tests reach it exactly the
    /// way `authenticated.rs`'s own test module does.
    fn internal_service_identity(user_id: u64) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            user_id,
            "internal-service",
            TenantId::new(1),
            vec![],
            false,
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    #[tokio::test]
    async fn internal_service_identity_short_circuits_even_when_blacklisted() {
        let (state, _dir) = test_state().await;
        let identity = internal_service_identity(9001);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1",
        )
        .expect("resolve client request scope");

        let result = check_request_admission(&state, &request, "point_get")
            .expect("internal-service identity must never be blocked");
        assert!(
            result.is_none(),
            "internal-service identity must short-circuit with Ok(None)"
        );
    }

    #[tokio::test]
    async fn regular_identity_is_blocked_when_blacklisted() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9002, AuthMethod::ScramSha256);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1",
        )
        .expect("resolve client request scope");

        let result = check_request_admission(&state, &request, "point_get");
        assert!(
            result.is_err(),
            "blacklisted regular identity must be rejected"
        );
    }

    /// Security-critical: `AuthMethod::Trust` alone must never confer
    /// exemption. `trust_identity` / `configured_trust_identity` build real
    /// external identities with `AuthMethod::Trust` for servers running in
    /// trust-auth mode — if the wrapper exempted on `auth_method ==
    /// AuthMethod::Trust` instead of the dedicated `is_internal_service`
    /// flag, every trust-mode external client would silently bypass
    /// blacklist and rate-limit enforcement. A `new_regular` identity
    /// carrying `AuthMethod::Trust` is exactly that shape, built through the
    /// normal external-identity constructor (not `new_internal_service`), so
    /// `is_internal_service()` is `false` and the guards must still apply.
    #[tokio::test]
    async fn trust_auth_method_alone_does_not_exempt_from_guards() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9003, AuthMethod::Trust);
        assert!(!identity.is_internal_service());
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1",
        )
        .expect("resolve client request scope");

        let result = check_request_admission(&state, &request, "point_get");
        assert!(
            result.is_err(),
            "a trust-mode identity built via the normal external path must not be exempt"
        );
    }

    #[tokio::test]
    async fn suspended_account_is_rejected_at_status_check() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9004, AuthMethod::ScramSha256);

        // `RequestAuthScope` has no public `auth_mut()`, so a pre-suspended
        // `AuthContext` must be built directly and adopted through the
        // builder rather than mutated after the fact.
        let mut ctx = crate::control::security::auth_context::AuthContext::from_identity(
            &identity,
            "s_test_suspended".into(),
        );
        ctx.status = AuthStatus::Suspended;
        let request = RequestAuthScope::builder(&identity, state.auth_stores())
            .with_session_database(Some(DatabaseId::DEFAULT))
            .with_adopted_auth_context(ctx)
            .build_for_client("127.0.0.1")
            .expect("resolve client request scope");

        let result = check_request_admission(&state, &request, "point_get");
        assert!(result.is_err(), "suspended account must be rejected");
    }

    #[tokio::test]
    async fn happy_path_returns_rate_limit_result() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9005, AuthMethod::ScramSha256);
        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1",
        )
        .expect("resolve client request scope");

        let result = check_request_admission(&state, &request, "point_get")
            .expect("non-blacklisted, active, unthrottled request must be admitted");
        assert!(
            result.is_some(),
            "checked path must report Some(rate limit result)"
        );
    }

    // ── Risk gate. Scoring is stamped by the scope builder from the real
    //    peer address; admission turns the decision into a refusal. ───────

    use crate::control::security::risk::RiskConfig;

    fn risk_config(enabled: bool, allow: f64, deny: f64) -> RiskConfig {
        RiskConfig {
            enabled,
            allow_threshold: allow,
            deny_threshold: deny,
            ..Default::default()
        }
    }

    fn scoped<'a, 'p>(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        peer_addr: &'p str,
    ) -> ClientRequestScope<'a, 'p> {
        ClientRequestScope::for_database(
            identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            peer_addr,
        )
        .expect("resolve client request scope")
    }

    fn rejection_reason(error: &crate::Error) -> String {
        match error {
            crate::Error::RejectedAuthz { resource, .. } => resource.clone(),
            other => panic!("expected an authz rejection, got {other:?}"),
        }
    }

    /// A scored, in-band request is admitted and its score is visible to RLS
    /// through `$auth.risk_score`.
    #[tokio::test]
    async fn scored_request_in_allow_band_is_admitted_and_exposes_its_score() {
        // Everything is allowed: the allow band covers the whole range.
        let (state, _dir) = test_state_with_risk(risk_config(true, 1.0, 2.0)).await;
        let identity = regular_identity(9201, AuthMethod::ScramSha256);
        let scope = scoped(&state, &identity, "10.0.0.1:5432");

        let result = check_request_admission(&state, &scope, "point_get")
            .expect("an in-band score must be admitted");
        assert!(result.is_some());

        let score = scope
            .scope()
            .auth()
            .risk_score
            .expect("an enabled scorer must stamp a score");
        let resolved =
            crate::control::security::predicate::PredicateValue::AuthRef("risk_score".to_string())
                .resolve(scope.scope().auth());
        assert_eq!(
            resolved,
            Some(serde_json::json!(score)),
            "$auth.risk_score must substitute into RLS predicates"
        );
    }

    #[tokio::test]
    async fn deny_band_refuses_the_request() {
        // Deny band starts at zero, so every score lands in it.
        let (state, _dir) = test_state_with_risk(risk_config(true, -1.0, 0.0)).await;
        let identity = regular_identity(9202, AuthMethod::ScramSha256);
        let scope = scoped(&state, &identity, "10.0.0.1:5432");

        let error = check_request_admission(&state, &scope, "point_get")
            .expect_err("a deny-band score must be refused");
        assert_eq!(rejection_reason(&error), "denied by risk policy");
    }

    /// The step-up band has no protocol behind it yet, so it fails closed —
    /// with its own reason, distinct from a plain deny.
    #[tokio::test]
    async fn step_up_band_refuses_with_a_distinct_reason() {
        // First request scores new_ip (0.15) + device_not_trusted (0.20) =
        // 0.35, which sits between these thresholds.
        let (state, _dir) = test_state_with_risk(risk_config(true, 0.3, 0.7)).await;
        let identity = regular_identity(9203, AuthMethod::ScramSha256);
        let scope = scoped(&state, &identity, "10.0.0.1:5432");

        let error = check_request_admission(&state, &scope, "point_get")
            .expect_err("the step-up band must not be admitted");
        assert_eq!(rejection_reason(&error), "step-up authentication required");
    }

    /// Server-owned work is exempt from the risk gate exactly as it is from
    /// the blacklist and rate-limit guards.
    #[tokio::test]
    async fn internal_service_identity_is_never_risk_refused() {
        let (state, _dir) = test_state_with_risk(risk_config(true, -1.0, 0.0)).await;
        let identity = internal_service_identity(9204);
        let scope = scoped(&state, &identity, "10.0.0.1:5432");

        assert_eq!(
            scope.scope().auth().risk_score,
            None,
            "exempt identities are unscored"
        );
        let result = check_request_admission(&state, &scope, "point_get")
            .expect("internal-service identities must never be risk-refused");
        assert!(result.is_none());
    }

    /// A scope built without a usable client address cannot be assessed, so
    /// the gate refuses rather than admitting an unscored request.
    #[tokio::test]
    async fn unassessed_request_fails_closed_when_scoring_is_enabled() {
        let (state, _dir) = test_state_with_risk(risk_config(true, 1.0, 2.0)).await;
        let identity = regular_identity(9205, AuthMethod::ScramSha256);
        // A transport label is not an address, so nothing can be scored
        // from it.
        let scope = scoped(&state, &identity, "http");

        let error = check_request_admission(&state, &scope, "point_get")
            .expect_err("an unassessed request must not be admitted");
        assert_eq!(
            rejection_reason(&error),
            "risk assessment unavailable for this request"
        );
    }

    /// The knob is not inert: the very same request that a deny-everything
    /// threshold refuses is admitted once the configuration says so, and
    /// nothing is scored at all while `enabled` is false.
    #[tokio::test]
    async fn configured_thresholds_change_the_outcome() {
        let identity = regular_identity(9206, AuthMethod::ScramSha256);

        let (denying, _dir_a) = test_state_with_risk(risk_config(true, -1.0, 0.0)).await;
        let scope = scoped(&denying, &identity, "10.0.0.1:5432");
        assert!(check_request_admission(&denying, &scope, "point_get").is_err());

        let (permitting, _dir_b) = test_state_with_risk(risk_config(true, 1.0, 2.0)).await;
        let scope = scoped(&permitting, &identity, "10.0.0.1:5432");
        assert!(check_request_admission(&permitting, &scope, "point_get").is_ok());

        let (disabled, _dir_c) = test_state_with_risk(risk_config(false, -1.0, 0.0)).await;
        let scope = scoped(&disabled, &identity, "10.0.0.1:5432");
        assert_eq!(scope.scope().auth().risk_score, None);
        assert!(check_request_admission(&disabled, &scope, "point_get").is_ok());
    }

    #[tokio::test]
    async fn blacklist_and_status_door_also_enforces_the_risk_gate() {
        let (state, _dir) = test_state_with_risk(risk_config(true, -1.0, 0.0)).await;
        let identity = regular_identity(9207, AuthMethod::ApiKey);
        let scope = scoped(&state, &identity, "10.0.0.1:5432");

        let error = check_blacklist_and_status(&state, &scope)
            .expect_err("the non-rate-limited door must refuse a deny-band request too");
        assert_eq!(rejection_reason(&error), "denied by risk policy");
    }

    // ── `check_blacklist_and_status` — the blacklist-only-plus-status door
    //    shared by ILP/OTLP ingest, CRDT delta sync, shape subscribe/resync,
    //    and pgwire COPY backup/restore. ──────────────────────────────────

    #[tokio::test]
    async fn blacklist_and_status_rejects_user_blacklisted_identity() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9101, AuthMethod::ApiKey);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1:5432",
        )
        .expect("resolve client request scope");

        let result = check_blacklist_and_status(&state, &request);
        assert!(
            result.is_err(),
            "a user-blacklisted identity must be rejected"
        );
    }

    /// The IP half of the gate — this is what "peer address threading" is
    /// for: a client whose IP was never blacklisted by user id must still be
    /// rejected once its real peer address matches a `BLACKLIST IP` entry.
    #[tokio::test]
    async fn blacklist_and_status_rejects_blacklisted_peer_ip() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9102, AuthMethod::ApiKey);
        state
            .blacklist
            .blacklist_ip("10.0.0.0/8", "test ip ban", "admin", 0)
            .expect("blacklist CIDR range");

        let outside = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "203.0.113.5:5432",
        )
        .expect("resolve client request scope");
        let allowed = check_blacklist_and_status(&state, &outside);
        assert!(
            allowed.is_ok(),
            "an address outside the blacklisted range must be admitted"
        );

        let inside = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "10.1.2.3:5432",
        )
        .expect("resolve client request scope");
        let denied = check_blacklist_and_status(&state, &inside);
        assert!(
            denied.is_err(),
            "an address inside the blacklisted CIDR range must be rejected, proving the real \
             peer address (not an empty placeholder) reaches the IP-blacklist check"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_rejects_suspended_account() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9103, AuthMethod::ApiKey);
        let mut ctx = crate::control::security::auth_context::AuthContext::from_identity(
            &identity,
            "s_test_suspended_no_ratelimit".into(),
        );
        ctx.status = AuthStatus::Suspended;
        let request = RequestAuthScope::builder(&identity, state.auth_stores())
            .with_session_database(Some(DatabaseId::DEFAULT))
            .with_adopted_auth_context(ctx)
            .build_for_client("127.0.0.1:5432")
            .expect("resolve client request scope");

        let result = check_blacklist_and_status(&state, &request);
        assert!(
            result.is_err(),
            "a suspended account must be rejected even though no rate limit runs on this door"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_exempts_internal_service_identity() {
        let (state, _dir) = test_state().await;
        let identity = internal_service_identity(9104);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");

        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1:5432",
        )
        .expect("resolve client request scope");

        let result = check_blacklist_and_status(&state, &request);
        assert!(
            result.is_ok(),
            "internal-service identities must never be blocked, even when blacklisted"
        );
    }

    #[tokio::test]
    async fn blacklist_and_status_allows_active_unblocked_identity() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9105, AuthMethod::ApiKey);
        let request = ClientRequestScope::for_database(
            &identity,
            state.auth_stores(),
            DatabaseId::DEFAULT,
            "127.0.0.1:5432",
        )
        .expect("resolve client request scope");

        let result = check_blacklist_and_status(&state, &request);
        assert!(
            result.is_ok(),
            "a non-blacklisted, active identity must be admitted with no rate limit involved"
        );
    }

    // ── Per-database request counting. ────────────────────────────────────

    fn qps(state: &SharedState) -> u64 {
        state.database_metrics.ensure_database(DatabaseId::DEFAULT);
        state.database_metrics.qps_total(DatabaseId::DEFAULT)
    }

    #[tokio::test]
    async fn admitted_request_counts_once() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9301, AuthMethod::ScramSha256);
        let request = scoped(&state, &identity, "127.0.0.1:5432");
        let before = qps(&state);

        check_request_admission(&state, &request, "point_get").expect("request is admitted");

        assert_eq!(qps(&state), before + 1);
    }

    #[tokio::test]
    async fn internal_service_request_is_not_counted() {
        let (state, _dir) = test_state().await;
        let identity = internal_service_identity(9302);
        let request = scoped(&state, &identity, "127.0.0.1:5432");
        let before = qps(&state);

        check_request_admission(&state, &request, "point_get").expect("exempt request passes");
        check_blacklist_and_status(&state, &request).expect("exempt request passes");

        assert_eq!(qps(&state), before);
    }

    #[tokio::test]
    async fn rate_limited_request_is_not_counted() {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_rate_limit_config(
            dispatcher,
            wal,
            crate::control::security::ratelimit::config::RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 1,
                ..Default::default()
            },
        )
        .expect("construct shared state");
        let identity = regular_identity(9303, AuthMethod::ScramSha256);
        let request = scoped(&state, &identity, "127.0.0.1:5432");
        let before = qps(&state);

        check_request_admission(&state, &request, "point_get").expect("first request is admitted");
        let refused = check_request_admission(&state, &request, "point_get");

        assert!(refused.is_err(), "second request exceeds the burst of one");
        assert_eq!(qps(&state), before + 1, "only the admitted request counts");
    }

    #[tokio::test]
    async fn blacklist_and_status_door_counts_an_admitted_request_once() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9304, AuthMethod::ApiKey);
        let request = scoped(&state, &identity, "127.0.0.1:5432");
        let before = qps(&state);

        check_blacklist_and_status(&state, &request).expect("request is admitted");

        assert_eq!(qps(&state), before + 1);
    }

    #[tokio::test]
    async fn refused_request_is_not_counted_at_either_door() {
        let (state, _dir) = test_state().await;
        let identity = regular_identity(9305, AuthMethod::ApiKey);
        state
            .blacklist
            .blacklist_user(&identity.user_id.to_string(), "test ban", "admin", 0)
            .expect("blacklist user");
        let request = scoped(&state, &identity, "127.0.0.1:5432");
        let before = qps(&state);

        assert!(check_request_admission(&state, &request, "point_get").is_err());
        assert!(check_blacklist_and_status(&state, &request).is_err());

        assert_eq!(qps(&state), before);
    }
}
