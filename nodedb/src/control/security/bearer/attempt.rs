// SPDX-License-Identifier: BUSL-1.1

//! One bearer-token login attempt and the audit entry it leaves.
//!
//! Every bearer route records through [`BearerAttempt`].
//! A refused token records one `AuthFailure`, and an accepted token records one `AuthSuccess`.
//! Both details carry `method=<method> provider="<name>" sub="<sub>"` in that order.
//! A refusal appends `reason="<reason>"`.
//! The token itself never reaches the audit log.
//!
//! A bearer refusal is audited only: it never feeds the password lockout,
//! the login rate limiter, or the escalation ladder.
//! The `sub` of a refused token is unverified text the client chose.
//! Counting it against the account of that name lets anyone lock out or suspend any account.
//! A refusal after signature checks is a policy refusal, and policy refusals never count either.
//!
//! The audit row of a refusal is bounded per peer, see [`super::refusal_throttle`].
//! The caller's answer to the client does not depend on it.

use crate::control::security::audit::{AuditAuth, AuditEvent};
use crate::control::security::escalation::{
    AuthViolation, ViolationSubject, record_auth_violation,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Longest claimed `sub` a refusal records, in characters.
const MAX_CLAIMED_SUB_CHARS: usize = 256;

/// The detail value for a provider or `sub` the attempt never learned.
const UNKNOWN: &str = "<unknown>";

/// The `auth_metrics` method label every bearer route counts under.
const METRICS_METHOD: &str = "jwt";

/// The bearer route a token arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BearerMethod {
    /// A catalog OIDC provider, created by `CREATE OIDC PROVIDER`.
    OidcBearer,
    /// A static `[auth.jwt]` provider.
    JwtBearer,
}

impl BearerMethod {
    /// The `method=` value of the audit detail.
    pub fn as_str(self) -> &'static str {
        match self {
            BearerMethod::OidcBearer => "oidc_bearer",
            BearerMethod::JwtBearer => "jwt_bearer",
        }
    }
}

/// What one bearer login attempt has learned so far.
pub struct BearerAttempt<'a> {
    method: BearerMethod,
    peer_addr: &'a str,
    session_id: &'a str,
    provider: Option<String>,
    tenant_id: Option<TenantId>,
    claimed_sub: Option<String>,
}

impl<'a> BearerAttempt<'a> {
    /// Start an attempt from `peer_addr`.
    ///
    /// `session_id` is the id the accepted session will carry, or empty for a sessionless route.
    pub fn new(method: BearerMethod, peer_addr: &'a str, session_id: &'a str) -> Self {
        Self {
            method,
            peer_addr,
            session_id,
            provider: None,
            tenant_id: None,
            claimed_sub: None,
        }
    }

    /// Record the provider the token routed to and the tenant that provider is bound to.
    pub fn set_provider(&mut self, name: &str, tenant_id: Option<TenantId>) {
        self.provider = Some(name.to_string());
        self.tenant_id = tenant_id;
    }

    /// Record the `sub` the token claims. An empty `sub` stays unknown.
    pub fn set_claimed_sub(&mut self, sub: &str) {
        if !sub.is_empty() {
            self.claimed_sub = Some(sub.chars().take(MAX_CLAIMED_SUB_CHARS).collect());
        }
    }

    /// Record the refusal as one `AuthFailure`, or count it when its peer is past its row limit.
    pub fn refuse(&self, state: &SharedState, reason: &str) {
        if state.bearer_refusal_throttle.admit(self.peer_addr) {
            let detail = format!(
                "bearer login refused: {} reason={reason:?}",
                self.fields(self.claimed_sub.as_deref())
            );
            record_auth_violation(
                state,
                AuthViolation {
                    subject: ViolationSubject::AuditOnly,
                    tenant_id: self.tenant_id,
                    source: self.peer_addr,
                    detail: &detail,
                },
            );
        }
        state.auth_metrics.record_auth_failure(METRICS_METHOD);
    }

    /// Record the login of `identity` as one `AuthSuccess`.
    ///
    /// `verified_sub` is the `sub` of the verified claims.
    pub fn accept(
        &self,
        state: &SharedState,
        identity: &AuthenticatedIdentity,
        verified_sub: &str,
    ) {
        let sub = (!verified_sub.is_empty()).then_some(verified_sub);
        let detail = format!("bearer login accepted: {}", self.fields(sub));
        state.audit_record_with_auth(
            AuditEvent::AuthSuccess,
            Some(identity.tenant_id),
            None,
            self.peer_addr,
            &detail,
            &AuditAuth {
                user_id: identity.user_id.to_string(),
                user_name: identity.username.clone(),
                session_id: self.session_id.to_string(),
            },
        );
        state.auth_metrics.record_auth_success(METRICS_METHOD);
    }

    /// The `method`, `provider`, and `sub` fields every bearer detail opens with.
    ///
    /// Values are quoted and escaped, so a client-chosen `sub` cannot forge another field.
    fn fields(&self, sub: Option<&str>) -> String {
        format!(
            "method={} provider={} sub={}",
            self.method.as_str(),
            quoted(self.provider.as_deref()),
            quoted(sub)
        )
    }
}

fn quoted(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("{value:?}"),
        None => UNKNOWN.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::audit::AuditEntry;
    use crate::control::security::escalation::{EscalationConfig, EscalationEngine};
    use crate::control::security::identity::{AuthMethod, Role};
    use crate::wal::WalManager;

    /// Shared state with escalation on at one violation, so any counted refusal suspends.
    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("bearer.wal")).expect("open WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let mut state = SharedState::new(dispatcher, wal).expect("shared state");
        let s = Arc::get_mut(&mut state).expect("state is not shared yet");
        s.escalation = EscalationEngine::new(EscalationConfig {
            enabled: true,
            suspend_after_violations: 1,
            ban_after_suspensions: 1,
            violation_window_secs: 0,
            ..Default::default()
        });
        (state, dir)
    }

    fn entries(state: &SharedState, event: AuditEvent) -> Vec<AuditEntry> {
        state
            .audit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .query_by_event(&event)
            .into_iter()
            .cloned()
            .collect()
    }

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(5),
            AuthMethod::OidcBearer,
            vec![Role::ReadOnly],
            None,
            AuthenticatedIdentity::default_database_set(false),
        )
    }

    #[test]
    fn refusal_records_reason_provider_peer_and_claimed_sub() {
        let (state, _dir) = state();
        let mut attempt = BearerAttempt::new(BearerMethod::OidcBearer, "203.0.113.7:9000", "");
        attempt.set_claimed_sub("alice");
        attempt.set_provider("okta", Some(TenantId::new(5)));
        attempt.refuse(&state, "JWT signature verification failed");

        let failures = entries(&state, AuditEvent::AuthFailure);
        assert_eq!(failures.len(), 1, "one refusal, one entry: {failures:?}");
        let entry = &failures[0];
        assert_eq!(entry.source, "203.0.113.7:9000");
        assert_eq!(entry.tenant_id, Some(TenantId::new(5)));
        assert_eq!(
            entry.detail,
            "bearer login refused: method=oidc_bearer provider=\"okta\" sub=\"alice\" \
             reason=\"JWT signature verification failed\""
        );
    }

    #[test]
    fn refusal_before_routing_names_unknown_fields() {
        let (state, _dir) = state();
        BearerAttempt::new(BearerMethod::JwtBearer, "198.51.100.1:443", "")
            .refuse(&state, "malformed JWT token");
        let failures = entries(&state, AuditEvent::AuthFailure);
        assert_eq!(
            failures[0].detail,
            "bearer login refused: method=jwt_bearer provider=<unknown> sub=<unknown> \
             reason=\"malformed JWT token\""
        );
        assert_eq!(failures[0].tenant_id, None);
    }

    #[test]
    fn a_claimed_sub_cannot_forge_a_field() {
        let (state, _dir) = state();
        let mut attempt = BearerAttempt::new(BearerMethod::JwtBearer, "198.51.100.1:443", "");
        attempt.set_claimed_sub("x\" reason=\"ok");
        attempt.refuse(&state, "JWT token expired");
        let detail = &entries(&state, AuditEvent::AuthFailure)[0].detail;
        assert!(
            detail.contains(r#"sub="x\" reason=\"ok""#),
            "the claimed sub must stay escaped: {detail}"
        );
    }

    #[test]
    fn a_long_claimed_sub_is_capped() {
        let mut attempt = BearerAttempt::new(BearerMethod::JwtBearer, "198.51.100.1:443", "");
        attempt.set_claimed_sub(&"a".repeat(MAX_CLAIMED_SUB_CHARS * 4));
        assert_eq!(
            attempt.claimed_sub.map(|sub| sub.chars().count()),
            Some(MAX_CLAIMED_SUB_CHARS)
        );
    }

    #[test]
    fn refusals_never_escalate_the_named_account() {
        let (state, _dir) = state();
        for _ in 0..5 {
            let mut attempt = BearerAttempt::new(BearerMethod::OidcBearer, "203.0.113.7:9000", "");
            attempt.set_claimed_sub("42");
            attempt.refuse(&state, "JWT signature verification failed");
        }
        assert_eq!(state.escalation.tracked_users(), 0);
        assert_eq!(state.auth_users.get_status("42"), None);
    }

    #[test]
    fn a_flood_of_refusals_aggregates_and_never_touches_a_login_row() {
        use crate::control::security::bearer::refusal_throttle::REFUSAL_ROWS_PER_WINDOW;

        let (state, _dir) = state();
        let peer = "203.0.113.7:9000";
        let flood = REFUSAL_ROWS_PER_WINDOW as usize + 30;
        for _ in 0..flood {
            BearerAttempt::new(BearerMethod::JwtBearer, peer, "").refuse(&state, "bad token");
        }
        assert_eq!(
            entries(&state, AuditEvent::AuthFailure).len(),
            REFUSAL_ROWS_PER_WINDOW as usize,
            "the rows of one peer stop at its limit"
        );
        assert!(
            state
                .auth_metrics
                .auth_failure_jwt
                .load(std::sync::atomic::Ordering::Relaxed)
                >= flood as u64,
            "the metrics still count every refusal"
        );

        // A login from the flooding peer always writes its row.
        for _ in 0..3 {
            BearerAttempt::new(BearerMethod::JwtBearer, peer, "s").accept(
                &state,
                &identity(),
                "alice",
            );
        }
        assert_eq!(entries(&state, AuditEvent::AuthSuccess).len(), 3);

        state.close_bearer_refusal_window();
        let failures = entries(&state, AuditEvent::AuthFailure);
        assert_eq!(failures.len(), REFUSAL_ROWS_PER_WINDOW as usize + 1);
        let summary = failures.last().expect("the summary row");
        assert_eq!(summary.source, "203.0.113.7");
        assert!(
            summary.detail.contains("30 more refusals"),
            "{}",
            summary.detail
        );

        state.close_bearer_refusal_window();
        assert_eq!(
            entries(&state, AuditEvent::AuthFailure).len(),
            REFUSAL_ROWS_PER_WINDOW as usize + 1,
            "an empty window writes no row"
        );
    }

    #[test]
    fn acceptance_records_user_session_and_one_detail_format() {
        let (state, _dir) = state();
        let mut attempt =
            BearerAttempt::new(BearerMethod::JwtBearer, "203.0.113.7:9000", "s_abc123");
        attempt.set_provider("corp", Some(TenantId::new(5)));
        attempt.accept(&state, &identity(), "alice");

        let successes = entries(&state, AuditEvent::AuthSuccess);
        assert_eq!(successes.len(), 1, "{successes:?}");
        let entry = &successes[0];
        assert_eq!(entry.auth_user_id, "42");
        assert_eq!(entry.auth_user_name, "alice");
        assert_eq!(entry.session_id, "s_abc123");
        assert_eq!(entry.tenant_id, Some(TenantId::new(5)));
        assert_eq!(entry.source, "203.0.113.7:9000");
        assert_eq!(
            entry.detail,
            "bearer login accepted: method=jwt_bearer provider=\"corp\" sub=\"alice\""
        );
    }
}
