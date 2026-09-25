// SPDX-License-Identifier: BUSL-1.1

//! The auth-configured parts of `SharedState::open`: metering, rate limits,
//! SIEM export, risk scoring, escalation, TLS policy, and the catalog-backed
//! auth-user, scope-grant and quota stores.

use std::sync::Arc;

use crate::control::security::credential::CredentialStore;
use crate::control::security::metering::config::MeteringConfig;
use crate::control::security::metering::quota::QuotaManager;
use crate::control::security::ratelimit::config::RateLimitConfig;

/// Everything `open` builds from `[auth]` configuration.
pub(super) struct AuthParts {
    pub(super) metering_config: MeteringConfig,
    pub(super) rate_limit_config: RateLimitConfig,
    pub(super) http_client: Arc<reqwest::Client>,
    pub(super) siem: crate::control::security::siem::SiemExporter,
    pub(super) risk_scorer: crate::control::security::risk::RiskScorer,
    pub(super) escalation: crate::control::security::escalation::EscalationEngine,
    pub(super) tls_policy: crate::control::security::tls_policy::TlsPolicy,
    pub(super) auth_users: crate::control::security::jit::auth_user::AuthUserStore,
    pub(super) scope_grants: crate::control::security::scope::grant::ScopeGrantStore,
    pub(super) quota_manager: QuotaManager,
}

/// Build the auth-configured parts from `auth_config` and the catalog.
pub(super) fn build(
    auth_config: &crate::config::auth::AuthConfig,
    credentials: &CredentialStore,
) -> crate::Result<AuthParts> {
    // `auth_config.metering` is `None` unless the operator configured a
    // `[metering]` section; fall back to `MeteringConfig::default()` so
    // the effective bounds always match a real `MeteringConfig` value
    // (same source `init.rs`'s test constructor pins to) instead of the
    // separately-hardcoded `UsageStore`/`QuotaManager` `Default` impls.
    let metering_defaults = MeteringConfig::default();
    let metering_config = auth_config.metering.clone().unwrap_or(metering_defaults);

    // `auth_config.rate_limit` is `None` unless the operator configured a
    // `[auth.rate_limit]` section; fall back to `RateLimitConfig::default()`
    // (same source `init.rs`'s test constructor pins to) so the limiter's
    // effective config always matches a real `RateLimitConfig` value
    // instead of the separately-hardcoded `RateLimiter::default()` impl.
    let rate_limit_defaults = RateLimitConfig::default();
    let rate_limit_config = auth_config
        .rate_limit
        .clone()
        .unwrap_or(rate_limit_defaults);

    // `auth_config.siem` is `None` unless the operator configured an
    // `[auth.siem]` section; the default leaves `destinations` empty and
    // `webhook_url` blank, so `is_configured()` is false and the export
    // path stays dormant. When it *is* configured the exporter shares the
    // process-wide HTTP client rather than building its own pool.
    let siem_config = auth_config.siem.clone().unwrap_or_default();
    let http_client = Arc::new(reqwest::Client::new());
    let siem = crate::control::security::siem::SiemExporter::with_client(
        siem_config,
        Arc::clone(&http_client),
    );

    // `auth_config.risk` is `None` unless the operator configured an
    // `[auth.risk]` section, and `RiskConfig::default()` has
    // `enabled = false`, so scoring stays dormant either way. When it is
    // configured the operator's weights and thresholds reach the scorer
    // here — the one place they can, since `RiskScorer` reads its config
    // only at construction.
    let risk_scorer = crate::control::security::risk::RiskScorer::new(
        auth_config.risk.clone().unwrap_or_default(),
    );

    // `auth_config.escalation` is `None` unless the operator configured an
    // `[auth.escalation]` section, and `EscalationConfig::default()` has
    // `enabled = false`, so no account is auto-suspended either way. When
    // it is configured the operator's thresholds reach the engine here —
    // the one place they can, since `EscalationEngine` reads its config
    // only at construction.
    let escalation = crate::control::security::escalation::EscalationEngine::new(
        auth_config.escalation.clone().unwrap_or_default(),
    );

    // `auth_config.tls_policy` is `None` unless the operator configured an
    // `[auth.tls_policy]` section, and `TlsPolicyConfig::default()` has
    // `enabled = false`, so no connection is refused on transport grounds
    // either way. When it *is* configured the operator's minimum version
    // is parsed here — the one place it can be — and an unparseable value
    // fails startup rather than being silently replaced by a default that
    // enforces something else.
    let tls_policy = crate::control::security::tls_policy::TlsPolicy::from_config(
        &auth_config.tls_policy.clone().unwrap_or_default(),
    )?;

    // Auth users are catalog-backed in production: an escalation verdict
    // written to a record has to still be there after a restart, and a
    // memory-only store would drop it.
    let auth_users = crate::control::security::jit::auth_user::AuthUserStore::open(
        credentials.catalog().clone(),
    )?;
    // Restore the suspend → ban ladder from the persisted records before
    // any request is served.
    for user in auth_users.list(false) {
        escalation.hydrate_suspensions(&user.id, user.escalation_suspensions);
    }

    // Scope grants are catalog-backed for the same reason: a grant — and
    // the `WHEN` / `REQUIRE` conditions restricting it — has to survive a
    // restart, and a memory-only store silently drops every grant the
    // operator issued.
    let scope_grants =
        crate::control::security::scope::grant::ScopeGrantStore::open(credentials.catalog())?;

    // Quota definitions are catalog objects for the same reason grants
    // are: a cap that lived only in memory would be lifted by every
    // restart, and a rolling deploy would quietly forgive every ceiling
    // the operator set.
    let quota_manager = QuotaManager::open(
        metering_config.max_tracked_quota_grantees,
        credentials.catalog(),
    )?;

    Ok(AuthParts {
        metering_config,
        rate_limit_config,
        http_client,
        siem,
        risk_scorer,
        escalation,
        tls_policy,
        auth_users,
        scope_grants,
        quota_manager,
    })
}
