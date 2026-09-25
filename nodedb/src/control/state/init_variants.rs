// SPDX-License-Identifier: BUSL-1.1

//! Test constructors of `SharedState`, each built on `new_inner`, and the
//! session-handle audit wiring production shares.

use std::sync::Arc;

use crate::bridge::dispatch::Dispatcher;
use crate::control::security::credential::CredentialStore;
use crate::control::security::metering::quota::QuotaManager;
use crate::wal::WalManager;

use super::SharedState;

impl SharedState {
    /// Create shared state with a pre-built credential store (for tests that need catalog).
    ///
    /// `is_cluster` is the static, deployment-time surrogate-registry mode
    /// choice — same predicate as `SharedState::open`'s `is_cluster`
    /// (whether this node's caller is about to wire it into a real Raft
    /// cluster), not a property of the credential store. Almost every
    /// caller is a single-process fixture and passes `false`; the cluster
    /// test harness passes `true`.
    pub fn new_with_credentials(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        credentials: Arc<CredentialStore>,
        is_cluster: bool,
    ) -> crate::Result<Arc<Self>> {
        let wal_for_assigner = Arc::clone(&wal);
        let mut state = Self::new_inner(dispatcher, wal)?;
        if let Some(s) = Arc::get_mut(&mut state) {
            // Rebuild the surrogate assigner against the supplied
            // credential store. `new_inner` constructs the assigner
            // from a fresh in-memory `CredentialStore` with its own
            // in-memory catalog; the supplied store carries the durable
            // catalog whose surrogate watermark this fixture must resume.
            let registry = Arc::clone(&s.surrogate_registry);
            // Seed the registry's high-watermark AND applied-reserve cursor
            // from the catalog so restarts in a re-opened test fixture pick up
            // where the previous session left off — and so cluster-mode
            // metadata-log replay skips already-applied reservations rather
            // than double-counting `G`.
            let catalog = credentials.catalog();
            // The catalog-derived floor mirrors the production bootstrap: the
            // singleton is flushed lazily, so the highest surrogate any live
            // binding refers to is the value the allocator can never start
            // below. Mode selection mirrors `init_prod/bootstrap.rs::run`:
            // `is_cluster` (never seed-list length) picks `Cluster` vs
            // `Local`, seeding the applied-reserve cursor only in the
            // former.
            if let Ok(hwm) = catalog.get_surrogate_hwm()
                && let Ok(bound_floor) = catalog.max_bound_surrogate()
                && let Ok(mut reg) = registry.write()
            {
                let floor = hwm.max(bound_floor.as_u32());
                *reg = if is_cluster {
                    let reserve_index = catalog.get_surrogate_reserve_index().unwrap_or(0);
                    crate::control::surrogate::SurrogateRegistry::from_persisted_cluster(
                        floor,
                        reserve_index,
                    )
                } else {
                    crate::control::surrogate::SurrogateRegistry::from_persisted_hwm(floor)
                };
            }
            let wal_appender: Arc<dyn crate::control::surrogate::SurrogateWalAppender> = Arc::new(
                crate::control::surrogate::WalSurrogateAppender::new(wal_for_assigner),
            );
            s.surrogate_assigner = Arc::new(crate::control::surrogate::SurrogateAssigner::new(
                Arc::clone(&registry),
                Arc::clone(&credentials),
                wal_appender,
            ));
            // Catalog-backed security stores, rebuilt for the same reason the
            // surrogate watermark above is: this constructor's whole purpose
            // is to resume a durable catalog, and a memory-only store here
            // silently drops every auth-user status and scope grant the
            // previous session persisted — so a restart fixture would report
            // a clean slate rather than what was actually saved.
            s.auth_users =
                crate::control::security::jit::auth_user::AuthUserStore::open(catalog.clone())?;
            s.scope_grants =
                crate::control::security::scope::grant::ScopeGrantStore::open(catalog)?;
            // Same reasoning as the grants above: a quota definition is a
            // durable catalog object, and a memory-only manager here would
            // report every cap as absent after a restart.
            s.quota_manager = QuotaManager::open(
                s.metering_config.max_tracked_quota_grantees,
                credentials.catalog(),
            )?;
            s.credentials = credentials;
            s.ep_topic_registry
                .load_from_catalog(s.credentials.catalog())?;
            crate::event::topic::hydrate_topic_buffers(s)?;
        }
        Ok(state)
    }

    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> crate::Result<Arc<Self>> {
        Self::new_inner(dispatcher, wal)
    }

    /// Create shared state whose risk scorer is built from `risk_config`
    /// instead of the disabled default (for tests that exercise the risk
    /// gate). Production wires the same configuration from `[auth.risk]`.
    pub fn new_with_risk_config(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        risk_config: crate::control::security::risk::RiskConfig,
    ) -> crate::Result<Arc<Self>> {
        let mut state = Self::new_inner(dispatcher, wal)?;
        let s = Arc::get_mut(&mut state).ok_or_else(|| crate::Error::Internal {
            detail: "shared state was already shared before the risk scorer could be installed"
                .into(),
        })?;
        s.risk_scorer = crate::control::security::risk::RiskScorer::new(risk_config);
        Ok(state)
    }

    /// Create shared state whose TLS policy is built from `tls_policy_config`
    /// instead of the disabled default (for tests that exercise transport
    /// enforcement). Production wires the same configuration from
    /// `[auth.tls_policy]`, through the same fallible parse: an unparseable
    /// `min_tls_version` is an error here exactly as it is at startup.
    pub fn new_with_tls_policy_config(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        tls_policy_config: crate::control::security::tls_policy::TlsPolicyConfig,
    ) -> crate::Result<Arc<Self>> {
        let policy =
            crate::control::security::tls_policy::TlsPolicy::from_config(&tls_policy_config)?;
        let mut state = Self::new_inner(dispatcher, wal)?;
        let s = Arc::get_mut(&mut state).ok_or_else(|| crate::Error::Internal {
            detail: "shared state was already shared before the TLS policy could be installed"
                .into(),
        })?;
        s.tls_policy = policy;
        Ok(state)
    }

    /// Point the session-handle store's audit hook at this state's
    /// `AuditLog`, so `SessionHandleFingerprintMismatch` and
    /// `SessionHandleResolveMissSpike` are hash-chained with
    /// the rest of the auth-plane event stream. Captures the audit Arc
    /// directly — a `Weak<Self>` would block the cluster wire-up phase's
    /// `Arc::get_mut` on `SharedState`.
    pub(super) fn wire_session_handle_audit(state: &Arc<Self>) {
        let audit = Arc::clone(&state.audit);
        state.session_handles.set_audit_hook(move |event| {
            if let Ok(mut log) = audit.lock() {
                let _ = log.record(event, None, "session_handle", "");
            }
        });
    }
}
