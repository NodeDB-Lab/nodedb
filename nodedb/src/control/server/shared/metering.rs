// SPDX-License-Identifier: BUSL-1.1

//! Post-dispatch usage metering: attribute a completed [`PhysicalTask`] to
//! its collection/engine/operation and record it against the caller's usage
//! bucket.
//!
//! [`PhysicalTask`]: nodedb_physical::physical_task::PhysicalTask

use std::sync::Arc;

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{Permission, required_permission};
use crate::control::security::metering::counter::UsageEvent;
use crate::control::security::permission::parse_permission;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::plan_util::{metered_collections, plan_engine};
use crate::control::state::SharedState;
use nodedb_types::calvin::EngineTag;

/// Map an [`EngineTag`] to its stable metering dimension string. Exhaustive
/// over every variant so a new engine forces this mapping to be updated
/// rather than silently billing under a wrong or missing label.
fn engine_tag_str(tag: EngineTag) -> &'static str {
    match tag {
        EngineTag::Vector => "vector",
        EngineTag::Graph => "graph",
        EngineTag::Document => "document",
        EngineTag::Kv => "kv",
        EngineTag::Text => "text",
        EngineTag::Columnar => "columnar",
        EngineTag::Timeseries => "timeseries",
        EngineTag::Spatial => "spatial",
        EngineTag::Crdt => "crdt",
        EngineTag::Query => "query",
        EngineTag::Meta => "meta",
        EngineTag::Array => "array",
        EngineTag::ClusterArray => "cluster_array",
    }
}

/// Map a physical plan to the metering `operation` cost-table key (see
/// `MeteringConfig::operation_costs`). Shared with the rate-limiter's
/// operation classification so the two never silently diverge and mis-bill.
pub(crate) fn operation_for_plan(plan: &PhysicalPlan) -> &'static str {
    match plan {
        PhysicalPlan::Vector(_) => "vector_search",
        PhysicalPlan::Graph(_) => "graph_hop",
        PhysicalPlan::Document(_) => "document_scan",
        PhysicalPlan::Kv(_) => "kv_scan",
        PhysicalPlan::Text(_) => "text_search",
        PhysicalPlan::Columnar(_) | PhysicalPlan::Timeseries(_) | PhysicalPlan::Spatial(_) => {
            "document_scan"
        }
        PhysicalPlan::Crdt(_) => "point_get",
        PhysicalPlan::Query(_) => "aggregate",
        PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => "sql",
    }
}

/// The metering-relevant shape of a [`PhysicalPlan`], captured before
/// dispatch consumes the plan by value. Avoids `plan.clone()`-ing payloads
/// (vector floats, row upserts) that metering never reads.
pub(crate) struct PlanMeteringInfo {
    /// First-seen order, no duplicates. A KV `TransferItem` writes two
    /// collections in one plan; attributing only the first leaves it unbilled.
    collections: Vec<String>,
    engine: EngineTag,
    operation: &'static str,
    /// Carried alongside `operation` rather than re-derived: `operation` is a
    /// coarse cost-table key several plan shapes share, the wrong input for
    /// deciding which scopes a request needs.
    permission: Permission,
}

impl PlanMeteringInfo {
    /// Every collection this dispatch attributes to. Empty for
    /// cluster/algo/meta plans with no user-facing collection.
    pub(crate) fn collections(&self) -> &[String] {
        &self.collections
    }

    /// The [`Permission`] this dispatch required.
    pub(crate) fn permission(&self) -> Permission {
        self.permission
    }

    /// Extract `plan`'s metering shape. Call only when metering is enabled —
    /// it clones the collection name.
    pub(crate) fn extract(plan: &PhysicalPlan) -> Self {
        Self {
            collections: metered_collections(plan),
            engine: plan_engine(plan),
            operation: operation_for_plan(plan),
            permission: required_permission(plan),
        }
    }

    /// Build a metering shape directly, for dispatch doors with no
    /// [`PhysicalPlan`] to extract from — e.g. whole-tenant backup/restore,
    /// which operates on a tenant, not a single collection's plan.
    pub(crate) fn for_collection(
        collection: String,
        engine: EngineTag,
        operation: &'static str,
        permission: Permission,
    ) -> Self {
        Self {
            collections: vec![collection],
            engine,
            operation,
            permission,
        }
    }
}

/// Does `scope_name`'s grant set cover `permission` on `collection`? A grant
/// covers it when permission matches and collection is exact or `"*"`.
/// Permission strings parse via [`parse_permission`] for alias resolution.
pub(crate) fn scope_covers_request(
    state: &SharedState,
    scope_name: &str,
    permission: Permission,
    collection: &str,
) -> bool {
    state
        .scope_defs
        .resolve(scope_name)
        .into_iter()
        .any(|(perm_str, coll)| {
            parse_permission(&perm_str) == Some(permission) && (coll == "*" || coll == collection)
        })
}

/// Charge `tokens` against every quota scope `grantee_id` holds that covers this
/// request (see [`scope_covers_request`]). Two covering scopes are both charged
/// the full amount — each is an independent ledger.
fn charge_quota_for_held_scopes(
    state: &SharedState,
    auth: &AuthContext,
    info: &PlanMeteringInfo,
    collection: &str,
    tokens: u64,
    now_secs: u64,
) {
    let effective = state.scope_grants.effective_scopes(&auth.id, &auth.org_ids);
    for scope_name in &effective {
        if scope_covers_request(state, scope_name, info.permission, collection) {
            state
                .quota_manager
                .record_usage(scope_name, &auth.id, tokens, now_secs);
        }
    }
}

/// Meter one completed [`PhysicalTask`](nodedb_physical::physical_task::PhysicalTask)
/// dispatch, per-task not per-statement. Call only on the success path;
/// no-ops when disabled, internal-service, or the plan has no collection.
pub(crate) fn meter_dispatch(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    info: &PlanMeteringInfo,
    rows: Option<u64>,
) {
    if !state.metering_config.enabled {
        return;
    }
    if scope.identity().is_internal_service() {
        return;
    }
    if info.collections.is_empty() {
        return;
    }
    let engine = engine_tag_str(info.engine);
    let operation = info.operation;
    let operation_cost = state
        .metering_config
        .operation_costs
        .get(operation)
        .copied()
        .unwrap_or(1);
    // Never charge zero: even a point-get miss performed a lookup.
    let tokens = operation_cost.saturating_mul(rows.unwrap_or(1).max(1));

    let now_secs = crate::control::security::time::now_secs();
    // One charge per collection the plan writes into.
    for collection in &info.collections {
        charge_quota_for_held_scopes(state, scope.auth(), info, collection, tokens, now_secs);
        state.usage_counter.record(&UsageEvent {
            auth_user_id: scope.auth().id.clone(),
            org_id: scope.auth().org_id.clone().unwrap_or_default(),
            tenant_id: scope.tenant_id().as_u64(),
            collection: collection.clone(),
            engine: engine.to_string(),
            operation: operation.to_string(),
            tokens,
            // Filled in by `UsageCounter::drain`, not the caller.
            timestamp_secs: 0,
        });
    }
}

/// Meter an in-transaction `Staged` write's dispatch. The overlay write is
/// real engine work performed now — COMMIT decides durability, not
/// billability, so this meters here, not at COMMIT replay. `rows: None`.
pub(crate) fn meter_staged_write(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    plan: &PhysicalPlan,
    resp: &Response,
) {
    if resp.status != Status::Ok || !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(plan);
    meter_dispatch(state, scope, &info, None);
}

/// Meter one non-stageable buffered write, replayed durably at COMMIT.
/// `InTxnRoute::Buffered` dispatches nothing until COMMIT replays it. Call
/// only after the buffered batch's COMMIT dispatch succeeded.
pub(crate) fn meter_buffered_write(
    state: &SharedState,
    scope: &RequestAuthScope<'_>,
    plan: &PhysicalPlan,
) {
    if !state.metering_config.enabled {
        return;
    }
    let info = PlanMeteringInfo::extract(plan);
    meter_dispatch(state, scope, &info, None);
}

/// A metering accumulator for a streaming response, live for the whole stream.
/// Unlike [`meter_dispatch`], rows accumulate via [`Self::add_rows`] and are
/// recorded as one usage event when this guard drops.
pub(crate) struct DetachedMeterGuard {
    state: Arc<SharedState>,
    auth_user_id: String,
    org_id: String,
    tenant_id: u64,
    collection: String,
    engine: &'static str,
    operation: &'static str,
    rows: u64,
    /// Covering scopes (see [`scope_covers_request`]) filtered once at
    /// construction — coverage depends only on `info`, fixed at construction,
    /// so filtering here changes nothing about which scopes end up charged.
    quota_scopes: std::collections::HashSet<String>,
}

impl DetachedMeterGuard {
    /// Build a guard for `info`, or `None` when nothing should be metered —
    /// mirrors [`meter_dispatch`]'s own gating.
    pub(crate) fn new(
        state: &Arc<SharedState>,
        scope: &RequestAuthScope<'_>,
        info: &PlanMeteringInfo,
    ) -> Option<Self> {
        if !state.metering_config.enabled || scope.identity().is_internal_service() {
            return None;
        }
        // A streamed response reads exactly one collection; multi-collection shapes are KV writes.
        let collection = info.collections.first().cloned()?;
        let held = state
            .scope_grants
            .effective_scopes(&scope.auth().id, &scope.auth().org_ids);
        let quota_scopes = held
            .into_iter()
            .filter(|scope_name| {
                scope_covers_request(state, scope_name, info.permission, &collection)
            })
            .collect();
        Some(Self {
            state: Arc::clone(state),
            auth_user_id: scope.auth().id.clone(),
            org_id: scope.auth().org_id.clone().unwrap_or_default(),
            tenant_id: scope.tenant_id().as_u64(),
            collection,
            engine: engine_tag_str(info.engine),
            operation: info.operation,
            rows: 0,
            quota_scopes,
        })
    }

    /// Record that `n` more rows were actually written to the client.
    pub(crate) fn add_rows(&mut self, n: u64) {
        self.rows += n;
    }
}

impl Drop for DetachedMeterGuard {
    fn drop(&mut self) {
        let operation_cost = self
            .state
            .metering_config
            .operation_costs
            .get(self.operation)
            .copied()
            .unwrap_or(1);
        // Never charge zero: a closed stream still performed a lookup.
        let tokens = operation_cost.saturating_mul(self.rows.max(1));
        let now_secs = crate::control::security::time::now_secs();
        for scope_name in &self.quota_scopes {
            self.state
                .quota_manager
                .record_usage(scope_name, &self.auth_user_id, tokens, now_secs);
        }
        self.state.usage_counter.record(&UsageEvent {
            auth_user_id: self.auth_user_id.clone(),
            org_id: self.org_id.clone(),
            tenant_id: self.tenant_id,
            collection: self.collection.clone(),
            engine: self.engine.to_string(),
            operation: self.operation.to_string(),
            tokens,
            timestamp_secs: 0,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::KvOp;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::{DatabaseId, TenantId};
    use crate::wal::WalManager;

    use super::*;

    /// Caller must keep the `TempDir` guard alive, or the WAL's backing file
    /// is removed out from under it.
    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (state, dir)
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

    fn kv_get_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "widgets"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    /// A plan with no extractable collection (a graph hop carries no
    /// top-level collection field).
    fn no_collection_plan() -> PhysicalPlan {
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::CreateSnapshot)
    }

    /// `metering_config` has no live-mutation path by design; reach in via
    /// `Arc::get_mut` while the test is the sole owner.
    fn enable_metering(state: &mut Arc<SharedState>) {
        Arc::get_mut(state)
            .expect("sole owner in test")
            .metering_config
            .enabled = true;
    }

    fn scope_for<'a>(
        identity: &'a AuthenticatedIdentity,
        state: &'a SharedState,
    ) -> RequestAuthScope<'a> {
        RequestAuthScope::for_database(identity, state.auth_stores(), DatabaseId::DEFAULT)
    }

    #[test]
    fn disabled_config_records_nothing() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = regular_identity(1);
        state
            .scope_defs
            .define(
                "pro:all",
                vec![("read".into(), "widgets".into())],
                vec![],
                "admin",
            )
            .expect("define scope");
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "1",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .expect("grant scope");
        state
            .quota_manager
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
        assert_eq!(
            state
                .quota_manager
                .get_status("pro:all", "1", 0)
                .expect("quota defined")
                .used_tokens,
            0,
            "metering disabled must not charge quota either"
        );
    }

    #[test]
    fn internal_service_identity_is_never_metered() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = internal_service_identity(2);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn plan_with_no_collection_is_not_metered() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(3);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&no_collection_plan()),
            None,
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn enabled_plan_records_exactly_one_event_with_correct_fields() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(4);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.collection, "widgets");
        assert_eq!(event.engine, "kv");
        assert_eq!(event.operation, "kv_scan");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(event.tokens, expected_cost * 3);
    }

    /// A metered dispatch by an identity holding a covering scope with a quota
    /// defined must charge it the same `tokens` value `UsageCounter::record` gets.
    #[test]
    fn metered_dispatch_charges_quota_for_covering_held_scope() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(20);
        state
            .scope_defs
            .define(
                "pro:all",
                vec![("read".into(), "widgets".into())],
                vec![],
                "admin",
            )
            .expect("define scope");
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "user",
                grantee_id: "20",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .expect("grant scope");
        state
            .quota_manager
            .define_quota(QuotaDefinition {
                scope_name: "pro:all".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1)
            * 3;
        let status = state
            .quota_manager
            .get_status("pro:all", "20", 0)
            .expect("quota defined for held scope");
        assert_eq!(status.used_tokens, expected_cost);
    }

    /// A held scope whose grants do not cover the request's `(permission,
    /// collection)` must not be charged.
    #[test]
    fn held_scope_not_covering_request_is_not_charged() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(21);
        // Held scope grants vector search only — doesn't cover a KV `Get` on "widgets".
        state
            .scope_defs
            .define(
                "vector:heavy",
                vec![("read".into(), "embeddings".into())],
                vec![],
                "admin",
            )
            .expect("define scope");
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "vector:heavy",
                grantee_type: "user",
                grantee_id: "21",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .expect("grant scope");
        state
            .quota_manager
            .define_quota(QuotaDefinition {
                scope_name: "vector:heavy".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let status = state
            .quota_manager
            .get_status("vector:heavy", "21", 0)
            .expect("quota defined for held scope");
        assert_eq!(
            status.used_tokens, 0,
            "a held scope that does not cover the request must not be charged"
        );
    }

    /// A wildcard `"*"` collection grant covers every collection — the same
    /// convention `CREATE CHANGE STREAM ... ON *` and retention-policy DDL
    /// already use elsewhere in the codebase.
    #[test]
    fn wildcard_collection_grant_is_charged() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(22);
        state
            .scope_defs
            .define(
                "all:read",
                vec![("read".into(), "*".into())],
                vec![],
                "admin",
            )
            .expect("define scope");
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "all:read",
                grantee_type: "user",
                grantee_id: "22",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .expect("grant scope");
        state
            .quota_manager
            .define_quota(QuotaDefinition {
                scope_name: "all:read".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let status = state
            .quota_manager
            .get_status("all:read", "22", 0)
            .expect("quota defined for held scope");
        assert!(
            status.used_tokens > 0,
            "a wildcard collection grant must cover any collection"
        );
    }

    /// Two held scopes both covering the request are both charged the full
    /// amount, independently — a data cap and a feature cap are separate
    /// meters on the same billable event, not a single charge to dedupe.
    #[test]
    fn two_covering_scopes_are_both_charged_in_full() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(23);
        for scope_name in ["data:cap", "feature:cap"] {
            state
                .scope_defs
                .define(
                    scope_name,
                    vec![("read".into(), "widgets".into())],
                    vec![],
                    "admin",
                )
                .expect("define scope");
            state
                .scope_grants
                .grant(ScopeGrantParams {
                    scope_name,
                    grantee_type: "user",
                    grantee_id: "23",
                    granted_by: "admin",
                    expires_at: 0,
                    grace_period_secs: 0,
                    on_expire_action: "",
                    conditions: Vec::new(),
                })
                .expect("grant scope");
            state
                .quota_manager
                .define_quota(QuotaDefinition {
                    scope_name: scope_name.into(),
                    max_tokens: 1000,
                    period_secs: 86400,
                    enforcement: QuotaEnforcement::Hard,
                    warning_threshold: 0.8,
                })
                .expect("define quota in test");
        }
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1)
            * 3;
        for scope_name in ["data:cap", "feature:cap"] {
            let status = state
                .quota_manager
                .get_status(scope_name, "23", 0)
                .expect("quota defined for held scope");
            assert_eq!(
                status.used_tokens, expected_cost,
                "scope '{scope_name}' must be charged the full amount independently"
            );
        }
    }

    /// No held scope covers the request → nothing is charged. This is the
    /// correct outcome, not a gap: an identity with no entitlement for this
    /// `(permission, collection)` pair has no meter to debit.
    #[test]
    fn no_covering_scope_charges_nothing() {
        use crate::control::security::metering::quota::{QuotaDefinition, QuotaEnforcement};
        use crate::control::security::scope::grant::ScopeGrantParams;

        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(24);
        // Held scope covers WRITE, not the READ this KV `Get` requires.
        state
            .scope_defs
            .define(
                "write:only",
                vec![("write".into(), "widgets".into())],
                vec![],
                "admin",
            )
            .expect("define scope");
        state
            .scope_grants
            .grant(ScopeGrantParams {
                scope_name: "write:only",
                grantee_type: "user",
                grantee_id: "24",
                granted_by: "admin",
                expires_at: 0,
                grace_period_secs: 0,
                on_expire_action: "",
                conditions: Vec::new(),
            })
            .expect("grant scope");
        state
            .quota_manager
            .define_quota(QuotaDefinition {
                scope_name: "write:only".into(),
                max_tokens: 1000,
                period_secs: 86400,
                enforcement: QuotaEnforcement::Hard,
                warning_threshold: 0.8,
            })
            .expect("define quota in test");
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(3),
        );

        let status = state
            .quota_manager
            .get_status("write:only", "24", 0)
            .expect("quota defined for held scope");
        assert_eq!(status.used_tokens, 0);
    }

    #[test]
    fn none_and_zero_rows_both_charge_at_least_one_unit() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(5);
        let scope = scope_for(&identity, &state);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            None,
        );
        let events_none = state.usage_counter.drain();
        assert_eq!(events_none.len(), 1);
        assert!(events_none[0].tokens >= 1);

        meter_dispatch(
            &state,
            &scope,
            &PlanMeteringInfo::extract(&kv_get_plan()),
            Some(0),
        );
        let events_zero = state.usage_counter.drain();
        assert_eq!(events_zero.len(), 1);
        assert!(events_zero[0].tokens >= 1);
    }

    /// `operation_for_plan` moved here from `shared::ddl::user_dispatch` —
    /// this pins its existing behavior for the operation strings that
    /// module's rate-limiter classification depends on.
    #[test]
    fn operation_for_plan_matches_expected_vocabulary() {
        assert_eq!(operation_for_plan(&kv_get_plan()), "kv_scan");
        assert_eq!(
            operation_for_plan(&no_collection_plan()),
            "sql",
            "Meta ops with no cost-table counterpart fall back to \"sql\""
        );
    }

    #[test]
    fn detached_guard_disabled_config_records_nothing() {
        let (state, _dir) = test_state();
        let identity = regular_identity(6);
        let scope = scope_for(&identity, &state);

        let guard =
            DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()));
        assert!(guard.is_none(), "disabled config must not build a guard");
        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// A stream that drops early (disconnect, error) still bills exactly what
    /// it wrote, not zero and not a full scan — the case that matters when
    /// `async_stream::stream!`'s generator future drops.
    #[test]
    fn detached_guard_bills_rows_written_before_early_drop() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(7);
        let scope = scope_for(&identity, &state);

        {
            let mut guard =
                DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()))
                    .expect("metering enabled, collection present");
            guard.add_rows(3);
            guard.add_rows(4);
            // Dropped mid-stream, simulating an early client disconnect after 7 rows.
        }

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1, "exactly one event, recorded on drop");
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
        let expected_cost = state
            .metering_config
            .operation_costs
            .get("kv_scan")
            .copied()
            .unwrap_or(1);
        assert_eq!(events[0].tokens, expected_cost * 7);
    }

    #[test]
    fn detached_guard_charges_one_unit_when_no_rows_were_ever_written() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(8);
        let scope = scope_for(&identity, &state);

        drop(
            DetachedMeterGuard::new(&state, &scope, &PlanMeteringInfo::extract(&kv_get_plan()))
                .expect("metering enabled, collection present"),
        );

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert!(events[0].tokens >= 1);
    }

    fn fake_response(status: Status) -> Response {
        Response {
            request_id: crate::types::RequestId::new(0),
            status,
            attempt: 0,
            partial: false,
            payload: crate::bridge::envelope::Payload::empty(),
            watermark_lsn: crate::types::Lsn::new(0),
            error_code: None,
            stage_vote: None,
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        }
    }

    /// A staged in-transaction write's overlay dispatch succeeded — this is
    /// the real engine work `staging_gate::stage_write` performs, so it must
    /// be billed regardless of what COMMIT later does with it.
    #[test]
    fn staged_write_records_on_ok_response() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(9);
        let scope = scope_for(&identity, &state);

        meter_staged_write(&state, &scope, &kv_get_plan(), &fake_response(Status::Ok));

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    /// A staged write's overlay dispatch was rejected (a statement-time
    /// constraint violation) — nothing durable happened, so nothing is
    /// billed.
    #[test]
    fn staged_write_records_nothing_on_error_response() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(10);
        let scope = scope_for(&identity, &state);

        meter_staged_write(
            &state,
            &scope,
            &kv_get_plan(),
            &fake_response(Status::Error),
        );

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    #[test]
    fn staged_write_records_nothing_when_metering_disabled() {
        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = regular_identity(11);
        let scope = scope_for(&identity, &state);

        meter_staged_write(&state, &scope, &kv_get_plan(), &fake_response(Status::Ok));

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// Pins that the call records unconditionally — the success gate lives in
    /// the caller, `session::commit::run_commit`.
    #[test]
    fn buffered_write_records_when_called() {
        let (mut state, _dir) = test_state();
        enable_metering(&mut state);
        let identity = regular_identity(12);
        let scope = scope_for(&identity, &state);

        meter_buffered_write(&state, &scope, &kv_get_plan());

        let events = state.usage_counter.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection, "widgets");
        assert_eq!(events[0].engine, "kv");
    }

    #[test]
    fn buffered_write_records_nothing_when_metering_disabled() {
        let (state, _dir) = test_state();
        assert!(!state.metering_config.enabled, "default config is disabled");
        let identity = regular_identity(13);
        let scope = scope_for(&identity, &state);

        meter_buffered_write(&state, &scope, &kv_get_plan());

        assert_eq!(state.usage_counter.total_tokens(), 0);
    }

    /// Pins the ILP-ingest metering shape: `TimeseriesOp::Ingest` extracts its
    /// `collection` and maps to the `timeseries` engine dimension.
    #[test]
    fn timeseries_ingest_plan_extracts_collection_and_engine() {
        let plan = PhysicalPlan::Timeseries(nodedb_physical::physical_plan::TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "cpu"),
            payload: Vec::new(),
            format: "ilp-msgpack".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let info = PlanMeteringInfo::extract(&plan);
        assert_eq!(info.collections, vec!["cpu".to_string()]);
        assert_eq!(engine_tag_str(info.engine), "timeseries");
    }
}
