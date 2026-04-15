//! Top-level dispatcher: iterate every registry verifier,
//! aggregate divergence counts per registry, and repair any
//! divergences found. A second verify pass after repair
//! detects bugs where `load_from` is not idempotent (the
//! same divergence re-appears after a fresh re-load).

use std::collections::HashMap;

use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

use super::super::divergence::Divergence;
use super::super::report::RegistryDivergenceCount;
use super::{
    alert, api_keys, blacklist, change_stream, consumer_group, credential, materialized_view,
    permissions, retention_policy, rls_policy, roles, schedule, triggers,
};

/// Outcome of the registry pass.
pub struct RegistryVerifyOutcome {
    /// Per-registry divergence count (detected + repaired).
    pub counts: HashMap<&'static str, RegistryDivergenceCount>,
    /// `true` if every registry that needed repair reported
    /// zero divergences on the post-repair verify pass.
    pub all_repairs_ok: bool,
    /// Full list of initial divergences observed, for
    /// logging.
    pub initial_divergences: Vec<Divergence>,
}

/// Run every registered verifier against `shared` + `catalog`.
/// Repair any divergences in place. Re-verify after repair
/// and flag any residual divergence as `all_repairs_ok = false`.
pub fn verify_registries(
    shared: &SharedState,
    catalog: &SystemCatalog,
) -> crate::Result<RegistryVerifyOutcome> {
    let mut counts: HashMap<&'static str, RegistryDivergenceCount> = HashMap::new();
    let mut initial_divergences: Vec<Divergence> = Vec::new();
    let mut all_repairs_ok = true;

    // ── permissions ─────────────────────────────────────
    run_one(
        "permissions",
        || permissions::verify_permissions(&shared.permissions, catalog),
        || permissions::repair_permissions(&shared.permissions, catalog),
        || permissions::verify_permissions(&shared.permissions, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── triggers ────────────────────────────────────────
    run_one(
        "triggers",
        || triggers::verify_triggers(&shared.trigger_registry, catalog),
        || triggers::repair_triggers(&shared.trigger_registry, catalog),
        || triggers::verify_triggers(&shared.trigger_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── roles ───────────────────────────────────────────
    run_one(
        "roles",
        || roles::verify_roles(&shared.roles, catalog),
        || roles::repair_roles(&shared.roles, catalog),
        || roles::verify_roles(&shared.roles, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── api_keys ────────────────────────────────────────
    run_one(
        "api_keys",
        || api_keys::verify_api_keys(&shared.api_keys, catalog),
        || api_keys::repair_api_keys(&shared.api_keys, catalog),
        || api_keys::verify_api_keys(&shared.api_keys, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── rls_policies ────────────────────────────────────
    run_one(
        "rls_policies",
        || rls_policy::verify_rls_policies(&shared.rls, catalog),
        || rls_policy::repair_rls_policies(&shared.rls, catalog),
        || rls_policy::verify_rls_policies(&shared.rls, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── blacklist ───────────────────────────────────────
    run_one(
        "blacklist",
        || blacklist::verify_blacklist(&shared.blacklist, catalog),
        || blacklist::repair_blacklist(&shared.blacklist, catalog),
        || blacklist::verify_blacklist(&shared.blacklist, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── schedules ───────────────────────────────────────
    run_one(
        "schedules",
        || schedule::verify_schedules(&shared.schedule_registry, catalog),
        || schedule::repair_schedules(&shared.schedule_registry, catalog),
        || schedule::verify_schedules(&shared.schedule_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── alert_rules ─────────────────────────────────────
    run_one(
        "alert_rules",
        || alert::verify_alerts(&shared.alert_registry, catalog),
        || alert::repair_alerts(&shared.alert_registry, catalog),
        || alert::verify_alerts(&shared.alert_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── streaming_mvs ────────────────────────────────────
    run_one(
        "streaming_mvs",
        || materialized_view::verify_mvs(&shared.mv_registry, catalog),
        || materialized_view::repair_mvs(&shared.mv_registry, catalog),
        || materialized_view::verify_mvs(&shared.mv_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── change_streams ───────────────────────────────────
    run_one(
        "change_streams",
        || change_stream::verify_change_streams(&shared.stream_registry, catalog),
        || change_stream::repair_change_streams(&shared.stream_registry, catalog),
        || change_stream::verify_change_streams(&shared.stream_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── consumer_groups ──────────────────────────────────
    run_one(
        "consumer_groups",
        || consumer_group::verify_consumer_groups(&shared.group_registry, catalog),
        || consumer_group::repair_consumer_groups(&shared.group_registry, catalog),
        || consumer_group::verify_consumer_groups(&shared.group_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── retention_policies ───────────────────────────────
    run_one(
        "retention_policies",
        || retention_policy::verify_retention_policies(&shared.retention_policy_registry, catalog),
        || retention_policy::repair_retention_policies(&shared.retention_policy_registry, catalog),
        || retention_policy::verify_retention_policies(&shared.retention_policy_registry, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    // ── credentials ──────────────────────────────────────
    run_one(
        "credentials",
        || credential::verify_credentials(&shared.credentials, catalog),
        || credential::repair_credentials(&shared.credentials, catalog),
        || credential::verify_credentials(&shared.credentials, catalog),
        &mut counts,
        &mut initial_divergences,
        &mut all_repairs_ok,
    )?;

    Ok(RegistryVerifyOutcome {
        counts,
        all_repairs_ok,
        initial_divergences,
    })
}

/// Run one verify → repair → re-verify cycle for a single registry.
///
/// Encapsulates the repetitive pattern to keep each call site a
/// single `run_one(...)` invocation rather than 15 lines of copy-paste.
fn run_one(
    name: &'static str,
    verify: impl Fn() -> crate::Result<Vec<Divergence>>,
    repair: impl Fn() -> crate::Result<()>,
    verify_post: impl Fn() -> crate::Result<Vec<Divergence>>,
    counts: &mut HashMap<&'static str, RegistryDivergenceCount>,
    initial_divergences: &mut Vec<Divergence>,
    all_repairs_ok: &mut bool,
) -> crate::Result<()> {
    let div = verify()?;
    if div.is_empty() {
        return Ok(());
    }

    counts.entry(name).or_default().detected += div.len();
    for d in &div {
        tracing::error!(divergence = %d, registry = name, "catalog sanity check: divergence");
    }
    initial_divergences.extend(div.iter().cloned());

    repair()?;

    let post = verify_post()?;
    if post.is_empty() {
        counts.entry(name).or_default().repaired += div.len();
    } else {
        *all_repairs_ok = false;
        tracing::error!(
            residual = post.len(),
            registry = name,
            "catalog sanity check: repair failed — residual divergences"
        );
    }
    Ok(())
}
