// SPDX-License-Identifier: BUSL-1.1

//! redb cross-table referential integrity checks.
//!
//! redb transactions are atomic per-write but NOT across
//! tables. A crash mid-apply (or a code bug in the applier)
//! can leave any of the following invariants broken:
//!
//! - Every parent-replicated DDL object (collection, function,
//!   procedure, trigger, materialized_view, sequence, schedule,
//!   change_stream) has a matching `StoredOwner` row keyed by
//!   its `object_type`. The primary row's `owner` field is
//!   canonical; the `OWNERS` table is the persistent backing
//!   for the in-memory `PermissionStore.owners` HashMap and
//!   must be rewritten in lockstep with every primary write.
//! - Every `StoredOwner.owner_username` resolves to a
//!   `StoredUser`.
//! - Every `StoredPermission.grantee` resolves to either a
//!   `StoredUser` (when prefixed `"user:"`) or a
//!   `StoredRole`.
//! - Every `StoredTrigger.collection` exists as a
//!   `StoredCollection` row.
//! - Every `StoredRlsPolicy.collection` exists as a
//!   `StoredCollection` row.
//!
//! None of these are auto-repaired. Redb is not the source of
//! truth — the raft log is — and the safe recovery for any
//! redb corruption is "re-run the applier from the log",
//! which is the operator's job. The integrity check reports
//! every violation and the sanity-check wrapper aborts
//! startup on any non-empty violation list.

use std::collections::HashSet;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;

use super::divergence::{Divergence, DivergenceKind};

type ScopedObjectKey = (u64, u64, String);
type ParentOwnerRows = (&'static str, Vec<ScopedObjectKey>);

/// Run every cross-table integrity invariant against the
/// current redb state and return every violation found.
/// Never panics, never writes.
pub fn verify_redb_integrity(catalog: &SystemCatalog) -> Vec<Divergence> {
    let mut violations: Vec<Divergence> = Vec::new();

    // Load every table the cross-checks below need, once, up front.
    // A table we cannot read — missing because it was never in the
    // catalog's `BOOTSTRAP_TABLES` registry, or a redb read error —
    // makes every check that touches it meaningless: cross-checking
    // against an empty stand-in manufactures phantom orphan /
    // dangling-reference reports. So a load failure records a
    // `TableLoadError` divergence and bails the walk. That divergence
    // is non-empty, so the `CatalogSanityCheck` wrapper aborts startup
    // — which is correct: a catalog we cannot fully read is not one we
    // can certify.
    macro_rules! load_table {
        ($table:literal, $expr:expr) => {
            match $expr {
                Ok(v) => v,
                Err(e) => {
                    violations.push(Divergence::new(DivergenceKind::TableLoadError {
                        table: $table,
                        detail: e.to_string(),
                    }));
                    return violations;
                }
            }
        };
    }

    let collections = load_table!(
        "collections",
        catalog.load_all_collections_across_databases()
    );
    let owners = load_table!("owners", catalog.load_all_owners());
    let users = load_table!("users", catalog.load_all_users());
    let roles = load_table!("roles", catalog.load_all_roles());
    let permissions = load_table!("permissions", catalog.load_all_permissions());
    let triggers = load_table!("triggers", catalog.load_all_triggers());
    let functions = load_table!("functions", catalog.load_all_functions());
    let procedures = load_table!("procedures", catalog.load_all_procedures());
    let materialized_views =
        load_table!("materialized_views", catalog.load_all_materialized_views());
    let sequences = load_table!("sequences", catalog.load_all_sequences());
    let schedules = load_table!("schedules", catalog.load_all_schedules());
    let change_streams = load_table!("change_streams", catalog.load_all_change_streams());
    let continuous_aggregates = load_table!(
        "continuous_aggregates",
        catalog.load_all_continuous_aggregates()
    );
    let rls = load_table!("rls_policies", catalog.load_all_rls_policies());

    // Build lookup sets once — every referential check is a
    // HashSet membership probe.
    let collection_keys: HashSet<(u64, String)> = collections
        .iter()
        .map(|c| (c.tenant_id, c.name.clone()))
        .collect();
    let user_names: HashSet<String> = users.iter().map(|u| u.username.clone()).collect();
    let role_names: HashSet<String> = roles.iter().map(|r| r.name.clone()).collect();
    let owner_keys: HashSet<(String, u64, u64, String)> = owners
        .iter()
        .map(|o| {
            (
                o.object_type.clone(),
                o.database_id,
                o.tenant_id,
                o.object_name.clone(),
            )
        })
        .collect();

    // ── Check 1: every parent-replicated DDL object has an owner. ──
    // Table-driven so a new parent-replicated type only needs one
    // row added here plus its `apply/<type>.rs::put` call to
    // `owner::put_parent_owner`. Omitting either half trips an
    // OrphanRow on the next restart.
    let parent_replicated: [ParentOwnerRows; 9] = [
        (
            object_type::COLLECTION,
            // Active AND soft-deleted collections both require an
            // owner row. `DeactivateCollection` preserves the
            // primary record for undrop and must preserve the
            // owner alongside it; splitting them would break
            // undrop ownership restoration.
            collections
                .iter()
                .map(|c| (c.database_id.as_u64(), c.tenant_id, c.name.clone()))
                .collect(),
        ),
        (
            object_type::FUNCTION,
            functions
                .iter()
                .map(|f| (0, f.tenant_id, f.name.clone()))
                .collect(),
        ),
        (
            object_type::PROCEDURE,
            procedures
                .iter()
                .map(|p| (0, p.tenant_id, p.name.clone()))
                .collect(),
        ),
        (
            object_type::TRIGGER,
            triggers
                .iter()
                .map(|t| (0, t.tenant_id, t.name.clone()))
                .collect(),
        ),
        (
            object_type::MATERIALIZED_VIEW,
            materialized_views
                .iter()
                .map(|m| (0, m.tenant_id, m.name.clone()))
                .collect(),
        ),
        (
            object_type::SEQUENCE,
            sequences
                .iter()
                .map(|s| (0, s.tenant_id, s.name.clone()))
                .collect(),
        ),
        (
            object_type::SCHEDULE,
            schedules
                .iter()
                .map(|s| (0, s.tenant_id, s.name.clone()))
                .collect(),
        ),
        (
            object_type::CHANGE_STREAM,
            change_streams
                .iter()
                .map(|c| (0, c.tenant_id, c.name.clone()))
                .collect(),
        ),
        (
            object_type::CONTINUOUS_AGGREGATE,
            continuous_aggregates
                .iter()
                .map(|c| (c.database_id, c.tenant_id, c.name.clone()))
                .collect(),
        ),
    ];
    for (kind, rows) in &parent_replicated {
        for (database_id, tenant, name) in rows {
            let key = ((*kind).to_string(), *database_id, *tenant, name.clone());
            if !owner_keys.contains(&key) {
                violations.push(Divergence::new(DivergenceKind::OrphanRow {
                    kind,
                    key: format!("{database_id}:{tenant}:{name}"),
                    expected_parent_kind: "owner",
                }));
            }
        }
    }

    // ── Check 2: every owner.owner_username resolves to a user. ──
    for o in &owners {
        if !user_names.contains(&o.owner_username) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "owner",
                from_key: format!(
                    "{}:{}:{}:{}",
                    o.object_type, o.database_id, o.tenant_id, o.object_name
                ),
                to_kind: "user",
                to_key: o.owner_username.clone(),
            }));
        }
    }

    // ── Check 3: every permission.grantee resolves. ──
    for p in &permissions {
        // `grantee` is either `"user:<name>"` or `"<role>"`.
        if let Some(username) = p.grantee.strip_prefix("user:") {
            if !user_names.contains(username) {
                violations.push(Divergence::new(DivergenceKind::DanglingReference {
                    from_kind: "permission",
                    from_key: format!("{}:{}", p.target, p.grantee),
                    to_kind: "user",
                    to_key: username.to_string(),
                }));
            }
        } else {
            // Role grantee — check role exists. Built-in
            // roles ("admin", "readonly", etc.) are NOT in the
            // StoredRole table (they live in the identity
            // module), so we only flag unknown custom names
            // that contain no built-in marker.
            if !role_names.contains(&p.grantee) && !is_builtin_role(&p.grantee) {
                violations.push(Divergence::new(DivergenceKind::DanglingReference {
                    from_kind: "permission",
                    from_key: format!("{}:{}", p.target, p.grantee),
                    to_kind: "role",
                    to_key: p.grantee.clone(),
                }));
            }
        }
    }

    // ── Check 4: every trigger.collection exists. ──
    for t in &triggers {
        let key = (t.tenant_id, t.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "trigger",
                from_key: format!("{}:{}", t.tenant_id, t.name),
                to_kind: "collection",
                to_key: format!("{}:{}", t.tenant_id, t.collection),
            }));
        }
    }

    // ── Check 5: every rls_policy.collection exists. ──
    for p in &rls {
        let key = (p.tenant_id, p.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "rls_policy",
                from_key: format!("{}:{}", p.tenant_id, p.name),
                to_kind: "collection",
                to_key: format!("{}:{}", p.tenant_id, p.collection),
            }));
        }
    }

    // ── Check 6: every materialized_view.source exists as a
    //              collection. ──
    //
    // An MV whose source was purged (or never existed on this node)
    // will silently refresh against nothing. Surface as a dangling
    // reference so operators know to drop the stale MV or restore
    // the source. Cascade-delete of MVs on `PurgeCollection` is the
    // preventive path; this check is the detective path.
    for mv in &materialized_views {
        let key = (mv.tenant_id, mv.source.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "materialized_view",
                from_key: format!("{}:{}", mv.tenant_id, mv.name),
                to_kind: "collection",
                to_key: format!("{}:{}", mv.tenant_id, mv.source),
            }));
        }
    }

    // ── Check 7: every change_stream.collection exists as a
    //              collection, unless it's the wildcard `*` which
    //              matches any collection for the tenant. ──
    for cs in &change_streams {
        if cs.collection == "*" {
            continue;
        }
        let key = (cs.tenant_id, cs.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "change_stream",
                from_key: format!("{}:{}", cs.tenant_id, cs.name),
                to_kind: "collection",
                to_key: format!("{}:{}", cs.tenant_id, cs.collection),
            }));
        }
    }

    // ── Check 8: every schedule.target_collection (when Some) exists
    //              as a collection. `None` means the schedule is
    //              cross-collection or opaque (runs on `_system`
    //              coordinator) and is exempt. ──
    for sch in &schedules {
        let Some(target) = &sch.target_collection else {
            continue;
        };
        let key = (sch.tenant_id, target.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "schedule",
                from_key: format!("{}:{}", sch.tenant_id, sch.name),
                to_kind: "collection",
                to_key: format!("{}:{}", sch.tenant_id, target),
            }));
        }
    }

    let _ = (functions, procedures, sequences);

    violations
}

/// Built-in role names that exist outside the `StoredRole`
/// table. These must match the set in
/// `security::identity::Role`.
fn is_builtin_role(name: &str) -> bool {
    matches!(
        name,
        "superuser" | "tenant_admin" | "readwrite" | "readonly" | "monitor"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_role_detection() {
        assert!(is_builtin_role("superuser"));
        assert!(is_builtin_role("readonly"));
        assert!(is_builtin_role("monitor"));
        assert!(!is_builtin_role("admin"));
        assert!(!is_builtin_role("custom_auditor"));
    }
}
