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
//! This module only *detects*; it never repairs. Redb is not the
//! source of truth — the raft log is — and the general recovery
//! for redb corruption is "re-run the applier from the log",
//! which is the operator's job.
//!
//! Three ownership/grant classes are healed before the abort gate,
//! because they are reachable from ordinary DDL rather than from
//! storage corruption, and each will otherwise leave an existing
//! data directory permanently unbootable with no repair path.
//! `verify_and_repair` runs `repair_integrity::heal_orphan_rows`
//! to a fixpoint over this module's output:
//!
//! - a primary row whose `StoredOwner` is missing — the owner row
//!   is rebuilt from the primary's in-band owner;
//! - `owner(...)` → `user(...)` dangling — the owner row is
//!   restored from the primary's in-band owner when that user
//!   still exists, otherwise reassigned to the tenant's resolved
//!   administrative principal;
//! - `permission(...)` → `user(...)` dangling — the grant is
//!   revoked, since a grant to a nonexistent user confers nothing.
//!
//! Every other violation above is reported and left alone.
//! Startup aborts only on violations that survive the pass.

use std::collections::HashSet;

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::{
    StoredMaterializedView, StoredOwner, StoredPermission, StoredRlsPolicy, StoredTrigger,
    SystemCatalog,
};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::event::scheduler::ScheduleDef;

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
    let streaming_materialized_views =
        load_table!("streaming_mvs", catalog.load_all_streaming_mvs());
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
    let collection_keys: HashSet<(u64, u64, String)> = collections
        .iter()
        .map(|c| (c.database_id.as_u64(), c.tenant_id, c.name.clone()))
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
    let parent_replicated: [ParentOwnerRows; 10] = [
        (
            object_type::COLLECTION,
            // Active AND soft-deleted collections both require an
            // owner row. `DeactivateCollection` preserves the
            // primary record for undrop and must preserve the
            // owner alongside it; splitting them will break
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
                .map(|f| (f.database_id.as_u64(), f.tenant_id, f.name.clone()))
                .collect(),
        ),
        (
            object_type::PROCEDURE,
            procedures
                .iter()
                .map(|p| (p.database_id.as_u64(), p.tenant_id, p.name.clone()))
                .collect(),
        ),
        (
            object_type::TRIGGER,
            triggers
                .iter()
                .map(|t| (t.database_id.as_u64(), t.tenant_id, t.name.clone()))
                .collect(),
        ),
        (
            object_type::MATERIALIZED_VIEW,
            materialized_views
                .iter()
                .map(|m| (m.database_id, m.tenant_id, m.name.clone()))
                .collect(),
        ),
        (
            object_type::STREAMING_MATERIALIZED_VIEW,
            streaming_materialized_views
                .iter()
                .map(|m| (m.database_id.as_u64(), m.tenant_id, m.name.clone()))
                .collect(),
        ),
        (
            object_type::SEQUENCE,
            sequences
                .iter()
                .map(|s| (s.database_id, s.tenant_id, s.name.clone()))
                .collect(),
        ),
        (
            object_type::SCHEDULE,
            schedules
                .iter()
                .map(|s| (s.database_id, s.tenant_id, s.name.clone()))
                .collect(),
        ),
        (
            object_type::CHANGE_STREAM,
            change_streams
                .iter()
                .map(|c| (c.database_id.as_u64(), c.tenant_id, c.name.clone()))
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
    check_parent_owners(&parent_replicated, &owner_keys, &mut violations);
    check_owner_users(&owners, &user_names, &mut violations);
    check_permission_grantees(&permissions, &user_names, &role_names, &mut violations);
    check_trigger_collections(&triggers, &collection_keys, &mut violations);
    check_rls_collections(&rls, &collection_keys, &mut violations);
    check_materialized_view_sources(&materialized_views, &collection_keys, &mut violations);
    check_change_stream_collections(&change_streams, &collection_keys, &mut violations);
    check_schedule_targets(&schedules, &collection_keys, &mut violations);

    violations
}

/// Database-scoped collection keys: `(database_id, tenant_id, name)`.
type CollectionKeys = HashSet<(u64, u64, String)>;
/// Owner row keys: `(object_type, database_id, tenant_id, name)`.
type OwnerKeys = HashSet<(String, u64, u64, String)>;

/// Check 1: every parent-replicated DDL object has an owner.
fn check_parent_owners(
    parent_replicated: &[ParentOwnerRows],
    owner_keys: &OwnerKeys,
    violations: &mut Vec<Divergence>,
) {
    for (kind, rows) in parent_replicated {
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
}

/// Check 2: every owner.owner_username resolves to a user.
fn check_owner_users(
    owners: &[StoredOwner],
    user_names: &HashSet<String>,
    violations: &mut Vec<Divergence>,
) {
    for o in owners {
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
}

/// Check 3: every permission.grantee resolves.
fn check_permission_grantees(
    permissions: &[StoredPermission],
    user_names: &HashSet<String>,
    role_names: &HashSet<String>,
    violations: &mut Vec<Divergence>,
) {
    for p in permissions {
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
}

/// Check 4: every trigger.collection exists.
fn check_trigger_collections(
    triggers: &[StoredTrigger],
    collection_keys: &CollectionKeys,
    violations: &mut Vec<Divergence>,
) {
    for t in triggers {
        let database_id = t.database_id.as_u64();
        let key = (database_id, t.tenant_id, t.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "trigger",
                from_key: format!("{database_id}:{}:{}", t.tenant_id, t.name),
                to_kind: "collection",
                to_key: format!("{database_id}:{}:{}", t.tenant_id, t.collection),
            }));
        }
    }
}

/// Check 5: every rls_policy.collection exists in the policy's database.
///
/// A collection qualifier that does not match the policy's database is a
/// dangling reference too: it names no collection that database holds.
fn check_rls_collections(
    rls: &[StoredRlsPolicy],
    collection_keys: &CollectionKeys,
    violations: &mut Vec<Divergence>,
) {
    for p in rls {
        let found = p.collection_key().is_ok_and(|key| {
            collection_keys.contains(&(p.database_id, p.tenant_id, key.name().to_owned()))
        });
        if !found {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "rls_policy",
                from_key: format!("{}:{}:{}", p.database_id, p.tenant_id, p.name),
                to_kind: "collection",
                to_key: format!("{}:{}:{}", p.database_id, p.tenant_id, p.collection),
            }));
        }
    }
}

/// Check 6: every materialized_view.source exists as a collection.
///
/// An MV whose source was purged (or never existed on this node)
/// will silently refresh against nothing. Surface as a dangling
/// reference so operators know to drop the stale MV or restore
/// the source. Cascade-delete of MVs on `PurgeCollection` is the
/// preventive path; this check is the detective path.
fn check_materialized_view_sources(
    materialized_views: &[StoredMaterializedView],
    collection_keys: &CollectionKeys,
    violations: &mut Vec<Divergence>,
) {
    for mv in materialized_views {
        let key = (mv.database_id, mv.tenant_id, mv.source.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "materialized_view",
                from_key: format!("{}:{}:{}", mv.database_id, mv.tenant_id, mv.name),
                to_kind: "collection",
                to_key: format!("{}:{}:{}", mv.database_id, mv.tenant_id, mv.source),
            }));
        }
    }
}

/// Check 7: every change_stream.collection exists as a collection,
/// unless it's the wildcard `*` which matches any collection for the
/// tenant.
fn check_change_stream_collections(
    change_streams: &[ChangeStreamDef],
    collection_keys: &CollectionKeys,
    violations: &mut Vec<Divergence>,
) {
    for cs in change_streams {
        if cs.collection == "*" {
            continue;
        }
        let database_id = cs.database_id.as_u64();
        let key = (database_id, cs.tenant_id, cs.collection.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "change_stream",
                from_key: format!("{database_id}:{}:{}", cs.tenant_id, cs.name),
                to_kind: "collection",
                to_key: format!("{database_id}:{}:{}", cs.tenant_id, cs.collection),
            }));
        }
    }
}

/// Check 8: every schedule.target_collection (when Some) exists as a
/// collection. `None` means the schedule is cross-collection or opaque
/// (runs on `_system` coordinator) and is exempt.
fn check_schedule_targets(
    schedules: &[ScheduleDef],
    collection_keys: &CollectionKeys,
    violations: &mut Vec<Divergence>,
) {
    for sch in schedules {
        let Some(target) = &sch.target_collection else {
            continue;
        };
        let key = (sch.database_id, sch.tenant_id, target.clone());
        if !collection_keys.contains(&key) {
            violations.push(Divergence::new(DivergenceKind::DanglingReference {
                from_kind: "schedule",
                from_key: format!("{}:{}:{}", sch.database_id, sch.tenant_id, sch.name),
                to_kind: "collection",
                to_key: format!("{}:{}:{}", sch.database_id, sch.tenant_id, target),
            }));
        }
    }
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
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::security::catalog::{StoredCollection, StoredTrigger};
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::DatabaseId;

    #[test]
    fn trigger_collection_relationship_is_scoped_to_trigger_database() {
        let directory = tempfile::tempdir().expect("create integrity catalog directory");
        let store = CredentialStore::open(&directory.path().join("system.redb"))
            .expect("open integrity catalog");
        let catalog = store.catalog();

        let collection = StoredCollection::stamped_for_test(1, "orders", "owner");
        catalog
            .put_collection(DatabaseId::DEFAULT, &collection)
            .expect("store default database collection");
        let trigger = StoredTrigger {
            tenant_id: 1,
            database_id: DatabaseId::new(55),
            name: "audit_orders".into(),
            collection: "orders".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "owner".into(),
            created_at: 0,
            descriptor_version: 1,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        catalog.put_trigger(&trigger).expect("store trigger");

        let violations = verify_redb_integrity(catalog);
        assert!(violations.iter().any(|violation| {
            matches!(
                &violation.kind,
                DivergenceKind::DanglingReference {
                    from_kind: "trigger",
                    from_key,
                    to_kind: "collection",
                    to_key,
                } if from_key == "55:1:audit_orders" && to_key == "55:1:orders"
            )
        }));
    }

    fn rls_policy(database_id: u64, collection: &str, display_collection: &str) -> StoredRlsPolicy {
        StoredRlsPolicy {
            tenant_id: 1,
            database_id,
            collection: collection.into(),
            display_collection: display_collection.into(),
            name: "own_rows".into(),
            policy_type_tag: 0,
            predicate_text: String::new(),
            mode_tag: 0,
            on_deny_json: "\"Silent\"".into(),
            enabled: true,
            created_by: "owner".into(),
            created_at: 0,
        }
    }

    fn rls_violations(policy: &StoredRlsPolicy) -> Vec<Divergence> {
        let directory = tempfile::tempdir().expect("create integrity catalog directory");
        let store = CredentialStore::open(&directory.path().join("system.redb"))
            .expect("open integrity catalog");
        let catalog = store.catalog();
        let database_id = DatabaseId::new(1024);
        let mut collection = StoredCollection::stamped_for_test(1, "pat", "owner");
        collection.database_id = database_id;
        catalog
            .put_collection(database_id, &collection)
            .expect("store named database collection");
        catalog.put_rls_policy(policy).expect("store rls policy");
        verify_redb_integrity(catalog)
            .into_iter()
            .filter(|violation| {
                matches!(
                    &violation.kind,
                    DivergenceKind::DanglingReference {
                        from_kind: "rls_policy",
                        ..
                    }
                )
            })
            .collect()
    }

    #[test]
    fn rls_policy_on_a_named_database_collection_is_not_dangling() {
        let violations = rls_violations(&rls_policy(1024, "1024/pat", "pat"));
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn rls_policy_row_without_display_name_resolves_its_qualified_name() {
        let violations = rls_violations(&rls_policy(1024, "1024/pat", ""));
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn rls_policy_qualified_for_another_database_is_dangling() {
        let violations = rls_violations(&rls_policy(1024, "2048/pat", ""));
        assert_eq!(violations.len(), 1, "{violations:?}");
    }

    #[test]
    fn builtin_role_detection() {
        assert!(is_builtin_role("superuser"));
        assert!(is_builtin_role("readonly"));
        assert!(is_builtin_role("monitor"));
        assert!(!is_builtin_role("admin"));
        assert!(!is_builtin_role("custom_auditor"));
    }
}
