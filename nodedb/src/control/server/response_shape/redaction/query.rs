// SPDX-License-Identifier: BUSL-1.1

//! Column-level redaction inputs for SELECT response shaping.
//!
//! Redaction is Control-Plane-only work: it needs the requester's roles, which
//! never cross the SPSC bridge, so it is applied to the decoded result rows
//! rather than inside an engine.
//!
//! [`QueryRedaction`] resolves the two per-query inputs — the requester's
//! roles and the plan's source collections — exactly ONCE. [`RedactionCtx`] is
//! the borrowed view handed to the shaper, and a streaming statement builds one
//! from the same `QueryRedaction` for every batch it shapes, so an early batch
//! can never ship rows a later batch would have redacted.
//!
//! The hooks that consume these inputs live in [`super::shapes`], one per wire
//! shape a client-facing path can deliver.

use nodedb_physical::physical_plan::QueryOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::redaction::RedactionStore;
use crate::control::server::shared::plan_util::extract_collection;
use nodedb_types::TenantId;

/// Everything the redaction hook needs to rewrite one query's result rows.
pub struct RedactionCtx<'a> {
    pub store: &'a RedactionStore,
    pub tenant_id: u64,
    pub roles: &'a [String],
    /// One entry per source collection the plan reads, as
    /// `(qualifier, collection)`. `qualifier` is the prefix that appears on
    /// this collection's keys in a row map — empty for a single-collection
    /// plan, and the join alias (or the collection name when there is no
    /// alias) for each side of a join.
    pub collections: &'a [(String, String)],
}

/// The per-query redaction inputs, resolved once and owned.
///
/// Owned rather than borrowed because a lazy streaming response outlives the
/// handler frame that resolved it: the row generator moves this in and hands
/// out a [`RedactionCtx`] per batch.
#[derive(Clone, Debug)]
pub struct QueryRedaction {
    tenant_id: u64,
    roles: Vec<String>,
    collections: Vec<(String, String)>,
}

impl QueryRedaction {
    /// Resolve the redaction inputs for a statement reading `plan`.
    pub fn for_plan(tenant_id: TenantId, auth: &AuthContext, plan: &PhysicalPlan) -> Self {
        Self::for_collections(tenant_id, auth, plan_source_collections(plan))
    }

    /// Resolve the redaction inputs for a statement whose rows come from
    /// several plans (set-op branches, clone/gateway merges, Calvin batches).
    ///
    /// The union of every branch's sources is used, so a column is redacted
    /// whichever branch produced the row it sits in.
    pub fn for_plans<'p, I>(tenant_id: TenantId, auth: &AuthContext, plans: I) -> Self
    where
        I: IntoIterator<Item = &'p PhysicalPlan>,
    {
        let mut collections: Vec<(String, String)> = Vec::new();
        for plan in plans {
            for source in plan_source_collections(plan) {
                if !collections.contains(&source) {
                    collections.push(source);
                }
            }
        }
        Self::for_collections(tenant_id, auth, collections)
    }

    /// Resolve the redaction inputs from an already-known source list.
    ///
    /// Used by producers with no `PhysicalPlan` in scope (the ClusterArray
    /// coordinator path, and the RESP surface, whose selected collection is the
    /// one every command in the session reads), which know their collection
    /// directly.
    pub fn for_collections(
        tenant_id: TenantId,
        auth: &AuthContext,
        collections: Vec<(String, String)>,
    ) -> Self {
        Self::new(tenant_id, auth.roles.clone(), collections)
    }

    /// Assemble from already-extracted roles and sources.
    pub fn new(
        tenant_id: TenantId,
        roles: Vec<String>,
        collections: Vec<(String, String)>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.as_u64(),
            roles,
            collections,
        }
    }

    /// True when this statement can never redact anything — no roles to match
    /// a policy against, or no source collection to key one on.
    pub fn is_inert(&self) -> bool {
        self.roles.is_empty() || self.collections.is_empty()
    }

    /// True when at least one source collection carries a rule for the
    /// requester's roles.
    ///
    /// Answers "could this statement's rows be rewritten at all?" without
    /// touching a row. Callers that must rewrite an encoded value in place
    /// (rather than a decoded row map) use this to leave the encoded bytes
    /// strictly untouched when no policy exists — see
    /// [`super::shapes::redact_stored_value_bytes`].
    pub fn has_any_rule(&self, store: &RedactionStore) -> bool {
        !self.is_inert()
            && self.collections.iter().any(|(_, collection)| {
                store.has_any_rule_for_collection(self.tenant_id, collection, &self.roles)
            })
    }

    /// True when a rule covers `field` on any of this statement's sources.
    ///
    /// Used by delivery paths that return a single named value computed from a
    /// stored field, where masking the result would report a value the row does
    /// not hold: they refuse instead of rewriting.
    pub fn field_has_rule(&self, store: &RedactionStore, field: &str) -> bool {
        !self.roles.is_empty()
            && self.collections.iter().any(|(_, collection)| {
                store.has_rule_for_field(self.tenant_id, collection, &self.roles, field)
            })
    }

    /// Borrow these inputs together with `store` as the shaper's hook input.
    pub fn ctx<'a>(&'a self, store: &'a RedactionStore) -> RedactionCtx<'a> {
        RedactionCtx {
            store,
            tenant_id: self.tenant_id,
            roles: &self.roles,
            collections: &self.collections,
        }
    }
}

/// The source collections a plan reads, as `(qualifier, collection)`.
///
/// `qualifier` is the prefix the executor puts on that source's columns in a
/// result row: empty for a single-collection plan, the alias (or collection
/// name) per side for a join or LATERAL.
///
/// This deliberately does NOT use `extract_collection` alone: that helper
/// reports only the LEFT side of a join, which would leave every right-side
/// column unredacted.
pub fn plan_source_collections(plan: &PhysicalPlan) -> Vec<(String, String)> {
    let mut out = Vec::new();
    collect_sources(plan, "", &mut out);
    out
}

/// Walk `plan`, attributing each source it reads to `qualifier`.
///
/// Only the relational [`QueryOp`]s can introduce a second source or rename a
/// qualifier; every other plan reads at most one collection, and resolving
/// that is delegated to `extract_collection`, whose match over `PhysicalPlan`
/// is exhaustive — so a new plan variant still forces a decision there.
fn collect_sources(plan: &PhysicalPlan, qualifier: &str, out: &mut Vec<(String, String)>) {
    if let PhysicalPlan::Query(op) = plan {
        match op {
            // A join side's rows may come from a resolved child plan
            // (`*_input`) or from a local scan of `*_collection`. The side's
            // qualifier prefixes its columns either way, so the collection is
            // recorded unconditionally and the child, when present, is walked
            // under that same qualifier.
            QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                left_input,
                right_input,
                ..
            } => {
                let left = left_alias.as_deref().unwrap_or(left_collection.as_str());
                push_source(out, left, left_collection.as_str());
                if let Some(child) = left_input {
                    collect_sources(child, left, out);
                }
                let right = right_alias.as_deref().unwrap_or(right_collection.as_str());
                push_source(out, right, right_collection.as_str());
                if let Some(child) = right_input {
                    collect_sources(child, right, out);
                }
                return;
            }
            // Neither variant takes a resolved child input: both sides are
            // always scanned locally, and neither carries an alias, so the
            // collection name is the qualifier.
            QueryOp::NestedLoopJoin {
                left_collection,
                right_collection,
                ..
            }
            | QueryOp::SortMergeJoin {
                left_collection,
                right_collection,
                ..
            } => {
                push_source(out, left_collection.as_str(), left_collection.as_str());
                push_source(out, right_collection.as_str(), right_collection.as_str());
                return;
            }
            QueryOp::LateralTopK {
                outer_plan,
                outer_alias,
                inner_collection,
                lateral_alias,
                ..
            }
            | QueryOp::LateralLoop {
                outer_plan,
                outer_alias,
                inner_collection,
                lateral_alias,
                ..
            } => {
                collect_sources(outer_plan, outer_alias, out);
                push_source(out, lateral_alias, inner_collection.as_str());
                return;
            }
            QueryOp::Exchange(exchange) => {
                collect_sources(&exchange.child, qualifier, out);
                return;
            }
            QueryOp::PostProcess { input, .. } => {
                collect_sources(input, qualifier, out);
                return;
            }
            // Every set-operation branch contributes rows under the same
            // derived-table qualifier.
            QueryOp::SetOp { inputs, .. } => {
                for input in inputs {
                    collect_sources(input, qualifier, out);
                }
                return;
            }
            QueryOp::Aggregate {
                collection, input, ..
            }
            | QueryOp::PartialAggregateState {
                collection, input, ..
            } => {
                push_source(out, qualifier, collection.as_str());
                if let Some(child) = input {
                    collect_sources(child, qualifier, out);
                }
                return;
            }
            // Every remaining relational op reads at most the single
            // collection `extract_collection` reports, resolved below.
            _ => {}
        }
    }

    if let Some(collection) = extract_collection(plan) {
        push_source(out, qualifier, collection);
    }
}

fn push_source(out: &mut Vec<(String, String)>, qualifier: &str, collection: &str) {
    if out
        .iter()
        .any(|(q, c)| q.as_str() == qualifier && c.as_str() == collection)
    {
        return;
    }
    out.push((qualifier.to_string(), collection.to_string()));
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, ExchangeMode, ExchangeOp};

    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    use super::*;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn store_with_mask(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
        QueryRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            vec![(String::new(), collection.to_string())],
        )
    }

    /// `has_any_rule` is the gate an encoded-value rewrite opens on: it must
    /// stay shut for a role no policy names, so those bytes are never even
    /// decoded, let alone re-encoded.
    #[test]
    fn has_any_rule_is_scoped_to_the_roles_and_collections_of_the_statement() {
        let store = store_with_mask("users", "support", "email");

        assert!(redaction_for("users", "support").has_any_rule(&store));
        assert!(!redaction_for("users", "analyst").has_any_rule(&store));
        assert!(!redaction_for("orders", "support").has_any_rule(&store));
        assert!(!redaction_for("users", "support").has_any_rule(&RedactionStore::new()));
    }

    /// `field_has_rule` is what a path returning a single computed value
    /// refuses on, so it must answer per column, not per collection.
    #[test]
    fn field_has_rule_reports_the_covered_column_only() {
        let store = store_with_mask("counters", "support", "value");
        let redaction = redaction_for("counters", "support");

        assert!(redaction.field_has_rule(&store, "value"));
        assert!(!redaction.field_has_rule(&store, "other"));
        assert!(!redaction_for("counters", "analyst").field_has_rule(&store, "value"));
    }

    /// A minimal single-collection leaf plan.
    fn scan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            field: "id".to_string(),
        })
    }

    fn hash_join(
        left_alias: Option<&str>,
        right_alias: Option<&str>,
        left_input: Option<PhysicalPlan>,
    ) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "workspaces"),
            right_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "boards"),
            left_alias: left_alias.map(str::to_string),
            right_alias: right_alias.map(str::to_string),
            on: Vec::new(),
            join_type: "inner".into(),
            limit: 0,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: left_input.map(Box::new),
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_scan_filters: Vec::new(),
            right_scan_filters: Vec::new(),
        })
    }

    #[test]
    fn single_collection_plan_uses_an_empty_qualifier() {
        assert_eq!(
            plan_source_collections(&scan("users")),
            vec![(String::new(), "users".to_string())]
        );
    }

    /// Both join sides must appear — the whole reason this does not reuse
    /// `extract_collection`, which reports only the left one.
    #[test]
    fn join_reports_both_sides_under_their_aliases() {
        assert_eq!(
            plan_source_collections(&hash_join(Some("w"), Some("b"), None)),
            vec![
                ("w".to_string(), "workspaces".to_string()),
                ("b".to_string(), "boards".to_string()),
            ]
        );
    }

    /// An unaliased side qualifies its columns with the collection name.
    #[test]
    fn join_without_aliases_qualifies_by_collection_name() {
        assert_eq!(
            plan_source_collections(&hash_join(None, None, None)),
            vec![
                ("workspaces".to_string(), "workspaces".to_string()),
                ("boards".to_string(), "boards".to_string()),
            ]
        );
    }

    /// A resolved child plan is walked under its side's qualifier, so a
    /// coordinator-resolved join side is still attributed.
    #[test]
    fn join_child_input_inherits_its_sides_qualifier() {
        let sources =
            plan_source_collections(&hash_join(Some("w"), Some("b"), Some(scan("audit"))));
        assert!(sources.contains(&("w".to_string(), "audit".to_string())));
        assert!(sources.contains(&("b".to_string(), "boards".to_string())));
    }

    /// Exchange is transparent: a gathered scan is still a single-collection
    /// plan with an empty qualifier.
    #[test]
    fn exchange_is_transparent() {
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(scan("users")),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        assert_eq!(
            plan_source_collections(&plan),
            vec![(String::new(), "users".to_string())]
        );
    }
}
