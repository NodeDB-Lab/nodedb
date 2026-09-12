// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for physical plans — both halves of it.
//!
//! Exhaustive over [`PhysicalPlan`] and every engine's own op enum (one
//! module per engine). Each variant resolves to one outcome: **Inject**
//! (op reads rows, plan carries a filter slot), **Refuse** (protected read
//! with no filter slot, or a write whose post-image isn't carried —
//! `Error::PlanError`), **Admit** (write carries its image in full, policy
//! evaluated, violation → `Error::RejectedAuthz`), or **No-op** (DDL/
//! maintenance, no stored row touched). A write is never a silent no-op.
//!
//! `redaction_refusal` walks the same plan for the same identity and
//! legitimately diverges in places (RLS injects where redaction has no
//! columns to mask; RLS refuses count shapes redaction can ignore).

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::RlsPolicyStore;
use nodedb_physical::physical_task::PhysicalTask;

use super::context::RlsCtx;

/// Inject RLS predicates into physical tasks after plan conversion: reads
/// get filters injected, a write's policy admits its image or refuses.
/// `Err` on a missing `$auth` field or an uncoverable read/write shape.
pub fn inject_rls(
    tasks: &mut [PhysicalTask],
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    for task in tasks.iter_mut() {
        let ctx = RlsCtx {
            store: rls_store,
            tenant_id: task.tenant_id.as_u64(),
            auth,
            database_id: task.database_id,
        };
        walk(&ctx, &mut task.plan)?;
        refuse_undecided_write_check(&task.plan)?;
    }
    Ok(())
}

/// Inject RLS into a single physical plan (public for native protocol dispatch).
pub fn inject_rls_for_single_plan(
    tenant_id: u64,
    database_id: nodedb_types::DatabaseId,
    plan: &mut PhysicalPlan,
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    let ctx = RlsCtx {
        store: rls_store,
        tenant_id,
        auth,
        database_id,
    };
    walk(&ctx, plan)?;
    refuse_undecided_write_check(plan)
}

/// Refuse a plan whose write-check slot this pass left undecided — an arm
/// can handle an op yet never touch its slot, invisible to the compiler.
/// Turns a silently unstamped arm into a loud, collection-naming error.
fn refuse_undecided_write_check(plan: &PhysicalPlan) -> crate::Result<()> {
    if !plan
        .rls_write_checks()
        .iter()
        .any(|check| check.is_pending_injection())
    {
        return Ok(());
    }
    Err(crate::Error::PlanError {
        detail: format!(
            "internal invariant break: RLS injection ran over the write plan for '{}' without \
             deciding its write-policy check; this is a missing decision in the injection pass, \
             not a policy rejection",
            plan.collection().unwrap_or("<unknown>")
        ),
    })
}

/// Core dispatch: resolve RLS for one physical plan. Exhaustive over
/// [`PhysicalPlan`], and each engine module is exhaustive over its own ops.
pub(super) fn walk(ctx: &RlsCtx<'_>, plan: &mut PhysicalPlan) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Document(op) => super::document::inject_document(ctx, op),
        PhysicalPlan::Kv(op) => super::kv::inject_kv(ctx, op),
        PhysicalPlan::Vector(op) => super::vector::inject_vector(ctx, op),
        PhysicalPlan::Text(op) => super::text::inject_text(ctx, op),
        PhysicalPlan::Columnar(op) => super::columnar::inject_columnar(ctx, op),
        PhysicalPlan::Timeseries(op) => super::columnar::inject_timeseries(ctx, op),
        PhysicalPlan::Spatial(op) => super::columnar::inject_spatial(ctx, op),
        PhysicalPlan::Graph(op) => super::graph::inject_graph(ctx, op),
        PhysicalPlan::Query(op) => super::query::inject_query(ctx, op),
        PhysicalPlan::Crdt(op) => super::crdt::inject_crdt(ctx, op),
        PhysicalPlan::Meta(op) => super::meta::inject_meta(ctx, op),
        PhysicalPlan::Array(op) => super::array::inject_array(ctx, op),
        PhysicalPlan::ClusterArray(op) => super::array::inject_cluster_array(ctx, op),
        PhysicalPlan::ClusterEvent(op) => super::array::inject_cluster_event(ctx, op),
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::{PolicyType, RlsPolicy, RlsPolicyStore};
    use crate::types::TenantId;

    pub(in crate::control::planner::rls_injection) const TENANT: u64 = 1;

    /// Stamp a plan's write check back to `PendingInjection`, undoing
    /// injection's only effect with no matching policy, so a test can
    /// assert nothing else changed. Covers only variants tests build.
    pub(in crate::control::planner::rls_injection) fn reset_write_check(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
    ) {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, GraphOp, TimeseriesOp};
        use nodedb_types::RlsWriteCheck::PendingInjection;

        match plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                rls_write_check, ..
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                rls_write_check, ..
            })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(
                DocumentOp::PointUpdate {
                    rls_write_check, ..
                }
                | DocumentOp::PointDelete {
                    rls_write_check, ..
                },
            ) => *rls_write_check = PendingInjection,
            other => panic!("reset_write_check does not cover {other:?}"),
        }
    }

    /// A store holding one restrictive read policy: `owner_id = $auth.id`.
    pub(in crate::control::planner::rls_injection) fn store_with_read_policy(
        collection: &str,
    ) -> RlsPolicyStore {
        let store = RlsPolicyStore::new();
        let policy = RlsPolicy {
            name: format!("{collection}_owner"),
            collection: collection.into(),
            display_collection: collection.into(),
            tenant_id: TENANT,
            policy_type: PolicyType::Read,
            compiled_predicate: Some(RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            }),
            mode: Default::default(),
            on_deny: Default::default(),
            enabled: true,
            created_by: "admin".into(),
            created_at: 0,
        };
        store
            .create_policy(policy)
            .expect("create read policy for test");
        store
    }

    /// A store holding one policy of `policy_type` restricting `collection` to
    /// `owner_id = $auth.id`.
    pub(in crate::control::planner::rls_injection) fn store_with_policy(
        collection: &str,
        policy_type: PolicyType,
    ) -> RlsPolicyStore {
        store_with_predicate(
            collection,
            policy_type,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
        )
    }

    /// A store holding one `Write` policy restricting `collection`.
    pub(in crate::control::planner::rls_injection) fn store_with_write_policy(
        collection: &str,
    ) -> RlsPolicyStore {
        store_with_policy(collection, PolicyType::Write)
    }

    /// A store holding one policy carrying an arbitrary predicate.
    pub(in crate::control::planner::rls_injection) fn store_with_predicate(
        collection: &str,
        policy_type: PolicyType,
        predicate: RlsPredicate,
    ) -> RlsPolicyStore {
        let store = RlsPolicyStore::new();
        store
            .create_policy(RlsPolicy {
                name: format!("{collection}_{policy_type:?}"),
                collection: collection.into(),
                display_collection: collection.into(),
                tenant_id: TENANT,
                policy_type,
                compiled_predicate: Some(predicate),
                mode: Default::default(),
                on_deny: Default::default(),
                enabled: true,
                created_by: "admin".into(),
                created_at: 0,
            })
            .expect("create policy for test");
        store
    }

    /// Assert the injector refused the write with a typed plan error naming
    /// `collection`.
    pub(in crate::control::planner::rls_injection) fn assert_write_refused(
        result: crate::Result<()>,
        collection: &str,
    ) {
        match result {
            Err(crate::Error::PlanError { detail }) => {
                assert!(
                    detail.contains(collection) && detail.contains("write policy"),
                    "refusal must name the collection and the write policy; got {detail}"
                )
            }
            other => panic!("expected PlanError write refusal, got {other:?}"),
        }
    }

    /// An ordinary (non-superuser) authenticated session.
    pub(in crate::control::planner::rls_injection) fn regular_auth() -> AuthContext {
        use crate::control::security::identity::{
            AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
        };

        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(TENANT),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
        );
        AuthContext::from_identity(&identity, "s_test".into())
    }

    /// Run the injector over `plan` with the policy store and a regular user.
    pub(in crate::control::planner::rls_injection) fn inject(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
        store: &RlsPolicyStore,
    ) -> crate::Result<()> {
        super::inject_rls_for_single_plan(
            TENANT,
            nodedb_types::DatabaseId::DEFAULT,
            plan,
            store,
            &regular_auth(),
        )
    }

    /// Run the injector with an empty policy store: nothing must change.
    pub(in crate::control::planner::rls_injection) fn inject_without_policy(
        plan: &mut crate::bridge::envelope::PhysicalPlan,
    ) -> crate::Result<()> {
        super::inject_rls_for_single_plan(
            TENANT,
            nodedb_types::DatabaseId::DEFAULT,
            plan,
            &RlsPolicyStore::new(),
            &regular_auth(),
        )
    }

    /// Assert the injector refused with a typed plan error naming `collection`.
    pub(in crate::control::planner::rls_injection) fn assert_refused(
        result: crate::Result<()>,
        collection: &str,
    ) {
        match result {
            Err(crate::Error::PlanError { detail }) => assert!(
                detail.contains(collection),
                "refusal must name the collection; got {detail}"
            ),
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        DocumentOp, ExchangeMode, ExchangeOp, GraphOp, MetaOp, QueryOp,
    };

    use super::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_types::{DatabaseId, QualifiedCollection};

    fn last_values(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Meta(MetaOp::QueryLastValues {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        })
    }

    fn rag_fusion(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::RagFusion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            query_vector: vec![0.0],
            vector_top_k: 4,
            edge_label: None,
            direction: nodedb_types::graph::Direction::Out,
            expansion_depth: 1,
            final_top_k: 4,
            rrf_k: (60.0, 60.0),
            rrf_k_triple: None,
            vector_field: String::new(),
            options: Default::default(),
            bm25_query: None,
            bm25_field: None,
        })
    }

    /// `GraphOp::RagFusion` returns document rows through a response shape with
    /// no filter slot, so a policy on the collection refuses the plan instead
    /// of silently returning rows the policy hides.
    #[test]
    fn rag_fusion_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("docs");
        let mut plan = rag_fusion("docs");
        assert_refused(inject(&mut plan, &store), "docs");
    }

    /// The same fusion is untouched when no policy applies.
    #[test]
    fn rag_fusion_without_a_policy_is_untouched() {
        let mut plan = rag_fusion("docs");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A policy on a different collection must not refuse this one.
    #[test]
    fn rag_fusion_on_an_unpoliced_collection_runs() {
        let store = store_with_read_policy("other");
        let mut plan = rag_fusion("docs");
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// `MetaOp::QueryLastValues` returns the last observed value of every
    /// series in the collection — stored rows, through a cache response with
    /// no filter slot.
    #[test]
    fn query_last_values_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("metrics");
        let mut plan = last_values("metrics");
        assert_refused(inject(&mut plan, &store), "metrics");
    }

    /// …and the single-series form is refused for the same reason.
    #[test]
    fn query_last_value_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("metrics");
        let mut plan = PhysicalPlan::Meta(MetaOp::QueryLastValue {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            series_id: 7,
        });
        assert_refused(inject(&mut plan, &store), "metrics");
    }

    /// Both last-value ops run unchanged when no policy applies.
    #[test]
    fn query_last_values_without_a_policy_is_untouched() {
        let mut plan = last_values("metrics");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// An affected op nested under `Exchange` is still resolved — the
    /// converter wraps sharded sources before this pass runs.
    #[test]
    fn affected_op_under_exchange_is_still_refused() {
        let store = store_with_read_policy("metrics");
        let mut plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(last_values("metrics")),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }));
        assert_refused(inject(&mut plan, &store), "metrics");
    }

    /// …and under `PostProcess`, the subquery-body wrapper.
    #[test]
    fn affected_op_under_post_process_is_still_refused() {
        let store = store_with_read_policy("docs");
        let mut plan = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(rag_fusion("docs")),
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert_refused(inject(&mut plan, &store), "docs");
    }

    /// A read policy restricts only reads: a write op under a collection that
    /// carries one is left exactly as planned. The write half of this pass is
    /// keyed on write policies, which this store holds none of.
    #[test]
    fn a_write_op_is_untouched_by_the_read_pass() {
        let store = store_with_read_policy("docs");
        let mut plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let before = plan.clone();
        assert!(inject(&mut plan, &store).is_ok());
        assert_eq!(plan, before);
    }
}
