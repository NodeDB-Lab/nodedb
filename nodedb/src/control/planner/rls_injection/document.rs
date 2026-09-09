// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for document-engine operations.

use nodedb_physical::physical_plan::DocumentOp;

use crate::engine::document::store::surrogate_to_doc_id;

use super::context::RlsCtx;

/// Exhaustive over [`DocumentOp`] so a new document operation forces a
/// decision between injecting, refusing, and no-op.
pub(super) fn inject_document(ctx: &RlsCtx<'_>, op: &mut DocumentOp) -> crate::Result<()> {
    match op {
        // Inject: the scan pushes its predicate into storage, so the policy
        // ANDs into the same slot the user's WHERE clause occupies.
        DocumentOp::Scan {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: fetched bodies are tested against `filters`, the residual
        // post-filter slot the policy ANDs into.
        DocumentOp::IndexedFetch {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: no pushdown slot, so the handler evaluates post-fetch. An
        // excluded row reads back as absent, not distinguishably denied.
        DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        }
        | DocumentOp::RangeScan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Inject both: read filter for `RETURNING`, write predicate ships
        // to the Data Plane since the image exists only after the read.
        DocumentOp::PointDelete {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::PointUpdate {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::BulkUpdate {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::BulkDelete {
            collection,
            rls_filters,
            rls_write_check,
            ..
        }
        // Source is read, but every row returned/written belongs to TARGET,
        // so the target's policy gates both halves.
        | DocumentOp::UpdateFromJoin {
            target_collection: collection,
            rls_filters,
            rls_write_check,
            ..
        }
        | DocumentOp::Merge {
            target_collection: collection,
            rls_filters,
            rls_write_check,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // Refuse: returns index entries rather than rows, so there is no row
        // body to evaluate a policy against.
        DocumentOp::IndexLookup { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the lookup returns index entries, not row bodies, so the row filter has nothing to \
             evaluate against",
        ),

        // Refuse: a scalar count carries no row for the filter to test, and
        // the row set itself (not a column value) is what RLS restricts.
        DocumentOp::EstimateCount { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the estimate is a row count, which the row filter cannot be evaluated against",
        ),

        // Refuse: streams raw triples with no filter slot — every stored
        // body would be copied regardless of policy.
        DocumentOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored bodies through a cursor payload that \
             carries no row filter",
        ),

        // Admit the write image now (plan carries the full post-image),
        // then inject the read filter for independent `RETURNING` visibility.
        DocumentOp::PointPut {
            collection,
            value,
            rls_filters,
            surrogate,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            value,
            rls_filters,
            surrogate,
            ..
        } => {
            let row_key = surrogate_to_doc_id(*surrogate);
            ctx.admit_document_write_image(collection, &row_key, value)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        DocumentOp::BatchInsert {
            collection,
            documents,
            rls_filters,
            surrogates,
            ..
        } => {
            // Every row is gated. A row whose surrogate is not yet assigned
            // falls back to its document id, which a declared key already
            // carries in the body.
            for (index, (document_id, value)) in documents.iter().enumerate() {
                let row_key = surrogates
                    .get(index)
                    .map_or_else(|| document_id.clone(), |s| surrogate_to_doc_id(*s));
                ctx.admit_document_write_image(collection, &row_key, value)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate: the conflict-merged body doesn't exist until
        // the handler reads the stored row.
        DocumentOp::Upsert {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Refuse: the rows come from a scan resolved after this pass, so the
        // plan carries no image to evaluate.
        DocumentOp::InsertSelect {
            target_collection, ..
        } => ctx.refuse_if_write_policy(
            target_collection,
            "the inserted rows are produced by a scan resolved after planning, so the plan carries \
             no row image",
        ),

        // Refuse: removes every row without reading one, so no image the
        // policy could be evaluated against exists.
        DocumentOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),

        // No-op: DDL writes no user row; authorized by the permission check
        // that precedes this pass.
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => Ok(()),

        // No-op: carries no user identity — its admission was already
        // decided when the SOURCE row it derives from was admitted.
        DocumentOp::ApplyBalanceDelta { .. } => Ok(()),

        // No-op: already decided by the resolve pass; re-injecting would
        // replace a verdict with a predicate no applying node can decide.
        DocumentOp::ResolvedWrite { .. } => Ok(()),

        // Recurse: the wrapped op is the intercepted write verbatim.
        DocumentOp::ResolveWrite(inner) => inject_document(ctx, inner),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy, store_with_predicate,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::PolicyType;

    /// `alice` in the shared fixture has user id 42, so this row satisfies
    /// `owner_id = $auth.id` and that one does not.
    fn body(owner_id: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack_or_empty(&serde_json::json!({
            "owner_id": owner_id,
            "amount": 100,
        }))
    }

    fn point_insert(collection: &str, owner_id: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            document_id: "d1".into(),
            value: body(owner_id),
            if_absent: false,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn point_update(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            document_id: "d1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &nodedb_types::RlsWriteCheck {
        match plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Merge {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The insert body is the row that will exist afterwards, so a conforming
    /// row is admitted and a violating one fails the statement.
    #[test]
    fn insert_is_admitted_or_rejected_on_its_own_post_image() {
        let store = store_with_write_policy("orders");

        let mut conforming = point_insert("orders", "42");
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = point_insert("orders", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A batch fails whole when any one of its rows violates the policy: a
    /// silently dropped row would report a write that never happened.
    #[test]
    fn batch_insert_is_rejected_when_any_row_violates_the_policy() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            documents: vec![("d1".into(), body("42")), ("d2".into(), body("99"))],
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// An unresolvable `$auth.*` reference in a write predicate denies the
    /// write rather than admitting it.
    #[test]
    fn insert_fails_closed_on_an_unresolvable_auth_reference() {
        let store = store_with_predicate(
            "orders",
            PolicyType::Write,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("nonexistent_field".into()),
            },
        );
        let mut plan = point_insert("orders", "42");
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The post-image of an update exists only after the stored row is read, so
    /// the compiled predicate rides along for the handler to test it against.
    #[test]
    fn point_update_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = point_update("orders");
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            write_check(&plan).has_predicate(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// A delete is gated the same way: the row it removes is only known after
    /// the handler reads it, so the predicate travels with the plan.
    #[test]
    fn bulk_delete_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// Every MERGE arm writes a row resolved against the target, so the
    /// target's write policy is the one that reaches the gate.
    #[test]
    fn merge_carries_the_target_write_predicate() {
        let store = store_with_write_policy("target");
        let mut plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "target",
            ),
            source_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "source",
            ),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            clauses: Vec::new(),
            returning: None,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// An upsert's conflict branch persists a merge with the stored row, so its
    /// gate is the same shipped predicate rather than a plan-time admission.
    #[test]
    fn upsert_carries_the_write_predicate() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::Upsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            document_id: "d1".into(),
            value: body("42"),
            on_conflict_updates: Vec::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// A `RETURNING` on an insert ships rows back, so a read-only policy must
    /// land in the insert's post-filter slot. Leaving it empty would return
    /// rows the same principal's `SELECT` hides.
    #[test]
    fn insert_receives_the_read_policy_filter() {
        let store = store_with_read_policy("orders");
        let mut plan = point_insert("orders", "42");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::PointInsert { rls_filters, .. }) => assert!(
                !rls_filters.is_empty(),
                "the read policy must gate RETURNING output"
            ),
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// An unresolvable `$auth.*` in a write predicate denies the statement
    /// instead of compiling to an empty (allow-everything) gate.
    #[test]
    fn a_gated_write_fails_closed_on_an_unresolvable_auth_reference() {
        let store = store_with_predicate(
            "orders",
            PolicyType::Write,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("nonexistent_field".into()),
            },
        );
        let mut plan = point_update("orders");
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A truncate removes every row without reading one, so there is no image
    /// the policy could decide.
    #[test]
    fn truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("orders");
        let mut plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
        });
        assert_write_refused(inject(&mut plan, &store), "orders");
    }

    /// A `FOR ALL` policy decides both halves: the read filter lands in the
    /// post-fetch slot and the write predicate lands in the gate slot, as two
    /// separate fields.
    #[test]
    fn a_for_all_policy_gates_the_read_slot_and_the_write() {
        let store = store_with_predicate(
            "orders",
            PolicyType::All,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
        );
        let mut plan = point_update("orders");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_filters,
                rls_write_check,
                ..
            }) => {
                assert!(!rls_filters.is_empty(), "read half must gate RETURNING");
                assert!(
                    rls_write_check.has_predicate(),
                    "write half must gate the write"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }

        let mut conforming = point_insert("orders", "42");
        assert!(inject(&mut conforming, &store).is_ok());
        let mut violating = point_insert("orders", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A read policy alone leaves the write gate empty — the write half is
    /// keyed on write policies only, so a `FOR SELECT` policy must not silently
    /// start rejecting writes.
    #[test]
    fn a_read_policy_alone_does_not_gate_writes() {
        let store = store_with_read_policy("orders");
        let mut insert = point_insert("orders", "99");
        assert!(inject(&mut insert, &store).is_ok());

        let mut update = point_update("orders");
        assert!(inject(&mut update, &store).is_ok());
        assert!(!write_check(&update).has_predicate());
    }

    /// With no policy at all every write shape runs untouched.
    #[test]
    fn writes_without_a_policy_record_that_injection_ran() {
        // `point_insert` has no write-check slot to stamp; only update is.
        let mut insert = point_insert("orders", "99");
        let insert_before = insert.clone();
        assert!(inject_without_policy(&mut insert).is_ok());
        assert_eq!(insert, insert_before);

        let mut update = point_update("orders");
        let update_before = update.clone();
        assert!(inject_without_policy(&mut update).is_ok());
        assert_eq!(
            write_check(&update),
            &nodedb_types::RlsWriteCheck::NoPolicyApplies
        );

        let mut normalized = update.clone();
        super::super::plan::test_support::reset_write_check(&mut normalized);
        assert_eq!(normalized, update_before, "nothing else may change");
    }

    fn indexed_fetch(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            path: "$.email".into(),
            value: "a@b.c".into(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
        })
    }

    /// The indexed fetch applies `filters` to every fetched body, so the
    /// policy lands there rather than refusing the plan.
    #[test]
    fn indexed_fetch_receives_the_policy_filter() {
        let store = store_with_read_policy("users");
        let mut plan = indexed_fetch("users");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::IndexedFetch { filters, .. }) => {
                assert!(!filters.is_empty(), "policy filter must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// With no policy the same fetch is untouched.
    #[test]
    fn indexed_fetch_without_a_policy_is_untouched() {
        let mut plan = indexed_fetch("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A `RETURNING` write ships rows back, so the policy lands in its
    /// post-filter slot — leaving the statement's own write predicate alone.
    #[test]
    fn bulk_update_receives_the_policy_filter() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::BulkUpdate {
                filters,
                rls_filters,
                ..
            }) => {
                assert!(
                    !rls_filters.is_empty(),
                    "policy must land in the post-filter slot"
                );
                assert!(
                    filters.is_empty(),
                    "the statement's own write predicate must stay untouched"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// Every row a MERGE returns belongs to the target, so the target's policy
    /// is the one injected — a policy on the source gates nothing here.
    #[test]
    fn merge_receives_the_target_collection_policy() {
        let store = store_with_read_policy("target");
        let mut plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "target",
            ),
            source_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "source",
            ),
            source_alias: "s".into(),
            target_join_col: "id".into(),
            source_join_col: "id".into(),
            clauses: Vec::new(),
            returning: None,
            resolved_inserts: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Document(DocumentOp::Merge { rls_filters, .. }) => {
                assert!(!rls_filters.is_empty(), "target policy must be injected")
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A cardinality estimate counts rows the policy hides.
    #[test]
    fn estimate_count_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            field: "id".into(),
        });
        assert_refused(inject(&mut plan, &store), "users");
    }
}
