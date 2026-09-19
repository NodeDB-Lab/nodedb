// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for key-value engine operations.

use nodedb_physical::physical_plan::KvOp;

use super::context::RlsCtx;

/// Exhaustive over [`KvOp`] so a new key-value operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_kv(ctx: &RlsCtx<'_>, op: &mut KvOp) -> crate::Result<()> {
    match op {
        // Inject: the predicate scan pushes filters down, so the policy ANDs
        // into the same slot as the user's predicate.
        KvOp::Scan {
            collection,
            filters,
            ..
        } => ctx.merge_into(collection, filters),

        // Inject: no pushdown slot, so the handler evaluates post-fetch. An
        // excluded row reads back as absent, indistinguishable from missing.
        KvOp::Get {
            collection,
            rls_filters,
            ..
        }
        | KvOp::BatchGet {
            collection,
            rls_filters,
            ..
        }
        | KvOp::FieldGet {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: no row body to filter, and answering discloses that a
        // hidden row exists.
        KvOp::GetTtl { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the reply is a TTL rather than a row body, so the row filter cannot be evaluated \
             and the answer alone discloses that the key exists",
        ),

        // Refuse: the clone materializer streams raw `(key, value)` pairs
        // through a cursor payload with no filter slot.
        KvOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored values through a cursor payload that \
             carries no row filter",
        ),

        // Refuse: plan names only the index, not its owning collection.
        // Falls back to the tenant-wide question.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => ctx.refuse_if_any_policy(
            "a sorted-index read returns ranked keys, a rank, or a count taken from stored rows, \
             and the plan names only the index",
        ),

        // Admit now: a single-scalar `value` write has no field to name,
        // so it fails the same evaluation rather than a carve-out.
        KvOp::Put {
            collection,
            value,
            rls_filters,
            ..
        }
        | KvOp::Insert {
            collection,
            value,
            rls_filters,
            ..
        }
        | KvOp::InsertIfAbsent {
            collection,
            value,
            rls_filters,
            ..
        } => {
            ctx.admit_write_image(collection, value)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Admit every entry: a silently dropped row would report a write
        // that never happened.
        KvOp::BatchPut {
            collection,
            entries,
            rls_filters,
            ..
        } => {
            for (_, value) in entries.iter() {
                ctx.admit_write_image(collection, value)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate: the persisted image (merge, removed row,
        // TTL body, field merge) exists only where it's persisted.
        KvOp::InsertOnConflictUpdate {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            // `RETURNING` output is a read — see the point-write arms above.
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate and gate `RETURNING` as a read: the removed row
        // or field merge exists only where it's persisted, and the rows the
        // clause hands back are bounded by the same read policy a `SELECT`
        // by this principal is. Row set of the predicate forms is resolved by
        // the Data Plane scan. Mirrors `ColumnarOp::{Update, Delete}`.
        KvOp::Delete {
            collection,
            rls_write_check,
            rls_filters,
            ..
        }
        | KvOp::FieldSet {
            collection,
            rls_write_check,
            rls_filters,
            ..
        }
        | KvOp::PredicateUpdate {
            collection,
            rls_write_check,
            rls_filters,
            ..
        }
        | KvOp::PredicateDelete {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate: the TTL body is the stored row, which exists
        // only where it's persisted.
        KvOp::Expire {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Persist {
            collection,
            rls_write_check,
            ..
        } => ctx.set_write_check(collection, rls_write_check),

        // Ship predicate, refuse under a read policy: the reply is computed
        // from the stored row with no filter slot to hide it.
        KvOp::Incr {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::IncrFloat {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Cas {
            collection,
            rls_write_check,
            ..
        }
        | KvOp::Transfer {
            collection,
            rls_write_check,
            ..
        } => {
            ctx.refuse_if_policy(collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // Inject both: read filter gates the old value returned (excluded
        // = absent), write predicate decides `new_value` separately.
        KvOp::GetSet {
            collection,
            rls_filters,
            rls_write_check,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            ctx.set_write_check(collection, rls_write_check)
        }

        // Two independent policies: each side ships its own predicate.
        // Read half refuses for the same reason as the atomics above.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            source_rls_write_check,
            dest_rls_write_check,
            ..
        } => {
            ctx.refuse_if_policy(source_collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.refuse_if_policy(dest_collection, KV_COMPUTED_REPLY_REASON)?;
            ctx.set_write_check(source_collection, source_rls_write_check)?;
            ctx.set_write_check(dest_collection, dest_rls_write_check)
        }

        // Refuse: removes every row without reading one, so no image
        // exists to evaluate against. Mirrors the document engine's truncate.
        KvOp::Truncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),

        // Recurse: the wrapped op is the intercepted write verbatim.
        KvOp::ResolveWrite(inner) => inject_kv(ctx, inner),

        // No-op: already decided before this write was proposed;
        // re-injecting would replace a verdict with an unevaluable predicate.
        KvOp::ResolvedWrite { .. } => Ok(()),

        // No-op: index DDL writes no user row, so no row policy restricts it.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

/// Why a read policy cannot be honored by a write that replies with a value
/// derived from the stored row.
const KV_COMPUTED_REPLY_REASON: &str = "the reply is a value computed from the stored row rather than a row body, so the row filter \
     cannot be evaluated against it and the answer alone discloses that the key exists";

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy, store_with_predicate,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
    use crate::control::security::rls::PolicyType;

    /// `alice` in the shared fixture has user id 42, so a row carrying
    /// `owner_id = "42"` satisfies `owner_id = $auth.id` and any other does not.
    fn body(owner_id: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack_or_empty(&serde_json::json!({
            "owner_id": owner_id,
            "amount": 100,
        }))
    }

    fn kv_put_row(collection: &str, owner_id: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            key: b"k1".to_vec(),
            value: body(owner_id),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn kv_delete(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            keys: vec![b"k1".to_vec()],
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &nodedb_types::RlsWriteCheck {
        match plan {
            PhysicalPlan::Kv(KvOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Expire {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::FieldSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Incr {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::GetSet {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A multi-column KV row is a MessagePack map, exactly like a document
    /// body, so the write policy decides it at plan time: a conforming row is
    /// admitted and a violating one fails the statement.
    #[test]
    fn kv_put_is_admitted_or_rejected_on_its_own_post_image() {
        let store = store_with_write_policy("sessions");

        let mut conforming = kv_put_row("sessions", "42");
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = kv_put_row("sessions", "99");
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A single-column `value` write stores one opaque scalar: it carries no
    /// field the predicate could name, so it fails closed rather than being
    /// waved through as "not a document".
    #[test]
    fn an_opaque_scalar_value_is_rejected_under_a_write_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Put {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A `RETURNING` on a KV write ships rows back, so a read-only policy must
    /// land in the write's post-filter slot. Leaving it empty would return rows
    /// the same principal's `SELECT` hides.
    #[test]
    fn a_kv_write_receives_the_read_policy_filter() {
        let store = store_with_read_policy("sessions");
        let mut plan = kv_put_row("sessions", "42");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::Put { rls_filters, .. }) => assert!(
                !rls_filters.is_empty(),
                "the read policy must gate RETURNING output"
            ),
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A batch fails whole when any one of its rows violates the policy.
    #[test]
    fn batch_put_is_rejected_when_any_row_violates_the_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::BatchPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            entries: vec![(b"k1".to_vec(), body("42")), (b"k2".to_vec(), body("99"))],
            ttl_ms: 0,
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The row a delete removes is only known once the handler reads it, so
    /// the compiled predicate travels with the plan instead of refusing it.
    #[test]
    fn kv_delete_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = kv_delete("sessions");
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            write_check(&plan).has_predicate(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// A TTL mutation leaves the body untouched, so the stored row is the image
    /// the policy decides — shipped, not refused.
    #[test]
    fn expire_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Expire {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
            ttl_ms: 1_000,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// A field merge exists only after the stored row is read.
    #[test]
    fn field_set_carries_the_write_predicate() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::FieldSet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
            updates: Vec::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            if_present: false,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// `RETURNING` on a keyed or predicate UPDATE / DELETE ships rows back,
    /// so a read policy lands in each op's post-filter slot the same way it
    /// does for the KV insert ops.
    #[test]
    fn kv_update_and_delete_receive_the_read_policy_filter() {
        let store = store_with_read_policy("sessions");
        let collection = || {
            nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, "sessions")
        };
        let ops = [
            KvOp::Delete {
                collection: collection(),
                keys: vec![b"k1".to_vec()],
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            },
            KvOp::FieldSet {
                collection: collection(),
                key: b"k1".to_vec(),
                updates: Vec::new(),
                surrogate: nodedb_types::Surrogate::ZERO,
                if_present: false,
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            },
            KvOp::PredicateUpdate {
                collection: collection(),
                filters: Vec::new(),
                updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            },
            KvOp::PredicateDelete {
                collection: collection(),
                filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
            },
        ];
        for op in ops {
            let mut plan = PhysicalPlan::Kv(op);
            assert!(inject(&mut plan, &store).is_ok());
            match &plan {
                PhysicalPlan::Kv(
                    KvOp::Delete { rls_filters, .. }
                    | KvOp::FieldSet { rls_filters, .. }
                    | KvOp::PredicateUpdate { rls_filters, .. }
                    | KvOp::PredicateDelete { rls_filters, .. },
                ) => assert!(
                    !rls_filters.is_empty(),
                    "the read policy must gate RETURNING output for {plan:?}"
                ),
                other => panic!("plan shape changed: {other:?}"),
            }
        }
    }

    /// The incremented value is computed inside the engine, so the predicate
    /// rides along for the engine to decide the computed image against.
    #[test]
    fn incr_carries_the_write_predicate() {
        let store = store_with_write_policy("counters");
        let mut plan = PhysicalPlan::Kv(KvOp::Incr {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "counters",
            ),
            key: b"k1".to_vec(),
            delta: 1,
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(write_check(&plan).has_predicate());
    }

    /// `GETSET` needs both halves: the read filter bounds the old value it
    /// hands back, the write predicate bounds the value it stores. They are two
    /// distinct slots, never the same bytes reused.
    #[test]
    fn getset_carries_both_halves_in_separate_slots() {
        let store = store_with_predicate(
            "sessions",
            PolicyType::All,
            RlsPredicate::Compare {
                field: "owner_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
        );
        let mut plan = PhysicalPlan::Kv(KvOp::GetSet {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
            new_value: body("42"),
            surrogate: nodedb_types::Surrogate::ZERO,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::GetSet {
                rls_filters,
                rls_write_check,
                ..
            }) => {
                assert!(
                    !rls_filters.is_empty(),
                    "read half must gate the returned old value"
                );
                assert!(
                    rls_write_check.has_predicate(),
                    "write half must gate the write"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A cross-collection move carries one predicate per side, so a policy on
    /// either end reaches the gate that decides that end's row.
    #[test]
    fn transfer_item_carries_a_predicate_for_each_side() {
        let store = store_with_write_policy("vault");
        let mut plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "vault",
            ),
            dest_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "inbox",
            ),
            item_key: b"i1".to_vec(),
            dest_key: b"d1".to_vec(),
            surrogate: nodedb_types::Surrogate::ZERO,
            source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_rls_write_check,
                dest_rls_write_check,
                ..
            }) => {
                assert!(
                    source_rls_write_check.has_predicate(),
                    "the policed source must reach its own gate"
                );
                assert!(
                    !dest_rls_write_check.has_predicate(),
                    "an unpoliced destination must not inherit the source's predicate"
                );
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A truncate removes every row without reading one, so there is no image
    /// the policy could decide.
    #[test]
    fn truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            restart_identity: false,
        });
        assert_write_refused(inject(&mut plan, &store), "sessions");
    }

    /// A read policy alone must not start rejecting writes: the write half is
    /// keyed on write policies only.
    #[test]
    fn a_read_policy_alone_leaves_the_write_gate_empty() {
        let store = store_with_read_policy("sessions");
        let mut put = kv_put_row("sessions", "99");
        assert!(inject(&mut put, &store).is_ok());

        let mut delete = kv_delete("sessions");
        assert!(inject(&mut delete, &store).is_ok());
        assert!(!write_check(&delete).has_predicate());
    }

    /// A policy on a different collection must not restrict this one.
    #[test]
    fn kv_put_on_an_unpoliced_collection_runs() {
        let store = store_with_write_policy("other");
        let mut plan = kv_put_row("sessions", "99");
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// With no policy the write is untouched.
    #[test]
    fn kv_put_without_a_policy_is_untouched() {
        let mut plan = kv_put_row("sessions", "99");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A TTL probe on a policed collection discloses that a hidden key exists.
    #[test]
    fn get_ttl_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
        });
        assert_refused(inject(&mut plan, &store), "sessions");
    }

    /// A sorted-index read names no collection, so a read policy anywhere in
    /// the tenant refuses it: its ranked keys come from stored rows and carry
    /// no filter slot the policy could be applied through.
    #[test]
    fn sorted_index_read_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        match inject(&mut plan, &store) {
            Err(crate::Error::PlanError { detail }) => {
                assert!(detail.contains("sorted-index"), "got {detail}")
            }
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// …and every other sorted-index shape is refused for the same reason.
    #[test]
    fn every_sorted_index_shape_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("scores");
        for op in [
            KvOp::SortedIndexRank {
                index_name: "leaderboard".into(),
                primary_key: b"p1".to_vec(),
            },
            KvOp::SortedIndexRange {
                index_name: "leaderboard".into(),
                score_min: None,
                score_max: None,
            },
            KvOp::SortedIndexCount {
                index_name: "leaderboard".into(),
            },
        ] {
            let mut plan = PhysicalPlan::Kv(op);
            assert!(
                inject(&mut plan, &store).is_err(),
                "expected refusal for {plan:?}"
            );
        }
    }

    /// With no policy in the tenant the read is untouched, so an authorized
    /// caller sees exactly what it saw before.
    #[test]
    fn sorted_index_read_without_a_policy_is_untouched() {
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
