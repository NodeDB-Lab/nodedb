// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for key-value engine operations.

use nodedb_physical::physical_plan::KvOp;

use super::context::{PermCtx, PermTreeLevel};

/// Exhaustive over [`KvOp`] so a new key-value operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_kv(ctx: &PermCtx<'_>, op: &mut KvOp) -> crate::Result<()> {
    match op {
        // Filter: the predicate scan pushes filters down, so the subtree ANDs
        // into the same slot as the user's predicate.
        KvOp::Scan {
            collection,
            filters,
            ..
        } => ctx.filter_into(collection, PermTreeLevel::Read, filters),

        // Filter: no pushdown slot, so the handler evaluates post-fetch.
        // A row outside the subtree reads back as absent.
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
        } => ctx.filter_into(collection, PermTreeLevel::Read, rls_filters),

        // Refuse: no row body to filter, and answering discloses that a
        // hidden key exists.
        KvOp::GetTtl { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the reply is a TTL rather than a row body, so the subtree filter cannot be evaluated \
             and the answer alone discloses that the key exists",
        ),

        // Refuse: the clone materializer streams raw `(key, value)` pairs
        // through a cursor payload with no filter slot.
        KvOp::MaterializeScan { collection, .. } => ctx.refuse_if_tree(
            collection,
            "the materializing scan streams raw stored values through a cursor payload that \
             carries no subtree filter",
        ),

        // Blanket write level: each writes a value at a key named directly,
        // no predicate to narrow.
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        | KvOp::Transfer { collection, .. }
        // A predicate update writes rows a scan selects, so no subtree filter
        // can narrow it: the identity must hold write access somewhere.
        | KvOp::PredicateUpdate { collection, .. } => {
            ctx.authorize(collection, PermTreeLevel::Write)
        }

        // Filter (delete level, blanket): all three remove rows the identity
        // does not enumerate — by key, by predicate, or wholesale.
        KvOp::Delete { collection, .. }
        | KvOp::PredicateDelete { collection, .. }
        | KvOp::Truncate { collection, .. } => ctx.authorize(collection, PermTreeLevel::Delete),

        // Blanket both levels: delete on source, write on destination.
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => {
            ctx.authorize(source_collection, PermTreeLevel::Delete)?;
            ctx.authorize(dest_collection, PermTreeLevel::Write)
        }

        // Refuse: plan names only the index, not its owning collection.
        // Falls back to the tenant-wide question, as RLS does.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. } => ctx.refuse_if_any_tree(
            "a sorted-index read returns ranked keys, a rank, or a count taken from stored rows, \
             and the plan names only the index",
        ),

        // Refuse: the reply is ranked keys, a rank, or a count, with no row
        // body to filter. The plan names the owning collection.
        KvOp::SortedIndexTxnRead { collection, .. } => ctx.refuse_if_tree(
            collection,
            "a sorted-index read returns ranked keys, a rank, or a count taken from stored rows, \
             so the subtree filter cannot be evaluated",
        ),

        // Resolve against the wrapped op: it is the intercepted write
        // verbatim, so it authorizes at exactly the level that write does.
        KvOp::ResolveWrite(inner) => apply_kv(ctx, inner),

        // Authorize every touched collection: a resolved `TransferItem`
        // spans two, so one blanket call would leave a side unauthorized.
        KvOp::ResolvedWrite { mutations, .. } => {
            for mutation in mutations.iter() {
                let level = match mutation {
                    nodedb_physical::physical_plan::KvResolvedMutation::Delete { .. } => {
                        PermTreeLevel::Delete
                    }
                    _ => PermTreeLevel::Write,
                };
                ctx.authorize(mutation.collection(), level)?;
            }
            Ok(())
        }

        // No-op: index DDL. It describes the collection rather than acting on
        // its rows, and is authorized as DDL rather than against a level.
        KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::KvOp;

    use super::super::plan::test_support::{
        apply, assert_refused, cache_with_tree, injected_resources, readable, sorted,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// A key-value get is narrowed to the readable subtree.
    #[test]
    fn get_receives_the_subtree_filter() {
        let cache = cache_with_tree("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        });
        assert!(apply(&mut plan, &cache).is_ok());
        match &plan {
            PhysicalPlan::Kv(KvOp::Get { rls_filters, .. }) => {
                assert_eq!(sorted(injected_resources(rls_filters)), readable());
            }
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// A TTL probe on a governed collection discloses that a hidden key
    /// exists.
    #[test]
    fn get_ttl_is_refused_under_a_tree() {
        let cache = cache_with_tree("sessions");
        let mut plan = PhysicalPlan::Kv(KvOp::GetTtl {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "sessions",
            ),
            key: b"k1".to_vec(),
        });
        assert_refused(apply(&mut plan, &cache), "sessions");
    }

    /// A sorted-index read names no collection, so a permission tree anywhere
    /// in the tenant refuses it: its ranked keys come from stored rows and
    /// carry no slot the subtree filter could go in.
    #[test]
    fn sorted_index_read_is_refused_under_a_tree() {
        let cache = cache_with_tree("scores");
        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        match apply(&mut plan, &cache) {
            Err(crate::Error::PlanError { detail }) => {
                assert!(detail.contains("sorted-index"), "got {detail}")
            }
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// With no tree in the tenant the read is untouched, so an authorized
    /// caller sees exactly what it saw before.
    #[test]
    fn sorted_index_read_without_a_tree_is_untouched() {
        use super::super::plan::test_support::apply_without_tree;

        let mut plan = PhysicalPlan::Kv(KvOp::SortedIndexTopK {
            index_name: "leaderboard".into(),
            k: 10,
        });
        let before = plan.clone();
        assert!(apply_without_tree(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
