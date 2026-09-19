// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for vector-engine operations.

use nodedb_physical::physical_plan::VectorOp;

use super::context::RlsCtx;
use super::plan::walk;

/// Exhaustive over [`VectorOp`] so a new vector operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_vector(ctx: &RlsCtx<'_>, op: &mut VectorOp) -> crate::Result<()> {
    match op {
        // Inject, then recurse: the policy lands in the post-candidate slot the
        // handler applies after HNSW returns candidates, and the prefilter
        // sub-plan is a full plan in its own right whose rows must be resolved
        // too — the redaction pass walks the same child for the same reason.
        VectorOp::Search {
            collection,
            rls_filters,
            inline_prefilter_plan,
            ..
        } => {
            ctx.set_post_filters(collection, rls_filters)?;
            match inline_prefilter_plan {
                Some(child) => walk(ctx, child),
                None => Ok(()),
            }
        }

        // Inject: the fused per-field results are filtered post-candidate.
        VectorOp::MultiSearch {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: both return scored document identities with no filter slot,
        // so the rows a policy hides would still be ranked and returned.
        VectorOp::SparseSearch { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the search returns scored document identities through a response shape that carries \
             no row filter",
        ),

        // Refuse: index statistics count the indexed rows, including the ones
        // the policy hides, and a statistics payload carries no row to filter.
        VectorOp::QueryStats { collection, .. } => ctx.refuse_if_policy(
            collection,
            "index statistics are counts over the indexed rows, which the row filter cannot be \
             evaluated against",
        ),

        // Admit: a vector-primary collection stores the row here and nowhere
        // else — no companion document write would gate it — and this op
        // carries `payload`, the MessagePack image of every non-vector column
        // the statement supplied, so the policy decides that image directly.
        //
        // The image holds what the STATEMENT wrote, not the collection's
        // declared columns: an INSERT that omits the governed column leaves the
        // predicate nothing to test, and the write fails closed. That is the
        // safe direction, and the same one a document insert missing the
        // governed column takes — but worth stating here, because only the
        // fields listed in `payload_indexes` are queryable afterwards, which
        // makes it easy to treat every other column as if it did not travel
        // with the write. It does, and the policy decides it.
        //
        // `payload` is a zerompk `HashMap<String, Value>`, whose values are
        // TAGGED, so it goes through the transcoding admission rather than the
        // raw-image one — see [`RlsCtx::admit_write_value_map_image`].
        VectorOp::DirectInsert {
            collection,
            payload,
            rls_filters,
            ..
        }
        | VectorOp::DirectInsertIfAbsent {
            collection,
            payload,
            rls_filters,
            ..
        } => {
            ctx.admit_write_value_map_image(collection, payload)?;
            // The read filter is independent of that write admission. A
            // `RETURNING` clause on this op ships the stored row back, and that
            // output is a read, so a row a read-only policy hides must not
            // become visible just because the statement wrote it. A collection
            // can carry a `FOR SELECT` policy and no write policy at all, in
            // which case the write is unrestricted and only the returned row
            // set shrinks.
            ctx.set_post_filters(collection, rls_filters)
        }

        // Admit the proposed image here; a conflict patch produces its merged
        // image on the Data Plane, so the predicate ships for that path.
        VectorOp::DirectUpsert {
            collection,
            payload,
            rls_filters,
            on_conflict_updates,
            rls_write_check,
            ..
        } => {
            ctx.admit_write_value_map_image(collection, payload)?;
            if on_conflict_updates.is_empty() {
                *rls_write_check = nodedb_types::RlsWriteCheck::decided_earlier_in_request();
            } else {
                ctx.set_write_check(collection, rls_write_check)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the predicate and gate `RETURNING` as a read: the removed row
        // or the patched post-image exists only where it is persisted, and
        // the rows the clause hands back are bounded by the same read policy a
        // `SELECT` by this principal is. Mirrors `KvOp::{Delete,
        // PredicateUpdate}`.
        VectorOp::DirectDelete {
            collection,
            rls_write_check,
            rls_filters,
            ..
        }
        | VectorOp::DirectUpdate {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }

        // Refuse: these carry an embedding, a surrogate, or an opaque document
        // id — never the row body a policy predicate names — so no image is
        // available for the write policy to be evaluated against. A vector
        // entry is a claim about a row, and admitting it unchecked would let an
        // identity the policy restricts make a hidden row reachable by search.
        //
        // `Insert` is also reachable from the document insert path, where the
        // full row is decided by the document write gate before this op is
        // dispatched; refusing here bounds the case where such a plan arrives
        // on its own, with no row body to decide.
        VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a vector write carries an embedding and a surrogate rather than the row body the \
             policy names, so no row image is available for it to be evaluated against",
        ),

        // Refuse: removes every row without reading one, so no image
        // exists to evaluate against. Mirrors `KvOp::Truncate`.
        VectorOp::DirectTruncate { collection, .. } => ctx.refuse_if_write_policy(
            collection,
            "a truncate removes every row without reading one, so no row image is available",
        ),

        // No-op: already decided by the resolve pass; re-injecting would
        // replace a verdict with a predicate no applying node can decide.
        VectorOp::ResolvedDirectWrite { .. } => Ok(()),

        // Recurse: the wrapped op is the intercepted write verbatim.
        VectorOp::ResolveDirectWrite(inner) => inject_vector(ctx, inner),

        // No-op: index parameters and index maintenance write no user row.
        VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, VectorOp};

    use super::super::plan::test_support::{
        assert_refused, assert_write_refused, inject, inject_without_policy,
        store_with_read_policy, store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    fn vector_insert(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            vector: vec![0.0],
            dim: 1,
            field_name: String::new(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: None,
            provenance: None,
        })
    }

    /// Indexing a row the policy hides makes it reachable by search, which is
    /// the disclosure the policy exists to prevent — and the plan carries an
    /// embedding, not the row body, so nothing here can be evaluated.
    #[test]
    fn vector_insert_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("docs");
        let mut plan = vector_insert("docs");
        assert_write_refused(inject(&mut plan, &store), "docs");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn vector_insert_without_a_policy_is_untouched() {
        let mut plan = vector_insert("docs");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    fn vector_truncate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::DirectTruncate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            field: "vec".into(),
            restart_identity: false,
        })
    }

    /// A truncate reads no row, so nothing exists for the write policy to
    /// decide against; it is refused like `KvOp::Truncate`.
    #[test]
    fn vector_truncate_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("docs");
        let mut plan = vector_truncate("docs");
        assert_write_refused(inject(&mut plan, &store), "docs");
    }

    #[test]
    fn vector_truncate_without_a_policy_is_untouched() {
        let mut plan = vector_truncate("docs");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    fn search_with_prefilter(collection: &str, prefilter: Option<PhysicalPlan>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            query_vector: vec![0.1, 0.2],
            top_k: 4,
            ef_search: 0,
            metric: nodedb_types::vector_distance::DistanceMetric::L2,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: prefilter.map(Box::new),
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
        })
    }

    /// The prefilter sub-plan is a real read of its own collection, so a
    /// refusable op nested there is still caught.
    #[test]
    fn inline_prefilter_plan_is_walked() {
        let store = store_with_read_policy("users");
        let mut plan = search_with_prefilter(
            "embeddings",
            Some(PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "users",
                ),
                path: "$.email".into(),
                value: "a@b.c".into(),
            })),
        );
        assert_refused(inject(&mut plan, &store), "users");
    }

    fn direct_upsert(collection: &str, payload: &[(&str, &str)]) -> PhysicalPlan {
        let map: std::collections::HashMap<String, nodedb_types::Value> = payload
            .iter()
            .map(|(k, v)| ((*k).to_string(), nodedb_types::Value::String((*v).into())))
            .collect();
        let payload = if map.is_empty() {
            Vec::new()
        } else {
            zerompk::to_msgpack_vec(&map).expect("encode payload")
        };
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            field: "emb".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            vector: vec![0.1, 0.2],
            payload,
            quantization: Default::default(),
            storage_dtype: Default::default(),
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })
    }

    /// A vector-primary row lives only in this op, so its payload image is
    /// decided here and a conforming one is admitted.
    ///
    /// The payload is built exactly as the planner builds it — a zerompk
    /// `HashMap<String, Value>`, whose values are tag-prefixed arrays — so this
    /// also pins that the image is transcoded before evaluation. Handing those
    /// bytes to the row evaluator raw rejects every row, conforming or not.
    #[test]
    fn conforming_direct_upsert_is_admitted() {
        let store = store_with_write_policy("docs");
        let mut plan = direct_upsert("docs", &[("owner_id", "42")]);
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// …and a violating payload fails the statement.
    #[test]
    fn violating_direct_upsert_is_rejected() {
        let store = store_with_write_policy("docs");
        let mut plan = direct_upsert("docs", &[("owner_id", "99")]);
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// `payload` carries what the statement wrote, so a row that omits the
    /// governed column gives the predicate nothing to test and fails closed.
    /// Pinned so the behavior is stated rather than discovered.
    #[test]
    fn direct_upsert_denies_when_the_statement_omits_the_governed_column() {
        let store = store_with_write_policy("docs");
        let mut plan = direct_upsert("docs", &[("region", "eu")]);
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// With no write policy the same upsert runs with its payload, filters,
    /// and patch untouched; only the write-check slot is stamped as decided.
    #[test]
    fn direct_upsert_without_a_policy_is_untouched() {
        let mut plan = direct_upsert("docs", &[("region", "eu")]);
        let mut before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        if let PhysicalPlan::Vector(VectorOp::DirectUpsert {
            rls_write_check, ..
        }) = &mut before
        {
            *rls_write_check = nodedb_types::RlsWriteCheck::decided_earlier_in_request();
        }
        assert_eq!(plan, before);
    }

    /// A sparse search has no filter slot, so a policy refuses it.
    #[test]
    fn sparse_search_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("docs");
        let mut plan = PhysicalPlan::Vector(VectorOp::SparseSearch {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            field_name: "sparse".into(),
            query_entries: vec![(1, 1.0)],
            top_k: 5,
        });
        assert_refused(inject(&mut plan, &store), "docs");
    }
}
