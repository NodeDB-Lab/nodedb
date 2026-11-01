// SPDX-License-Identifier: Apache-2.0

//! Secondary-index prefilter of a vector search.
//!
//! An equality conjunct on a `Ready` secondary-indexed field of a
//! `VectorSearch` names a row set the index answers without decoding any
//! body. The pass records it in `index_prefilter`. The convert layer lowers
//! it to an index lookup whose surrogates gate the candidate set, so the
//! search ranks only matching rows. Left as a residual filter alone, the
//! conjunct decodes every ranked candidate's body and widens the window
//! until enough rows pass. The conjunct stays in `filters`: every row the
//! index returns satisfies it.

use nodedb_types::DatabaseId;

use crate::engine_rules::find_indexed_equality;
use crate::error::Result;
use crate::types::*;

/// Record the indexed equality of a `VectorSearch` plan's filters in its
/// `index_prefilter`. Other plans are untouched.
///
/// The index holds the current state only. A bitemporal collection and a
/// vector-primary collection (its payload indexes serve the search) keep the
/// residual filter alone.
pub(super) fn apply_vector_index_prefilter(
    plan: &mut SqlPlan,
    catalog: &dyn SqlCatalog,
) -> Result<()> {
    let SqlPlan::VectorSearch {
        collection,
        filters,
        index_prefilter,
        ..
    } = plan
    else {
        return Ok(());
    };
    if index_prefilter.is_some() {
        return Ok(());
    }
    let Some(info) = catalog.get_collection(DatabaseId::DEFAULT, collection)? else {
        return Ok(());
    };
    if info.bitemporal || info.vector_primary.is_some() {
        return Ok(());
    }
    if let Some(found) = find_indexed_equality(filters, &info.indexes) {
        *index_prefilter = Some(IndexPrefilter {
            field: found.index.field.clone(),
            value: found.value,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::plan_select_sql;
    use crate::types::*;

    /// The index prefilter and residual filter count of a vector search plan.
    fn parts(sql: &str) -> (Option<IndexPrefilter>, usize) {
        match plan_select_sql(sql) {
            SqlPlan::VectorSearch {
                index_prefilter,
                filters,
                ..
            } => (index_prefilter, filters.len()),
            other => panic!("expected VectorSearch, got {other:?}"),
        }
    }

    #[test]
    fn an_equality_on_an_indexed_field_is_the_prefilter_and_stays_a_residual() {
        let (found, residual) = parts(
            "SELECT id FROM tagged_embeddings WHERE tag = 'a' AND embedding <-> ARRAY[1.0, 0.0] LIMIT 2",
        );
        assert_eq!(
            found,
            Some(IndexPrefilter {
                field: "$.tag".into(),
                value: SqlValue::String("a".into()),
            })
        );
        assert_eq!(residual, 1);
    }

    #[test]
    fn an_equality_inside_a_conjunction_is_the_prefilter() {
        let (found, _) = parts(
            "SELECT id FROM tagged_embeddings WHERE score > 0.5 AND tag = 'a' AND embedding <-> ARRAY[1.0, 0.0] LIMIT 2",
        );
        assert_eq!(found.map(|f| f.field), Some("$.tag".to_string()));
    }

    #[test]
    fn an_unindexed_field_is_no_prefilter() {
        let (found, _) = parts(
            "SELECT id FROM tagged_embeddings WHERE owner = 'a' AND embedding <-> ARRAY[1.0, 0.0] LIMIT 2",
        );
        assert_eq!(found, None);
    }

    #[test]
    fn a_disjunction_is_no_prefilter() {
        let (found, _) = parts(
            "SELECT id FROM tagged_embeddings WHERE (tag = 'a' OR owner = 'b') AND embedding <-> ARRAY[1.0, 0.0] LIMIT 2",
        );
        assert_eq!(found, None);
    }

    #[test]
    fn a_bitemporal_collection_is_no_prefilter() {
        let (found, residual) = parts(
            "SELECT id FROM tagged_embeddings_bitemporal WHERE tag = 'a' AND embedding <-> ARRAY[1.0, 0.0] LIMIT 2",
        );
        assert_eq!(found, None);
        assert_eq!(residual, 1);
    }

    #[test]
    fn no_filter_is_no_prefilter() {
        let (found, _) = parts(
            "SELECT id FROM tagged_embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(found, None);
    }
}
