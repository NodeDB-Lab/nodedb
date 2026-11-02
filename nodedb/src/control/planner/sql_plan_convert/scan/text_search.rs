// SPDX-License-Identifier: BUSL-1.1

//! `SqlPlan::TextSearch` → `TextOp` conversion.

use nodedb_physical::physical_plan::{TextOp, TextScoreSpec};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::types::TextSearchShape;

use super::super::filter::serialize_filters;
use super::super::scan_params::TextSearchConvertParams;
use crate::bridge::envelope::PhysicalPlan;

/// Lower a text search. A `Match` becomes `Search` (or `PhraseSearch` for a
/// quoted phrase); a `ScoreScan` becomes `BM25ScoreScan`. A `Match` with no
/// top-k bound returns every match (`usize::MAX`).
pub(in crate::control::planner::sql_plan_convert) fn convert_text_search(
    p: TextSearchConvertParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let collection_key = nodedb_types::CollectionKey::from_bare(p.database_id, p.collection);
    let collection = nodedb_types::QualifiedCollection::new(p.database_id, p.collection);
    let filters = serialize_filters(p.filters)?;
    let scores: Vec<TextScoreSpec> = p
        .scores
        .iter()
        .map(|s| TextScoreSpec {
            field: s.field.clone(),
            query: s.query.clone(),
            mode: s.mode,
            fuzzy: s.fuzzy,
            alias: s.alias.clone(),
        })
        .collect();

    let op = match p.shape {
        TextSearchShape::ScoreScan => TextOp::BM25ScoreScan {
            collection,
            filters,
            rls_filters: Vec::new(),
            scores,
            bound: None,
            system_time: p.system_time,
        },
        // The phrase words go as written. The Data Plane analyzes them with
        // the collection's analyzer, the analysis its indexed text took.
        TextSearchShape::Match {
            field,
            query: FtsQuery::Phrase(terms),
            top_k,
            ..
        } => TextOp::PhraseSearch {
            collection,
            field: field.clone(),
            terms: terms.clone(),
            top_k: top_k.unwrap_or(usize::MAX),
            prefilter: None,
            filters,
            rls_filters: Vec::new(),
            scores,
            system_time: p.system_time,
        },
        TextSearchShape::Match {
            field,
            query,
            mode,
            top_k,
        } => {
            let query_str = query
                .to_plain_string()
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: "unsupported FTS query form; use plain terms, AND/OR combinations, \
                             or phrase queries with text_match(field, '\"phrase here\"')"
                        .into(),
                })?;
            TextOp::Search {
                collection,
                field: field.clone(),
                query: query_str,
                top_k: top_k.unwrap_or(usize::MAX),
                mode: *mode,
                fuzzy: query.is_fuzzy(),
                prefilter: None,
                filters,
                rls_filters: Vec::new(),
                scores,
                system_time: p.system_time,
            }
        }
    };

    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: collection_key.vshard(),
        database_id: p.database_id,
        plan: PhysicalPlan::Text(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
