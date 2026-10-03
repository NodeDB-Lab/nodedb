// SPDX-License-Identifier: BUSL-1.1

//! Text search plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;
use nodedb_types::text_search::TextSearchParams;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::TextOp;

/// The query options a request names. An absent field takes its
/// [`TextSearchParams::default`] value, the default of SQL `text_match` and
/// of every `text_search` client.
fn search_params(fields: &TextFields) -> TextSearchParams {
    let defaults = TextSearchParams::default();
    TextSearchParams {
        mode: fields.text_mode.unwrap_or(defaults.mode),
        fuzzy: fields.fuzzy.unwrap_or(defaults.fuzzy),
    }
}

pub(crate) async fn build_search(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let params = search_params(fields);

    Ok(PhysicalPlan::Text(TextOp::Search {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        // An absent or empty field reads the whole-document index.
        field: fields.field.clone().filter(|f| !f.is_empty()),
        query: query_text.to_string(),
        top_k,
        mode: params.mode,
        fuzzy: params.fuzzy,
        prefilter: None,
        filters: Vec::new(),
        rls_filters: Vec::new(),
        scores: Vec::new(),
    }))
}

pub(crate) async fn build_hybrid_search(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let query_vector = fields
        .query_vector
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_vector'".to_string(),
        })?;
    let query_text = fields
        .query_text
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_text'".to_string(),
        })?;
    let top_k = fields.top_k.unwrap_or(10) as usize;
    let vector_weight = fields.vector_weight.unwrap_or(0.5) as f32;
    let ef_search = fields.ef_search.unwrap_or(0) as usize;
    let params = search_params(fields);

    Ok(PhysicalPlan::Text(TextOp::HybridSearch {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        // `field` names the vector column; absent keys the default index.
        vector_field: fields.field.clone().unwrap_or_default(),
        query_vector: query_vector.clone(),
        // The native frame names no text column: the text leg reads the
        // whole-document index.
        text_field: None,
        query_text: query_text.clone(),
        filters: Vec::new(),
        top_k,
        ef_search,
        mode: params.mode,
        fuzzy: params.fuzzy,
        vector_weight,
        filter_bitmap: None,
        rls_filters: Vec::new(),
        // Native-protocol path: the wire schema does not carry a
        // SELECT-style alias. Falls back to the executor's default name.
        score_alias: None,
    }))
}

#[cfg(test)]
mod tests {
    use nodedb_types::text_search::QueryMode;

    use super::*;

    #[test]
    fn absent_options_take_the_trait_default() {
        assert_eq!(
            search_params(&TextFields::default()),
            TextSearchParams::default()
        );
    }

    #[test]
    fn named_options_reach_the_params() {
        let fields = TextFields {
            text_mode: Some(QueryMode::And),
            fuzzy: Some(true),
            ..Default::default()
        };
        assert_eq!(
            search_params(&fields),
            TextSearchParams {
                mode: QueryMode::And,
                fuzzy: true,
            }
        );
    }
}
