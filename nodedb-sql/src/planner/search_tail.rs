// SPDX-License-Identifier: Apache-2.0

//! Computed SELECT-list items over a search plan.
//!
//! A text, vector, sparse, multi-vector, or hybrid search returns hit rows
//! and evaluates no expression. Its own score reaches each row under a fixed
//! key: a text score under its alias, an RRF score under the hybrid score
//! alias, a vector or sparse score under `distance`. Every other computed
//! item runs in a [`SqlPlan::Subquery`] tail over the hit rows, and a score
//! item is read from its key there.
//!
//! The pass runs once ORDER BY and LIMIT are applied, so the search keeps
//! its own top-k bound and a sort on a computed alias reads the tail's rows.

use crate::functions::registry::{FunctionRegistry, SearchTrigger};
use crate::planner::computed_tail::{TailBounds, body_projection, with_computed_tail};
use crate::types::{Projection, SqlExpr, SqlPlan};

/// The row key a vector-family search writes its score under.
const DISTANCE_KEY: &str = "distance";

/// The row key a hybrid search writes its RRF score under without an alias.
const DEFAULT_RRF_KEY: &str = "rrf_score";

/// Lift the computed items of the search plan at the bottom of `plan` into
/// a tail. A plan that is not a search, or holds no computed item the
/// search cannot answer, returns unchanged.
pub fn lift_search_computed(plan: SqlPlan, functions: &FunctionRegistry) -> SqlPlan {
    match plan {
        SqlPlan::Subquery {
            input,
            filters,
            projection,
            window_functions,
            sort_keys,
            offset,
            distinct,
            limit,
        } => SqlPlan::Subquery {
            input: Box::new(lift_search_computed(*input, functions)),
            filters,
            projection,
            window_functions,
            sort_keys,
            offset,
            distinct,
            limit,
        },
        leaf => lift_leaf(leaf, functions),
    }
}

/// How a search plan delivers its own score.
enum ScoreSource {
    /// Text scores, each under its alias.
    Text(Vec<String>),
    /// The RRF score under one key.
    Hybrid(String),
    /// The vector or sparse score under `distance`. `field` is the column
    /// the search ranks by, when the plan names one.
    Vector(Option<String>),
}

impl ScoreSource {
    fn of(plan: &SqlPlan) -> Option<Self> {
        match plan {
            SqlPlan::TextSearch(search) => Some(Self::Text(
                search.scores.iter().map(|s| s.alias.clone()).collect(),
            )),
            SqlPlan::HybridSearch(search) => Some(Self::hybrid(search.score_alias.as_deref())),
            SqlPlan::HybridSearchTriple(search) => {
                Some(Self::hybrid(search.score_alias.as_deref()))
            }
            SqlPlan::VectorSearch { field, .. } | SqlPlan::SparseSearch { field, .. } => {
                Some(Self::Vector(Some(field.clone())))
            }
            SqlPlan::MultiVectorSearch { .. } => Some(Self::Vector(None)),
            _ => None,
        }
    }

    fn hybrid(alias: Option<&str>) -> Self {
        Self::Hybrid(alias.unwrap_or(DEFAULT_RRF_KEY).to_string())
    }

    /// The tail item that reads `item` as the search's own score, if it is
    /// one.
    fn score_item(&self, item: &Projection, functions: &FunctionRegistry) -> Option<Projection> {
        let Projection::Computed { expr, alias } = item else {
            return None;
        };
        match self {
            Self::Text(aliases) => aliases
                .contains(alias)
                .then(|| Projection::Column(alias.clone())),
            Self::Hybrid(key) => (trigger(expr, functions) == Some(SearchTrigger::HybridSearch))
                .then(|| read_key(key, alias)),
            Self::Vector(field) => {
                let SqlExpr::Function { args, .. } = expr else {
                    return None;
                };
                let ranks = matches!(
                    trigger(expr, functions),
                    Some(
                        SearchTrigger::VectorSearch
                            | SearchTrigger::MultiVectorSearch
                            | SearchTrigger::SparseSearch
                    )
                );
                // A one-argument call (`vector_distance(ARRAY[...])`) ranks
                // by the collection's own vector column.
                let same_column = match (field, args.as_slice()) {
                    (None, _) | (Some(_), [_]) => true,
                    (Some(field), [SqlExpr::Column { name, .. }, ..]) => name == field,
                    (Some(_), _) => false,
                };
                (ranks && same_column).then(|| read_key(DISTANCE_KEY, alias))
            }
        }
    }

    /// Whether the search already answers a score item without a tail.
    fn answers_scores(&self) -> bool {
        match self {
            Self::Text(_) | Self::Hybrid(_) => true,
            Self::Vector(_) => false,
        }
    }
}

fn trigger(expr: &SqlExpr, functions: &FunctionRegistry) -> Option<SearchTrigger> {
    match expr {
        SqlExpr::Function { name, .. } => Some(functions.search_trigger(name)),
        _ => None,
    }
}

/// A tail item that emits row key `key` under `alias`.
fn read_key(key: &str, alias: &str) -> Projection {
    Projection::Computed {
        expr: SqlExpr::Column {
            table: None,
            name: key.to_string(),
        },
        alias: alias.to_string(),
    }
}

/// Whether `read` emits a row key under that same name.
fn reads_own_key(read: &Projection) -> bool {
    match read {
        Projection::Column(_) => true,
        Projection::Computed {
            expr: SqlExpr::Column { table: None, name },
            alias,
        } => name == alias,
        Projection::Computed { .. }
        | Projection::CpComputed { .. }
        | Projection::Star
        | Projection::QualifiedStar(_) => false,
    }
}

fn lift_leaf(mut plan: SqlPlan, functions: &FunctionRegistry) -> SqlPlan {
    let Some(source) = ScoreSource::of(&plan) else {
        return plan;
    };
    let Some(original) = projection_mut(&mut plan).cloned() else {
        return plan;
    };

    let mut tail = Vec::with_capacity(original.len());
    let mut others = Vec::with_capacity(original.len());
    let mut scores = Vec::new();
    let mut needs_tail = false;
    for item in &original {
        match source.score_item(item, functions) {
            Some(read) => {
                // A score read under its own key needs no tail: the hit row
                // already carries it under the name the schema announces.
                needs_tail |= !source.answers_scores() && !reads_own_key(&read);
                scores.push(item.clone());
                tail.push(read);
            }
            None => {
                needs_tail |= matches!(item, Projection::Computed { .. });
                others.push(item.clone());
                tail.push(item.clone());
            }
        }
    }
    if !needs_tail {
        return plan;
    }

    let has_star = original
        .iter()
        .any(|p| matches!(p, Projection::Star | Projection::QualifiedStar(_)));
    let mut body = if has_star {
        vec![Projection::Star]
    } else {
        body_projection(&others)
    };
    match &source {
        // The body announces each score under its own key, typed as the
        // search types it.
        ScoreSource::Text(_) | ScoreSource::Hybrid(_) => body.extend(scores),
        ScoreSource::Vector(_) if !has_star => body.push(Projection::Column(DISTANCE_KEY.into())),
        ScoreSource::Vector(_) => {}
    }
    if let Some(projection) = projection_mut(&mut plan) {
        *projection = body;
    }
    // `tail` holds a computed item here, so the wrap always applies.
    with_computed_tail(plan, &tail, TailBounds::default())
}

/// The SELECT list a search plan carries.
fn projection_mut(plan: &mut SqlPlan) -> Option<&mut Vec<Projection>> {
    match plan {
        SqlPlan::TextSearch(search) => Some(&mut search.projection),
        SqlPlan::HybridSearch(search) => Some(&mut search.projection),
        SqlPlan::HybridSearchTriple(search) => Some(&mut search.projection),
        SqlPlan::VectorSearch { projection, .. }
        | SqlPlan::SparseSearch { projection, .. }
        | SqlPlan::MultiVectorSearch { projection, .. } => Some(projection),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TextScoreColumn, TextSearchPlan, TextSearchShape};
    use nodedb_types::text_search::QueryMode;

    fn column(name: &str) -> SqlExpr {
        SqlExpr::Column {
            table: None,
            name: name.into(),
        }
    }

    fn text_search(projection: Vec<Projection>) -> SqlPlan {
        SqlPlan::TextSearch(TextSearchPlan {
            collection: "docs".into(),
            shape: TextSearchShape::ScoreScan,
            filters: Vec::new(),
            scores: vec![TextScoreColumn {
                field: Some("body".into()),
                query: "x".into(),
                mode: QueryMode::Or,
                fuzzy: false,
                alias: "s".into(),
            }],
            projection,
            temporal: crate::temporal::TemporalScope::default(),
        })
    }

    fn score_item() -> Projection {
        Projection::Computed {
            expr: SqlExpr::Function {
                name: "bm25_score".into(),
                args: vec![column("body")],
                distinct: false,
            },
            alias: "s".into(),
        }
    }

    #[test]
    fn a_search_with_only_its_score_stays_whole() {
        let plan = text_search(vec![Projection::Column("id".into()), score_item()]);
        let lifted = lift_search_computed(plan, &FunctionRegistry::new());
        assert!(matches!(lifted, SqlPlan::TextSearch(_)), "{lifted:?}");
    }

    #[test]
    fn a_computed_item_moves_to_a_tail_that_reads_the_score() {
        let upper = Projection::Computed {
            expr: SqlExpr::Function {
                name: "upper".into(),
                args: vec![column("title")],
                distinct: false,
            },
            alias: "u".into(),
        };
        let plan = text_search(vec![Projection::Column("id".into()), score_item(), upper]);
        let SqlPlan::Subquery {
            input, projection, ..
        } = lift_search_computed(plan, &FunctionRegistry::new())
        else {
            panic!("expected a tail");
        };
        assert!(matches!(&projection[1], Projection::Column(name) if name == "s"));
        assert!(matches!(&projection[2], Projection::Computed { alias, .. } if alias == "u"));
        let SqlPlan::TextSearch(search) = *input else {
            panic!("expected the search as the body");
        };
        let body: Vec<String> = search
            .projection
            .iter()
            .map(|p| match p {
                Projection::Column(name) => name.clone(),
                Projection::Computed { alias, .. } => format!("score:{alias}"),
                other => panic!("unexpected body item {other:?}"),
            })
            .collect();
        assert_eq!(body, ["id", "title", "score:s"]);
    }
}
