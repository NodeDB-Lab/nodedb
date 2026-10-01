// SPDX-License-Identifier: Apache-2.0

//! Plan-time refusal of index-owned search functions in row-evaluated
//! positions.
//!
//! `bm25_score`, `search_score`, `text_match`, `search`, `rrf_score`,
//! `sparse_score`, `graph_score`, `multi_vector_score` and
//! `multi_vector_search` read a search index. The planner lowers each call it
//! recognises into its search plan, which serves the score as a column. A
//! call the planner could not lower stays in a filter, projection, sort key,
//! assignment or aggregate argument. The row evaluator has no index and no
//! value for it, so this pass refuses the statement at plan time. The
//! refusal does not depend on whether the collection holds rows.
//!
//! A wrapper plan (a subquery tail, aggregate, join or lateral join) over a
//! search plan is not checked for its own expressions: its projection names
//! the score column the search plan serves.

mod expressions;
mod lookup;
mod scope;

pub use scope::refuse_row_scoped_search_functions;
