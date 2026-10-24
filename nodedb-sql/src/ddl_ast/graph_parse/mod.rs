// SPDX-License-Identifier: Apache-2.0

//! Typed parsing for the graph DSL (`GRAPH ...`, `MATCH ...`).
//!
//! A quote-, brace- and bracket-aware tokeniser feeds one parser per
//! statement. Each parser reads its clauses left to right and produces a
//! typed [`NodedbStatement`]. A token that is not a clause of the statement
//! is a parse error, so a misspelled clause keyword never runs the
//! statement with a default. Every graph DSL command flows through here
//! before reaching a handler, so the handlers never touch raw SQL.
//!
//! Numeric *ranges* and engine-level caps stay unvalidated here and are
//! enforced at the pgwire boundary, which keeps this module free of
//! `pgwire` dependencies and out of the `nodedb` → `nodedb-sql` edge.
//!
//! Structure is validated here, because the boundary cannot see it: a
//! required clause that is absent, or a clause value outside its own
//! vocabulary, means no statement reaches the boundary at all. Both are
//! reported as a parse error naming what is wrong.
//!
//! The tokenizer drops `(`, `)`, and `,`, so the batched edge statements check
//! the shape of their `VALUES` tail on the statement text (`batch_shape`): a
//! comma-separated list of parenthesized triples. A missing comma or a bare
//! word list is a parse error.

mod batch_shape;
mod cursor;
mod edge_predicate;
mod entry;
pub mod fusion_params;
mod tokenizer;
mod variants;

pub use entry::try_parse;
pub use fusion_params::{
    FusionKeywords, FusionParams, RAG_FUSION_KEYWORDS, SEARCH_FUSION_KEYWORDS,
    parse_search_using_fusion,
};
