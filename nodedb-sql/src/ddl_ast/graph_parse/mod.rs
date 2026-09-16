// SPDX-License-Identifier: Apache-2.0

//! Typed parsing for the graph DSL (`GRAPH ...`, `MATCH ...`).
//!
//! The handler layer historically parsed graph statements with
//! `upper.find("KEYWORD")` substring matching, which collapsed when
//! a node id, label, or property value shadowed a DSL keyword. This
//! module is the structural replacement: a quote- and brace-aware
//! tokeniser feeds a variant-building parser that produces a typed
//! [`NodedbStatement`]. Every graph DSL command flows through here
//! before reaching a pgwire handler, so the handlers never touch
//! raw SQL again.
//!
//! Numeric *ranges* and engine-level caps stay unvalidated here and are
//! enforced at the pgwire boundary, which keeps this module free of
//! `pgwire` dependencies and out of the `nodedb` → `nodedb-sql` edge.
//!
//! Structure is validated here, because the boundary cannot see it: a
//! required clause that is absent, or a clause value outside its own
//! vocabulary, means no statement reaches the boundary at all. Both are
//! reported as a parse error naming what is wrong.

mod cursor;
mod entry;
pub mod fusion_params;
mod helpers;
mod tokenizer;
mod variants;

pub use entry::try_parse;
pub use fusion_params::{
    FusionKeywords, FusionParams, RAG_FUSION_KEYWORDS, SEARCH_FUSION_KEYWORDS,
    parse_search_using_fusion,
};
