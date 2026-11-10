// SPDX-License-Identifier: Apache-2.0

//! Lexer-aware parameter binding for DSL statements.
//!
//! # The architectural problem
//!
//! NodeDB's DSL statements — `UPSERT INTO`, `SEARCH`, `GRAPH`, `MATCH`,
//! `OPTIONAL MATCH`, `CRDT MERGE`, `CREATE VECTOR INDEX`,
//! `CREATE FULLTEXT INDEX`, `CREATE SEARCH INDEX`, `CREATE SPARSE INDEX`
//! — are dispatched from raw SQL text because they aren't part of
//! sqlparser's grammar. The planned-SQL path binds parameters on the
//! parsed AST, so nothing touches the DSL text: `$N` survives into the
//! dispatcher and reaches the engine as a literal string.
//!
//! The observed symptom was `cannot parse '$2' as INT` from the binary-
//! tuple encoder. That's a symptom of a class: any DSL reached through
//! the pgwire extended-query path silently skips parameter binding.
//!
//! # The fix
//!
//! A single chokepoint — `bind_dsl` — that every DSL-bound prepared
//! statement must go through before execution. It tokenizes the DSL
//! SQL with sqlparser's own tokenizer to locate each `Token::Placeholder`,
//! then splices the parameter's quoted literal in at that source position,
//! copying every other byte unchanged. This respects string boundaries,
//! quoted identifiers, and comments for free — sqlparser's lexer already
//! classified them — and never re-spells a token, so a quote inside an
//! existing literal stays escaped.
//!
//! The `BoundDslSql` newtype exists so the DSL execute path takes
//! `BoundDslSql`, not `&str`. The compiler refuses to execute a DSL
//! statement whose parameters haven't been bound. That is the
//! architectural enforcement — the mechanism that made this class of
//! bug possible (a `&str` flowing straight to the dispatcher) is
//! structurally gone.

mod bind;
mod literal;
mod splice;

pub use bind::{BoundDslSql, bind_dsl, bind_values};
