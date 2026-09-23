// SPDX-License-Identifier: BUSL-1.1

//! Full-text inverted index for Origin, backed by redb.
//!
//! Wraps `nodedb_fts::FtsIndex<RedbFtsBackend>` to provide persistent
//! full-text search with BM25 scoring. All scoring, tokenization, and
//! fuzzy logic live in `nodedb-fts`; this module adds Origin-specific
//! features: transaction-participating indexing and structural tenant
//! purge that bypass the LSM memtable.
//!
//! The public API takes `TenantId` as a first-class parameter. Every
//! persistent redb table is keyed by the structural tuple
//! `(tenant_id, collection, …)` — per-tenant drops are a tuple range scan,
//! not a lexical-prefix scan.

mod compaction;
mod core;
mod corpus_stats;
mod doc_image;
mod doc_terms;
mod errors;
mod indexing;
mod removal;
mod search;
mod synonyms;

pub use core::InvertedIndex;
pub use doc_image::FtsDocImage;
pub use indexing::IndexDocScope;
pub use nodedb_fts::FtsSearchParams;
pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};
pub use search::PhraseSearchParams;
