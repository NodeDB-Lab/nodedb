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
//! `(database_id, tenant_id, collection, field, …)` — per-tenant drops are a
//! tuple range scan, not a lexical-prefix scan. Each collection has a
//! whole-document index and one index per top-level string field.

mod batch_removal;
mod compaction;
mod core;
mod corpus_stats;
mod doc_fields;
mod doc_image;
mod doc_terms;
mod document;
mod errors;
mod indexing;
mod rebuild_install;
mod rebuild_journal;
mod rebuild_snapshot;
mod removal;
mod search;
mod staged_search;
mod synonyms;
#[cfg(test)]
pub(crate) mod test_support;

pub use core::InvertedIndex;
pub use doc_image::FtsDocImage;
pub use indexing::IndexDocScope;
pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};
pub use nodedb_fts::{DocumentText, FtsSearchParams, IndexScope};
pub use rebuild_install::{FtsInstallOutcome, FtsRebuildRefusal};
pub use rebuild_journal::FTS_REBUILD_JOURNAL_MAX_DOCS;
pub use rebuild_snapshot::{FtsRebuildTicket, FtsRebuilt, FtsSnapshot};
pub use search::PhraseSearchParams;
pub use staged_search::TextDocScorer;
