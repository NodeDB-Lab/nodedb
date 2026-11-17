// SPDX-License-Identifier: Apache-2.0

//! History fixtures for the inline tests of this module.

use nodedb_types::Surrogate;

use crate::backend::memory::MemoryBackend;
use crate::history::{HistoryDoc, end_version, start_version};
use crate::scope::IndexScope;

/// Document `surrogate` of the whole-document index of `collection`.
pub(crate) fn doc_in(collection: &str, surrogate: u32) -> HistoryDoc<'_> {
    HistoryDoc {
        database_id: 0,
        tid: 1,
        index: IndexScope::document(collection),
        surrogate: Surrogate::new(surrogate),
    }
}

/// Document `surrogate` of `docs`.
pub(crate) fn doc(surrogate: u32) -> HistoryDoc<'static> {
    doc_in("docs", surrogate)
}

/// A write that indexes `surrogate` in `collection` at `at`.
pub(crate) fn start_in(store: &mut MemoryBackend, collection: &str, surrogate: u32, at: i64) {
    start_version(store, doc_in(collection, surrogate), at).expect("start");
}

/// A write that ends the version of `surrogate` in `collection` at `at`,
/// whose text is `tokens`.
pub(crate) fn end_in(
    store: &mut MemoryBackend,
    collection: &str,
    surrogate: u32,
    at: i64,
    tokens: &[&str],
) {
    let tokens: Vec<String> = tokens.iter().map(|t| t.to_string()).collect();
    end_version(store, doc_in(collection, surrogate), at, || Ok(tokens)).expect("end");
}

/// [`start_in`] on `docs`.
pub(crate) fn start(store: &mut MemoryBackend, surrogate: u32, at: i64) {
    start_in(store, "docs", surrogate, at);
}

/// [`end_in`] on `docs`.
pub(crate) fn end(store: &mut MemoryBackend, surrogate: u32, at: i64, tokens: &[&str]) {
    end_in(store, "docs", surrogate, at, tokens);
}
