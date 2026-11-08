// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::backend::{FtsBackend, FtsHistoryRead, FtsHistoryWrite};
use crate::history::{EndedVersion, TextCut, cut_view};
use crate::posting::{Bm25Params, QueryMode};
use crate::scope::IndexScope;
use crate::test_support::test_governor;
use crate::{FtsSearchParams, Surrogate};

const SCOPE: IndexScope<'static> = IndexScope::document("docs");

fn hits(index: &FtsIndex<MemoryBackend>, query: &str) -> Vec<u32> {
    let mut ids: Vec<_> = index
        .search(
            0,
            1,
            SCOPE,
            FtsSearchParams {
                query,
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::Or,
                prefilter: None,
            },
        )
        .expect("search")
        .into_iter()
        .map(|hit| hit.doc_id.0)
        .collect();
    ids.sort_unstable();
    ids
}

fn history(index: &FtsIndex<MemoryBackend>, cut: i64) -> Vec<EndedVersion> {
    index
        .backend()
        .ended_seen_at(0, 1, SCOPE, TextCut::at(cut))
        .expect("history")
}

#[test]
fn preparation_preserves_history_and_isolates_document_changes() {
    let mut source = FtsIndex::with_params(
        MemoryBackend::new(),
        Bm25Params { k1: 2.0, b: 0.5 },
        test_governor(),
    );
    source
        .set_collection_analyzer(0, 1, "docs", "german")
        .expect("analyzer");
    source
        .set_collection_language(0, 1, "docs", "de")
        .expect("language");
    source
        .set_collection_fuzzy(0, 1, "docs", true)
        .expect("fuzzy");
    source
        .index_analyzed_document(0, 1, SCOPE, Surrogate(1), &["bravo".into()])
        .expect("index");
    source
        .backend_mut()
        .set_live_from(0, 1, SCOPE, Surrogate(1), 20)
        .expect("start");
    source
        .backend_mut()
        .put_ended(
            0,
            1,
            SCOPE,
            &EndedVersion {
                surrogate: Surrogate(1),
                sys_from: 10,
                sys_to: 20,
                tokens: vec!["alpha".into()],
            },
        )
        .expect("ended");
    source.flush_memtable(0, 1, SCOPE).expect("flush");
    source
        .index_analyzed_document(0, 1, SCOPE, Surrogate(2), &["delta".into()])
        .expect("index");
    let mut candidate = source.clone_for_preparation();
    assert_eq!(hits(&candidate, "bravo"), vec![1]);
    assert_eq!(hits(&candidate, "delta"), vec![2]);
    assert_eq!(history(&candidate, 15), history(&source, 15));
    assert_eq!(
        candidate
            .backend()
            .live_from(0, 1, SCOPE, Surrogate(1))
            .expect("start"),
        Some(20)
    );
    assert_eq!(
        candidate
            .backend()
            .read_doc_length(0, 1, SCOPE, Surrogate(2))
            .expect("length"),
        Some(1)
    );
    assert_eq!(
        candidate.memtable().stats(0, 1, SCOPE),
        source.memtable().stats(0, 1, SCOPE)
    );
    assert_eq!(candidate.bm25_params.k1, 2.0);
    assert_eq!(candidate.bm25_params.b, 0.5);
    assert_eq!(
        candidate
            .get_collection_analyzer(0, 1, "docs")
            .expect("analyzer")
            .as_deref(),
        Some("german")
    );
    assert_eq!(
        candidate
            .get_collection_language(0, 1, "docs")
            .expect("language")
            .as_deref(),
        Some("de")
    );
    assert!(candidate.get_collection_fuzzy(0, 1, "docs").expect("fuzzy"));
    assert_eq!(
        candidate.next_segment_id.load(Ordering::Relaxed),
        source.next_segment_id.load(Ordering::Relaxed)
    );

    candidate
        .backend_mut()
        .put_ended(
            0,
            1,
            SCOPE,
            &EndedVersion {
                surrogate: Surrogate(1),
                sys_from: 20,
                sys_to: 30,
                tokens: vec!["bravo".into()],
            },
        )
        .expect("ended");
    candidate
        .backend_mut()
        .clear_live_from(0, 1, SCOPE, Surrogate(1))
        .expect("clear start");
    candidate
        .remove_document(0, 1, SCOPE, Surrogate(1))
        .expect("remove");
    candidate
        .remove_document(0, 1, SCOPE, Surrogate(2))
        .expect("remove");
    candidate
        .index_analyzed_document(0, 1, SCOPE, Surrogate(1), &["zebra".into()])
        .expect("index");
    candidate
        .backend_mut()
        .set_live_from(0, 1, SCOPE, Surrogate(1), 30)
        .expect("start");
    candidate.flush_memtable(0, 1, SCOPE).expect("flush");
    assert_eq!(hits(&candidate, "zebra"), vec![1]);
    assert!(hits(&candidate, "bravo").is_empty());
    assert!(hits(&candidate, "delta").is_empty());
    assert_eq!(history(&candidate, 25)[0].tokens, vec!["bravo"]);
    assert!(history(&source, 25).is_empty());
    assert_eq!(history(&source, 15)[0].tokens, vec!["alpha"]);
    let source_cut = cut_view(source.backend(), 0, 1, SCOPE, TextCut::at(15)).expect("cut");
    assert_eq!(source_cut.docs[0].tokens, vec!["alpha"]);
    assert_eq!(hits(&source, "bravo"), vec![1]);
    assert_eq!(hits(&source, "delta"), vec![2]);
    assert_eq!(
        source
            .backend()
            .collection_stats(0, 1, SCOPE)
            .expect("stats"),
        (2, 2)
    );
    assert_eq!(
        source
            .backend()
            .list_segments(0, 1, SCOPE)
            .expect("segments")
            .len(),
        1
    );
    assert_eq!(
        candidate
            .backend()
            .list_segments(0, 1, SCOPE)
            .expect("segments")
            .len(),
        2
    );

    candidate.memtable().drain_collection(0, 1, "docs");
    candidate
        .backend()
        .purge_collection(0, 1, "docs")
        .expect("purge");
    assert!(hits(&candidate, "zebra").is_empty());
    assert!(history(&candidate, 15).is_empty());
    assert_eq!(hits(&source, "bravo"), vec![1]);
    assert_eq!(hits(&source, "delta"), vec![2]);
    assert_eq!(history(&source, 15)[0].tokens, vec!["alpha"]);
}

#[test]
fn retained_estimate_includes_current_documents_and_ended_token_storage() {
    let mut index = FtsIndex::new(MemoryBackend::new(), test_governor());
    let empty = index.estimated_clone_bytes();
    index
        .index_analyzed_document(0, 1, SCOPE, Surrogate(1), &["alpha".into(), "alpha".into()])
        .expect("index");
    let current = index.estimated_clone_bytes();
    assert!(current > empty);
    index
        .backend_mut()
        .put_ended(
            0,
            1,
            SCOPE,
            &EndedVersion {
                surrogate: Surrogate(1),
                sys_from: 10,
                sys_to: 20,
                tokens: vec!["alpha".into()],
            },
        )
        .expect("ended");
    let ended = index.estimated_clone_bytes();
    assert!(ended > current);
    index
        .backend_mut()
        .put_ended(
            0,
            1,
            SCOPE,
            &EndedVersion {
                surrogate: Surrogate(1),
                sys_from: 10,
                sys_to: 20,
                tokens: vec!["alpha".repeat(1024)],
            },
        )
        .expect("larger ended tokens");
    assert!(index.estimated_clone_bytes() >= ended + 5000);
    assert_eq!(hits(&index, "alpha"), vec![1]);
    let copy = index.clone_for_preparation();
    assert!(index.estimated_clone_bytes() >= copy.estimated_clone_bytes());
}

#[test]
fn preparation_keeps_spill_thresholds() {
    let source = FtsIndex::with_memtable_config(
        MemoryBackend::new(),
        crate::lsm::memtable::MemtableConfig {
            max_postings: 1,
            max_terms: 1,
        },
        test_governor(),
    );
    let candidate = source.clone_for_preparation();
    candidate
        .index_analyzed_document(0, 1, SCOPE, Surrogate(1), &["alpha".into()])
        .expect("index");
    assert!(candidate.memtable().is_empty());
    assert_eq!(
        candidate
            .backend()
            .list_segments(0, 1, SCOPE)
            .expect("segments")
            .len(),
        1
    );
    assert!(
        source
            .backend()
            .list_segments(0, 1, SCOPE)
            .expect("segments")
            .is_empty()
    );
    assert!(hits(&source, "alpha").is_empty());
}

#[test]
fn preparation_isolates_backend_posting_replacements() {
    let source = FtsIndex::new(MemoryBackend::new(), test_governor());
    let posting = crate::Posting {
        doc_id: Surrogate(1),
        term_freq: 2,
        positions: vec![0, 3],
    };
    source
        .backend()
        .upsert_posting(0, 1, SCOPE, "alpha", &posting)
        .expect("posting");
    let candidate = source.clone_for_preparation();
    assert_eq!(
        candidate
            .backend()
            .read_postings(0, 1, SCOPE, "alpha")
            .expect("read")
            .into_iter()
            .map(|p| (p.doc_id, p.term_freq, p.positions))
            .collect::<Vec<_>>(),
        vec![(posting.doc_id, posting.term_freq, posting.positions.clone())]
    );
    candidate
        .backend()
        .upsert_posting(
            0,
            1,
            SCOPE,
            "alpha",
            &crate::Posting {
                doc_id: Surrogate(1),
                term_freq: 1,
                positions: vec![9],
            },
        )
        .expect("replacement");
    assert_eq!(
        source
            .backend()
            .read_postings(0, 1, SCOPE, "alpha")
            .expect("source")
            .into_iter()
            .map(|p| (p.doc_id, p.term_freq, p.positions))
            .collect::<Vec<_>>(),
        vec![(posting.doc_id, posting.term_freq, posting.positions)]
    );
    assert_eq!(
        candidate
            .backend()
            .read_postings(0, 1, SCOPE, "alpha")
            .expect("candidate")[0]
            .positions,
        vec![9]
    );
    candidate
        .backend()
        .remove_postings(0, 1, SCOPE, "alpha")
        .expect("remove");
    assert_eq!(
        source
            .backend()
            .read_postings(0, 1, SCOPE, "alpha")
            .expect("source")[0]
            .positions,
        vec![0, 3]
    );
}
