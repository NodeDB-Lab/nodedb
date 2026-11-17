// SPDX-License-Identifier: BUSL-1.1

//! The on-core half of a collection rebuild: the atomic cutover.
//!
//! One redb write transaction on the live database:
//!
//! 1. drops every indexed row of the collection, keeping its analyzer,
//!    language and fuzzy rows and its synced text,
//! 2. copies the scratch database's rows of the collection in, leaving out
//!    every document the journal noted,
//! 3. purges the copied text history at the highest horizon a history
//!    purge applied during the rebuild,
//! 4. hands each noted document to the caller's replay, which derives it
//!    again from its live versions and frames in the same transaction. A
//!    frame write notes its document even when it indexes nothing.
//!
//! Readers see the transaction's state before or after the commit, never a
//! mix.
//!
//! A pass whose journal passed its bound does not cut over. The install
//! hands it back as a catch-up round: see `rebuild_round`.

use nodedb_fts::history;
use redb::{ReadableDatabase as _, WriteTransaction};

use super::core::InvertedIndex;
use super::errors::inverted_err;
use super::rebuild_round::PassEnd;
use super::rebuild_snapshot::{FtsCatchUp, FtsRebuilt};
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::backend::RedbHistoryTxn;
use crate::engine::sparse::fts_redb::backend::rebuild_copy::{
    CollectionCopy, copy_collection_rows,
};
use crate::engine::storage_bytes::StorageDelta;

/// Why a cutover left the live index as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FtsRebuildRefusal {
    /// The journal of this rebuild is gone: it was aborted, or a newer
    /// rebuild of the collection replaced it.
    #[error("the rebuild's journal is closed; a newer rebuild or an abort replaced it")]
    Superseded,
    /// The collection or its tenant was purged during the rebuild.
    #[error("the collection was purged during the rebuild")]
    Purged,
    /// Writes still passed the journal bound after every catch-up round.
    #[error(
        "writes outpaced the rebuild: {docs_per_sec} distinct documents per second \
         were written after {rounds} catch-up rounds; run the rebuild again at a lower \
         write rate"
    )]
    WritesOutpaceRebuild { rounds: u32, docs_per_sec: u64 },
}

/// What a cutover did.
#[derive(Debug)]
pub enum FtsInstallOutcome {
    /// The rebuilt rows are live.
    Installed {
        /// Documents written during the rebuild and replayed.
        replayed: usize,
    },
    /// More documents were written during the pass than the cutover
    /// replays on the core. The live index is unchanged. A catch-up round
    /// derives them off the core, then installs again.
    CatchUp(Box<FtsCatchUp>),
    /// The live index is unchanged.
    Refused(FtsRebuildRefusal),
}

/// Re-derives the documents the journal noted, inside the cutover
/// transaction. Gets the noted surrogates in ascending order.
pub type JournalReplay<'r> = dyn FnMut(&WriteTransaction, &[u32]) -> crate::Result<()> + 'r;

impl InvertedIndex {
    /// Install `rebuilt` over the live rows of its collection, with every
    /// document the journal noted replayed by `replay`, in one transaction.
    ///
    /// Closes the journal in every case but a catch-up round, which keeps
    /// it recording. A catch-up, a refusal or an error leaves the live
    /// index as it is.
    pub fn install_rebuild(
        &self,
        rebuilt: FtsRebuilt,
        replay: &mut JournalReplay<'_>,
    ) -> crate::Result<FtsInstallOutcome> {
        let journal = match self.end_rebuild_pass(rebuilt.token)? {
            PassEnd::Gone => {
                return Ok(FtsInstallOutcome::Refused(FtsRebuildRefusal::Superseded));
            }
            PassEnd::Purged => return Ok(FtsInstallOutcome::Refused(FtsRebuildRefusal::Purged)),
            PassEnd::Outpaced {
                rounds,
                docs_per_sec,
            } => {
                return Ok(FtsInstallOutcome::Refused(
                    FtsRebuildRefusal::WritesOutpaceRebuild {
                        rounds,
                        docs_per_sec,
                    },
                ));
            }
            PassEnd::CatchUp { ticket, touched } => {
                return Ok(FtsInstallOutcome::CatchUp(Box::new(FtsCatchUp {
                    ticket,
                    previous: rebuilt,
                    touched,
                })));
            }
            PassEnd::CutOver(journal) => journal,
        };
        let skip = journal.touched;
        // A bitmap iterates in ascending order.
        let touched: Vec<u32> = skip.iter().collect();

        let (db, tid, collection) = (rebuilt.database_id, rebuilt.tid, &rebuilt.collection);
        let backend = self.inner.backend();
        let scratch = rebuilt
            .scratch
            .begin_read()
            .map_err(|e| inverted_err("rebuild scratch read txn", e))?;
        let txn = backend
            .db()
            .begin_write()
            .map_err(|e| inverted_err("rebuild cutover txn", e))?;

        let mut delta = StorageDelta::default();
        backend.clear_collection_data_in_txn(&txn, db, tid, collection, &mut delta)?;
        copy_collection_rows(
            &scratch,
            &txn,
            CollectionCopy {
                database_id: db,
                tid,
                collection,
                skip: &skip,
                on_row: &|| Ok(()),
            },
            &mut delta,
        )?;
        if let Some(horizon) = journal.purged_below {
            // A text-history purge ran during the rebuild. The snapshot
            // still held the versions it dropped.
            let horizon_of = |d: u64, t: u64, c: &str| {
                (d == db && t == tid && c == collection.as_str()).then_some(horizon)
            };
            let mut store = RedbHistoryTxn::new(&txn, &mut delta);
            history::purge_below(&mut store, &horizon_of)?;
        }
        delta.apply(&txn, SPARSE_COUNTER)?;
        replay(&txn, &touched)?;

        txn.commit()
            .map_err(|e| inverted_err("rebuild cutover commit", e))?;
        self.inner.memtable().drain_collection(db, tid, collection);
        backend.release_collection_segments(db, tid, collection);
        Ok(FtsInstallOutcome::Installed {
            replayed: touched.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_fts::posting::QueryMode;
    use nodedb_fts::{DocumentText, FtsSearchParams, IndexScope};
    use nodedb_types::{Surrogate, TenantId};

    use super::*;
    use crate::engine::durability_gate::GatedDatabase;
    use crate::engine::sparse::inverted::FtsRebuildBounds;
    use crate::engine::sparse::inverted::IndexDocScope;
    use crate::engine::sparse::inverted::test_support::{body, fields};

    const DB: u64 = 0;
    const T: TenantId = TenantId::new(1);

    fn open_at(dir: &tempfile::TempDir, name: &str) -> (InvertedIndex, Arc<GatedDatabase>) {
        let db = Arc::new(GatedDatabase::create(dir.path().join(name)).unwrap());
        let idx = InvertedIndex::open(
            Arc::clone(&db),
            crate::data::executor::core_loop::test_governor(),
        )
        .unwrap();
        (idx, db)
    }

    fn hits_in(idx: &InvertedIndex, index: IndexScope<'_>, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = idx
            .search(
                DB,
                T,
                index,
                FtsSearchParams {
                    query,
                    top_k: 100,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .unwrap()
            .into_iter()
            .map(|r| r.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    fn hits(idx: &InvertedIndex, query: &str) -> Vec<u32> {
        hits_in(idx, IndexScope::document("docs"), query)
    }

    fn scope(surrogate: u32, ordinal: i64) -> IndexDocScope<'static> {
        IndexDocScope {
            database_id: DB,
            tid: T,
            collection: "docs",
            surrogate: Surrogate::new(surrogate),
            ordinal,
        }
    }

    /// The text each document holds after a test's writes: `None` for a
    /// removed document.
    type Truth = HashMap<u32, Option<DocumentText>>;

    /// Rebuild `docs` of `idx`: `derive` fills the scratch index the way a
    /// rebuild thread does, `during` writes to the live index while the
    /// rebuild runs, and the cutover replays each noted document from
    /// `truth`.
    fn rebuild_with(
        idx: &InvertedIndex,
        dir: &tempfile::TempDir,
        derive: impl FnOnce(&InvertedIndex),
        during: impl FnOnce(&InvertedIndex),
        truth: &Truth,
    ) -> FtsInstallOutcome {
        let ticket = idx
            .begin_rebuild(DB, T, "docs", FtsRebuildBounds::DEFAULT)
            .unwrap();
        let path = dir.path().join("scratch.redb");
        let (scratch, scratch_db) = open_at(dir, "scratch.redb");
        ticket.seed_scratch_config(&scratch).unwrap();
        derive(&scratch);
        drop(scratch);
        during(idx);
        let rebuilt = ticket.finish(scratch_db, path);
        idx.install_rebuild(rebuilt, &mut |txn, touched| {
            for &s in touched {
                match truth.get(&s).and_then(Option::as_ref) {
                    Some(text) => idx.index_document_in_txn(txn, scope(s, 0), text)?,
                    None => idx.remove_document_in_txn(txn, scope(s, 0))?,
                }
            }
            Ok(())
        })
        .unwrap()
    }

    #[test]
    fn writes_during_the_rebuild_survive_the_cutover() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        for i in 1..=3u32 {
            idx.index_document(DB, T, "docs", Surrogate::new(i), &body("alpha"))
                .unwrap();
        }
        let truth: Truth = [(4, Some(body("alpha"))), (1, Some(body("beta"))), (2, None)]
            .into_iter()
            .collect();
        let outcome = rebuild_with(
            &idx,
            &dir,
            |scratch| {
                for i in 1..=3u32 {
                    scratch
                        .index_document(DB, T, "docs", Surrogate::new(i), &body("alpha"))
                        .unwrap();
                }
            },
            |idx| {
                idx.index_document(DB, T, "docs", Surrogate::new(4), &body("alpha"))
                    .unwrap();
                idx.index_document(DB, T, "docs", Surrogate::new(1), &body("beta"))
                    .unwrap();
                idx.remove_document(DB, T, "docs", Surrogate::new(2))
                    .unwrap();
            },
            &truth,
        );
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Installed { replayed: 3 }
        ));
        assert_eq!(hits(&idx, "alpha"), vec![3, 4]);
        assert_eq!(hits(&idx, "beta"), vec![1]);
        let (count, avg_len) = idx.corpus_stats(DB, T, "docs").unwrap();
        assert_eq!(count, 3, "stats count each document once");
        assert_eq!(avg_len, 1.0);
    }

    /// The scratch rows replace the live rows: a field the rebuild no
    /// longer derives leaves the live index.
    #[test]
    fn the_cutover_replaces_every_live_row_of_the_collection() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        idx.index_document(
            DB,
            T,
            "docs",
            Surrogate::new(1),
            &fields(&[("title", "rust"), ("body", "guide")]),
        )
        .unwrap();
        idx.index_document(DB, T, "other", Surrogate::new(1), &body("rust"))
            .unwrap();
        let title = IndexScope::field("docs", "title").unwrap();
        let outcome = rebuild_with(
            &idx,
            &dir,
            |scratch| {
                scratch
                    .index_document(
                        DB,
                        T,
                        "docs",
                        Surrogate::new(1),
                        &fields(&[("body", "guide")]),
                    )
                    .unwrap();
            },
            |_| {},
            &Truth::new(),
        );
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Installed { replayed: 0 }
        ));
        assert!(hits_in(&idx, title, "rust").is_empty(), "the title left");
        assert!(hits(&idx, "rust").is_empty());
        assert_eq!(hits(&idx, "guide"), vec![1]);
        assert_eq!(
            hits_in(&idx, IndexScope::document("other"), "rust"),
            vec![1],
            "another collection keeps its text"
        );
        assert_eq!(idx.corpus_stats(DB, T, title).unwrap().0, 0);
        assert_eq!(idx.corpus_stats(DB, T, "docs").unwrap().0, 1);

        // The copied field set lets a delete clear every scope.
        idx.remove_document(DB, T, "docs", Surrogate::new(1))
            .unwrap();
        assert!(hits(&idx, "guide").is_empty());
        assert_eq!(idx.corpus_stats(DB, T, "docs").unwrap().0, 0);
    }

    /// The text history the scratch index holds is live after the cutover.
    #[test]
    fn the_cutover_carries_the_text_history() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        let outcome = rebuild_with(
            &idx,
            &dir,
            |scratch| {
                scratch
                    .index_document_at(DB, T, "docs", Surrogate::new(5), &body("alpha"), 10)
                    .unwrap();
                scratch
                    .index_document_at(DB, T, "docs", Surrogate::new(5), &body("beta"), 20)
                    .unwrap();
            },
            |_| {},
            &Truth::new(),
        );
        assert!(matches!(outcome, FtsInstallOutcome::Installed { .. }));
        let tokens_at = |at: i64| -> Vec<String> {
            idx.text_cut_view(DB, T, IndexScope::document("docs"), at)
                .unwrap()
                .docs
                .into_iter()
                .filter(|d| d.doc_id == Surrogate::new(5))
                .flat_map(|d| d.tokens)
                .collect()
        };
        // A cut view lists ended versions only: alpha ended at 20. Beta is
        // live at 25, so the view lists nothing and the live index serves it.
        assert!(tokens_at(15).iter().any(|t| t.contains("alpha")));
        assert!(tokens_at(25).is_empty());
        assert_eq!(hits(&idx, "beta"), vec![5]);
    }

    #[test]
    fn a_purge_during_the_rebuild_refuses_the_cutover() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha"))
            .unwrap();
        let outcome = rebuild_with(
            &idx,
            &dir,
            |_| {},
            |idx| {
                idx.purge_collection(DB, T, "docs").unwrap();
            },
            &Truth::new(),
        );
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Refused(FtsRebuildRefusal::Purged)
        ));
        assert!(hits(&idx, "alpha").is_empty(), "the purge is not undone");
    }

    /// A journal past its bound hands the pass back as a catch-up round:
    /// a fresh snapshot that holds the pass's writes, and their documents.
    /// A round whose journal stays under the bound cuts over.
    #[test]
    fn an_overflowing_journal_runs_a_catch_up_round_then_cuts_over() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        let bounds = FtsRebuildBounds {
            max_docs: 1,
            max_rounds: 1,
        };
        let ticket = idx.begin_rebuild(DB, T, "docs", bounds).unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), &body("alpha"))
            .unwrap();
        let (_scratch, scratch_db) = open_at(&dir, "scratch.redb");
        let rebuilt = ticket.finish(scratch_db, dir.path().join("scratch.redb"));
        let outcome = idx.install_rebuild(rebuilt, &mut |_, _| Ok(())).unwrap();
        let FtsInstallOutcome::CatchUp(catch_up) = outcome else {
            panic!("expected a catch-up round, got {outcome:?}");
        };
        assert_eq!(catch_up.touched.iter().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(catch_up.ticket.round(), 1);
        assert!(idx.rebuild_is_open(catch_up.ticket.token()));
        assert_eq!(
            hits(&idx, "alpha"),
            vec![1, 2],
            "the live index is unchanged"
        );

        // The round derives both documents from its snapshot.
        let (round, round_db) = open_at(&dir, "round.redb");
        catch_up.ticket.seed_scratch_config(&round).unwrap();
        catch_up.carry_rows(&round, &|| Ok(())).unwrap();
        for i in 1..=2u32 {
            round
                .index_document(DB, T, "docs", Surrogate::new(i), &body("alpha"))
                .unwrap();
        }
        drop(round);
        let rebuilt = catch_up
            .ticket
            .finish(round_db, dir.path().join("round.redb"));
        let outcome = idx.install_rebuild(rebuilt, &mut |_, _| Ok(())).unwrap();
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Installed { replayed: 0 }
        ));
        assert_eq!(hits(&idx, "alpha"), vec![1, 2]);
        assert_eq!(idx.corpus_stats(DB, T, "docs").unwrap().0, 2);
    }

    /// Writes that pass the bound after the last round refuse the rebuild
    /// with the write rate. The live index keeps every write.
    #[test]
    fn writes_that_outpace_every_round_refuse_the_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        let bounds = FtsRebuildBounds {
            max_docs: 1,
            max_rounds: 0,
        };
        let ticket = idx.begin_rebuild(DB, T, "docs", bounds).unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha"))
            .unwrap();
        idx.index_document(DB, T, "docs", Surrogate::new(2), &body("alpha"))
            .unwrap();
        let (_scratch, scratch_db) = open_at(&dir, "scratch.redb");
        let rebuilt = ticket.finish(scratch_db, dir.path().join("scratch.redb"));
        let token = rebuilt.token();
        let outcome = idx.install_rebuild(rebuilt, &mut |_, _| Ok(())).unwrap();
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Refused(FtsRebuildRefusal::WritesOutpaceRebuild { rounds: 0, .. })
        ));
        assert!(
            !idx.rebuild_is_open(token),
            "the refusal closes the journal"
        );
        assert_eq!(hits(&idx, "alpha"), vec![1, 2]);
    }

    #[test]
    fn an_aborted_rebuild_is_superseded() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        idx.index_document(DB, T, "docs", Surrogate::new(1), &body("alpha"))
            .unwrap();
        let ticket = idx
            .begin_rebuild(DB, T, "docs", FtsRebuildBounds::DEFAULT)
            .unwrap();
        idx.abort_rebuild(ticket.token());
        let (_scratch, scratch_db) = open_at(&dir, "scratch.redb");
        let rebuilt = ticket.finish(scratch_db, dir.path().join("scratch.redb"));
        let outcome = idx.install_rebuild(rebuilt, &mut |_, _| Ok(())).unwrap();
        assert!(matches!(
            outcome,
            FtsInstallOutcome::Refused(FtsRebuildRefusal::Superseded)
        ));
        assert_eq!(hits(&idx, "alpha"), vec![1], "the live index is unchanged");
    }

    #[test]
    fn a_second_rebuild_of_the_collection_is_refused_while_one_runs() {
        let dir = tempfile::tempdir().unwrap();
        let (idx, _db) = open_at(&dir, "live.redb");
        let _ticket = idx
            .begin_rebuild(DB, T, "docs", FtsRebuildBounds::DEFAULT)
            .unwrap();
        assert!(
            idx.begin_rebuild(DB, T, "docs", FtsRebuildBounds::DEFAULT)
                .is_err()
        );
    }
}
