// SPDX-License-Identifier: BUSL-1.1

//! `TextOp::RebuildIndex`: make sure a full-text rebuild of one collection
//! over the given fields runs or ran on this core, and answer where it
//! stands.
//!
//! The rebuild itself is the REINDEX full-text rebuild: off the core, with
//! the core serving every request meanwhile, cut over atomically with every
//! write made during it kept. This handler only starts it or reads it.
//!
//! The core keeps one record per collection: the latest rebuild an op
//! started or attached to, and the fields it indexes.
//!
//! - Same fields, running: answer `Running` with its progress.
//! - Same fields, cut over, and the declared fields unchanged since:
//!   answer `Installed`. No rebuild starts. A step that two drivers run,
//!   or that a driver polls, rebuilds once per core.
//! - Same fields, failed: answer the error once and forget the record. The
//!   next op starts a new rebuild.
//! - Other fields: cancel the record's running rebuild, if any, and start
//!   one. A running REINDEX rebuild over the same fields is attached to.
//!
//! Every field-set change comes from a full-text index DDL step, and every
//! step sends this op with the fields it leaves declared. A record over
//! other fields is then replaced before its fields return, so an
//! `Installed` record always covers every write since its cutover.
//!
//! A core restart forgets every record. The next op then starts a rebuild.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_physical::physical_plan::TextRebuildProgress;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::control::reindex::RebuildTarget;
use crate::data::executor::handlers::control::reindex::pending::PendingBuild;
use crate::data::executor::handlers::fts_analyzer::TextAnalysis;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

/// How a rebuild a record names ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) enum TextRebuildOutcome {
    Installed { replayed: u64 },
    Failed { detail: String },
}

/// Where the rebuild a record names stands.
enum RecordState {
    Running {
        token: u64,
        progress: Arc<AtomicU64>,
    },
    Ended(TextRebuildOutcome),
}

/// The latest `TextOp::RebuildIndex` rebuild of one collection on this
/// core.
pub(in crate::data::executor) struct TextRebuildRecord {
    target: RebuildTarget,
    text_fields: Vec<String>,
    state: RecordState,
}

impl CoreLoop {
    /// Handle `TextOp::RebuildIndex` for one collection on this core.
    pub(in crate::data::executor) fn execute_rebuild_text_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        text_fields: &[String],
        analysis: TextAnalysis<'_>,
    ) -> Response {
        let mut fields = text_fields.to_vec();
        fields.sort();
        fields.dedup();
        let target = RebuildTarget {
            database_id: task.request.database_id,
            tenant_id: TenantId::new(tid),
            collection: collection.to_string(),
        };
        if let Err(e) = self.install_rebuild_analysis(&target, analysis) {
            return self.response_error(task, e);
        }
        let answer = self
            .ensure_text_rebuild(&target, fields)
            .and_then(|progress| {
                zerompk::to_msgpack_vec(&progress).map_err(|e| crate::Error::Codec {
                    detail: format!("text rebuild progress encode: {e}"),
                })
            });
        match answer {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Start, attach to, or read the rebuild of `target` over `fields`.
    pub(in crate::data::executor) fn ensure_text_rebuild(
        &mut self,
        target: &RebuildTarget,
        fields: Vec<String>,
    ) -> crate::Result<TextRebuildProgress> {
        if let Some(pos) = self.text_record_of(target) {
            let record = &self.maintenance.text_rebuilds[pos];
            if record.text_fields == fields {
                match &record.state {
                    RecordState::Running { progress, .. } => {
                        return Ok(TextRebuildProgress::Running {
                            replayed: progress.load(Ordering::Relaxed),
                        });
                    }
                    RecordState::Ended(TextRebuildOutcome::Installed { replayed })
                        if self.current_text_fields(target) == fields =>
                    {
                        return Ok(TextRebuildProgress::Installed {
                            replayed: *replayed,
                        });
                    }
                    RecordState::Ended(TextRebuildOutcome::Installed { .. }) => {}
                    RecordState::Ended(TextRebuildOutcome::Failed { detail }) => {
                        let err = rebuild_failed(target, detail);
                        self.maintenance.text_rebuilds.swap_remove(pos);
                        return Err(err);
                    }
                }
            }
            let replaced = self.maintenance.text_rebuilds.swap_remove(pos);
            if let RecordState::Running { token, .. } = replaced.state {
                self.cancel_fts_rebuild(token);
            }
        }

        let state = match self.attach_running_rebuild(target, &fields) {
            Some(state) => state,
            None => match self.start_fts_rebuild(target, fields.clone())? {
                Some(started) => RecordState::Running {
                    token: started.token,
                    progress: started.progress,
                },
                // Nothing to rebuild: the index already holds no text.
                None => RecordState::Ended(TextRebuildOutcome::Installed { replayed: 0 }),
            },
        };
        let progress = match &state {
            RecordState::Running { progress, .. } => TextRebuildProgress::Running {
                replayed: progress.load(Ordering::Relaxed),
            },
            RecordState::Ended(_) => TextRebuildProgress::Installed { replayed: 0 },
        };
        self.maintenance.text_rebuilds.push(TextRebuildRecord {
            target: target.clone(),
            text_fields: fields,
            state,
        });
        Ok(progress)
    }

    /// A REINDEX full-text rebuild of `target` that runs over `fields`, as
    /// a record state. A running rebuild over other fields is aborted.
    fn attach_running_rebuild(
        &mut self,
        target: &RebuildTarget,
        fields: &[String],
    ) -> Option<RecordState> {
        let mut attached = None;
        let mut aborted = Vec::new();
        for pending in self
            .maintenance
            .pending_reindex
            .iter()
            .filter(|p| p.target == *target)
        {
            if let PendingBuild::Fts {
                token,
                text_fields,
                progress,
                ..
            } = &pending.build
            {
                if !self.inverted.rebuild_is_open(*token) {
                    // Cancelled or purged: it never cuts over.
                    continue;
                }
                if text_fields == fields && attached.is_none() {
                    attached = Some(RecordState::Running {
                        token: *token,
                        progress: Arc::clone(progress),
                    });
                } else {
                    aborted.push(*token);
                }
            }
        }
        // A cancelled rebuild's thread stops, and its result is discarded.
        for token in aborted {
            self.cancel_fts_rebuild(token);
        }
        attached
    }

    /// Record how the rebuild `token` of `target` ended, when a record
    /// names it.
    pub(in crate::data::executor) fn note_text_rebuild_outcome(
        &mut self,
        target: &RebuildTarget,
        token: u64,
        outcome: TextRebuildOutcome,
    ) {
        if let Some(record) = self.maintenance.text_rebuilds.iter_mut().find(|r| {
            r.target == *target
                && matches!(r.state, RecordState::Running { token: t, .. } if t == token)
        }) {
            record.state = RecordState::Ended(outcome);
        }
    }

    /// Install `analysis` for `target` before a rebuild starts or answers.
    ///
    /// The registered config takes it too, so a clear that keeps the
    /// registration installs it again. A changed analyzer ends every rebuild
    /// of `target`: each copied the old analyzer into its scratch index, and
    /// its postings would not match a query under the new one.
    fn install_rebuild_analysis(
        &mut self,
        target: &RebuildTarget,
        analysis: TextAnalysis<'_>,
    ) -> crate::Result<()> {
        let changed = self.install_analysis(
            target.database_id,
            target.tenant_id,
            &target.collection,
            analysis,
        )?;
        let key = (
            target.database_id,
            target.tenant_id,
            target.collection.clone(),
        );
        if let Some(config) = self.doc_configs.get_mut(&key) {
            let set = std::mem::take(&mut config.text_fields);
            config.text_fields = set.with_analysis(
                analysis.analyzer.map(str::to_string),
                analysis.fuzzy_default,
            );
        }
        if changed {
            self.forget_text_rebuilds(target);
        }
        Ok(())
    }

    /// End every rebuild of `target`: the record's, and every REINDEX
    /// rebuild still open. Each one is cancelled and never cuts over.
    fn forget_text_rebuilds(&mut self, target: &RebuildTarget) {
        if let Some(pos) = self.text_record_of(target) {
            let record = self.maintenance.text_rebuilds.swap_remove(pos);
            if let RecordState::Running { token, .. } = record.state {
                self.cancel_fts_rebuild(token);
            }
        }
        let tokens: Vec<u64> = self
            .maintenance
            .pending_reindex
            .iter()
            .filter(|pending| pending.target == *target)
            .filter_map(|pending| match &pending.build {
                PendingBuild::Fts { token, .. } => Some(*token),
                PendingBuild::Csr { .. } => None,
            })
            .collect();
        for token in tokens {
            self.cancel_fts_rebuild(token);
        }
    }

    fn text_record_of(&self, target: &RebuildTarget) -> Option<usize> {
        self.maintenance
            .text_rebuilds
            .iter()
            .position(|r| r.target == *target)
    }
}

fn rebuild_failed(target: &RebuildTarget, detail: &str) -> crate::Error {
    crate::Error::ObjectNotInPrerequisiteState {
        object: format!("full-text index of collection \"{}\"", target.collection),
        detail: format!("its rebuild failed: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_fts::FtsSearchParams;
    use nodedb_fts::posting::QueryMode;
    use nodedb_types::{StorageKey, Surrogate};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::control::reindex::ReindexWaiter;
    use crate::data::executor::handlers::spill::cleanup::SpillKind;
    use crate::engine::sparse::btree_versioned::{
        TAG_LIVE, TAG_TOMBSTONE, encode_value, versioned_doc_key,
    };
    use crate::engine::sparse::inverted::FtsRebuildBounds;
    use crate::types::DatabaseId;

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLL: &str = "rebuild_fts";

    impl CoreLoop {
        /// Run a fresh `TextOp::RebuildIndex` rebuild of `collection` over
        /// `text_fields` to its end, polling the core as its tick does.
        /// Returns the versions the rebuild replayed.
        fn rebuild_text_index(
            &mut self,
            database_id: u64,
            tid: u64,
            collection: &str,
            text_fields: &[String],
        ) -> crate::Result<u64> {
            // Forget the last record, so the op starts a rebuild.
            self.maintenance.text_rebuilds.clear();
            let target = RebuildTarget {
                database_id: DatabaseId::new(database_id),
                tenant_id: TenantId::new(tid),
                collection: collection.to_string(),
            };
            let mut fields = text_fields.to_vec();
            fields.sort();
            fields.dedup();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                match self.ensure_text_rebuild(&target, fields.clone())? {
                    TextRebuildProgress::Installed { replayed } => return Ok(replayed),
                    TextRebuildProgress::Running { .. } => {
                        assert!(std::time::Instant::now() < deadline, "rebuild never ended");
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        self.poll_pending_reindex();
                    }
                }
            }
        }
    }

    fn fields(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// Register `COLL` with no declared text field, as a collection with no
    /// full-text index registers.
    fn register(core: &mut CoreLoop) {
        core.doc_configs.insert(
            (DatabaseId::new(DB), TenantId::new(TID), COLL.to_string()),
            crate::engine::document::store::CollectionConfig::new(COLL),
        );
    }

    /// Store a live version of row `id` holding `doc` at `sys_from`, with no
    /// text-index side effect, as a row stored before the index existed.
    fn store(core: &CoreLoop, id: u32, sys_from: i64, doc: &serde_json::Value) {
        let key = StorageKey::for_surrogate(Surrogate::new(id));
        let value = encode_value(
            TAG_LIVE,
            i64::MIN,
            i64::MAX,
            &doc_format::encode_to_msgpack(doc),
        );
        core.sparse
            .put_versioned_document_raw(&versioned_doc_key(DB, TID, COLL, &key, sys_from), &value)
            .expect("store version");
    }

    /// Store a tombstone of row `id` at `sys_from`.
    fn delete(core: &CoreLoop, id: u32, sys_from: i64) {
        let key = StorageKey::for_surrogate(Surrogate::new(id));
        let value = encode_value(TAG_TOMBSTONE, i64::MIN, i64::MAX, &[]);
        core.sparse
            .put_versioned_document_raw(&versioned_doc_key(DB, TID, COLL, &key, sys_from), &value)
            .expect("store tombstone");
    }

    /// The rows a current read of `index` finds for `term`.
    fn hits(core: &CoreLoop, index: nodedb_fts::IndexScope<'_>, term: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = core
            .inverted
            .search(
                DB,
                TenantId::new(TID),
                index,
                FtsSearchParams {
                    query: term,
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: QueryMode::And,
                    prefilter: None,
                },
            )
            .expect("search")
            .into_iter()
            .map(|hit| hit.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    fn whole() -> nodedb_fts::IndexScope<'static> {
        nodedb_fts::IndexScope::document(COLL)
    }

    fn field(name: &'static str) -> nodedb_fts::IndexScope<'static> {
        nodedb_fts::IndexScope::field(COLL, name).expect("a named field has an index")
    }

    #[test]
    fn rebuild_indexes_only_declared_fields_of_existing_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(
            &core,
            1,
            10,
            &serde_json::json!({"title": "alpha", "body": "bravo"}),
        );
        store(
            &core,
            2,
            11,
            &serde_json::json!({"title": "charlie", "body": "alpha"}),
        );
        assert!(
            hits(&core, whole(), "alpha").is_empty(),
            "nothing indexed yet"
        );

        let replayed = core
            .rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild");
        assert_eq!(replayed, 2);

        assert_eq!(
            hits(&core, whole(), "alpha"),
            vec![1],
            "only the title counts"
        );
        assert!(
            hits(&core, whole(), "bravo").is_empty(),
            "body is undeclared"
        );
        assert_eq!(hits(&core, field("title"), "charlie"), vec![2]);
        assert!(
            hits(&core, field("body"), "alpha").is_empty(),
            "an undeclared field has no index"
        );
        assert_eq!(
            core.declared_text_fields(DB, TID, COLL),
            fields(&["title"]).as_slice(),
            "the core's writes index the rebuilt fields"
        );

        // Rebuilding with `body` alone drops the title postings.
        core.rebuild_text_index(DB, TID, COLL, &fields(&["body"]))
            .expect("rebuild body");
        assert_eq!(hits(&core, whole(), "alpha"), vec![2]);
        assert!(
            hits(&core, field("title"), "charlie").is_empty(),
            "the dropped field's index is gone"
        );
    }

    #[test]
    fn an_empty_field_list_leaves_no_postings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(&core, 1, 10, &serde_json::json!({"title": "alpha"}));
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild");
        assert_eq!(hits(&core, whole(), "alpha"), vec![1]);

        let replayed = core.rebuild_text_index(DB, TID, COLL, &[]).expect("purge");
        assert_eq!(replayed, 0, "an empty field list replays nothing");
        assert!(hits(&core, whole(), "alpha").is_empty());
        assert!(hits(&core, field("title"), "alpha").is_empty());
        assert!(core.declared_text_fields(DB, TID, COLL).is_empty());
    }

    #[test]
    fn rebuild_keeps_the_text_history_of_every_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(&core, 5, 10, &serde_json::json!({"body": "alpha"}));
        store(&core, 5, 20, &serde_json::json!({"body": "beta"}));
        delete(&core, 5, 30);

        let replayed = core
            .rebuild_text_index(DB, TID, COLL, &fields(&["body"]))
            .expect("rebuild");
        assert_eq!(replayed, 3);

        let tokens_at = |at: i64| -> Vec<String> {
            core.inverted
                .text_cut_view(DB, TenantId::new(TID), whole(), at)
                .expect("cut view")
                .docs
                .into_iter()
                .filter(|d| d.doc_id == Surrogate::new(5))
                .flat_map(|d| d.tokens)
                .collect()
        };
        assert!(tokens_at(15).iter().any(|t| t.contains("alpha")));
        assert!(!tokens_at(15).iter().any(|t| t.contains("beta")));
        assert!(tokens_at(25).iter().any(|t| t.contains("beta")));
        assert!(tokens_at(35).is_empty(), "deleted at 30");
        assert!(
            hits(&core, whole(), "beta").is_empty(),
            "the current read sees the delete"
        );
    }

    /// Index a sync frame for row `id`, as a Lite client sends one.
    fn sync_frame(core: &mut CoreLoop, id: u32, fields: &[(&str, &str)]) {
        let task = crate::data::executor::core_loop::tests::make_default_task();
        let pairs: Vec<(String, String)> = fields
            .iter()
            .map(|(f, t)| (f.to_string(), t.to_string()))
            .collect();
        let response =
            core.execute_fts_index_doc(&task, TID, COLL, Surrogate::new(id), &pairs, None);
        assert_eq!(response.status, crate::bridge::envelope::Status::Ok);
    }

    /// Text a sync frame delivered for a row with no stored body survives a
    /// rebuild, including a frame that arrived before any field was
    /// declared. A row with a stored body takes its text from the body.
    #[test]
    fn rebuild_keeps_synced_text_of_rows_with_no_stored_body() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(&core, 1, 10, &serde_json::json!({"title": "alpha"}));
        sync_frame(&mut core, 1, &[("title", "zulu")]);
        sync_frame(&mut core, 7, &[("title", "delta"), ("body", "echo")]);
        assert!(
            hits(&core, whole(), "delta").is_empty(),
            "nothing declared yet"
        );

        let replayed = core
            .rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild");
        assert_eq!(
            replayed, 3,
            "the walk visits one stored version, the frame its body overrides, \
             and one body-less frame"
        );
        assert_eq!(hits(&core, field("title"), "delta"), vec![7]);
        assert!(
            hits(&core, whole(), "echo").is_empty(),
            "body is undeclared"
        );
        assert_eq!(hits(&core, whole(), "alpha"), vec![1], "the body wins");
        assert!(hits(&core, whole(), "zulu").is_empty());

        // A frame with no fields stores a tombstone version and ends the
        // text, before and after a rebuild.
        sync_frame(&mut core, 7, &[]);
        assert!(hits(&core, whole(), "delta").is_empty());
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild after removal");
        assert!(
            hits(&core, whole(), "delta").is_empty(),
            "the tombstone wins"
        );
    }

    /// Index a sync frame for row `id` at system-time ordinal `ordinal`.
    fn sync_frame_at(core: &CoreLoop, id: u32, ordinal: i64, fields: &[(&str, &str)]) {
        let pairs: Vec<(String, String)> = fields
            .iter()
            .map(|(f, t)| (f.to_string(), t.to_string()))
            .collect();
        core.write_synced_text(DB, TID, COLL, Surrogate::new(id), &pairs, ordinal)
            .expect("write frame");
    }

    /// The tokens a read of the whole-document index at `at` sees for `id`.
    fn frame_tokens_at(core: &CoreLoop, id: u32, at: i64) -> Vec<String> {
        core.inverted
            .text_cut_view(DB, TenantId::new(TID), whole(), at)
            .expect("cut view")
            .docs
            .into_iter()
            .filter(|d| d.doc_id == Surrogate::new(id))
            .flat_map(|d| d.tokens)
            .collect()
    }

    /// Every frame version of a body-less row replays at its own ordinal: a
    /// read at an old cut after the rebuild sees the frame visible there.
    #[test]
    fn rebuild_keeps_the_text_history_of_every_frame() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        sync_frame_at(&core, 7, 10, &[("title", "alpha")]);
        sync_frame_at(&core, 7, 20, &[("title", "beta")]);
        sync_frame_at(&core, 7, 30, &[]);
        sync_frame_at(&core, 7, 40, &[("title", "gamma")]);

        let replayed = core
            .rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild");
        assert_eq!(replayed, 4, "every frame version replays");

        let has = |at: i64, word: &str| {
            frame_tokens_at(&core, 7, at)
                .iter()
                .any(|t| t.contains(word))
        };
        assert!(has(15, "alpha"));
        assert!(!has(15, "beta"));
        assert!(has(25, "beta"));
        assert!(
            frame_tokens_at(&core, 7, 35).is_empty(),
            "the tombstone at 30"
        );
        assert_eq!(hits(&core, whole(), "gamma"), vec![7]);
        assert!(hits(&core, whole(), "beta").is_empty());
    }

    /// The text-history purge drops the frame versions below its horizon
    /// with the index's history, so a rebuild after it brings none back.
    #[test]
    fn frame_versions_purge_with_text_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("declare title");
        sync_frame_at(&core, 7, 10, &[("title", "alpha")]);
        sync_frame_at(&core, 7, 20, &[("title", "beta")]);
        sync_frame_at(&core, 7, 30, &[("title", "gamma")]);
        sync_frame_at(&core, 8, 10, &[("title", "delta")]);
        sync_frame_at(&core, 8, 20, &[]);

        let frames = |core: &CoreLoop| {
            let mut out = Vec::new();
            core.inverted
                .for_each_synced_text(DB, TenantId::new(TID), Some(COLL), |f| {
                    out.push((f.surrogate.as_u32(), f.ordinal));
                    Ok(())
                })
                .expect("frames");
            out
        };
        assert_eq!(frames(&core).len(), 5);

        core.inverted.purge_text_versions_below(25).expect("purge");
        // Row 7 keeps the version a read at 25 sees and the later one. Row
        // 8 ended at 20: nothing of it is readable at or above 25.
        assert_eq!(frames(&core), vec![(7, 20), (7, 30)]);

        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("rebuild after purge");
        let has = |at: i64, word: &str| {
            frame_tokens_at(&core, 7, at)
                .iter()
                .any(|t| t.contains(word))
        };
        assert!(!has(15, "alpha"), "the purged version stays gone");
        assert!(has(25, "beta"));
        assert_eq!(hits(&core, whole(), "gamma"), vec![7]);
        assert!(hits(&core, whole(), "delta").is_empty());
    }

    #[test]
    fn an_unregistered_collection_refuses_declared_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        assert!(matches!(
            core.rebuild_text_index(DB, TID, COLL, &fields(&["body"])),
            Err(crate::Error::CollectionNotFound { .. })
        ));
        assert_eq!(
            core.rebuild_text_index(DB, TID, COLL, &[]).expect("purge"),
            0
        );
    }

    fn target() -> RebuildTarget {
        RebuildTarget {
            database_id: DatabaseId::new(DB),
            tenant_id: TenantId::new(TID),
            collection: COLL.to_string(),
        }
    }

    /// A second op over the fields a rebuild cut over answers `Installed`
    /// and starts nothing. An op over other fields starts a rebuild.
    #[test]
    fn a_repeat_op_over_the_same_fields_starts_no_second_rebuild() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(
            &core,
            1,
            10,
            &serde_json::json!({"title": "alpha", "body": "bravo"}),
        );
        assert_eq!(
            core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
                .expect("rebuild"),
            1
        );

        let again = core
            .ensure_text_rebuild(&target(), fields(&["title"]))
            .expect("repeat");
        assert_eq!(again, TextRebuildProgress::Installed { replayed: 1 });
        assert!(
            core.maintenance.pending_reindex.is_empty(),
            "no second rebuild starts"
        );

        let other = core
            .ensure_text_rebuild(&target(), fields(&["body"]))
            .expect("other fields");
        assert!(matches!(other, TextRebuildProgress::Running { .. }));
        assert_eq!(core.maintenance.pending_reindex.len(), 1);
    }

    /// A rebuild over other fields replaces a running one: the first
    /// rebuild is cancelled, the second one's cutover is live.
    #[test]
    fn a_rebuild_over_other_fields_replaces_a_running_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(
            &core,
            1,
            10,
            &serde_json::json!({"title": "alpha", "body": "bravo"}),
        );
        let first = core
            .ensure_text_rebuild(&target(), fields(&["title"]))
            .expect("first");
        assert!(matches!(first, TextRebuildProgress::Running { .. }));
        let replayed = core
            .rebuild_text_index(DB, TID, COLL, &fields(&["body"]))
            .expect("second");
        assert_eq!(replayed, 1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !core.maintenance.pending_reindex.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the first rebuild never ended"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
            core.poll_pending_reindex();
        }
        assert_eq!(hits(&core, whole(), "bravo"), vec![1]);
        assert!(
            hits(&core, whole(), "alpha").is_empty(),
            "the title rebuild lost"
        );
    }

    /// Start a fresh rebuild of `COLL` over `title`. Its snapshot is pinned
    /// on return, and its cutover waits for [`finish_pending`].
    fn start_title_rebuild(core: &mut CoreLoop) {
        core.maintenance.text_rebuilds.clear();
        let started = core
            .ensure_text_rebuild(&target(), fields(&["title"]))
            .expect("start");
        assert!(matches!(started, TextRebuildProgress::Running { .. }));
    }

    /// Cut over every running rebuild, polling the core as its tick does.
    fn finish_pending(core: &mut CoreLoop) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !core.maintenance.pending_reindex.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the rebuild never ended"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
            core.poll_pending_reindex();
        }
    }

    /// A frame version stored below the newest one, as a RESTORE re-issues
    /// it, indexes nothing on its own. Written during a rebuild, it still
    /// lands in the text history the cutover installs.
    #[test]
    fn a_stored_only_frame_written_during_a_rebuild_survives_the_cutover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("declare title");
        sync_frame_at(&core, 7, 10, &[("title", "alpha")]);
        sync_frame_at(&core, 7, 30, &[("title", "gamma")]);

        start_title_rebuild(&mut core);
        core.install_synced_text(
            DB,
            TID,
            COLL,
            Surrogate::new(7),
            &[("title".to_string(), "beta".to_string())],
            20,
        )
        .expect("store the older frame");
        finish_pending(&mut core);

        let has = |at: i64, word: &str| {
            frame_tokens_at(&core, 7, at)
                .iter()
                .any(|t| t.contains(word))
        };
        assert!(
            has(25, "beta"),
            "the frame stored during the rebuild is history"
        );
        assert!(!has(25, "alpha"), "the older frame ends at the stored one");
        assert!(has(15, "alpha"));
        assert_eq!(hits(&core, whole(), "gamma"), vec![7]);
    }

    /// A text-history purge that runs during a rebuild holds after the
    /// cutover, though the rebuild's snapshot held the purged versions.
    #[test]
    fn a_history_purge_during_a_rebuild_holds_after_the_cutover() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("declare title");
        sync_frame_at(&core, 7, 10, &[("title", "alpha")]);
        sync_frame_at(&core, 7, 20, &[("title", "beta")]);
        sync_frame_at(&core, 7, 30, &[("title", "gamma")]);

        start_title_rebuild(&mut core);
        core.inverted.purge_text_versions_below(25).expect("purge");
        finish_pending(&mut core);

        let has = |at: i64, word: &str| {
            frame_tokens_at(&core, 7, at)
                .iter()
                .any(|t| t.contains(word))
        };
        assert!(!has(15, "alpha"), "the purged version stays gone");
        assert!(has(25, "beta"));
        assert_eq!(hits(&core, whole(), "gamma"), vec![7]);
    }

    /// Writes during the first pass pass the journal bound. A catch-up
    /// round derives them off the core, and the rebuild cuts over.
    #[test]
    fn an_overflowing_journal_converges_through_a_catch_up_round() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(&core, 1, 10, &serde_json::json!({"title": "alpha"}));
        store(&core, 2, 11, &serde_json::json!({"title": "alpha"}));
        let bounds = FtsRebuildBounds {
            max_docs: 1,
            max_rounds: 2,
        };
        core.start_fts_rebuild_bounded(&target(), fields(&["title"]), bounds)
            .expect("start")
            .expect("a rebuild runs");
        // Two documents written before the first cutover pass the bound.
        sync_frame_at(&core, 7, 20, &[("title", "bravo")]);
        sync_frame_at(&core, 8, 21, &[("title", "bravo")]);
        finish_pending(&mut core);

        assert_eq!(
            hits(&core, whole(), "alpha"),
            vec![1, 2],
            "the stored rows cut over"
        );
        assert_eq!(
            hits(&core, whole(), "bravo"),
            vec![7, 8],
            "the catch-up round keeps the writes made during the first pass"
        );
    }

    /// A rebuild cancelled by a rebuild over other fields ends silently: no
    /// refusal reaches a waiting REINDEX, and its scratch file is gone.
    #[test]
    fn a_cancelled_rebuild_files_no_refusal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(
            &core,
            1,
            10,
            &serde_json::json!({"title": "alpha", "body": "bravo"}),
        );
        core.maintenance
            .reindex_waiters
            .push(ReindexWaiter::for_test(
                crate::data::executor::core_loop::tests::make_default_task(),
                target(),
            ));
        start_title_rebuild(&mut core);
        core.ensure_text_rebuild(&target(), fields(&["body"]))
            .expect("the body rebuild replaces the title rebuild");
        finish_pending(&mut core);

        assert!(
            core.maintenance.reindex_waiters[0].first_error().is_none(),
            "a cancelled rebuild notes no refusal"
        );
        assert_eq!(hits(&core, whole(), "bravo"), vec![1]);
        assert!(hits(&core, whole(), "alpha").is_empty());
        let scratch_dir = SpillKind::TextRebuild.core_dir(&core.data_dir, core.core_id);
        let left = std::fs::read_dir(&scratch_dir).map_or(0, |entries| entries.count());
        assert_eq!(left, 0, "every scratch file is removed");
    }

    /// A start that errors leaves the collection's field list unchanged.
    #[test]
    fn a_start_error_leaves_the_field_list_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        register(&mut core);
        store(
            &core,
            1,
            10,
            &serde_json::json!({"title": "alpha", "body": "bravo"}),
        );
        core.rebuild_text_index(DB, TID, COLL, &fields(&["title"]))
            .expect("declare title");
        // A rebuild held open makes the next journal open fail.
        let _held = core
            .inverted
            .begin_rebuild(DB, TenantId::new(TID), COLL, FtsRebuildBounds::DEFAULT)
            .expect("hold a rebuild open");

        assert!(
            core.start_fts_rebuild(&target(), fields(&["body"]))
                .is_err()
        );
        assert_eq!(
            core.declared_text_fields(DB, TID, COLL),
            fields(&["title"]).as_slice(),
            "the field list is unchanged"
        );
        assert!(core.maintenance.pending_reindex.is_empty());
    }
}
