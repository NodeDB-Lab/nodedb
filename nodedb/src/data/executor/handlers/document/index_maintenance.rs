// SPDX-License-Identifier: BUSL-1.1

//! Document secondary-index maintenance: BackfillIndex, DropIndex.

use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner};
use tracing::debug;

use super::index_maintained_set::{FencedStep, PathChange, parse_predicate};
use super::index_top_level::TopLevelTarget;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::maintained_indexes::PreparedSet;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::{FenceVerdict, MaintainedOwner};

/// Parameters for [`CoreLoop::execute_backfill_index`].
pub(in crate::data::executor) struct BackfillIndexParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    /// Whether a UNIQUE build judges the current rows for a duplicate. A
    /// live build does. Restart replay does not: the live build judged the
    /// rows at the record's position.
    pub judge_unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
    /// The index or declared field the build adds to the path.
    pub owner: &'a DocIndexOwner,
    /// The descriptor the build derives from.
    pub fence: DocIndexFence,
}

impl CoreLoop {
    /// Backfill an index: walk every version of every document in the
    /// collection and populate versioned index entries for the given field.
    /// Atomic — one write transaction covers the whole backfill, and a
    /// UNIQUE violation among the current rows aborts it before any entry is
    /// written, leaving the index empty (the caller's Building→Ready flip is
    /// skipped, so readers never see a partial-index view).
    pub(in crate::data::executor) fn execute_backfill_index(
        &mut self,
        task: &ExecutionTask,
        params: BackfillIndexParams<'_>,
    ) -> Response {
        match self.backfill_document_index(task.request.database_id, params) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(task, crate::bridge::envelope::ErrorCode::from(e)),
        }
    }

    /// The build [`Self::execute_backfill_index`] answers for, shared with
    /// restart replay of a `DocumentIndexBuild` record. A UNIQUE duplicate is
    /// `Error::RejectedConstraint`, returned before any entry is written and
    /// with the maintained set unchanged.
    ///
    /// The build adds its owner to the path in the collection's maintained
    /// set. The entries and the set commit in one transaction, and the
    /// core's mirror takes the set after the commit. An owner the set
    /// already holds with the same spec returns before any row is read. A
    /// top-level owner puts the set in top-level mode instead of adding a
    /// path.
    pub(in crate::data::executor) fn backfill_document_index(
        &mut self,
        database_id: crate::types::DatabaseId,
        params: BackfillIndexParams<'_>,
    ) -> crate::Result<()> {
        let BackfillIndexParams {
            tid,
            collection,
            path,
            is_array,
            unique,
            judge_unique,
            case_insensitive,
            predicate,
            owner,
            fence,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %path,
            %owner,
            unique,
            case_insensitive,
            partial = predicate.is_some(),
            "backfill index"
        );
        let database_raw = database_id.as_u64();
        let spec = match owner {
            DocIndexOwner::DeclaredTopLevel => {
                return self.build_top_level_index(TopLevelTarget {
                    database_id: database_raw,
                    tid,
                    collection,
                    fence,
                });
            }
            DocIndexOwner::Named(_) | DocIndexOwner::Declared => MaintainedOwner {
                owner: owner.clone(),
                is_array,
                unique,
                case_insensitive,
                predicate: predicate.map(str::to_owned),
            },
        };
        let step = FencedStep {
            database_id: database_raw,
            tid,
            collection,
            path,
            owner,
            fence,
        };
        // Both checks read the committed set and return before any row is
        // read. A build from an older descriptor than one already applied
        // for this owner changes nothing. A build whose owner the set
        // already holds with this exact spec only raises the fence. A
        // re-delivered build at a fence the set holds writes nothing.
        let committed = self
            .sparse
            .maintained_indexes(database_raw, tid, collection)?;
        match committed.judge(path, owner, fence) {
            FenceVerdict::Stale => return Ok(()),
            FenceVerdict::Apply if committed.holds_owner(path, &spec) => {
                return self.raise_fence_only(&committed, &step);
            }
            FenceVerdict::Apply | FenceVerdict::NewIncarnation => {}
        }
        if let Some(ref m) = self.metrics {
            m.record_document_index_backfill();
        }

        // Parse the partial-index predicate once, up front. A predicate
        // that does not parse refuses the build.
        let parsed_predicate = parse_predicate(predicate)?;

        // The encoding of these stored bodies is resolved from the collection's
        // registered kind, never sniffed from the bytes: a strict collection
        // stores Binary Tuples and a vector-primary one stores tagged sidecars,
        // and the schemaless MessagePack decoder reads neither. Decoding them
        // all as documents is what made `CREATE INDEX` on a strict collection
        // build an EMPTY index and report success.
        let body_format =
            self.sparse_body_format(database_id, crate::types::TenantId::new(tid), collection);
        let database_id = database_id.as_u64();

        // A UNIQUE index judges the current version of every row before any
        // entry is written: a duplicate among them refuses the backfill and
        // leaves the index empty. Rows stream one at a time, so no row count
        // caps the judgement.
        if unique && judge_unique {
            let crs = self.crs_context();
            let mut seen: std::collections::HashMap<
                String,
                crate::engine::document::store::StorageKey,
            > = std::collections::HashMap::new();
            self.for_each_current_row(database_id, tid, collection, |doc_id, bytes| {
                // A row skipped here is a row the finished index permanently
                // omits, and the index is then reported as built.
                let doc = decode_scanned_document(bytes, body_format.as_format_ref())?;
                // Partial-index predicate: only rows for which the predicate
                // is explicitly true are indexed (Postgres partial-index
                // semantics).
                if let Some(ref p) = parsed_predicate
                    && !p.evaluate_json(&doc, &crs)
                {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
                for raw in
                    crate::engine::document::store::extract_index_values(&doc, path, is_array)
                {
                    let stored = if case_insensitive {
                        raw.to_lowercase()
                    } else {
                        raw
                    };
                    if let Some(prev) = seen.get(&stored)
                        && prev != doc_id
                    {
                        return Err(crate::Error::RejectedConstraint {
                            collection: collection.to_string(),
                            constraint: "unique".to_string(),
                            detail: format!(
                                "unique index backfill: duplicate value '{stored}' on \
                                     '{path}' (existing '{prev}', new '{doc_id}')"
                            ),
                        });
                    }
                    seen.insert(stored, *doc_id);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })?;
        }

        let txn = self.sparse.begin_write()?;
        let mut set = self
            .sparse
            .maintained_indexes_in_txn(&txn, database_id, tid, collection)?;
        // The judgement above read this same committed set, so the step is
        // not stale here. A newer incarnation discards the old set first.
        self.fence_step_in_txn(&txn, &mut set, &step)?;
        let before = set.shape(path);
        set.add_owner(path, spec);
        set.raise_fence(path, owner, fence);
        // Every version of every row, each at its own system time, so a
        // lookup at any cut finds each row by the values it held then. A
        // path another owner already keeps in the same shape holds every
        // entry already.
        self.settle_maintained_path_in_txn(
            &txn,
            &set,
            PathChange {
                database_id,
                tid,
                collection,
                path,
                before,
                body_format: body_format.as_format_ref(),
            },
        )?;
        self.sparse.put_maintained_indexes_in_txn(&txn, &set)?;
        let prepared = PreparedSet::of(&set)?;

        txn.commit().map_err(|e| crate::Error::Internal {
            detail: format!("backfill commit: {e}"),
        })?;
        self.maintained_indexes.apply(prepared);
        Ok(())
    }

    /// Remove `owner` from a field, and every version of the field's entries
    /// when no owner is left. Returns `{"removed": N}` as the response
    /// payload.
    pub(in crate::data::executor) fn execute_drop_document_index(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field: &str,
        owner: &DocIndexOwner,
        fence: DocIndexFence,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field,
            %owner,
            "drop document index"
        );

        let removed = self.purge_document_index_field(
            task.request.database_id.as_u64(),
            tid,
            collection,
            field,
            owner,
            fence,
        );
        match removed {
            Ok(removed) => {
                match super::super::super::response_codec::encode_count("removed", removed) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => {
                        self.response_error(task, crate::bridge::envelope::ErrorCode::from(e))
                    }
                }
            }
            Err(e) => self.response_error(task, crate::bridge::envelope::ErrorCode::from(e)),
        }
    }

    /// Remove `owner` from `field` in the collection's maintained set, in one
    /// write transaction, and return how many entries left. Shared with
    /// restart replay of a `DocumentIndexRetire` record.
    ///
    /// The entries go when no owner is left. When another owner keeps the
    /// field in another shape, the entries are written again in that shape.
    /// An owner that does not keep the field changes nothing. A retire from
    /// an older descriptor than one already applied for this owner changes
    /// nothing either. Every other retire raises the owner's fence, so a
    /// build the retire overtook stays out.
    pub(in crate::data::executor) fn purge_document_index_field(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        owner: &DocIndexOwner,
        fence: DocIndexFence,
    ) -> crate::Result<usize> {
        match owner {
            DocIndexOwner::DeclaredTopLevel => {
                return self.retire_top_level_index(TopLevelTarget {
                    database_id,
                    tid,
                    collection,
                    fence,
                });
            }
            DocIndexOwner::Named(_) | DocIndexOwner::Declared => {}
        }
        // The entries are keyed by the extraction path.
        let path = field.strip_suffix("[]").unwrap_or(field);
        let body_format = self.sparse_body_format(
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection,
        );
        let txn = self.sparse.begin_write()?;
        let mut set = self
            .sparse
            .maintained_indexes_in_txn(&txn, database_id, tid, collection)?;
        let step = FencedStep {
            database_id,
            tid,
            collection,
            path,
            owner,
            fence,
        };
        match self.fence_step_in_txn(&txn, &mut set, &step)? {
            FenceVerdict::Stale => return Ok(0),
            FenceVerdict::Apply | FenceVerdict::NewIncarnation => {}
        }
        let before = set.shape(path);
        set.remove_owner(path, owner);
        set.raise_fence(path, owner, fence);
        let removed = self.settle_maintained_path_in_txn(
            &txn,
            &set,
            PathChange {
                database_id,
                tid,
                collection,
                path,
                before,
                body_format: body_format.as_format_ref(),
            },
        )?;
        self.sparse.put_maintained_indexes_in_txn(&txn, &set)?;
        let prepared = PreparedSet::of(&set)?;
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("drop index commit: {e}"),
        })?;
        self.maintained_indexes.apply(prepared);
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner, DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId};

    use super::BackfillIndexParams;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_request};
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::CollectionConfig;

    const COLLECTION: &str = "unique_repeat";

    fn put(core: &mut CoreLoop, id: &str, surrogate: u32, email: &str) {
        let response = core.execute(&ExecutionTask::new(make_request(PhysicalPlan::Document(
            DocumentOp::PointPut {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                document_id: id.into(),
                value: nodedb_types::json_to_msgpack(&serde_json::json!({ "email": email }))
                    .expect("encode"),
                surrogate: Surrogate::new(surrogate),
                pk_bytes: id.as_bytes().to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
            },
        ))));
        assert_eq!(response.status, Status::Ok, "{response:?}");
    }

    fn build_unique(core: &mut CoreLoop, owner: &DocIndexOwner) -> crate::Result<()> {
        core.backfill_document_index(
            DatabaseId::DEFAULT,
            BackfillIndexParams {
                tid: 1,
                collection: COLLECTION,
                path: "$.email",
                is_array: false,
                unique: true,
                judge_unique: true,
                case_insensitive: false,
                predicate: None,
                owner,
                fence: DocIndexFence::default(),
            },
        )
    }

    #[test]
    fn a_repeated_unique_build_with_an_identical_owner_never_walks_the_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
        core.seed_doc_configs(&[(
            (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
            CollectionConfig::new(COLLECTION),
        )]);
        let owner = DocIndexOwner::Named("by_email".into());
        put(&mut core, "r1", 1, "a@x.test");
        build_unique(&mut core, &owner).expect("first build");

        // The registered config enforces no UNIQUE, so the rows hold a
        // duplicate that any walk of them refuses.
        put(&mut core, "r2", 2, "a@x.test");
        assert!(matches!(
            build_unique(&mut core, &DocIndexOwner::Named("other".into())),
            Err(crate::Error::RejectedConstraint { .. })
        ));

        build_unique(&mut core, &owner).expect("the repeated build reads no row");
    }

    fn at(version: u64) -> DocIndexFence {
        DocIndexFence {
            incarnation: nodedb_types::Hlc::new(1, 0),
            descriptor_version: version,
        }
    }

    fn build_at(core: &mut CoreLoop, owner: &DocIndexOwner, fence: DocIndexFence) {
        core.backfill_document_index(
            DatabaseId::DEFAULT,
            BackfillIndexParams {
                tid: 1,
                collection: COLLECTION,
                path: "$.email",
                is_array: false,
                unique: false,
                judge_unique: true,
                case_insensitive: false,
                predicate: None,
                owner,
                fence,
            },
        )
        .expect("a build answers Ok");
    }

    #[test]
    fn a_build_below_an_applied_retire_leaves_the_set_without_the_owner() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
        core.seed_doc_configs(&[(
            (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
            CollectionConfig::new(COLLECTION),
        )]);
        let owner = DocIndexOwner::Named("by_email".into());
        put(&mut core, "r1", 1, "a@x.test");
        build_at(&mut core, &owner, at(1));
        core.purge_document_index_field(0, 1, COLLECTION, "$.email", &owner, at(3))
            .expect("retire");

        build_at(&mut core, &owner, at(2));
        let set = core
            .sparse
            .maintained_indexes(0, 1, COLLECTION)
            .expect("set");
        assert!(
            set.paths.is_empty(),
            "the stale build added no owner: {set:?}"
        );
        assert_eq!(set.fence_of("$.email", &owner), Some(at(3)));
        assert!(!core.maintained_indexes.maintains_any(0, 1, COLLECTION));
        assert!(
            core.sparse
                .versioned_index_lookup_as_of(0, 1, COLLECTION, "$.email", "a@x.test", None)
                .expect("lookup")
                .is_empty()
        );

        // A build from the retire's descriptor or a newer one applies.
        build_at(&mut core, &owner, at(4));
        assert!(core.maintained_indexes.maintains_any(0, 1, COLLECTION));
    }
}
