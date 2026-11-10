// SPDX-License-Identifier: BUSL-1.1

//! The reply and the staged images of one Calvin `RETURNING` plan.
//!
//! Each image is the full source row before projection. The reply projects
//! the same documents, so the two never disagree on a row. A document
//! or CRDT after-image is the row its install stores, derived by the
//! install's own steps (`install_image`), so the reply renders it. The
//! write set's locks hold every row the install reads at its staged state
//! from grant to install. A hash-chained document plan waits for the whole
//! slice, since its link follows every row the slice puts (`chained`).

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_types::{Surrogate, Value};

use super::chained::ChainedPlan;
use super::crdt_writes::StagedCrdtWrite;
use super::images::{ReturningImageRow, RowLocation, staged_row_image};
use super::install_image::WrittenRow;
use super::reply::CalvinReply;
use super::staged_images::{PlanRows, plan_images};
use super::target::{CollKey, ReplyImage, ReturningTarget, RowEngine};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::row_values_to_user_object;
use crate::data::executor::handlers::timeseries::StampedIngest;
use crate::data::executor::handlers::transaction::overlay::{
    Staged, TouchedSlot, decode_staged_row,
};
use crate::data::executor::response_codec::StagedPlanImages;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::ilp;
use crate::types::{TenantId, TxnId};
use crate::util::rmpv_value::rmpv_to_value;

/// What staging one `RETURNING` plan decides.
pub(super) struct PlanReturning {
    /// The reply the plan answers with.
    pub reply: CalvinReply,
    /// The plan's staged images.
    pub images: Option<StagedPlanImages>,
    /// A hash-chained plan, whose reply and images wait for the whole
    /// slice. `reply` and `images` stand empty for it.
    pub chained: Option<ChainedPlan>,
}

/// The plan a `RETURNING` reply stages for: its target, its staging start in
/// the overlay journal, its index in the stage's local slice, and the CRDT
/// row writes of the plans staged before it.
pub(super) struct ReturningPlan<'p, 't> {
    pub plan: &'p PhysicalPlan,
    pub target: &'t ReturningTarget<'p>,
    pub marker: usize,
    pub index: u32,
    pub earlier_crdt: &'t [StagedCrdtWrite],
}

impl CoreLoop {
    /// The reply and the images of a `RETURNING` plan.
    pub(super) fn calvin_returning_reply(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        at_plan: ReturningPlan<'_, '_>,
    ) -> Result<PlanReturning, ErrorCode> {
        let ReturningPlan {
            plan,
            target,
            marker,
            index,
            earlier_crdt,
        } = at_plan;
        let database_id = task.request.database_id;
        let coll_key: CollKey = (
            database_id,
            TenantId::new(tid),
            target.collection.to_string(),
        );
        let overlay = self
            .txn_overlays
            .get(&txn_id)
            .ok_or_else(|| ErrorCode::Internal {
                detail: "calvin RETURNING: the transaction overlay is missing after staging".into(),
            })?;
        let slots = overlay.slots_touched_since(marker, &coll_key);
        let at = RowLocation {
            engine: target.engine,
            database_id: database_id.as_u64(),
            tid,
            collection: target.collection,
            vector_field: target.vector_field,
        };
        let written = slots.iter().filter_map(|slot| match slot.after {
            Some(Staged::Put(body)) => Some((slot, body.as_slice())),
            Some(Staged::Tombstone) | None => None,
        });

        let rows = match (target.image, target.engine) {
            (ReplyImage::Before, _) => {
                let rows =
                    self.calvin_removed_rows(&at, target, &slots, overlay.base_visible(&coll_key))?;
                self.calvin_image_docs(&at, &rows)?
            }
            (ReplyImage::After, RowEngine::Document) if self.hash_chained(&coll_key) => {
                return Ok(PlanReturning {
                    reply: CalvinReply::Rows(Vec::new()),
                    images: None,
                    chained: Some(ChainedPlan {
                        index,
                        coll_key,
                        spec: target.spec.clone(),
                        rls_filters: target.rls_filters.to_vec(),
                        rows: written
                            .map(|(slot, _)| (slot.doc_id.clone(), Surrogate::new(slot.surrogate)))
                            .collect(),
                    }),
                });
            }
            (ReplyImage::After, RowEngine::Document | RowEngine::Crdt) => {
                let written: Vec<WrittenRow<'_, '_>> = written.collect();
                let rows = self.calvin_installed_rows(&at, plan, earlier_crdt, &written)?;
                self.calvin_image_docs(&at, &rows)?
            }
            (ReplyImage::After, RowEngine::Kv | RowEngine::Vector) => {
                let rows = written
                    .map(|(slot, body)| {
                        let (bytes, vector) = staged_row_image(target.engine, body)?;
                        Ok(ReturningImageRow {
                            identity: slot.doc_id.clone(),
                            surrogate: Surrogate::new(slot.surrogate),
                            bytes,
                            vector,
                        })
                    })
                    .collect::<Result<Vec<_>, ErrorCode>>()?;
                self.calvin_image_docs(&at, &rows)?
            }
            (ReplyImage::After, RowEngine::Columnar) => {
                let schema = self
                    .columnar_engines
                    .get(&coll_key)
                    .map(|engine| engine.schema().clone())
                    .ok_or_else(|| ErrorCode::Internal {
                        detail: format!(
                            "calvin RETURNING: columnar collection '{}' has no engine after \
                             staging",
                            target.collection
                        ),
                    })?;
                let mut rows = PlanRows::default();
                for (slot, body) in written {
                    let doc = decode_staged_row(body)
                        .map(|row| row_values_to_user_object(&schema, &row))
                        .ok_or_else(|| ErrorCode::Internal {
                            detail: format!(
                                "calvin RETURNING: staged columnar row {} of '{}' does not decode",
                                slot.surrogate, target.collection
                            ),
                        })?;
                    rows.push(slot.doc_id.as_str().as_bytes().to_vec(), doc);
                }
                rows
            }
            (ReplyImage::After, RowEngine::Timeseries) => {
                let docs = self.calvin_timeseries_docs(task, txn_id, tid, plan)?;
                PlanRows::keyed_by_ordinal(docs)?
            }
        };
        let reply =
            self.calvin_render_docs(target.engine, target.spec, target.rls_filters, &rows.docs)?;
        let images = plan_images(
            index,
            target.collection,
            target.rls_filters,
            rows,
            &self.crs_context(),
        )?;
        Ok(PlanReturning {
            reply: CalvinReply::Rows(reply),
            images: Some(images),
            chained: None,
        })
    }

    /// Whether `coll_key` links each row it stores into a hash chain.
    fn hash_chained(&self, coll_key: &CollKey) -> bool {
        self.doc_configs
            .get(coll_key)
            .is_some_and(|config| config.enforcement.hash_chain)
    }

    /// The documents of `rows`, each keyed by its row identity.
    pub(super) fn calvin_image_docs(
        &self,
        at: &RowLocation<'_>,
        rows: &[ReturningImageRow],
    ) -> Result<PlanRows, ErrorCode> {
        let docs = self.calvin_row_docs(at, rows)?;
        Ok(PlanRows {
            keys: rows
                .iter()
                .map(|row| row.identity.as_str().as_bytes().to_vec())
                .collect(),
            docs,
        })
    }

    /// The prior image of every row a delete removed: the row as an earlier
    /// plan of the transaction staged it, else its base row.
    fn calvin_removed_rows(
        &self,
        at: &RowLocation<'_>,
        target: &ReturningTarget<'_>,
        slots: &[TouchedSlot<'_>],
        base_visible: bool,
    ) -> Result<Vec<ReturningImageRow>, ErrorCode> {
        let mut rows = Vec::new();
        for slot in slots {
            if !matches!(slot.after, Some(Staged::Tombstone)) {
                continue;
            }
            let surrogate = Surrogate::new(slot.surrogate);
            let prior = match slot.before {
                Some(Staged::Put(body)) => Some(staged_row_image(at.engine, body)?),
                Some(Staged::Tombstone) => None,
                None if base_visible => self
                    .calvin_removed_base_row(at, target, slot, surrogate)?
                    .map(|bytes| (bytes, None)),
                None => None,
            };
            if let Some((bytes, vector)) = prior {
                rows.push(ReturningImageRow {
                    identity: slot.doc_id.clone(),
                    surrogate,
                    bytes,
                    vector,
                });
            }
        }
        Ok(rows)
    }

    /// The base image of a row a delete removed. A vector-primary row whose
    /// node is bound but whose sidecar is absent has an empty image, as the
    /// live delete reports it.
    fn calvin_removed_base_row(
        &self,
        at: &RowLocation<'_>,
        target: &ReturningTarget<'_>,
        slot: &TouchedSlot<'_>,
        surrogate: Surrogate,
    ) -> Result<Option<Vec<u8>>, ErrorCode> {
        let base = self.calvin_base_row(at, slot.doc_id, surrogate)?;
        match (base, target.vector_field) {
            (None, Some(field)) => {
                let index_key =
                    CoreLoop::vector_index_key(at.database_id, at.tid, at.collection, field);
                Ok(self
                    .vector_direct_node(&index_key, surrogate)
                    .map(|_| Vec::new()))
            }
            (base, _) => Ok(base),
        }
    }

    /// The rows of an unresolved timeseries ingest: the lines its resolve
    /// stamps, as the raw-scan row emitter renders them.
    fn calvin_timeseries_docs(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        plan: &PhysicalPlan,
    ) -> Result<Vec<Value>, ErrorCode> {
        let PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            surrogates,
            ..
        }) = plan
        else {
            return Err(ErrorCode::Internal {
                detail: "calvin RETURNING: a timeseries reply target is not an ingest".into(),
            });
        };
        let database_id = task.request.database_id;
        let tenant = TenantId::new(tid);
        let coll_key: CollKey = (database_id, tenant, collection.as_str().to_string());
        // The instant the staged batch read, which resolve stamps its untimed
        // rows with.
        let now_ms = surrogates
            .first()
            .and_then(|first| {
                self.txn_overlays
                    .get(&txn_id)?
                    .ingest_now(&coll_key, first.as_u32())
            })
            .unwrap_or_else(|| self.ingest_stamp_ms());
        let lines = self.stamped_ingest_lines(StampedIngest {
            database_id,
            tid: tenant,
            collection: collection.as_str(),
            payload,
            format,
            now_ms,
        })?;
        let joined = lines.join("\n");
        let parsed = ilp::parse_batch(&joined).map_err(|e| ErrorCode::RejectedPrevalidation {
            reason: format!("calvin RETURNING: unparsable line protocol: {e}"),
        })?;
        let rows = self.preview_ilp_ingest_rows(
            task,
            tenant,
            collection.as_str(),
            parsed.lines(),
            now_ms,
        )?;
        Ok(rows.iter().map(rmpv_to_value).collect())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
    use nodedb_types::Value;

    use super::super::fixtures::{
        TID, coll, columnar_insert, doc_delete, doc_insert, kv_delete, kv_put, kv_put_at, text,
        ts_ingest, vector_insert,
    };
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::control::calvin::{CalvinExecCtx, CalvinSnapshot};
    use crate::data::executor::response_codec::{StagedPlanImages, StagedReturningImages};
    use crate::types::TenantId;

    /// Stage `plans` as one static Calvin slice on a fresh core and return
    /// the images its stage response carries.
    fn stage(plans: &[PhysicalPlan]) -> StagedReturningImages {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let response = core.execute_calvin_execute_static(
            &make_default_task(),
            CalvinExecCtx {
                epoch: 1,
                position: 0,
                epoch_system_ms: 0,
            },
            &TenantId::new(TID),
            plans,
            CalvinSnapshot::NONE,
            &[],
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        if response.payload.is_empty() {
            return StagedReturningImages::default();
        }
        zerompk::from_msgpack(response.payload.as_bytes()).expect("stage images")
    }

    /// The only plan's images of a slice.
    fn only(images: StagedReturningImages) -> StagedPlanImages {
        assert_eq!(images.plans.len(), 1, "{images:?}");
        images.plans.into_iter().next().expect("one plan")
    }

    fn field<'a>(fields: &'a Value, name: &str) -> Option<&'a Value> {
        fields.get(name)
    }

    /// A KV put `plan` with its `RETURNING` clause removed.
    fn without_returning(mut plan: PhysicalPlan) -> PhysicalPlan {
        if let PhysicalPlan::Kv(KvOp::Put { returning, .. }) = &mut plan {
            *returning = None;
        }
        plan
    }

    #[test]
    fn a_kv_put_stages_its_row_under_its_identity() {
        let images = only(stage(&[kv_put("put")]));
        assert_eq!(images.plan, 0);
        assert_eq!(images.collection, coll("cache").as_str());
        assert_eq!(images.rows.len(), 1);
        let row = &images.rows[0];
        assert_eq!(row.key, hex::encode(b"k").into_bytes());
        assert_eq!(row.ordinal, 0);
        assert_eq!(field(&row.fields, "v"), Some(&text("put")));
    }

    /// Two `RETURNING` plans stage two sets, each keyed by its own slice
    /// index. The delete's set is its before-image: the row the put staged.
    #[test]
    fn two_returning_plans_stage_two_keyed_sets() {
        let images = stage(&[kv_put("put"), kv_delete()]);
        let plans: Vec<u32> = images.plans.iter().map(|plan| plan.plan).collect();
        assert_eq!(plans, vec![0, 1]);
        for plan in &images.plans {
            assert_eq!(plan.rows.len(), 1, "{plan:?}");
            assert_eq!(plan.rows[0].key, hex::encode(b"k").into_bytes());
            assert_eq!(plan.rows[0].ordinal, 0);
            assert_eq!(field(&plan.rows[0].fields, "v"), Some(&text("put")));
        }
    }

    /// Rows of one plan take ordinals in write order. A plan without
    /// `RETURNING` stages no set but still takes a slice index.
    #[test]
    fn rows_take_ordinals_and_plans_keep_slice_indexes() {
        let ingest = ts_ingest(&["cpu value=1i 1000000000", "cpu value=2i 2000000000"]);
        let images = only(stage(&[
            without_returning(kv_put_at(b"a", 4, "plain")),
            ingest,
        ]));
        assert_eq!(images.plan, 1);
        let rows: Vec<(Vec<u8>, u32, Option<&Value>)> = images
            .rows
            .iter()
            .map(|row| (row.key.clone(), row.ordinal, field(&row.fields, "value")))
            .collect();
        assert_eq!(
            rows,
            vec![
                (0u32.to_be_bytes().to_vec(), 0, Some(&Value::Integer(1))),
                (1u32.to_be_bytes().to_vec(), 1, Some(&Value::Integer(2))),
            ]
        );
    }

    #[test]
    fn a_vector_primary_insert_stages_its_payload_and_vector() {
        let images = only(stage(&[vector_insert()]));
        assert_eq!(images.collection, coll("vp").as_str());
        assert_eq!(images.rows.len(), 1);
        let row = &images.rows[0];
        assert!(!row.key.is_empty());
        assert_eq!(row.ordinal, 0);
        assert_eq!(field(&row.fields, "label"), Some(&text("tagged")));
        assert!(field(&row.fields, "vec").is_some(), "{:?}", row.fields);
    }

    #[test]
    fn a_columnar_insert_stages_its_row() {
        let images = only(stage(&[columnar_insert()]));
        assert_eq!(images.collection, coll("metrics_col").as_str());
        assert_eq!(images.rows.len(), 1);
        let row = &images.rows[0];
        assert!(!row.key.is_empty());
        assert_eq!(row.ordinal, 0);
        assert_eq!(field(&row.fields, "id"), Some(&text("c1")));
        assert_eq!(field(&row.fields, "note"), Some(&text("noted")));
    }

    /// A document insert stages the row its install stores. A delete
    /// stages its before-image: the row the insert staged.
    #[test]
    fn a_document_insert_stages_its_row_and_a_delete_its_before_image() {
        let images = stage(&[
            doc_insert("o1", 7, "first", false, true),
            doc_delete("o1", 7),
        ]);
        let plans: Vec<u32> = images.plans.iter().map(|plan| plan.plan).collect();
        assert_eq!(plans, vec![0, 1]);
        for plan in &images.plans {
            assert_eq!(plan.rows.len(), 1, "{plan:?}");
            let row = &plan.rows[0];
            assert_eq!(row.key, b"o1".to_vec());
            assert_eq!(field(&row.fields, "a"), Some(&text("first")));
        }
    }

    #[test]
    fn a_slice_without_returning_answers_an_empty_payload() {
        assert!(stage(&[without_returning(kv_put("put"))]).plans.is_empty());
    }
}
