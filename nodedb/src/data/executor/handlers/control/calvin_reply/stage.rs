// SPDX-License-Identifier: BUSL-1.1

//! Stage one Calvin plan and fold its reply into the transaction's reply.
//!
//! A Calvin flush installs the transaction's redo record, and a redo record
//! carries no reply. Each plan therefore decides its reply when it stages.
//!
//! - A plan without `RETURNING` answers its staging handler's reply: the
//!   affected count against BASE ∪ OVERLAY.
//! - A `RETURNING` plan answers the rows it touched. The overlay journal
//!   names them: every slot the plan's staging mutated. Each row's image is
//!   taken from BASE ∪ OVERLAY, so a row an earlier plan of the same
//!   transaction wrote is reported as that plan left it.
//! - A delete reports each removed row's image from before the plan.
//! - A document or CRDT write reports each row as the flush's install
//!   stored it, read back from base after the install. The install can
//!   rewrite a body, as hash chaining does.
//! - A KV, vector-primary or columnar write reports the rows it staged,
//!   which are the exact bytes the install stores.
//! - A timeseries ingest reports its stamped rows rendered through the
//!   raw-scan row emitter, as the live ingest does.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_types::{Surrogate, Value};

use super::images::{RowLocation, StoredRow, staged_row_bytes};
use super::reply::{CalvinReply, PostImages};
use super::target::{ReplyImage, ReturningTarget, RowEngine, returning_target};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::row_values_to_object;
use crate::data::executor::handlers::returning_rows::build_rows_payload;
use crate::data::executor::handlers::timeseries::StampedIngest;
use crate::data::executor::handlers::transaction::overlay::{
    Staged, TouchedSlot, decode_staged_row,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::ilp;
use crate::types::{DatabaseId, TenantId, TxnId};
use crate::util::rmpv_value::rmpv_to_value;

type CollKey = (DatabaseId, TenantId, String);

impl CoreLoop {
    /// Stage one Calvin plan under `txn_id` and fold its reply into `reply`.
    ///
    /// A `RETURNING` plan's rows replace `reply`. A later plan without
    /// `RETURNING` keeps them, and replaces only an affected count.
    pub(in crate::data::executor) fn stage_calvin_plan(
        &mut self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
        reply: &mut CalvinReply,
    ) -> Result<(), ErrorCode> {
        let tid = tenant_id.as_u64();
        // Every core that stages a plan holds the overlay, even when the plan
        // stages no row. `CalvinResolve` refuses a missing one.
        let marker = self.txn_overlay_mut(txn_id).journal_len();
        let staged = self.stage_calvin_overlay(task, txn_id, tenant_id, plan)?;
        match returning_target(plan) {
            Some(target) => {
                *reply = self.calvin_returning_reply(task, txn_id, tid, plan, &target, marker)?;
            }
            None => {
                self.settle_rows_before_later_write(task, txn_id, tid, reply, marker)?;
                if !reply.has_rows() {
                    *reply = CalvinReply::Count(staged);
                }
            }
        }
        Ok(())
    }

    /// The reply of a `RETURNING` plan whose staging began at journal
    /// position `marker`.
    fn calvin_returning_reply(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        plan: &PhysicalPlan,
        target: &ReturningTarget<'_>,
        marker: usize,
    ) -> Result<CalvinReply, ErrorCode> {
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
        };
        let written = slots.iter().filter_map(|slot| match slot.after {
            Some(Staged::Put(body)) => Some((slot, body.as_slice())),
            Some(Staged::Tombstone) | None => None,
        });

        match (target.image, target.engine) {
            (ReplyImage::Before, _) => {
                let rows =
                    self.calvin_removed_rows(&at, target, &slots, overlay.base_visible(&coll_key))?;
                self.calvin_render_rows(&at, target.spec, target.rls_filters, &rows)
                    .map(CalvinReply::Rows)
            }
            (ReplyImage::After, RowEngine::Document | RowEngine::Crdt) => {
                Ok(CalvinReply::PostImages(PostImages {
                    spec: target.spec.clone(),
                    rls_filters: target.rls_filters.to_vec(),
                    collection: target.collection.to_string(),
                    engine: target.engine,
                    rows: written
                        .map(|(slot, _)| (slot.doc_id.clone(), Surrogate::new(slot.surrogate)))
                        .collect(),
                }))
            }
            (ReplyImage::After, RowEngine::Kv | RowEngine::Vector) => {
                let rows = written
                    .map(|(slot, body)| {
                        Ok(StoredRow {
                            identity: slot.doc_id.clone(),
                            surrogate: Surrogate::new(slot.surrogate),
                            bytes: staged_row_bytes(target.engine, body)?,
                        })
                    })
                    .collect::<Result<Vec<_>, ErrorCode>>()?;
                self.calvin_render_rows(&at, target.spec, target.rls_filters, &rows)
                    .map(CalvinReply::Rows)
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
                let docs = written
                    .map(|(slot, body)| {
                        decode_staged_row(body)
                            .map(|row| row_values_to_object(&schema, &row))
                            .ok_or_else(|| ErrorCode::Internal {
                                detail: format!(
                                    "calvin RETURNING: staged columnar row {} of '{}' does not \
                                     decode",
                                    slot.surrogate, target.collection
                                ),
                            })
                    })
                    .collect::<Result<Vec<Value>, ErrorCode>>()?;
                build_rows_payload(target.spec, target.rls_filters, &docs)
                    .map(CalvinReply::Rows)
                    .map_err(ErrorCode::from)
            }
            (ReplyImage::After, RowEngine::Timeseries) => self
                .calvin_timeseries_rows(task, txn_id, tid, plan, target)
                .map(CalvinReply::Rows),
        }
    }

    /// The prior image of every row a delete removed: the row as an earlier
    /// plan of the transaction staged it, else its base row.
    fn calvin_removed_rows(
        &self,
        at: &RowLocation<'_>,
        target: &ReturningTarget<'_>,
        slots: &[TouchedSlot<'_>],
        base_visible: bool,
    ) -> Result<Vec<StoredRow>, ErrorCode> {
        let mut rows = Vec::new();
        for slot in slots {
            if !matches!(slot.after, Some(Staged::Tombstone)) {
                continue;
            }
            let surrogate = Surrogate::new(slot.surrogate);
            let prior = match slot.before {
                Some(Staged::Put(body)) => Some(staged_row_bytes(at.engine, body)?),
                Some(Staged::Tombstone) => None,
                None if base_visible => {
                    self.calvin_removed_base_row(at, target, slot, surrogate)?
                }
                None => None,
            };
            if let Some(bytes) = prior {
                rows.push(StoredRow {
                    identity: slot.doc_id.clone(),
                    surrogate,
                    bytes,
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

    /// The `RETURNING` rows of a timeseries ingest: the lines its resolve
    /// stamps, rendered through the raw-scan row emitter.
    fn calvin_timeseries_rows(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        plan: &PhysicalPlan,
        target: &ReturningTarget<'_>,
    ) -> Result<Vec<u8>, ErrorCode> {
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
            .unwrap_or_else(|| self.ingest_now_ms());
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
        let docs: Vec<Value> = rows.iter().map(rmpv_to_value).collect();
        build_rows_payload(target.spec, target.rls_filters, &docs).map_err(ErrorCode::from)
    }

    /// Decide a document or CRDT post-image reply now when a later plan,
    /// staged from journal position `marker`, rewrote one of its rows. A base
    /// read after the install would report the later plan's row, so each row
    /// takes the image the `RETURNING` plan staged instead.
    fn settle_rows_before_later_write(
        &self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tid: u64,
        reply: &mut CalvinReply,
        marker: usize,
    ) -> Result<(), ErrorCode> {
        let CalvinReply::PostImages(images) = &*reply else {
            return Ok(());
        };
        let database_id = task.request.database_id;
        let coll_key: CollKey = (database_id, TenantId::new(tid), images.collection.clone());
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        let later = overlay.slots_touched_since(marker, &coll_key);
        let rewritten = |surrogate: Surrogate| {
            later
                .iter()
                .find(|slot| slot.surrogate == surrogate.as_u32())
        };
        if !images.rows.iter().any(|(_, s)| rewritten(*s).is_some()) {
            return Ok(());
        }
        let at = RowLocation {
            engine: images.engine,
            database_id: database_id.as_u64(),
            tid,
            collection: &images.collection,
        };
        let mut rows = Vec::with_capacity(images.rows.len());
        for (identity, surrogate) in &images.rows {
            // The row as the `RETURNING` plan left it: the later plan's prior
            // value when that plan touched it, else the overlay's value now.
            let staged = match rewritten(*surrogate) {
                Some(slot) => slot.before,
                None => overlay.get(&coll_key, surrogate.as_u32()),
            };
            if let Some(Staged::Put(body)) = staged {
                rows.push(StoredRow {
                    identity: identity.clone(),
                    surrogate: *surrogate,
                    bytes: staged_row_bytes(images.engine, body)?,
                });
            }
        }
        let payload = self.calvin_render_rows(&at, &images.spec, &images.rls_filters, &rows)?;
        *reply = CalvinReply::Rows(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        ColumnarInsertIntent, ColumnarOp, CrdtOp, CrdtWriteVerb, DocumentOp, KvOp, PhysicalPlan,
        ReturningColumns, ReturningItem, ReturningSpec, TimeseriesOp, UpdateValue, VectorOp,
    };
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, Value};

    use crate::bridge::envelope::{Response, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::response_codec::RowsPayload;

    const TID: u64 = 1;

    fn returning(columns: &[&str]) -> Option<ReturningSpec> {
        Some(ReturningSpec {
            columns: ReturningColumns::Named(
                columns
                    .iter()
                    .map(|name| ReturningItem {
                        name: (*name).to_string(),
                        alias: None,
                    })
                    .collect(),
            ),
        })
    }

    fn coll(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    /// A row body as the planner encodes one: a standard MessagePack map.
    /// A KV value is stored and read back in exactly this form.
    fn object(fields: &[(&str, Value)]) -> Vec<u8> {
        let map: std::collections::HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode object")
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    /// Commit `plans` as one Calvin transaction on a fresh core prepared by
    /// `setup`, and return the flush's reply.
    fn commit(plans: &[PhysicalPlan], setup: impl FnOnce(&mut CoreLoop)) -> Response {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        setup(&mut core);
        core.calvin_commit_for_test(&make_default_task(), TID, plans, 1, 100)
    }

    /// The first returned column of every row in `response`.
    fn returned(response: &Response) -> Vec<Value> {
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let payload: RowsPayload =
            zerompk::from_msgpack(response.payload.as_bytes()).expect("RETURNING rows");
        payload
            .rows
            .iter()
            .map(|row| {
                row.first()
                    .map(|cell| cell.0.clone())
                    .unwrap_or(Value::Null)
            })
            .collect()
    }

    fn doc_insert(id: &str, surrogate: u32, a: &str, if_absent: bool, ret: bool) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: coll("orders"),
            document_id: id.to_string(),
            value: object(&[("a", text(a))]),
            if_absent,
            surrogate: Surrogate::new(surrogate),
            returning: if ret { returning(&["a"]) } else { None },
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn doc_put(id: &str, surrogate: u32, a: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: coll("orders"),
            document_id: id.to_string(),
            value: object(&[("a", text(a))]),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        })
    }

    fn doc_update(id: &str, surrogate: u32, a: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: coll("orders"),
            document_id: id.to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            updates: vec![(
                "a".to_string(),
                UpdateValue::Literal(nodedb_types::value_to_msgpack(&text(a)).expect("literal")),
            )],
            returning: returning(&["a"]),
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn doc_delete(id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: coll("orders"),
            document_id: id.to_string(),
            surrogate: Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            returning: returning(&["a"]),
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        })
    }

    /// A point insert answers the row it stored. A later `ON CONFLICT DO
    /// NOTHING` insert of the same key stages nothing and answers no rows.
    #[test]
    fn a_point_insert_answers_its_row_and_a_skipped_insert_answers_none() {
        let inserted = commit(&[doc_insert("o1", 7, "kept", false, true)], |_| {});
        assert_eq!(returned(&inserted), vec![text("kept")]);

        let skipped = commit(
            &[
                doc_insert("o1", 7, "kept", false, false),
                doc_insert("o1", 7, "skipped", true, true),
            ],
            |_| {},
        );
        assert!(returned(&skipped).is_empty());
    }

    /// An update's RETURNING reports the row an earlier plan of the same
    /// statement inserted, with the update applied.
    #[test]
    fn an_update_returns_the_post_image_of_the_statements_own_insert() {
        let response = commit(
            &[
                doc_insert("o1", 7, "first", false, false),
                doc_update("o1", 7, "second"),
            ],
            |_| {},
        );
        assert_eq!(returned(&response), vec![text("second")]);
    }

    /// A delete's RETURNING reports the row an earlier plan of the same
    /// statement inserted.
    #[test]
    fn a_delete_returns_the_row_the_statement_inserted_before_it() {
        let response = commit(
            &[
                doc_insert("o1", 7, "first", false, false),
                doc_delete("o1", 7),
            ],
            |_| {},
        );
        assert_eq!(returned(&response), vec![text("first")]);
    }

    /// A bulk delete's RETURNING reports a predicted row as an earlier plan
    /// of the same statement updated it.
    #[test]
    fn a_bulk_delete_returns_the_row_as_the_statement_updated_it() {
        let mut update = doc_update("1", 1, "changed");
        if let PhysicalPlan::Document(DocumentOp::PointUpdate { returning, .. }) = &mut update {
            *returning = None;
        }
        let bulk_delete = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: coll("orders"),
            filters: Vec::new(),
            returning: returning(&["a"]),
            ollp_predicted_surrogates: Some(vec![1]),
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let response = commit(&[update, bulk_delete], |core| {
            let body = crate::data::executor::doc_format::canonicalize_document_for_storage(
                &object(&[("a", text("seeded"))]),
            );
            core.sparse
                .put(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "orders",
                    &nodedb_types::StorageKey::for_surrogate(Surrogate::new(1)),
                    &body,
                )
                .expect("seed row");
        });
        assert_eq!(returned(&response), vec![text("changed")]);
    }

    /// A later plan without RETURNING keeps the rows an earlier RETURNING
    /// plan answered.
    #[test]
    fn a_later_count_only_plan_keeps_the_returning_rows() {
        let response = commit(
            &[
                doc_insert("o1", 7, "kept", false, true),
                doc_insert("o2", 8, "other", false, false),
            ],
            |_| {},
        );
        assert_eq!(returned(&response), vec![text("kept")]);
    }

    /// A later plan that rewrites a returned row leaves the reply at the row
    /// the RETURNING plan wrote.
    #[test]
    fn a_later_rewrite_of_a_returned_row_keeps_the_returning_plans_image() {
        let response = commit(
            &[
                doc_insert("o1", 7, "first", false, true),
                doc_put("o1", 7, "second"),
            ],
            |_| {},
        );
        assert_eq!(returned(&response), vec![text("first")]);
    }

    /// A document batch insert answers every row it stored, in plan order.
    #[test]
    fn a_batch_insert_answers_every_row() {
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: coll("orders"),
            documents: vec![
                ("o1".to_string(), object(&[("a", text("one"))])),
                ("o2".to_string(), object(&[("a", text("two"))])),
            ],
            surrogates: vec![Surrogate::new(7), Surrogate::new(8)],
            returning: returning(&["a"]),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let response = commit(&[plan], |_| {});
        assert_eq!(returned(&response), vec![text("one"), text("two")]);
    }

    fn kv_put(value: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            collection: coll("cache"),
            key: b"k".to_vec(),
            value: object(&[("v", text(value))]),
            ttl_ms: 0,
            surrogate: Surrogate::new(5),
            returning: returning(&["v"]),
            rls_filters: Vec::new(),
        })
    }

    #[test]
    fn a_kv_put_answers_the_value_it_stored() {
        assert_eq!(
            returned(&commit(&[kv_put("put")], |_| {})),
            vec![text("put")]
        );
    }

    /// A KV delete answers the row it removed, as an earlier plan of the
    /// same statement left it.
    #[test]
    fn a_kv_delete_answers_the_row_the_statement_put_before_it() {
        let delete = PhysicalPlan::Kv(KvOp::Delete {
            collection: coll("cache"),
            keys: vec![b"k".to_vec()],
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: returning(&["v"]),
            rls_filters: Vec::new(),
        });
        let response = commit(&[kv_put("put"), delete], |_| {});
        assert_eq!(returned(&response), vec![text("put")]);
    }

    /// A KV row live at the epoch instant and expired by the wall clock is
    /// live for the stage and the reply. Every replica reads at the epoch
    /// instant, whenever its core runs the transaction.
    #[test]
    fn a_kv_delete_reads_liveness_at_the_epoch_instant() {
        let now = crate::engine::kv::current_ms();
        let delete = PhysicalPlan::Kv(KvOp::Delete {
            collection: coll("cache"),
            keys: vec![b"k".to_vec()],
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: returning(&["v"]),
            rls_filters: Vec::new(),
        });
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        // Expires 5 s before the wall clock, 3 s after the epoch instant.
        let value = object(&[("v", text("old"))]);
        core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TID,
            collection: "cache",
            key: b"k",
            value: &value,
            ttl_ms: 5_000,
            now_ms: now - 10_000,
            surrogate: Surrogate::new(5),
        });

        let epoch_ms = i64::try_from(now - 8_000).expect("epoch instant fits i64");
        let response =
            core.calvin_commit_at_for_test(&make_default_task(), TID, &[delete], 1, epoch_ms, 100);

        assert_eq!(returned(&response), vec![text("old")]);
    }

    #[test]
    fn a_vector_primary_insert_answers_its_payload() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectInsert {
            collection: coll("vp"),
            field: "vec".into(),
            surrogate: Surrogate::new(61),
            pk_bytes: b"r".to_vec(),
            vector: vec![1.0, 0.0],
            payload: zerompk::to_msgpack_vec(&std::collections::HashMap::from([(
                "label".to_string(),
                text("tagged"),
            )]))
            .expect("encode payload"),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: returning(&["label"]),
            rls_filters: Vec::new(),
        });
        assert_eq!(returned(&commit(&[plan], |_| {})), vec![text("tagged")]);
    }

    #[test]
    fn a_crdt_upsert_answers_the_row_the_install_materialized() {
        let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: coll("tasks"),
            document_id: "t1".to_string(),
            fields_json: r#"{"title":"kept"}"#.to_string(),
            surrogate: Surrogate::new(71),
            partial: false,
            verb: CrdtWriteVerb::Insert,
            returning: returning(&["title"]),
            rls_filters: Vec::new(),
        });
        assert_eq!(returned(&commit(&[plan], |_| {})), vec![text("kept")]);
    }

    #[test]
    fn a_columnar_insert_answers_its_row() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("note", ColumnType::String),
        ])
        .expect("valid columnar schema");
        let row = Value::Object(std::collections::HashMap::from([
            ("id".to_string(), text("c1")),
            ("note".to_string(), text("noted")),
        ]));
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: coll("metrics_col"),
            payload: nodedb_types::value_to_msgpack(&Value::Array(vec![row]))
                .expect("encode columnar payload"),
            format: "msgpack".to_string(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: vec![Surrogate::new(81)],
            schema_bytes: zerompk::to_msgpack_vec(&schema).expect("encode schema"),
            provenance: None,
            wal_lsn: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: returning(&["note"]),
            rls_filters: Vec::new(),
        });
        assert_eq!(returned(&commit(&[plan], |_| {})), vec![text("noted")]);
    }

    #[test]
    fn a_timeseries_ingest_answers_its_rows() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: coll("cpu"),
            payload: zerompk::to_msgpack_vec(&vec!["cpu value=7i 1000000000"])
                .expect("canonical ILP payload"),
            format: "ilp-msgpack".to_owned(),
            wal_lsn: None,
            surrogates: vec![Surrogate::new(91)],
            provenance: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: returning(&["value"]),
            rls_filters: Vec::new(),
        });
        assert_eq!(returned(&commit(&[plan], |_| {})), vec![Value::Integer(7)]);
    }
}
