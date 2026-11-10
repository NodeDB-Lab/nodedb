// SPDX-License-Identifier: BUSL-1.1

//! Stage one Calvin plan and fold its reply into the transaction's reply.
//!
//! A Calvin slice installs from its redo record, and a redo record carries no
//! reply. Each plan therefore decides its reply when it stages.
//!
//! - A plan without `RETURNING` answers its staging handler's reply: the
//!   affected count against BASE ∪ OVERLAY.
//! - A `RETURNING` plan answers the rows it touched. The overlay journal
//!   names them: every slot the plan's staging mutated. Each row's image is
//!   taken from BASE ∪ OVERLAY, so a row an earlier plan of the same
//!   transaction wrote is reported as that plan left it.
//! - A delete reports each removed row's image from before the plan.
//! - A document or CRDT write reports each row as the install stored it,
//!   read back from base after the install. The install can rewrite a body,
//!   as hash chaining does.
//! - A KV, vector-primary or columnar write reports the rows it staged,
//!   which are the exact bytes the install stores.
//! - A timeseries ingest reports its stamped rows rendered through the
//!   raw-scan row emitter, as the live ingest does.
//!
//! A `RETURNING` plan also stages its images: the full source rows its reply
//! projects. They ride the stage response, keyed by the plan's slice index.
//! A document or CRDT after-image and a resolved ingest's rows are the rows
//! the install stores, derived by the install's own steps when the plan
//! stages (`install_image`, `crdt_writes`, `ingest_images`). The reply of
//! those plans still reads what the install stored.

use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};

use super::crdt_writes::StagedCrdtWrite;
use super::edge_count::owned_edge_count;
use super::reply::{CalvinReply, CalvinStaging, InstalledTimeseries};
use super::returning::ReturningPlan;
use super::target::returning_target;
use crate::bridge::envelope::ErrorCode;
use crate::control::server::response_shape::types::plan_count_reads_reply;
use crate::control::server::shared::sql::affected_payload::read_affected_fields;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT;
use crate::types::{TenantId, TxnId};

impl CoreLoop {
    /// Stage one Calvin plan under `txn_id` and fold its reply into `staging`.
    ///
    /// A `RETURNING` plan's rows replace the reply. A later plan without
    /// `RETURNING` keeps them. A counted plan adds to `staging.total`.
    pub(in crate::data::executor) fn stage_calvin_plan(
        &mut self,
        task: &ExecutionTask,
        txn_id: TxnId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
        staging: &mut CalvinStaging,
    ) -> Result<(), ErrorCode> {
        let tid = tenant_id.as_u64();
        let plan_index = staging.plans_staged;
        staging.plans_staged = plan_index
            .checked_add(1)
            .ok_or_else(|| ErrorCode::Internal {
                detail: "calvin stage: the plan slice exceeds u32 plans".into(),
            })?;
        // Every timeseries ingest becomes one redo sub-record, in plan order,
        // so its position among them names its install.
        let ts_ordinal = staging.ts_ingests;
        if matches!(plan, PhysicalPlan::Timeseries(TimeseriesOp::Ingest { .. })) {
            staging.ts_ingests += 1;
        }
        // Every core that stages a plan holds the overlay, even when the plan
        // stages no row. `CalvinResolve` refuses a missing one.
        let marker = self.txn_overlay_mut(txn_id).journal_len();
        let staged = self.stage_calvin_overlay(task, txn_id, tenant_id, plan)?;
        let staged = owned_edge_count(task.request.vshard_id, plan, staged, staging)?;
        // A derived plan, such as a graph edge or a guard beside a document
        // delete, never replaces a count a plan of the statement decided.
        let derived = crate::control::planner::calvin::write_class::is_derived_side_effect(plan);
        let count_is_the_statements = staging.user_count;
        let reply = &mut staging.reply;
        match returning_target(plan) {
            // A resolved ingest's install decides which rows land and how
            // they read, so the reply renders them from that install.
            Some(target) if is_resolved_ingest(plan) => {
                let images =
                    self.calvin_resolved_ingest_images(task, tid, plan, &target, plan_index)?;
                staging.images.push(images);
                *reply = CalvinReply::InstalledTimeseries(InstalledTimeseries {
                    spec: target.spec.clone(),
                    rls_filters: target.rls_filters.to_vec(),
                    collection: target.collection.to_string(),
                    ordinal: ts_ordinal,
                });
            }
            Some(target) => {
                let staged = self.calvin_returning_reply(
                    task,
                    txn_id,
                    tid,
                    ReturningPlan {
                        plan,
                        target: &target,
                        marker,
                        index: plan_index,
                        earlier_crdt: &staging.crdt_writes,
                    },
                )?;
                *reply = staged.reply;
                staging.images.extend(staged.images);
            }
            None => {
                self.settle_rows_before_later_write(task, txn_id, tid, reply, marker)?;
                // A plan that staged no reply here, such as a TRUNCATE's
                // edge share, keeps the reply an earlier plan of the slice
                // decided.
                if is_resolved_ingest(plan) {
                    // The install answers this plan's count, and a derived
                    // plan's count never answers beside it.
                    staging.resolved_ingest = true;
                    staging.user_count = true;
                } else if !reply.has_rows()
                    && !staged.is_empty()
                    && !(derived && count_is_the_statements)
                {
                    // A count the statement's plans decided adds up: one plan
                    // that matched no row never hides another's count, and no
                    // plan without a count replaces their total.
                    let counted = (!derived && plan_count_reads_reply(plan))
                        .then(|| read_affected_fields(&staged))
                        .flatten();
                    match counted {
                        Some(fields) => staging.total.get_or_insert_default().add(&fields),
                        None if staging.total.is_none() => *reply = CalvinReply::Count(staged),
                        None => {}
                    }
                    staging.user_count |= !derived;
                }
            }
        }
        // A later plan's CRDT image folds this plan's write to the row.
        staging.crdt_writes.extend(StagedCrdtWrite::of_plan(plan));
        Ok(())
    }
}

/// Whether `plan` is a timeseries ingest resolved to its rows.
fn is_resolved_ingest(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { format, .. })
            if format == RESOLVED_INGEST_FORMAT
    )
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{CrdtOp, CrdtWriteVerb, DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, RlsWriteCheck, Surrogate, Value};

    use super::super::fixtures::{
        TID, coll, columnar_insert, doc_delete, doc_insert, doc_put, doc_update, kv_delete, kv_put,
        object, resolved_ingest, returning, text, ts_ingest, vector_insert,
    };
    use crate::bridge::envelope::{Response, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::response_codec::RowsPayload;

    /// Commit `plans` as one Calvin transaction on a fresh core prepared by
    /// `setup`, and return the install's reply.
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
            system_time: nodedb_types::SystemTimeScope::Current,
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
                    core.bitemporal_ordinal(),
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

    /// A derived plan after the statement's own write keeps the statement's
    /// count. A node delete's guard stages a count of zero.
    #[test]
    fn a_derived_plan_keeps_the_statements_count() {
        let guard = PhysicalPlan::Graph(nodedb_physical::physical_plan::GraphOp::NodeEdgeGuard {
            collection: coll("orders"),
            node_id: "o9".into(),
            expected: Vec::new(),
        });
        let response = commit(&[doc_insert("o1", 7, "kept", false, false), guard], |_| {});
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            crate::control::server::shared::sql::staging_predicates::extract_affected_count(
                response.payload.as_bytes()
            ),
            Some(1)
        );
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
            if_absent: false,
            surrogates: vec![Surrogate::new(7), Surrogate::new(8)],
            returning: returning(&["a"]),
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        let response = commit(&[plan], |_| {});
        assert_eq!(returned(&response), vec![text("one"), text("two")]);
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
        let delete = kv_delete();
        let response = commit(&[kv_put("put"), delete], |_| {});
        assert_eq!(returned(&response), vec![text("put")]);
    }

    /// A KV row live at the epoch instant and expired by the wall clock is
    /// live for the stage and the reply. Every replica reads at the epoch
    /// instant, whenever its core runs the transaction.
    #[test]
    fn a_kv_delete_reads_liveness_at_the_epoch_instant() {
        let now = crate::engine::kv::current_ms();
        let delete = kv_delete();
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        // Expires 5 s before the wall clock, 3 s after the epoch instant.
        let value = object(&[("v", text("old"))]);
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: TID,
                collection: "cache",
                key: b"k",
                value: &value,
                ttl_ms: 5_000,
                now_ms: now - 10_000,
                surrogate: Surrogate::new(5),
            })
            .expect("a bound row writes");

        let epoch_ms = i64::try_from(now - 8_000).expect("epoch instant fits i64");
        let response =
            core.calvin_commit_at_for_test(&make_default_task(), TID, &[delete], 1, epoch_ms, 100);

        assert_eq!(returned(&response), vec![text("old")]);
    }

    #[test]
    fn a_vector_primary_insert_answers_its_payload() {
        assert_eq!(
            returned(&commit(&[vector_insert()], |_| {})),
            vec![text("tagged")]
        );
    }

    #[test]
    fn a_crdt_upsert_answers_the_row_the_install_materialized() {
        let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: coll("tasks"),
            document_id: "t1".to_string(),
            fields: nodedb_physical::physical_plan::encode_fields_body(
                &std::collections::HashMap::from([(
                    "title".to_string(),
                    nodedb_types::Value::String("kept".into()),
                )]),
            )
            .expect("fields body encodes"),
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
        assert_eq!(
            returned(&commit(&[columnar_insert()], |_| {})),
            vec![text("noted")]
        );
    }

    /// Two `RETURNING` ingests into one collection in one transaction. The
    /// transaction answers its last `RETURNING` plan's rows, and those are
    /// the rows that plan's own install stored, not the other plan's.
    #[test]
    fn each_resolved_ingest_returns_only_its_own_rows() {
        let first = resolved_ingest("cpu value=7 1000000000");
        let second = resolved_ingest("cpu value=9 2000000000");
        assert_eq!(
            returned(&commit(&[first.clone(), second.clone()], |_| {})),
            vec![Value::Float(9.0)]
        );
        let last_is_first = commit(&[second, first], |_| {});
        assert_eq!(returned(&last_is_first), vec![Value::Float(7.0)]);
    }

    #[test]
    fn a_timeseries_ingest_answers_its_rows() {
        let plan = ts_ingest(&["cpu value=7i 1000000000"]);
        assert_eq!(returned(&commit(&[plan], |_| {})), vec![Value::Integer(7)]);
    }
}
