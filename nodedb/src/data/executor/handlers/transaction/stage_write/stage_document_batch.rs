// SPDX-License-Identifier: BUSL-1.1

//! Staging for `BatchInsert`, which only a Calvin transaction stages, and
//! `ApplyBalanceDelta`, which a Calvin transaction and an in-transaction
//! `StageWrite` both stage.
//!
//! A multi-shard statement commits through Calvin. Calvin stages every plan
//! into the transaction overlay, and the flush installs the redo record that
//! resolve builds from that overlay. A plan that stages nothing therefore
//! installs nothing.
//!
//! - `BatchInsert` stages each row the way a document `PointInsert` stages
//!   it, as the live batch judges it: a key present in BASE ∪ OVERLAY — an
//!   earlier row of the same page included — is a `unique` violation, or a
//!   skipped row under `if_absent`.
//! - `ApplyBalanceDelta` reads the target row under BASE ∪ OVERLAY, adds the
//!   delta to the balance column, and stages the new row. The arithmetic,
//!   the encoding and the refusals are those of
//!   `CoreLoop::apply_balance_delta`, the live handler's read-modify-write.

use std::str::FromStr;

use nodedb_types::{RowIdentity, StorageKey, Surrogate};
use rust_decimal::Decimal;

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::materialized_sum::delta::json_to_decimal;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};

/// Inputs for [`CoreLoop::stage_document_batch_insert`].
pub(in crate::data::executor) struct StageBatchInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    /// `(client identity, body)` per row, in plan order.
    pub documents: &'a [(String, Vec<u8>)],
    /// One surrogate per row, parallel to `documents`.
    pub surrogates: &'a [Surrogate],
    /// `ON CONFLICT DO NOTHING`: a present key skips its row.
    pub if_absent: bool,
}

/// The row count a staged point write answered with.
fn staged_affected(response: &Response) -> crate::Result<usize> {
    let value: serde_json::Value = nodedb_types::json_from_msgpack(response.payload.as_ref())
        .map_err(|e| crate::Error::Internal {
            detail: format!("staged insert count: {e}"),
        })?;
    value
        .get("affected")
        .and_then(serde_json::Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| crate::Error::Internal {
            detail: format!("staged insert answered no affected count: {value}"),
        })
}

/// Inputs for [`CoreLoop::stage_apply_balance_delta`].
pub(in crate::data::executor) struct StageBalanceDeltaParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    /// TARGET collection: the one the balance row lives in.
    pub collection: &'a str,
    /// The target row's client identity.
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// The balance column being moved.
    pub column: &'a str,
    /// Signed amount to add, as an exact decimal string.
    pub delta: &'a str,
    pub join_column: &'a str,
    pub join_value: &'a str,
    /// The target collection's declared `PRIMARY KEY` column, when it has one.
    pub declared_primary_key: Option<&'a str>,
}

impl CoreLoop {
    /// Stage every row of a document batch insert. Answers the number of
    /// rows staged. A row that fails its primary-key or UNIQUE check fails
    /// the whole plan; under `if_absent` a present key skips its row.
    pub(in crate::data::executor) fn stage_document_batch_insert(
        &mut self,
        params: StageBatchInsertParams<'_>,
    ) -> Response {
        let StageBatchInsertParams {
            task,
            tid,
            txn_id,
            collection,
            documents,
            surrogates,
            if_absent,
        } = params;
        // Every overlay slot and every index is keyed by surrogate, so a row
        // without one cannot be staged.
        if surrogates.len() != documents.len() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "document batch insert for '{collection}' carries {} documents but {} \
                         surrogates; every row needs its surrogate to be staged",
                        documents.len(),
                        surrogates.len(),
                    ),
                },
            );
        }
        // One unbound row refuses the whole batch before any row is staged.
        for surrogate in surrogates {
            if let Some(refusal) =
                crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                    "document", collection, *surrogate,
                )
            {
                return self.response_error(task, refusal);
            }
        }
        let mut affected = 0usize;
        for ((document_id, value), surrogate) in documents.iter().zip(surrogates) {
            let ctx = StageCtx::new(
                task,
                tid,
                txn_id,
                collection,
                RowIdentity::from_user_key(document_id.as_str()),
                *surrogate,
            );
            // An earlier row of this page is staged by now, so a key the page
            // names twice is present for the later row.
            let staged = self.stage_point_insert(&ctx, value, if_absent);
            if staged.status == Status::Error {
                return staged;
            }
            match staged_affected(&staged) {
                Ok(n) => affected += n,
                Err(e) => return self.response_error(task, e),
            }
        }
        self.stage_count_response(task, affected)
    }

    /// Stage the target row of a materialized-sum balance move with the delta
    /// added to its balance column. Answers one affected row.
    pub(in crate::data::executor) fn stage_apply_balance_delta(
        &mut self,
        params: StageBalanceDeltaParams<'_>,
    ) -> Response {
        let task = params.task;
        match self.staged_balance_row(&params) {
            Ok((identity, body)) => {
                let ctx = StageCtx::new(
                    task,
                    params.tid,
                    params.txn_id,
                    params.collection,
                    identity,
                    params.surrogate,
                );
                if let Err(e) = self.stage_put_capped(&ctx, body) {
                    return self.response_error(task, e);
                }
                self.stage_count_response(task, 1)
            }
            Err(e) => self.response_error(task, e),
        }
    }

    /// The target row's identity and stored body after the balance move.
    fn staged_balance_row(
        &self,
        params: &StageBalanceDeltaParams<'_>,
    ) -> Result<(RowIdentity, Vec<u8>), ErrorCode> {
        // A delta that does not parse is a malformed plan. Applying zero
        // would report a balance move that never happened.
        let delta = Decimal::from_str(params.delta).map_err(|e| ErrorCode::Internal {
            detail: format!(
                "materialized-sum delta '{}' for {}.{} is not a decimal: {e}",
                params.delta, params.collection, params.column
            ),
        })?;
        let database_id = params.task.request.database_id;
        let tenant = TenantId::new(params.tid);
        let format = self.sparse_body_format(database_id, tenant, params.collection);
        // A vector-primary row is a tagged sidecar. A document body written
        // over it would read back as tag arrays.
        if matches!(format, SparseBodyFormat::VectorSidecar) {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target collection '{}' is vector-primary; its rows are metadata \
                     sidecars and cannot carry a materialized sum",
                    params.collection
                ),
            }
            .into());
        }

        let read_ctx = StageCtx::new(
            params.task,
            params.tid,
            params.txn_id,
            params.collection,
            RowIdentity::from_user_key(params.document_id),
            params.surrogate,
        );
        let Some(current) = self.stage_current_body(&read_ctx)? else {
            return Err(crate::Error::MaterializedSumTargetNotFound {
                target_collection: params.collection.to_string(),
                join_column: params.join_column.to_string(),
                join_value: params.join_value.to_string(),
            }
            .into());
        };

        let storage_key = StorageKey::for_surrogate(params.surrogate);
        let mut target_doc = decode_scanned_document(&current, format.as_format_ref())?;
        let balance = target_doc
            .get(params.column)
            .and_then(json_to_decimal)
            .unwrap_or(Decimal::ZERO)
            + delta;
        let Some(object) = target_doc.as_object_mut() else {
            return Err(crate::Error::Storage {
                engine: "materialized_sum".into(),
                detail: format!(
                    "target row {}/{storage_key} is not an object",
                    params.collection
                ),
            }
            .into());
        };
        // A balance is stored as text: `f64` loses digits past 15.
        object.insert(
            params.column.to_string(),
            serde_json::Value::String(balance.to_string()),
        );

        let submitted = doc_format::encode_to_msgpack(&target_doc);
        let identity =
            RowIdentity::of_stored_row(&submitted, params.declared_primary_key, storage_key);
        let body = self.stage_encode_put_body(
            database_id.as_u64(),
            params.tid,
            params.collection,
            params.surrogate,
            &submitted,
        )?;
        Ok((identity, body))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::{DatabaseId, QualifiedCollection, StorageKey, Surrogate, Value};

    use crate::bridge::envelope::{Response, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::doc_format;

    const TID: u64 = 1;

    fn object(fields: &[(&str, &str)]) -> Vec<u8> {
        let map: std::collections::HashMap<String, Value> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), Value::String((*v).to_string())))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode object")
    }

    fn seed(core: &mut CoreLoop, collection: &str, surrogate: u32, fields: &[(&str, &str)]) {
        let body = doc_format::canonicalize_document_for_storage(&object(fields));
        core.sparse
            .put(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                collection,
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
                &body,
                core.bitemporal_ordinal(),
            )
            .expect("seed row");
    }

    fn field(core: &CoreLoop, collection: &str, surrogate: u32, name: &str) -> Option<Value> {
        let body = core
            .sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                collection,
                &StorageKey::for_surrogate(Surrogate::new(surrogate)),
            )
            .expect("read row")?;
        match doc_format::decode_document_value(&body).expect("decode row") {
            Value::Object(map) => map.get(name).cloned(),
            _ => None,
        }
    }

    fn commit(core: &mut CoreLoop, plans: &[PhysicalPlan]) -> Response {
        core.calvin_commit_for_test(&make_default_task(), TID, plans, 1, 100)
    }

    fn balance_delta(delta: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "accounts"),
            document_id: "acc1".to_string(),
            surrogate: Surrogate::new(9),
            column: "balance".to_string(),
            delta: delta.to_string(),
            join_column: "id".to_string(),
            join_value: "acc1".to_string(),
            declared_primary_key: None,
        })
    }

    /// A Calvin balance move stages the target row with the delta added, and
    /// the flush installs it.
    #[test]
    fn a_calvin_balance_delta_adds_to_the_stored_balance() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(
            &mut core,
            "accounts",
            9,
            &[("id", "acc1"), ("balance", "10")],
        );

        let response = commit(&mut core, &[balance_delta("5"), balance_delta("-2.5")]);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            field(&core, "accounts", 9, "balance"),
            Some(Value::String("12.5".into())),
            "both moves land, the second on the first's staged row"
        );
    }

    /// A session transaction stages a balance move through `StageWrite`, and
    /// its redo record installs the moved balance.
    #[test]
    fn a_session_balance_delta_commits_through_the_redo_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(
            &mut core,
            "accounts",
            9,
            &[("id", "acc1"), ("balance", "10")],
        );

        let response = core.commit_plans_for_test(
            &make_default_task(),
            TID,
            &[balance_delta("5"), balance_delta("-2.5")],
            100,
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            field(&core, "accounts", 9, "balance"),
            Some(Value::String("12.5".into()))
        );
    }

    /// A balance move whose target row is absent refuses the transaction
    /// rather than dropping the move.
    #[test]
    fn a_calvin_balance_delta_on_a_missing_target_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let response = commit(&mut core, &[balance_delta("5")]);

        assert_eq!(response.status, Status::Error);
        assert_eq!(field(&core, "accounts", 9, "balance"), None);
    }

    /// A Calvin batch insert installs every row it staged.
    #[test]
    fn a_calvin_batch_insert_installs_every_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            documents: vec![
                ("o1".to_string(), object(&[("a", "one")])),
                ("o2".to_string(), object(&[("a", "two")])),
            ],
            if_absent: false,
            surrogates: vec![Surrogate::new(7), Surrogate::new(8)],
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });

        let response = commit(&mut core, &[plan]);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            field(&core, "orders", 7, "a"),
            Some(Value::String("one".into()))
        );
        assert_eq!(
            field(&core, "orders", 8, "a"),
            Some(Value::String("two".into()))
        );
    }

    /// A batch insert whose surrogates do not pair with its rows is refused
    /// and installs nothing.
    #[test]
    fn a_calvin_batch_insert_without_a_surrogate_per_row_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            documents: vec![("o1".to_string(), object(&[("a", "one")]))],
            if_absent: false,
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });

        let response = commit(&mut core, &[plan]);

        assert_eq!(response.status, Status::Error);
        assert_eq!(field(&core, "orders", 7, "a"), None);
    }

    fn batch(rows: &[(&str, u32, &str)], if_absent: bool) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            documents: rows
                .iter()
                .map(|(id, _, a)| (id.to_string(), object(&[("a", *a)])))
                .collect(),
            if_absent,
            surrogates: rows.iter().map(|(_, s, _)| Surrogate::new(*s)).collect(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    /// A Calvin batch row whose key is stored, or named twice by the page,
    /// is refused, and the stored row stays.
    #[test]
    fn a_calvin_batch_insert_refuses_a_duplicate_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core, "orders", 7, &[("a", "stored")]);

        let stored_dup = commit(&mut core, &[batch(&[("o7", 7, "over")], false)]);
        assert_eq!(stored_dup.status, Status::Error);
        assert_eq!(
            field(&core, "orders", 7, "a"),
            Some(Value::String("stored".into()))
        );

        let page_dup = commit(
            &mut core,
            &[batch(&[("o8", 8, "one"), ("o8", 8, "two")], false)],
        );
        assert_eq!(page_dup.status, Status::Error);
        assert_eq!(field(&core, "orders", 8, "a"), None);
    }

    /// Under `if_absent` a Calvin batch skips a present key and installs the
    /// rest.
    #[test]
    fn a_calvin_batch_insert_skips_a_present_key_under_if_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core, "orders", 7, &[("a", "stored")]);

        let response = commit(
            &mut core,
            &[batch(&[("o7", 7, "over"), ("o9", 9, "new")], true)],
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(
            field(&core, "orders", 7, "a"),
            Some(Value::String("stored".into()))
        );
        assert_eq!(
            field(&core, "orders", 9, "a"),
            Some(Value::String("new".into()))
        );
    }

    /// A Calvin truncate removes every base row of the collection.
    #[test]
    fn a_calvin_truncate_removes_the_base_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        seed(&mut core, "orders", 1, &[("a", "one")]);
        seed(&mut core, "orders", 2, &[("a", "two")]);
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "orders"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });

        let response = commit(&mut core, &[plan]);

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert_eq!(field(&core, "orders", 1, "a"), None);
        assert_eq!(field(&core, "orders", 2, "a"), None);
    }
}
