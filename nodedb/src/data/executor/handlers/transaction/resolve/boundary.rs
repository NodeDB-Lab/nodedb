// SPDX-License-Identifier: BUSL-1.1

//! The document and KV writes a staged transaction's redo record will carry,
//! read before the resolve.
//!
//! The Calvin stage judges its commit boundary on these writes. Each one
//! comes from the same overlay rows and the same helpers the resolve
//! serializes:
//!
//! * a KV row through `serialize_kv_collection`;
//! * a document row through `staged_document_rows`;
//! * a base row a staged TRUNCATE removes through `truncated_base_rows` and
//!   `truncated_row_entries`;
//! * a RESTORE batch's rows through `restored_row_ops`.
//!
//! The resolve adds only what no check reads: the version stamps it pins.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::redo_apply::{
    RedoDocOp, RedoKvOp, document_ops, kv_ops,
};
use crate::data::executor::task::ExecutionTask;
use crate::types::{TenantId, TxnId};
use crate::wal::RedoSubRecord;

use super::classify::{classify_document_op, classify_kv_op};
use super::document::staged_document_rows;
use super::document_truncate::{truncate_declared_primary_keys, truncated_row_entries};
use super::kv::serialize_kv_collection;

/// The document and KV writes of one staged transaction slice, in the
/// order its redo record carries them.
pub(in crate::data::executor) struct StagedBoundaryWrites {
    pub documents: Vec<RedoDocOp>,
    pub kv: Vec<RedoKvOp>,
}

impl CoreLoop {
    /// The document and KV writes the resolve of `plans`, staged under
    /// `txn_id`, puts in its redo record. Reads only.
    pub(in crate::data::executor) fn staged_boundary_writes(
        &self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        plans: &[PhysicalPlan],
    ) -> crate::Result<StagedBoundaryWrites> {
        let mut kv_collections: BTreeSet<String> = BTreeSet::new();
        let mut doc_collections: BTreeSet<String> = BTreeSet::new();
        for plan in plans {
            match plan {
                PhysicalPlan::Kv(op) => classify_kv_op(op, &mut kv_collections)?,
                PhysicalPlan::Document(op) => classify_document_op(op, &mut doc_collections)?,
                PhysicalPlan::Graph(_)
                | PhysicalPlan::Crdt(_)
                | PhysicalPlan::Text(_)
                | PhysicalPlan::Query(_)
                | PhysicalPlan::Meta(_)
                | PhysicalPlan::Vector(_)
                | PhysicalPlan::Array(_)
                | PhysicalPlan::Timeseries(_)
                | PhysicalPlan::Columnar(_)
                | PhysicalPlan::Spatial(_)
                | PhysicalPlan::ClusterArray(_)
                | PhysicalPlan::ClusterEvent(_) => {}
            }
        }

        let database_id = task.request.database_id;
        let mut documents: Vec<RedoDocOp> = Vec::new();
        let mut records: Vec<RedoSubRecord> = Vec::new();
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            for collection in &kv_collections {
                let coll_key = (database_id, TenantId::new(tid), collection.clone());
                serialize_kv_collection(overlay, &coll_key, collection, &mut records)?;
            }
            let truncate_primary_keys = truncate_declared_primary_keys(plans);
            for collection in &doc_collections {
                let coll_key = (database_id, TenantId::new(tid), collection.clone());
                let strict_schema =
                    self.resolve_strict_schema(database_id.as_u64(), tid, collection);
                if overlay.is_truncated(&coll_key) {
                    let rows =
                        self.truncated_base_rows(task, tid, collection, overlay, &coll_key)?;
                    let declared = truncate_primary_keys
                        .get(collection.as_str())
                        .and_then(|pk| pk.as_deref());
                    let removed = truncated_row_entries(&rows, strict_schema.as_ref(), declared);
                    documents.extend(removed.into_values().map(|surrogate| RedoDocOp::Delete {
                        collection: collection.clone(),
                        surrogate,
                    }));
                }
                for row in staged_document_rows(overlay, &coll_key, strict_schema.as_ref()) {
                    let row = row?;
                    documents.push(match row.value {
                        Some(value) => RedoDocOp::Put {
                            collection: collection.clone(),
                            value,
                            surrogate: row.surrogate,
                            clone_copy: false,
                        },
                        None => RedoDocOp::Delete {
                            collection: collection.clone(),
                            surrogate: row.surrogate,
                        },
                    });
                }
            }
        }
        // A RESTORE batch stages nothing: its rows come from the plan.
        for plan in plans {
            let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan else {
                continue;
            };
            let scope = self.restore_scope(database_id, tid, batch)?;
            let (rows, _) = self.restored_row_ops(scope, batch)?;
            records.extend(rows);
        }
        documents.extend(document_ops(&records));
        Ok(StagedBoundaryWrites {
            documents,
            kv: kv_ops(&records),
        })
    }
}
