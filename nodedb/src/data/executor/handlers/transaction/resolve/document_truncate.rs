// SPDX-License-Identifier: BUSL-1.1

//! The redo sub-records of a staged document TRUNCATE: each base row the
//! truncate removes travels as its own `Delete`.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::columnar::StrictSchema;
use nodedb_types::{RowIdentity, StorageKey};

use super::document::delete_sub_record;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::TxnOverlay;
use crate::data::executor::handlers::transaction::stage_write::stored_row_identity;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

impl CoreLoop {
    /// The base rows a staged TRUNCATE of `collection` removes at COMMIT:
    /// every base row with no overlay entry. The redo record is the only
    /// thing a replica installs, so each removed row travels as its own
    /// `Delete`. Read-only against base.
    pub(super) fn truncated_base_rows(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        overlay: &TxnOverlay,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> crate::Result<Vec<(StorageKey, Vec<u8>)>> {
        // Each base row at its current version.
        let database_id = task.request.database_id.as_u64();
        self.current_rows_where(
            database_id,
            tid,
            collection,
            &|key: &StorageKey, _: &[u8]| overlay.get(coll_key, key.surrogate().as_u32()).is_none(),
        )
    }
}

/// The declared primary key of every document collection a `Truncate` plan
/// names, keyed by collection.
pub(super) fn truncate_declared_primary_keys(
    plans: &[PhysicalPlan],
) -> BTreeMap<String, Option<String>> {
    plans
        .iter()
        .filter_map(|plan| match plan {
            PhysicalPlan::Document(DocumentOp::Truncate {
                collection,
                declared_primary_key,
                ..
            }) => Some((collection.to_string(), declared_primary_key.clone())),
            PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => None,
        })
        .collect()
}

/// The base rows a staged TRUNCATE removes at COMMIT, with what names each
/// one in its redo entry.
pub(super) struct TruncatedBaseRows<'a> {
    pub collection: &'a str,
    /// `(storage key, stored body)` of every base row with no overlay entry.
    pub rows: &'a [(StorageKey, Vec<u8>)],
    pub strict_schema: Option<&'a StrictSchema>,
    /// The collection's declared `PRIMARY KEY` column, from the plan.
    pub declared_primary_key: Option<&'a str>,
    /// Each removed row's tombstone ordinal, keyed by surrogate.
    pub tombstone_sys_from: &'a BTreeMap<u32, i64>,
}

/// Append one `RecordType::Delete` redo sub-record per base row a staged
/// TRUNCATE removes, in deterministic doc-id order. Every replica removes the
/// same rows from the record, and a WAL-only restart does not replay a
/// removed row's original `Put` and resurrect it or its HNSW vector.
pub(super) fn serialize_truncated_base_rows(
    params: TruncatedBaseRows<'_>,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    let TruncatedBaseRows {
        collection,
        rows,
        strict_schema,
        declared_primary_key,
        tombstone_sys_from,
    } = params;
    let mut entries: BTreeMap<RowIdentity, u32> = BTreeMap::new();
    for (key, body) in rows {
        let identity = stored_row_identity(body, strict_schema, declared_primary_key, *key);
        entries.insert(identity, key.surrogate().as_u32());
    }
    for (doc_id, surrogate) in &entries {
        let sys_from =
            tombstone_sys_from
                .get(surrogate)
                .copied()
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "document resolve: truncated row '{doc_id}' of '{collection}' has no \
                     tombstone ordinal"
                    ),
                })?;
        ops.push(delete_sub_record(collection, doc_id, *surrogate, sys_from)?);
    }
    Ok(())
}
