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

use super::doc_ordinal::DocOrdinals;
use super::sink::OpSink;

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
        // Each base row at its current version. The predicate returns a
        // `bool`, so a spilled overlay tier that cannot be read is held here
        // and returned once the scan ends.
        let database_id = task.request.database_id.as_u64();
        let overlay_err: std::cell::Cell<Option<crate::Error>> = std::cell::Cell::new(None);
        let rows = self.current_rows_where(
            database_id,
            tid,
            collection,
            &|key: &StorageKey, _: &[u8]| match overlay.get(coll_key, key.surrogate().as_u32()) {
                Ok(staged) => staged.is_none(),
                Err(e) => {
                    overlay_err.set(Some(e));
                    false
                }
            },
        )?;
        match overlay_err.take() {
            Some(e) => Err(e),
            None => Ok(rows),
        }
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

/// A document collection a staged TRUNCATE wipes, with what names each
/// removed base row in its redo entry.
pub(super) struct TruncatedCollection<'a> {
    pub overlay: &'a TxnOverlay,
    pub coll_key: &'a (DatabaseId, TenantId, String),
    pub strict_schema: Option<&'a StrictSchema>,
    /// The collection's declared `PRIMARY KEY` column, from the plan.
    pub declared_primary_key: Option<&'a str>,
    /// The stamp of each removed row's tombstone.
    pub ordinals: &'a DocOrdinals<'a>,
}

impl CoreLoop {
    /// Append one `RecordType::Delete` redo sub-record per base row a staged
    /// TRUNCATE removes: every base row with no overlay entry, in storage-key
    /// order. Every replica removes the same rows from the record, and a
    /// WAL-only restart does not replay a removed row's original `Put` and
    /// resurrect it or its HNSW vector. The scan streams the base rows and
    /// holds none it has passed. Read-only against base.
    pub(super) fn serialize_truncated_base_rows(
        &self,
        truncated: TruncatedCollection<'_>,
        ops: &mut dyn OpSink,
    ) -> crate::Result<()> {
        let TruncatedCollection {
            overlay,
            coll_key,
            strict_schema,
            declared_primary_key,
            ordinals,
        } = truncated;
        let collection = coll_key.2.as_str();
        self.sparse.scan_documents_for_each(
            coll_key.0.as_u64(),
            coll_key.1.as_u64(),
            collection,
            usize::MAX,
            |key, body| {
                let surrogate = key.surrogate().as_u32();
                if overlay.get(coll_key, surrogate)?.is_some() {
                    return Ok(());
                }
                let doc_id = stored_row_identity(body, strict_schema, declared_primary_key, *key);
                let sys_from = ordinals.for_row(key)?;
                ops.push(delete_sub_record(collection, &doc_id, surrogate, sys_from)?);
                Ok(())
            },
        )
    }
}

/// The identity and surrogate of every base row in `rows`, in
/// deterministic doc-id order: the rows a staged TRUNCATE removes. The
/// Calvin stage judges each at its commit boundary.
pub(super) fn truncated_row_entries(
    rows: &[(StorageKey, Vec<u8>)],
    strict_schema: Option<&StrictSchema>,
    declared_primary_key: Option<&str>,
) -> BTreeMap<RowIdentity, u32> {
    rows.iter()
        .map(|(key, body)| {
            (
                stored_row_identity(body, strict_schema, declared_primary_key, *key),
                key.surrogate().as_u32(),
            )
        })
        .collect()
}
