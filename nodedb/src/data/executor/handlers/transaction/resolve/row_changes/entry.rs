// SPDX-License-Identifier: BUSL-1.1

//! Collect the net change of every row a committing transaction writes.
//!
//! The overlay holds each staged row's final state: a value or a tombstone.
//! Whether the row existed before the commit is the committed state resolve
//! reads here. A staged TRUNCATE hides every committed row, so a row the
//! transaction writes after its TRUNCATE did not exist before it. The two
//! facts give the net kind (see [`RedoRowKind::net`]).
//!
//! Each engine names its rows as its autocommit change events name them:
//! - a document, a CRDT document or a KV row by its identity, one entry per
//!   row ([`super::keyed`]);
//! - a vector-primary row by its surrogate's identity, one entry per row
//!   ([`super::keyed`]);
//! - a columnar or spatial collection, an array or a timeseries collection
//!   as a whole, one entry per kind of net change it holds
//!   ([`super::grouped`]).
//!
//! Entries follow engine order, then collection order, then row order, so
//! every replica that resolves the same transaction records the same
//! entries.
//!
//! [`RedoRowKind::net`]: crate::wal::RedoRowKind::net

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::PhysicalPlan;

use super::super::columnar_image::ColumnarCollections;
use super::super::vector_primary::VectorPrimaryCollections;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TxnId};
use crate::wal::RedoRowChange;

/// The collections a transaction staged rows in, and its plans.
pub(in crate::data::executor::handlers::transaction::resolve) struct StagedRowCollections<'a> {
    pub documents: &'a BTreeSet<String>,
    pub kv: &'a BTreeSet<String>,
    pub columnar: &'a ColumnarCollections,
    pub vector_primary: &'a VectorPrimaryCollections,
    /// The transaction's plans: CRDT, array and timeseries writes are named
    /// by their plan nodes.
    pub plans: &'a [PhysicalPlan],
}

/// Where the transaction being resolved lives.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct ResolveScope {
    pub database_id: DatabaseId,
    pub tid: u64,
    pub txn_id: TxnId,
}

impl CoreLoop {
    /// The net change of every row `txn_id` wrote.
    pub(in crate::data::executor::handlers::transaction::resolve) fn resolve_row_changes(
        &self,
        database_id: DatabaseId,
        tid: u64,
        txn_id: TxnId,
        collections: &StagedRowCollections<'_>,
    ) -> crate::Result<Vec<RedoRowChange>> {
        let scope = ResolveScope {
            database_id,
            tid,
            txn_id,
        };
        let mut changes = Vec::new();
        self.document_row_changes(scope, collections.documents, &mut changes)?;
        self.crdt_row_changes(scope, collections.plans, &mut changes)?;
        self.kv_row_changes(scope, collections.kv, &mut changes)?;
        self.vector_primary_row_changes(scope, collections.vector_primary, &mut changes)?;
        self.columnar_row_changes(scope, collections.columnar, &mut changes)?;
        self.array_row_changes(scope, collections.plans, &mut changes)?;
        super::grouped::timeseries_row_changes(collections.plans, &mut changes);
        Ok(changes)
    }
}
