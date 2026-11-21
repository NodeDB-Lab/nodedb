// SPDX-License-Identifier: BUSL-1.1

//! The data-log steps a committed catalog change owes its collections.
//!
//! A DDL that changes a collection descriptor can owe steps on the
//! collection's data Raft log: the CRDT constraint install, and the document
//! index builds and retires. Each proposer computes them from the stamped
//! entries before they apply, and runs them once the entries applied here.

use crate::control::catalog_entry::CatalogEntry;
use crate::control::constraint_install::{
    ConstraintTarget, constraint_targets, install_constraints,
};
use crate::control::doc_index_install::{DocIndexTarget, doc_index_targets, install_doc_indexes};
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;

/// Every data-log step a set of stamped entries owes.
#[derive(Debug, Clone, Default)]
pub(crate) struct DataLogTargets {
    constraints: Vec<ConstraintTarget>,
    doc_indexes: Vec<DocIndexTarget>,
}

impl DataLogTargets {
    /// The steps stamped, not yet applied `entries` owe. Call it after the
    /// stamp and before the entries apply, so the committed rows are the
    /// prior ones.
    pub(crate) fn of(catalog: &SystemCatalog, entries: &[CatalogEntry]) -> crate::Result<Self> {
        Ok(Self {
            constraints: constraint_targets(catalog, entries)?,
            doc_indexes: doc_index_targets(catalog, entries)?,
        })
    }

    /// Run every step, once the entries applied on this node.
    ///
    /// The entries are committed, so no step fails the DDL. A failed step
    /// goes to the recorder and the log, and the reconcile loop delivers
    /// it.
    pub(crate) async fn install(&self, shared: &SharedState) {
        install_constraints(shared, &self.constraints).await;
        install_doc_indexes(shared, &self.doc_indexes).await;
    }
}
