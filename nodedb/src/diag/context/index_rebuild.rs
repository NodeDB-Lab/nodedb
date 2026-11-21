// SPDX-License-Identifier: BUSL-1.1

//! Forensic payload for a REINDEX of a full-text or CSR index that did not
//! install.
//!
//! REINDEX CONCURRENTLY acknowledges once the rebuild starts. A rebuild the
//! core refuses at cutover, or one whose build fails, leaves the live index
//! in place with every write it took. Nothing but this report and a log
//! line shows that the requested rebuild never happened.
//!
//! A document index step a committed descriptor owes reports here too when
//! its proposal fails after the commit.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// One rebuild of one index that did not install.
pub(in crate::diag) struct IndexRebuildNotInstalled<'a> {
    /// `fts` or `csr`.
    pub index: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// What stopped the install, without the per-occurrence detail.
    pub cause_class: &'a str,
}

impl DomainContext for IndexRebuildNotInstalled<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.index_rebuild_not_installed"
    }

    fn grouping_key(&self) -> String {
        // The index kind and the cause name the bug. The collection is the
        // occurrence, so repeated REINDEX runs that hit one cause file one
        // report.
        format!("index={};cause={}", self.index, self.cause_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "index": self.index,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "cause_class": self.cause_class,
            "why_reported": "REINDEX acknowledged this rebuild when it started. The \
                             rebuilt index was discarded, so the live index stays as it \
                             was, with every write it took. Results stay correct; the \
                             compaction the rebuild was meant to install never happened",
            "operator_action": "read the cause: a journal overflow means more writes landed \
                                during the rebuild than its journal holds, so run REINDEX \
                                again at a quieter time. A purge or supersede means the \
                                collection was dropped or rebuilt again, and needs no action. \
                                Any other cause is a storage or snapshot error: check the \
                                core's log for the same collection",
        })
    }
}

/// A document index build or retire that a committed collection descriptor
/// owes, and that did not reach the collection's data log.
pub(in crate::diag) struct DocIndexStepDeferred<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for DocIndexStepDeferred<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.doc_index_step_deferred"
    }

    fn grouping_key(&self) -> String {
        // The error class names the cause. The collection is the
        // occurrence, so one unreachable group leader files one report.
        format!("cause={}", self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "error_class": self.error_class,
            "why_reported": "the DDL committed a collection descriptor that owes document \
                             index builds or retires, and the proposal on the collection's \
                             data Raft log failed. Until a step applies, an index read \
                             through its path is refused as retryable on every replica that \
                             lacks it",
            "operator_action": "check that the collection's data group has a reachable \
                                leader. The reconcile loop on the metadata leader delivers \
                                every owed step. A report that keeps counting means that \
                                delivery fails too",
        })
    }
}

/// A full-text index build or drop the boot reconcile could not finish.
pub(in crate::diag) struct TextIndexStepOwed<'a> {
    /// `build` or `drop`.
    pub step: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub index: &'a str,
    /// The SQLSTATE the step failed with.
    pub sqlstate: &'a str,
}

impl DomainContext for TextIndexStepOwed<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.text_index_step_owed"
    }

    fn grouping_key(&self) -> String {
        // The step and the SQLSTATE name the cause. The index is the
        // occurrence, so every boot that hits one cause files one report.
        format!("step={};sqlstate={}", self.step, self.sqlstate)
    }

    fn to_json(&self) -> Value {
        json!({
            "step": self.step,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "index": self.index,
            "sqlstate": self.sqlstate,
            "why_reported": "a crash or an error interrupted this full-text index step, and \
                             the boot reconcile could not finish it. A `build` leaves the \
                             index missing rows stored before it. A `drop` leaves the record \
                             listed in the `Dropping` state, and no write indexes its fields",
            "operator_action": "read the node log for the same index. A peer that was down \
                                at boot is the usual cause: the next boot finishes the step. \
                                Run DROP INDEX on the index to finish a drop, or to remove a \
                                build that keeps failing",
        })
    }
}
