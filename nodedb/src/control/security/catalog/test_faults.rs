// SPDX-License-Identifier: BUSL-1.1

//! Test-only fault switches of the system catalog: each makes the next write
//! of one kind raise, so a caller's error path runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::system_catalog::SystemCatalog;

/// The fault switches and read counters a test arms on a catalog. Clones of
/// one catalog share them.
#[derive(Clone, Default)]
pub(super) struct CatalogTestFaults {
    pub(super) fail_next_calvin_barrier_persist: Arc<AtomicBool>,
    pub(super) fail_next_calvin_barrier_replace: Arc<AtomicBool>,
    pub(super) fail_next_user_counter_write: Arc<AtomicBool>,
    pub(super) fail_next_function_wasm_write: Arc<AtomicBool>,
    pub(super) fail_next_collection_write: Arc<AtomicBool>,
    pub(super) fail_next_surrogate_write: Arc<AtomicBool>,
    pub(super) fail_next_redo_snapshot_owed_write: Arc<AtomicBool>,
    /// Audit rows the DML position read decoded, so a test can tell a range
    /// read from a full scan.
    pub(super) dml_audit_rows_read: Arc<AtomicU64>,
}

impl SystemCatalog {
    pub(crate) fn fail_next_user_counter_write_for_test(&self) {
        self.faults
            .fail_next_user_counter_write
            .store(true, Ordering::SeqCst);
    }

    pub(crate) fn fail_next_function_wasm_write_for_test(&self) {
        self.faults
            .fail_next_function_wasm_write
            .store(true, Ordering::SeqCst);
    }

    /// Make the next `put_collection` raise, so an applier's error path runs.
    pub(crate) fn fail_next_collection_write_for_test(&self) {
        self.faults
            .fail_next_collection_write
            .store(true, Ordering::SeqCst);
    }

    /// Make the next surrogate watermark write raise, so an applier's error
    /// path runs.
    pub(crate) fn fail_next_surrogate_write_for_test(&self) {
        self.faults
            .fail_next_surrogate_write
            .store(true, Ordering::SeqCst);
    }

    /// Make the next `save_redo_snapshot_owed` raise, so a held group's debt
    /// write fails once.
    pub(crate) fn fail_next_redo_snapshot_owed_write_for_test(&self) {
        self.faults
            .fail_next_redo_snapshot_owed_write
            .store(true, Ordering::SeqCst);
    }

    /// Make the next `persist_calvin_barrier_logs` raise. The pending rows
    /// stay pending.
    pub(crate) fn fail_next_calvin_barrier_persist_for_test(&self) {
        self.faults
            .fail_next_calvin_barrier_persist
            .store(true, Ordering::SeqCst);
    }

    /// Make the next `replace_calvin_barrier_logs` raise before it commits.
    pub(crate) fn fail_next_calvin_barrier_replace_for_test(&self) {
        self.faults
            .fail_next_calvin_barrier_replace
            .store(true, Ordering::SeqCst);
    }
}
