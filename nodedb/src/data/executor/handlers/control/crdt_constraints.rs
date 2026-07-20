// SPDX-License-Identifier: BUSL-1.1

//! CRDT constraint-install handlers.
//!
//! A committed `ConstraintChange` on the per-vshard data Raft log decodes to a
//! `SetConstraints` / `DropConstraints` op and lands here so every replica
//! installs the same constraint set into its per-core (`!Send`) CRDT validator,
//! keyed by collection. The installed set is in-memory: it is rebuilt on
//! restart from Raft-log replay of these entries.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Install a collection's constraint set into the tenant CRDT validator.
    ///
    /// Each blob is a zerompk-encoded `nodedb_crdt::Constraint`. Decode is
    /// loud: a malformed blob fails the whole op rather than silently dropping
    /// a constraint, which would weaken the invariant set on this replica.
    pub(in crate::data::executor) fn execute_crdt_set_constraints(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        constraint_version: u64,
        constraints: &[Vec<u8>],
    ) -> Response {
        debug!(core = self.core_id, %collection, constraint_version, count = constraints.len(), "crdt set constraints");
        let mut decoded = Vec::with_capacity(constraints.len());
        for blob in constraints {
            match zerompk::from_msgpack::<nodedb_crdt::Constraint>(blob) {
                Ok(c) => decoded.push(c),
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "crdt constraint decode failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("constraint decode failed: {e}"),
                        },
                    );
                }
            }
        }
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        // A `false` return means the incoming version is older than the one
        // already installed: a stale duplicate was correctly ignored by the
        // fence, which is success, not an error.
        if engine.set_collection_constraints(collection, constraint_version, decoded) {
            self.checkpoint_coordinator.mark_dirty("crdt", 1);
        } else {
            debug!(core = self.core_id, %collection, constraint_version, "stale constraint version ignored");
        }
        self.response_ok(task)
    }

    /// Remove every constraint scoped to `collection` from the tenant CRDT
    /// validator.
    pub(in crate::data::executor) fn execute_crdt_drop_constraints(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        constraint_version: u64,
    ) -> Response {
        debug!(core = self.core_id, %collection, constraint_version, "crdt drop constraints");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        // `false` means a stale (older-version) drop was correctly ignored by
        // the fence — success, not an error.
        if engine.drop_collection_constraints(collection, constraint_version) {
            self.checkpoint_coordinator.mark_dirty("crdt", 1);
        } else {
            debug!(core = self.core_id, %collection, constraint_version, "stale constraint version ignored");
        }
        self.response_ok(task)
    }

    /// Read the constraint set installed in this replica's CRDT validator for
    /// `collection`. Read-only — no `mark_dirty`. The installed
    /// `Vec<nodedb_crdt::Constraint>` is zerompk-encoded into the response
    /// payload so a caller can inspect exactly what this node's validator holds
    /// (catalog replication does not prove the validator installed — the
    /// validator itself must be read).
    pub(in crate::data::executor) fn execute_crdt_read_constraints(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "crdt read constraints");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let constraints = engine.constraints_for_collection(collection);
        match zerompk::to_msgpack_vec(&constraints) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("constraint encode failed: {e}"),
                },
            ),
        }
    }
}
