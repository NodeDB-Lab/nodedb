// SPDX-License-Identifier: BUSL-1.1

//! Storing the dead-letter entry a rejected CRDT delta leaves, and the
//! refusal the writer receives.

use nodedb_types::DatabaseId;
use nodedb_types::sync::violation::ViolationType;

use crate::bridge::envelope::ErrorCode;
use crate::types::{Lsn, TenantId};

use super::CoreLoop;

impl CoreLoop {
    /// Bind the entry the latest validated apply on `(database_id,
    /// tenant_id)` enqueued to the record at `source_lsn`, and store it.
    ///
    /// Call it after every validated apply that returned `Rejected`. A
    /// record already bound keeps its one entry. A write with no record has
    /// nothing to replay, so its entry stays in memory only.
    ///
    /// A store error is returned, and the entry is removed from the queue:
    /// nothing on this node records the rejection. The caller fails or holds
    /// the write that needed the entry.
    pub(in crate::data::executor) fn store_crdt_dead_letter(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        source_lsn: Option<Lsn>,
    ) -> crate::Result<()> {
        let Some(source_lsn) = source_lsn.filter(|lsn| *lsn != Lsn::ZERO) else {
            return Ok(());
        };
        let Some(engine) = self.crdt_engines.get_mut(&(database_id, tenant_id)) else {
            return Ok(());
        };
        let Some(entry) = engine.bind_dead_letter_source(source_lsn.as_u64()) else {
            return Ok(());
        };
        let stored = self.sparse.put_crdt_dead_letter(
            database_id.as_u64(),
            tenant_id.as_u64(),
            source_lsn.as_u64(),
            &entry,
        );
        if let Err(error) = &stored {
            crate::diag::crdt_dead_letter_not_stored(
                error,
                database_id.as_u64(),
                tenant_id.as_u64(),
                &entry,
                source_lsn.as_u64(),
            );
            engine.discard_dead_letter(entry.id);
        }
        stored
    }
}

impl CoreLoop {
    /// Store the entry the rejection of the replayed record at `lsn`
    /// produced. Returns whether replay may count the record as replayed.
    ///
    /// A store error stops restart replay at the record: the core
    /// fail-stops, no checkpoint covers the record, and boot refuses to start
    /// with a rejection nothing records. A committed-redo apply fails instead.
    pub(in crate::data::executor) fn store_replayed_dead_letter(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        lsn: u64,
    ) -> bool {
        match self.store_crdt_dead_letter(database_id, tenant_id, Some(Lsn::new(lsn))) {
            Ok(()) => true,
            Err(error) => {
                self.replay_record_unapplied(
                    "crdt",
                    "dead_letter_store",
                    lsn,
                    &format!("the rejected delta's dead-letter entry could not be stored: {error}"),
                );
                false
            }
        }
    }
}

/// The refusal for a CRDT delta rejected by constraint `violation`.
///
/// Nothing applied, so the write's record is cancelled. The rejected delta
/// stays in the dead-letter queue.
pub(in crate::data::executor) fn crdt_rejection(
    collection: &str,
    target: &str,
    violation: &ViolationType,
) -> ErrorCode {
    let text = violation.to_string();
    let constraint = text
        .split_once(':')
        .map_or(text.as_str(), |(kind, _)| kind)
        .to_string();
    ErrorCode::RejectedConstraint {
        constraint,
        detail: format!(
            "delta for {collection}/{target} violates {violation}; nothing was applied, \
             and the delta is in the dead-letter queue"
        ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::{ErrorCode as PublicCode, sqlstate};

    use super::*;
    use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
    use crate::error_from_data_plane::data_plane_code_to_public;

    /// Every violation the validator emits reaches pgwire with its own
    /// constraint or permission SQLSTATE, and the native client with a
    /// classified code. None degrades to an internal error.
    #[test]
    fn each_validator_violation_keeps_its_client_class() {
        let cases = [
            (
                ViolationType::UniqueViolation {
                    field: "email".into(),
                    value: "x@y.com".into(),
                },
                sqlstate::UNIQUE_VIOLATION,
                PublicCode::CONSTRAINT_VIOLATION,
            ),
            (
                ViolationType::ForeignKeyMissing {
                    referenced_id: "org:1".into(),
                },
                sqlstate::FOREIGN_KEY_VIOLATION,
                PublicCode::CONSTRAINT_VIOLATION,
            ),
            (
                ViolationType::NotNullViolation {
                    field: "email".into(),
                },
                sqlstate::NOT_NULL_VIOLATION,
                PublicCode::CONSTRAINT_VIOLATION,
            ),
            (
                ViolationType::SchemaViolation {
                    field: "email".into(),
                    reason: "expected a string".into(),
                },
                sqlstate::INTEGRITY_CONSTRAINT_VIOLATION,
                PublicCode::CONSTRAINT_VIOLATION,
            ),
            (
                ViolationType::ConstraintViolation {
                    detail: "check: amount > 0".into(),
                },
                sqlstate::INTEGRITY_CONSTRAINT_VIOLATION,
                PublicCode::CONSTRAINT_VIOLATION,
            ),
            (
                ViolationType::RlsPolicyViolation {
                    policy_name: "own_rows".into(),
                },
                sqlstate::INSUFFICIENT_PRIVILEGE,
                PublicCode::AUTHORIZATION_DENIED,
            ),
        ];
        for (violation, expected_sqlstate, expected_public) in cases {
            let code = crdt_rejection("users", "b", &violation);
            let (_, state, _) = error_code_to_sqlstate(&code);
            assert_eq!(state, expected_sqlstate, "sqlstate for {violation}");
            assert_eq!(
                data_plane_code_to_public(code).code(),
                expected_public,
                "public code for {violation}"
            );
        }
    }
}
