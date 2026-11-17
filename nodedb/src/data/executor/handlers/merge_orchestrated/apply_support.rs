// SPDX-License-Identifier: BUSL-1.1

//! Row-level helpers for the MERGE APPLY pass: undo capture for the in-memory
//! indexes, actual stored-row policy admission, prepared events, and RETURNING rows.

use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::document::store::{RowIdentity, StorageKey};

/// A public row event prepared while the MERGE can still roll back.
pub(super) enum MergeEvent {
    Put {
        collection: String,
        identity: RowIdentity,
        event: crate::types::DocumentPutEvent,
    },
    Delete {
        collection: String,
        identity: RowIdentity,
        old_value: Vec<u8>,
    },
}

impl MergeEvent {
    pub(super) fn publish(
        self,
        core: &mut crate::data::executor::core_loop::CoreLoop,
        task: &crate::data::executor::task::ExecutionTask,
    ) {
        match self {
            Self::Put {
                collection,
                identity,
                event,
            } => core.emit_prepared_put_event(task, &collection, identity, &event),
            Self::Delete {
                collection,
                identity,
                old_value,
            } => core.emit_event_with_row_id(
                task,
                &collection,
                crate::event::WriteOp::Delete,
                crate::event::types::RowId::row(identity),
                None,
                Some(&old_value),
            ),
        }
    }
}

/// Retain target effects in the order the source hook applied them.
pub(super) fn retain_target_writes(
    targets: Vec<crate::data::executor::enforcement::materialized_sum::apply::TargetWrite>,
    undo_log: &mut Vec<UndoEntry>,
    applied_keys: &mut Vec<(String, StorageKey)>,
    events: &mut Vec<MergeEvent>,
    write_set: &mut Vec<crate::bridge::envelope::WriteSetEntry>,
) {
    for mut target in targets {
        applied_keys.push((
            target.collection.clone(),
            StorageKey::for_surrogate(target.surrogate),
        ));
        record_put_index_undo(undo_log, &mut target.outcome);
        write_set.push(
            crate::bridge::envelope::WriteSetEntry::installed_put(
                target.surrogate.as_u32(),
                target.identity.clone(),
                target.outcome.installed,
                target.event.clone(),
            )
            .in_collection(target.collection.clone()),
        );
        events.push(MergeEvent::Put {
            collection: target.collection,
            identity: target.identity,
            event: target.event,
        });
    }
}

/// Record the in-memory index mutations a successful
/// [`crate::data::executor::core_loop::CoreLoop::apply_point_put`] performed as
/// undo entries. The vector, sparse and spatial indexes live OUTSIDE the
/// shared redb transaction, so dropping that transaction on abort does not
/// reverse them. Drains the outcome's undo
/// entries and leaves `prior_value` for the caller's event emission.
pub(super) fn record_put_index_undo(undo_log: &mut Vec<UndoEntry>, outcome: &mut PointPutOutcome) {
    undo_log.append(&mut outcome.memory_undo);
}

/// Judge the actual row image using its known storage schema.
pub(super) fn gate_merge_row(
    core: &crate::data::executor::core_loop::CoreLoop,
    row: &crate::data::executor::core_loop::redo_image::WriteSetRow<'_>,
    check: &nodedb_types::RlsWriteCheck,
    stored: &[u8],
    schema: Option<&nodedb_types::columnar::StrictSchema>,
) -> crate::Result<()> {
    let identity_column = core.identity_column(row.database_id, row.tid, row.collection);
    rls_write_gate::admit_stored_row(
        check,
        rls_write_gate::GatedRow {
            body: stored,
            identity: &row.identity,
            strict_schema: schema,
            identity_column: &identity_column,
        },
        row.tid,
        row.collection,
        &core.crs_context(),
    )
}

/// Decode one merge row body into the `Value` document a RETURNING
/// projection reads. Same shape the point and bulk DML RETURNING paths
/// emit, so a MERGE row projects identically.
///
/// `key` is the row's storage key: every caller's `MergeUpdate::key`,
/// `MergeDelete::key`, or a freshly minted insert key. This function converts
/// it to the client-visible identity before decoding.
///
/// Put callers pass their installed canonical document, already decoded from
/// any strict tuple. The helper retains native values and projects the row identity.
pub(super) fn returning_doc(
    body: &[u8],
    key: &StorageKey,
    identity_column: &str,
) -> crate::Result<nodedb_types::Value> {
    let identity = key.to_identity();
    super::super::returning_doc::from_stored(body, &identity, None, identity_column)
}
