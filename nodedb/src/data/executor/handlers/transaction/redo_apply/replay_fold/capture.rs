// SPDX-License-Identifier: BUSL-1.1

//! The write set restart replay stores for a record it folded.
//!
//! It is the write set the record's install would have stored: the fold
//! target rows, with the group the record opened. Boot journals it as the
//! group's parts exactly as it journals a live install's stored write set.
//! The record's WAL copy already holds its origin, so the stored plan never
//! journals the origin again. It names the record as its WAL copy holds it.

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RedoOrigin, RedoSumTargets};

use crate::data::executor::enforcement::write_hook::target_write_set;
use crate::wal::{CapturedEntry, OriginAppend, WriteSetCapture};

use super::state::OpenFold;

/// The write set of the record `open` folded. `targets` are its sum targets.
pub(super) fn fold_capture(
    open: &OpenFold,
    targets: Vec<RedoSumTargets>,
) -> crate::Result<WriteSetCapture> {
    let collections: Vec<String> = targets
        .iter()
        .map(|targets| targets.collection.clone())
        .collect();
    let collection = collections.first().cloned().unwrap_or_default();
    let plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: open.payload.clone(),
        collections,
        sum_targets: targets,
        origin: RedoOrigin::Commit,
        calvin: None,
        reads: Vec::new(),
        snapshot_ordinal: None,
    });
    let plan = zerompk::to_msgpack_vec(&plan).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("replay fold write set plan encode: {e}"),
    })?;
    let mut schemas = Vec::new();
    let entries = target_write_set(&open.target_writes)
        .iter()
        .map(|entry| CapturedEntry::from_entry(entry, &mut schemas))
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(WriteSetCapture {
        tenant_id: open.tenant_id,
        vshard_id: open.vshard_id,
        database_id: open.database_id,
        collection,
        origin: open.lsn,
        entries,
        schemas,
        origin_append: OriginAppend {
            plan,
            apply_key: open.apply_key,
            event_source: open.event_source,
            commit_hlc: Some(open.commit_hlc),
            resolved_now_ms: None,
            change_position: None,
        },
    })
}
