// SPDX-License-Identifier: BUSL-1.1

//! The record one batch of a stream's ops runs as.
//!
//! The record holds only the batch's ops, with the content header's
//! version and user roles and the marker's Calvin stamp and cross-shard
//! key, at the marker's LSN. The streamed validate and the batched install
//! build every batch this way, so both run the arms over the same records.

use nodedb_wal::WalRecord;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::{CalvinStamp, CrossShardAppliedKey, RedoRecord, RedoSubRecord};

use super::super::super::passes::{final_refusal, redo_wal_record};
use super::super::super::stream_validate::StreamHeader;

/// The marker fields every batch record carries.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchStamps {
    pub calvin_stamp: Option<CalvinStamp>,
    pub cross_shard_applied: Option<CrossShardAppliedKey>,
}

impl BatchStamps {
    /// The stamps of the stream marker `marker`.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn of(
        marker: &RedoRecord,
    ) -> Self {
        Self {
            calvin_stamp: marker.calvin_stamp.clone(),
            cross_shard_applied: marker.cross_shard_applied.clone(),
        }
    }
}

/// The `TransactionRedo` WAL record of the batch `ops`, at `lsn` on the
/// vShard `task` names. A record that does not encode, or that the WAL
/// refuses, refuses alike on every replica, so the refusal is final.
pub(in crate::data::executor::handlers::transaction::redo_apply) fn batch_wal_record(
    task: &ExecutionTask,
    lsn: Lsn,
    header: &StreamHeader,
    stamps: &BatchStamps,
    ops: Vec<RedoSubRecord>,
) -> Result<WalRecord, ErrorCode> {
    let redo = RedoRecord {
        version: header.version,
        ops,
        calvin_stamp: stamps.calvin_stamp.clone(),
        cross_shard_applied: stamps.cross_shard_applied.clone(),
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
        user_roles: header.user_roles.clone(),
        sum_targets: Vec::new(),
        pins: Default::default(),
    };
    let payload = redo
        .to_bytes()
        .map_err(|error| final_refusal(error.into()))?;
    redo_wal_record(task, lsn, payload)
}
