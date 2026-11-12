// SPDX-License-Identifier: BUSL-1.1

//! The lines a COMMIT-time resolve rejected, read from its resolved
//! timeseries sub-records.

use nodedb_wal::record::RecordType;

use super::resolved_ingest::{RESOLVED_INGEST_FORMAT, ResolvedTsBatch};
use crate::wal::RedoSubRecord;

/// The collection and the line count the resolve behind `op` rejected, when
/// `op` is a `ts-resolved` sub-record that rejected a line. A timeseries
/// sub-record of another shape or format counts no line.
pub fn resolved_rejection(op: &RedoSubRecord) -> Option<(String, u64)> {
    if op.record_type != RecordType::TimeseriesBatch as u32 {
        return None;
    }
    let record = crate::wal::decode_batch_record(&op.payload).ok()?;
    if record.format.as_deref() != Some(RESOLVED_INGEST_FORMAT) {
        return None;
    }
    let batch = ResolvedTsBatch::from_bytes(&record.payload).ok()?;
    (batch.rejected > 0).then_some((record.collection, batch.rejected))
}
