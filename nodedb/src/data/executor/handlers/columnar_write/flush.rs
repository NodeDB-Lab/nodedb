// SPDX-License-Identifier: BUSL-1.1

//! Post-insert memtable flush: encodes the columnar memtable to a segment
//! once the flush threshold is reached, retains the segment bytes and their
//! surrogate sidecar in memory, then drains the memtable.
//!
//! The segment encodes from the memtable in place. The memtable drains only
//! after the segment bytes are retained. An encode error leaves every row in
//! the memtable, so the rows stay readable and the next flush retries them.

use nodedb_columnar::memtable::DICT_ENCODE_MAX_CARDINALITY;
use nodedb_columnar::{ColumnarError, MutationEngine, SegmentWriter};
use nodedb_types::Surrogate;
use nodedb_wal::crypto::WalEncryptionKey;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// What one memtable flush moved to a segment.
#[derive(Debug, PartialEq, Eq)]
pub(in crate::data::executor) struct FlushedMemtable {
    /// The segment id the memtable rows now live under.
    pub segment_id: u64,
    /// The number of rows the segment holds. `0` when the memtable was empty
    /// and no segment was written.
    pub row_count: usize,
}

/// Encode `engine`'s memtable to a segment, hand the segment bytes and the
/// index-aligned row surrogates to `retain`, then drain the memtable and
/// remap its rows to the new segment id.
///
/// An encode error returns before `retain` runs and before the drain, so the
/// memtable keeps every row. An empty memtable writes no segment.
pub(in crate::data::executor) fn flush_memtable_to_segment(
    engine: &mut MutationEngine,
    writer: &SegmentWriter,
    kek: Option<&WalEncryptionKey>,
    retain: impl FnOnce(Vec<u8>, Vec<Option<Surrogate>>),
) -> Result<FlushedMemtable, ColumnarError> {
    let segment_id = engine.next_segment_id();
    let row_count = engine.memtable().row_count();
    if row_count > 0 {
        // Dictionary encoding rewrites low-cardinality string columns in
        // place. The memtable reads a dictionary column like a plain one, so
        // the rows stay readable if the encode below fails.
        engine
            .memtable_mut()
            .try_dict_encode_columns(DICT_ENCODE_MAX_CARDINALITY);
        let memtable = engine.memtable();
        let bytes = writer.write_segment(memtable.schema(), memtable.columns(), row_count, kek)?;
        // The surrogates are index-aligned with the memtable rows the segment
        // holds. `on_memtable_flushed` below clears them.
        retain(bytes, engine.memtable_surrogates().to_vec());
        engine.memtable_mut().drain();
    }
    engine.on_memtable_flushed(segment_id)?;
    Ok(FlushedMemtable {
        segment_id,
        row_count,
    })
}

impl CoreLoop {
    /// Flush the columnar memtable at `engine_key` to a segment if the
    /// flush threshold has been reached. No-op otherwise.
    ///
    /// An encode error fails the write with the memtable rows intact. The
    /// write's rows are applied and logged, so the error is `Internal`, which
    /// the write funnel treats as a write that can have landed.
    pub(in crate::data::executor) fn flush_columnar_memtable_if_needed(
        &mut self,
        task: &ExecutionTask,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        collection: &str,
    ) -> Result<(), Response> {
        let Some(engine) = self.columnar_engines.get_mut(engine_key) else {
            return Err(self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar engine missing after insert loop".into(),
                },
            ));
        };
        if !engine.should_flush() {
            return Ok(());
        }

        let writer = SegmentWriter::new(
            nodedb_columnar::writer::PROFILE_PLAIN,
            nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                engine_key.0,
                engine_key.1,
                nodedb_mem::EngineId::Columnar,
            ),
        );
        let kek = self.segment_keks.columnar_segment_kek.as_ref();
        let flushed_segments = &mut self.columnar_flushed_segments;
        let flushed_surrogates = &mut self.columnar_flushed_surrogates;
        // Lockstep invariant: `retain` pushes to BOTH maps for the same key in
        // the same order, so the segment-bytes Vec and the surrogate sidecar
        // stay equal-length and index-aligned (outer index == segment index,
        // segment_id == index + 1). An encode error pushes to neither.
        let outcome = flush_memtable_to_segment(engine, &writer, kek, |bytes, surrogates| {
            flushed_segments
                .entry(engine_key.clone())
                .or_default()
                .push(bytes);
            flushed_surrogates
                .entry(engine_key.clone())
                .or_default()
                .push(surrogates);
        });

        match outcome {
            Ok(flushed) => {
                tracing::debug!(
                    core = self.core_id,
                    %collection,
                    new_segment_id = flushed.segment_id,
                    row_count = flushed.row_count,
                    "columnar memtable flushed and segment bytes retained in memory"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    core = self.core_id,
                    %collection,
                    error = %e,
                    "columnar memtable flush failed; the rows stay in the memtable"
                );
                Err(self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "columnar memtable flush of '{collection}' failed, \
                             the rows stay in the memtable: {e}"
                        ),
                    },
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_columnar::writer::PROFILE_PLAIN;
    use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::{DatabaseId, TenantId, Value};

    use super::*;

    /// A writer whose columnar budget is `per_engine` bytes.
    fn writer(per_engine: usize) -> SegmentWriter {
        let governor = Arc::new(
            MemoryGovernor::new(GovernorConfig {
                global_ceiling: per_engine * EngineId::ALL.len(),
                engine_limits: EngineLimits::uniform(per_engine),
            })
            .expect("test governor"),
        );
        SegmentWriter::new(
            PROFILE_PLAIN,
            ScopedMemory::new(
                governor,
                DatabaseId::DEFAULT,
                TenantId::new(0),
                EngineId::Columnar,
            ),
        )
    }

    /// An engine whose memtable holds rows `(1, "a")`, `(2, "b")`, `(3, "a")`.
    fn engine_with_rows() -> MutationEngine {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("tag", ColumnType::String),
        ])
        .expect("schema");
        let mut engine = MutationEngine::new("flush_test".to_string(), schema);
        for (id, tag) in [(1, "a"), (2, "b"), (3, "a")] {
            engine
                .insert(&[Value::Integer(id), Value::String(tag.to_string())], id)
                .expect("insert");
        }
        engine
    }

    fn memtable_rows(engine: &MutationEngine) -> Vec<Vec<Value>> {
        engine
            .scan_memtable_rows()
            .map(|row| row.expect("read memtable row"))
            .collect()
    }

    #[test]
    fn an_encode_error_keeps_every_memtable_row() {
        let mut engine = engine_with_rows();
        let before = memtable_rows(&engine);
        let segment_id = engine.next_segment_id();

        let mut retained = false;
        let err = flush_memtable_to_segment(&mut engine, &writer(1), None, |_, _| {
            retained = true;
        })
        .expect_err("a one-byte columnar budget refuses the segment encode");

        assert!(
            matches!(err, ColumnarError::BudgetExhausted(_)),
            "unexpected error: {err:?}"
        );
        assert!(!retained, "a failed encode retains no segment");
        assert_eq!(engine.memtable().row_count(), 3);
        assert_eq!(memtable_rows(&engine), before);
        assert_eq!(
            engine.next_segment_id(),
            segment_id,
            "a failed flush allocates no segment id"
        );
    }

    #[test]
    fn a_flush_retains_the_segment_then_drains() {
        let mut engine = engine_with_rows();
        let segment_id = engine.next_segment_id();

        let mut retained: Option<(Vec<u8>, usize)> = None;
        let flushed = flush_memtable_to_segment(
            &mut engine,
            &writer(usize::MAX / EngineId::COUNT),
            None,
            |bytes, surrogates| retained = Some((bytes, surrogates.len())),
        )
        .expect("flush");

        assert_eq!(
            flushed,
            FlushedMemtable {
                segment_id,
                row_count: 3,
            }
        );
        let (bytes, surrogate_count) = retained.expect("the segment is retained");
        assert!(!bytes.is_empty());
        assert_eq!(surrogate_count, 3);
        assert_eq!(engine.memtable().row_count(), 0);
    }
}
