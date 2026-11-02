// SPDX-License-Identifier: BUSL-1.1

//! Resolve canonical ingest lines to the exact rows they store.
//!
//! The lines are ingested into a scratch memtable that carries the
//! collection's current schema, evolved for the lines (`returning_preview`).
//! Each line the scratch memtable accepts becomes one resolved row: its
//! values in the scratch schema's column order, and, when
//! some Event Plane consumer reads the collection or the statement projects
//! rows, its scan image. The live memtable is not touched.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest::{self, RowStamp};
use crate::engine::timeseries::resolved_ingest::{ResolvedTsBatch, ResolvedTsRow, TsDriftPolicy};
use crate::types::TenantId;
use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL};

use super::raw_scan::emit_memtable_rows_at;

/// One ingest to resolve.
pub(in crate::data::executor) struct TsResolveInput<'a> {
    pub tid: TenantId,
    pub collection: &'a str,
    /// Canonical line protocol, every row's timestamp stamped.
    pub lines: &'a [String],
    /// The statement instant: the timestamp of an untimed row.
    pub now_ms: i64,
    pub drift: TsDriftPolicy,
    /// Whether the statement projects its stored rows, so every row needs
    /// its image whether or not a consumer reads the collection.
    pub needs_images: bool,
    /// The schema to resolve against instead of the live memtable's: the
    /// schema an earlier ingest of the same transaction resolved to.
    pub base: Option<&'a ColumnarSchema>,
}

impl CoreLoop {
    /// Resolve `input.lines` to the rows they store in `input.collection`.
    pub(in crate::data::executor) fn resolve_ts_batch(
        &self,
        task: &ExecutionTask,
        input: TsResolveInput<'_>,
    ) -> crate::Result<ResolvedTsBatch> {
        let TsResolveInput {
            tid,
            collection,
            lines,
            now_ms,
            drift,
            needs_images,
            base,
        } = input;
        let joined = lines.join("\n");
        let parsed = ilp::parse_batch(&joined)
            .map_err(|error| crate::Error::Internal {
                detail: format!(
                    "timeseries resolve: resolved lines of '{collection}' do not parse: {error}"
                ),
            })?
            .into_lines();
        let Some(measurement) = parsed.first().map(|line| line.measurement.to_string()) else {
            return Err(crate::Error::Internal {
                detail: format!("timeseries resolve: '{collection}' resolved to no lines"),
            });
        };
        let (outcome, scratch) =
            self.scratch_ilp_ingest(task, tid, collection, &parsed, now_ms, base);
        if outcome.rejected > 0 {
            tracing::warn!(
                collection,
                accepted = outcome.accepted,
                rejected = outcome.rejected,
                first_rejection = outcome.first_rejection.as_deref().unwrap_or(""),
                "timeseries ingest lines rejected as invalid rows"
            );
        }
        let schema = scratch.schema().clone();
        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        // The resolved rows carry the last ordinal of the statement instant
        // in `_ts_system`. The apply restamps every row with the batch's
        // ordinal (`ts_batch_ordinal`).
        let stamp = RowStamp {
            system_ordinal: nodedb_types::ms_to_ordinal_upper(now_ms),
            bitemporal,
        };
        let emits_events = self
            .events
            .interest
            .consumes(task.request.database_id, collection);
        let images = if emits_events || needs_images {
            emit_memtable_rows_at(&scratch, &outcome.accepted_row_indices)?
        } else {
            Vec::new()
        };

        let mut rows = Vec::with_capacity(outcome.accepted_line_indices.len());
        for (position, line_index) in outcome.accepted_line_indices.iter().enumerate() {
            let Some(line) = parsed.get(*line_index) else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "timeseries resolve: '{collection}' accepted line {line_index} of {}",
                        parsed.len()
                    ),
                });
            };
            let (timestamp_ms, values) = ilp_ingest::line_row(&schema, line, now_ms, Some(stamp));
            let image = match images.get(position) {
                Some(row) => {
                    let mut bytes = Vec::new();
                    rmpv::encode::write_value(&mut bytes, row).map_err(|error| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("timeseries row image of '{collection}': {error}"),
                        }
                    })?;
                    bytes
                }
                None => Vec::new(),
            };
            rows.push(ResolvedTsRow {
                line: *line_index as u64,
                timestamp_ms,
                values,
                absent: unnamed_columns(&schema, line, bitemporal),
                image,
            });
        }
        Ok(ResolvedTsBatch {
            measurement,
            columns: schema.columns.clone(),
            timestamp_idx: schema.timestamp_idx as u64,
            drift,
            now_ms,
            resolved_bytes: scratch.memory_bytes() as u64,
            emits_events,
            rows,
            rejected: outcome.rejected as u64,
            first_rejection: outcome.first_rejection,
            carried_system: false,
        })
    }
}

/// Positions in `schema` that `line` does not name. The time column, the
/// system-time and series-id columns and, in a bitemporal collection, the
/// valid-time columns always hold a value. Every other column holds one only when a tag
/// or field names it.
fn unnamed_columns(schema: &ColumnarSchema, line: &ilp::IlpLine<'_>, bitemporal: bool) -> Vec<u32> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(index, (name, _))| {
            let stamped = *index == schema.timestamp_idx
                || name == TS_SYSTEM
                || name == TS_SERIES
                || (bitemporal && matches!(name.as_str(), TS_VALID_FROM | TS_VALID_UNTIL));
            let named = line.tags.iter().any(|(key, _)| key.as_ref() == name)
                || line.fields.iter().any(|(key, _)| key.as_ref() == name);
            !stamped && !named
        })
        .filter_map(|(index, _)| u32::try_from(index).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{ColumnType, TimeKind};

    #[test]
    fn a_column_the_line_does_not_name_is_absent() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        };
        let parsed = ilp::parse_batch("cpu,host=a value=1.5 1000000000")
            .expect("parse line")
            .into_lines();
        let line = parsed.first().expect("one line");
        assert_eq!(unnamed_columns(&schema, line, false), vec![3]);
    }
}
