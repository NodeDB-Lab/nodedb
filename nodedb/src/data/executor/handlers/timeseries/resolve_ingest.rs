// SPDX-License-Identifier: BUSL-1.1

//! Read-only resolve pass for a `TimeseriesOp::Ingest`. It normalizes the
//! payload into line protocol, stamps timestamps, decides the write policy,
//! and resolves the lines to the exact rows they store
//! ([`crate::engine::timeseries::resolved_ingest`]). The writer logs those
//! rows and installs them as a `ts-resolved` ingest. Every autocommit ingest
//! resolves here before its WAL record is appended, and a governed ingest
//! resolves here before it is proposed: a follower has no writing identity.
//! Resolution stays on the Data Plane because the time column, the default
//! timestamp and the collection schema are this core's local state.

use nodedb_physical::physical_plan::{TimeseriesOp, TimeseriesResolve};

use super::normalize;
use super::rls_gate;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::ColumnValue;
use crate::engine::timeseries::ilp::IlpLine;
use crate::engine::timeseries::ilp_ingest::line_system_ordinal;
use crate::engine::timeseries::resolved_ingest::{
    CLONE_COPY_INGEST_FORMAT, ResolveBase, ResolvedTsBatch, TsDriftPolicy,
};
use nodedb_types::columnar::schema::TS_SYSTEM;

/// One ingest payload to normalize and stamp.
pub(in crate::data::executor) struct StampedIngest<'a> {
    pub database_id: crate::types::DatabaseId,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    /// The default timestamp of a row that carries none.
    pub now_ms: i64,
}

/// The measurement an ingest writes to, taken from its routing collection.
fn measurement_of(collection: &str) -> &str {
    collection
        .split_once(':')
        .map(|(_, name)| name)
        .unwrap_or(collection)
}

impl CoreLoop {
    /// Resolve the wrapped ingest to the rows it stores, as an encoded
    /// [`crate::engine::timeseries::resolved_ingest::ResolvedTsBatch`] under
    /// [`TsDriftPolicy::Refuse`]. A writer whose install must not refuse sets
    /// its own policy on the batch.
    ///
    /// A resolve that names a base resolves against it in place of the live
    /// schema: the schema an earlier ingest of the same transaction into the
    /// same collection resolved to.
    pub(in crate::data::executor) fn execute_timeseries_resolve_ingest(
        &mut self,
        task: &ExecutionTask,
        resolve: &TimeseriesResolve,
    ) -> Response {
        let base = match resolve
            .base
            .as_deref()
            .map(ResolveBase::from_bytes)
            .transpose()
        {
            Ok(base) => base.map(|base| base.schema()),
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("timeseries resolve: invalid base schema: {error}"),
                    },
                );
            }
        };
        let inner = &resolve.ingest;
        let TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            rls_write_check,
            returning,
            ..
        } = inner
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "timeseries resolve pass wraps a plan that is not an ingest".into(),
                },
            );
        };

        let tid = task.request.tenant_id;
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection.as_str())
            .map(str::to_string);
        let now_ms = self.ingest_stamp_ms();
        let lines = match self.stamped_ingest_lines(StampedIngest {
            database_id: task.request.database_id,
            tid,
            collection: collection.as_str(),
            payload,
            format,
            now_ms,
        }) {
            Ok(lines) => lines,
            Err(error) => return self.response_error(task, error),
        };

        // Decide the policy against the stamped lines — the exact images the
        // proposed ingest will store on every replica.
        let joined = lines.join("\n");
        let parsed = match crate::engine::timeseries::ilp::parse_batch(&joined) {
            Ok(parsed) => parsed,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: unparsable line protocol: {error}"),
                    },
                );
            }
        };
        if let Err(error) = rls_gate::admit_ilp_lines(
            rls_write_check,
            parsed.lines(),
            time_key.as_deref(),
            now_ms,
            tid.as_u64(),
            collection.as_str(),
            &self.crs_context(),
        ) {
            return self.response_error(task, error);
        }

        // The rows the lines store. The writer's install stores exactly these
        // rows, or refuses when a concurrent write changed the schema.
        let batch = self.resolve_ts_batch(
            task,
            super::TsResolveInput {
                tid,
                collection: collection.as_str(),
                lines: &lines,
                now_ms,
                drift: TsDriftPolicy::Refuse,
                needs_images: returning.is_some(),
                base: base.as_ref(),
            },
        );
        // A clone copy's rows keep their source versions' ordinals.
        let batch = if format == CLONE_COPY_INGEST_FORMAT {
            batch.and_then(|batch| carry_source_ordinals(batch, parsed.lines()))
        } else {
            batch
        };
        // A `RETURNING` ingest takes every row or none: its row set has no
        // place to report a rejected row.
        if returning.is_some()
            && let Ok(resolved) = &batch
            && resolved.rows.len() < lines.len()
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "timeseries ingest with RETURNING would reject {} of {} rows, and a row \
                         set cannot report a rejected row; first rejection: {}",
                        lines.len() - resolved.rows.len(),
                        lines.len(),
                        resolved
                            .first_rejection
                            .as_deref()
                            .unwrap_or("no reason recorded")
                    ),
                },
            );
        }
        match batch.and_then(|batch| batch.to_bytes()) {
            Ok(encoded) => self.response_with_payload(task, encoded),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("timeseries resolve: could not resolve rows: {error}"),
                },
            ),
        }
    }

    /// The canonical lines `payload` stores, with every row that carries no
    /// timestamp stamped `now_ms`. A transaction's resolve and the governed
    /// ingest's resolve pass both stamp here, once, before the write is
    /// proposed, so every replica stores identical rows.
    pub(in crate::data::executor) fn stamped_ingest_lines(
        &self,
        ingest: StampedIngest<'_>,
    ) -> Result<Vec<String>, ErrorCode> {
        let StampedIngest {
            database_id,
            tid,
            collection,
            payload,
            format,
            now_ms,
        } = ingest;
        let time_key = self.declared_ts_time_key(database_id, tid, collection);
        let batch = Self::normalized_ilp_batch(collection, payload, format, time_key)?;
        let lines = normalize::stamp_timestamps(&batch, now_ms).map_err(|error| {
            ErrorCode::RejectedPrevalidation {
                reason: format!("timeseries resolve: unparsable line protocol: {error}"),
            }
        })?;
        if lines.is_empty() {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: format!("timeseries resolve: '{collection}' payload holds no rows"),
            });
        }
        Ok(lines)
    }

    /// Rewrite `payload` into line protocol, mirroring the ingest handler's
    /// format dispatch so the lines returned are what ingest would parse.
    fn normalized_ilp_batch(
        collection: &str,
        payload: &[u8],
        format: &str,
        time_key: Option<&str>,
    ) -> Result<String, ErrorCode> {
        let measurement = measurement_of(collection);
        match format {
            "ilp" => String::from_utf8(payload.to_vec()).map_err(|error| {
                ErrorCode::RejectedPrevalidation {
                    reason: format!("timeseries resolve: line protocol is not UTF-8: {error}"),
                }
            }),
            "ilp-msgpack" => {
                let lines: Vec<String> = zerompk::from_msgpack(payload).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!(
                            "timeseries resolve: invalid canonical ILP payload: {error}"
                        ),
                    }
                })?;
                Ok(lines.join("\n"))
            }
            "msgpack" | CLONE_COPY_INGEST_FORMAT => {
                let rows =
                    super::msgpack_decode::decode_msgpack_rows(payload).map_err(|error| {
                        ErrorCode::RejectedPrevalidation {
                            reason: format!("timeseries resolve: msgpack decode error: {error}"),
                        }
                    })?;
                normalize::msgpack_rows_to_ilp(&rows, measurement, time_key).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: {error}"),
                    }
                })
            }
            "json" => {
                let rows: sonic_rs::Array = sonic_rs::from_slice(payload).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: JSON parse error: {error}"),
                    }
                })?;
                normalize::json_rows_to_ilp(&rows, measurement, time_key).map_err(|error| {
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries resolve: {error}"),
                    }
                })
            }
            other => Err(ErrorCode::Internal {
                detail: format!("timeseries resolve: unknown ingest format: {other}"),
            }),
        }
    }
}

/// `batch` with every row's `_ts_system` set to the ordinal its line carries,
/// and marked so the install keeps those ordinals: a clone copy installs each
/// row as its source version.
pub(in crate::data::executor) fn carry_source_ordinals(
    mut batch: ResolvedTsBatch,
    lines: &[IlpLine<'_>],
) -> crate::Result<ResolvedTsBatch> {
    let copy_error = |detail: String| crate::Error::Internal {
        detail: format!("timeseries clone copy: {detail}"),
    };
    let system_idx = batch
        .columns
        .iter()
        .position(|(name, _)| name == TS_SYSTEM)
        .ok_or_else(|| {
            copy_error(format!(
                "'{}' resolved no {TS_SYSTEM} column",
                batch.measurement
            ))
        })?;
    for row in &mut batch.rows {
        let line = usize::try_from(row.line)
            .ok()
            .and_then(|index| lines.get(index))
            .ok_or_else(|| copy_error(format!("row names line {} of {}", row.line, lines.len())))?;
        let ordinal = line_system_ordinal(line).ok_or_else(|| {
            copy_error(format!("line {} carries no {TS_SYSTEM} ordinal", row.line))
        })?;
        let cell = row.values.get_mut(system_idx).ok_or_else(|| {
            copy_error(format!("row of line {} has no {TS_SYSTEM} cell", row.line))
        })?;
        *cell = ColumnValue::Int64(ordinal);
    }
    batch.carried_system = true;
    // A clone copy changes nothing a reader of the clone sees: no install
    // and no WAL catch-up emits an event for its rows.
    batch.emits_events = false;
    Ok(batch)
}
