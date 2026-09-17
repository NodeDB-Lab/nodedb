// SPDX-License-Identifier: BUSL-1.1

//! MessagePack + JSON ingest formats for timeseries.

use super::ingest_dispatch::TimeseriesIngestParams;
use super::msgpack_decode;
use super::normalize;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

/// Decode the canonical ILP row envelope used by the Calvin path.
///
/// Caller-provided malformed bytes are rejected before ingest. An inability to
/// re-encode an already decoded value is an internal serialization failure.
fn decode_canonical_ilp_lines(payload: &[u8]) -> Result<Vec<String>, ErrorCode> {
    let lines: Vec<String> =
        zerompk::from_msgpack(payload).map_err(|error| ErrorCode::RejectedPrevalidation {
            reason: format!("invalid canonical ILP payload: {error}"),
        })?;
    if lines.is_empty() {
        return Err(ErrorCode::RejectedPrevalidation {
            reason: "empty canonical ILP payload".into(),
        });
    }
    let canonical = zerompk::to_msgpack_vec(&lines).map_err(|error| ErrorCode::Internal {
        detail: format!("canonical ILP payload re-encode failed: {error}"),
    })?;
    if canonical != payload
        || lines
            .iter()
            .any(|line| line.contains('\n') || line.contains('\r'))
    {
        return Err(ErrorCode::RejectedPrevalidation {
            reason: "malformed canonical ILP line payload".into(),
        });
    }
    Ok(lines)
}

impl CoreLoop {
    /// Decode the canonical Calvin ILP representation without reformatting any
    /// identifiers, unsigned values, escaped tags, or nanosecond timestamps.
    pub(super) fn execute_ilp_msgpack_ingest(
        &mut self,
        params: TimeseriesIngestParams<'_>,
    ) -> Response {
        let TimeseriesIngestParams { task, payload, .. } = &params;
        // Calvin produces canonical zerompk. This rejects trailing bytes and
        // alternate encodings before any memtable mutation.
        let lines = match decode_canonical_ilp_lines(payload) {
            Ok(lines) => lines,
            Err(error) => return self.response_error(task, error),
        };
        let joined = lines.join("\n");
        self.execute_ilp_ingest(TimeseriesIngestParams {
            payload: joined.as_bytes(),
            ..params
        })
    }

    /// Payload is a msgpack array of maps (same schema as JSON ingest but in msgpack).
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_msgpack_ingest(
        &mut self,
        params: TimeseriesIngestParams<'_>,
    ) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Carried through, never blanked, or RETURNING silently no-ops here.
            returning,
            rls_filters,
        } = params;
        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        // Allows the optional `<db_id>/` db-qualifier slash alongside
        // `[a-zA-Z0-9_-]` — it's a wire routing key, not the measurement.
        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-/] allowed"
                    ),
                },
            );
        }

        let rows = match msgpack_decode::decode_msgpack_rows(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("msgpack decode error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty msgpack rows array".into(),
                },
            );
        }

        // The declared TIME_KEY is resolved once per batch: it is a property
        // of the collection, not of any individual row.
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);

        let ilp_buf = match normalize::msgpack_rows_to_ilp(&rows, measurement, time_key.as_deref())
        {
            Ok(buf) => buf,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: format!("timeseries ingest: {error}"),
                    },
                );
            }
        };

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in msgpack payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload: ilp_buf.as_bytes(),
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Forwarded to `execute_ilp_ingest` — the point exists only after
            // the rewrite above, so projecting earlier reports submitted values.
            returning,
            rls_filters,
        })
    }

    /// Payload is a JSON array like: `[{"id":"e1","ts":"2024-01-01T00:00:00Z","value":42.0}]`.
    /// Converts each row to an ILP line and delegates to the ILP ingest path.
    pub(super) fn execute_json_ingest(&mut self, params: TimeseriesIngestParams<'_>) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Carried through, never blanked, or RETURNING silently no-ops here.
            returning,
            rls_filters,
        } = params;
        let rows: sonic_rs::Array = match sonic_rs::from_slice(payload) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("JSON parse error: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty JSON rows array".into(),
                },
            );
        }

        let measurement = collection
            .split_once(':')
            .map(|(_, name)| name)
            .unwrap_or(collection);

        // Allows the optional `<db_id>/` db-qualifier slash alongside
        // `[a-zA-Z0-9_-]` — it's a wire routing key, not the measurement.
        if !measurement
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '/')
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "invalid measurement name '{measurement}': only [a-zA-Z0-9_-/] allowed"
                    ),
                },
            );
        }

        // The declared TIME_KEY is resolved once per batch: it is a property
        // of the collection, not of any individual row.
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);

        let ilp_buf = normalize::json_rows_to_ilp(&rows, measurement, time_key.as_deref());

        if ilp_buf.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid rows in JSON payload".into(),
                },
            );
        }

        self.execute_ilp_ingest(TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload: ilp_buf.as_bytes(),
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            // Forwarded to `execute_ilp_ingest` — the point exists only after
            // the rewrite above, so projecting earlier reports submitted values.
            returning,
            rls_filters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::decode_canonical_ilp_lines;
    use crate::bridge::envelope::ErrorCode;

    #[test]
    fn canonical_ilp_msgpack_roundtrip_preserves_raw_protocol_values() {
        let lines =
            vec![r"cpu,host=west\,1 u=18446744073709551615u 1234567890123456789".to_owned()];
        let payload = zerompk::to_msgpack_vec(&lines).expect("encode");
        assert_eq!(decode_canonical_ilp_lines(&payload).expect("decode"), lines);
    }

    #[test]
    fn canonical_ilp_msgpack_rejects_trailing_or_multiline_values() {
        let mut payload =
            zerompk::to_msgpack_vec(&vec!["cpu value=1i".to_owned()]).expect("encode");
        payload.push(0);
        assert!(matches!(
            decode_canonical_ilp_lines(&payload),
            Err(ErrorCode::RejectedPrevalidation { .. })
        ));
        let newline = zerompk::to_msgpack_vec(&vec!["cpu value=1i\nmem value=2i".to_owned()])
            .expect("encode");
        assert!(matches!(
            decode_canonical_ilp_lines(&newline),
            Err(ErrorCode::RejectedPrevalidation { .. })
        ));
    }
}
