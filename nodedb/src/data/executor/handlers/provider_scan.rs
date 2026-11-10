// SPDX-License-Identifier: BUSL-1.1

//! Executor handler for `QueryOp::ProviderScan`.
//!
//! Decodes the pre-materialized msgpack row array, applies predicate
//! filtering, window functions + computed columns, sort, distinct
//! deduplication, offset, column projection, and limit — in that order —
//! then emits the resulting rows via `response_with_payload`. Windows and
//! computed columns run before sort so `ORDER BY` can name their aliases.
//! Sort runs before offset so `ORDER BY ... OFFSET n` skips the first `n`
//! rows of the sorted set, not the decoded set.

use nodedb_query::msgpack_scan;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::provider_scan_compute::apply_windows_and_computed;
use crate::data::executor::handlers::sort_utils::sort_msgpack_rows;
use crate::data::executor::response_codec::encode_binary_rows;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::object_msgpack::{ObjectFormat, ObjectView};

/// Parameters for [`CoreLoop::execute_provider_scan`].
pub(in crate::data::executor) struct ProviderScanParams<'a> {
    pub rows_bytes: &'a [u8],
    pub filters_bytes: &'a [u8],
    pub projection: &'a [String],
    pub computed_columns_bytes: &'a [u8],
    pub window_functions_bytes: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub limit: Option<usize>,
    pub offset: usize,
    pub distinct: bool,
}

impl CoreLoop {
    /// Execute a `ProviderScan` plan node.
    ///
    /// Processing order: decode rows → filter → windows + computed columns →
    /// sort → distinct → offset → project → limit → emit.
    pub(in crate::data::executor) fn execute_provider_scan(
        &mut self,
        task: &ExecutionTask,
        params: ProviderScanParams<'_>,
    ) -> Response {
        let crs = self.crs_context();
        let ProviderScanParams {
            rows_bytes,
            filters_bytes,
            projection,
            computed_columns_bytes,
            window_functions_bytes,
            sort_keys,
            limit,
            offset,
            distinct,
        } = params;
        // ── 1. Decode the flat msgpack row array. ────────────────────────────
        // ProviderScan rows are flat column maps *by contract*: catalog and
        // constant results are produced flat, and a gathered storage side is
        // flattened at the Exchange-resolution boundary (`exchange::resolve`)
        // before it is embedded here. The relational-operator layer therefore
        // sees exactly one row shape — this handler never sniffs or unwraps a
        // `{id, data}` storage wrapper.
        let mut rows = match decode_flat_row_array(rows_bytes) {
            Ok(rows) => rows,
            Err(error) => return self.response_error(task, error),
        };

        // ── 2. Filter. ────────────────────────────────────────────────────────
        if !filters_bytes.is_empty() {
            let predicates: Vec<ScanFilter> = match zerompk::from_msgpack(filters_bytes) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("ProviderScan: malformed filter bytes: {e}"),
                        },
                    );
                }
            };
            if !predicates.is_empty() {
                // `Vec::retain`'s closure must return `bool`, so an
                // evaluation error is held and returned once the retain ends.
                let held = crate::data::executor::core_loop::filter_match::HeldEvalError::default();
                rows.retain(|row| {
                    held.check(|| ScanFilter::all_match_binary(&predicates, row, &crs))
                });
                if let Err(e) = held.finish::<_, ErrorCode>(()) {
                    return self.response_error(task, e);
                }
            }
        }

        // ── 3. Window functions + computed columns. ─────────────────────────
        // Runs before sort so `ORDER BY` can name a window or computed alias.
        // Each window spec orders its own partitions, so the row order here
        // does not affect window results. Skipped entirely (zero-decode
        // msgpack path) when both byte slices are empty.
        if !window_functions_bytes.is_empty() || !computed_columns_bytes.is_empty() {
            rows = match apply_windows_and_computed(
                rows,
                window_functions_bytes,
                computed_columns_bytes,
                &crs,
            ) {
                Ok(r) => r,
                Err(e) => return self.response_error(task, e),
            };
        }

        // ── 4. Sort. ──────────────────────────────────────────────────────────
        // Runs before offset: `ORDER BY ... OFFSET n` skips the first `n` rows
        // of the SORTED set, not the decoded set.
        if !sort_keys.is_empty()
            && let Err(e) = sort_msgpack_rows(&mut rows, sort_keys, &crs)
        {
            return self.response_error(task, e);
        }

        // ── 5. Distinct (on the would-be projected row). ──────────────────────
        // Deduplicate on the projected shape so SQL DISTINCT semantics are
        // honoured: two rows with the same projected columns but different
        // non-projected columns are considered equal.
        if distinct {
            let mut seen: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
            let mut unique = Vec::with_capacity(rows.len());
            for row in rows {
                let key = distinct_row_key(&row, projection);
                let key = match key {
                    Ok(key) => key,
                    Err(error) => return self.response_error(task, error),
                };
                if seen.insert(key) {
                    unique.push(row);
                }
            }
            rows = unique;
        }

        // ── 6. Offset. ────────────────────────────────────────────────────────
        if offset > 0 {
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows.drain(..offset);
            }
        }

        // ── 7. Project. ───────────────────────────────────────────────────────
        let rows: Vec<Vec<u8>> = if projection.is_empty() {
            rows
        } else {
            match rows
                .into_iter()
                .map(|row| project_row_by_names(&row, projection))
                .collect::<crate::Result<Vec<_>>>()
            {
                Ok(rows) => rows,
                Err(error) => return self.response_error(task, error),
            }
        };

        // ── 8. Limit. ─────────────────────────────────────────────────────────
        let rows = if let Some(n) = limit {
            rows.into_iter().take(n).collect()
        } else {
            rows
        };

        // ── 9. Emit. ──────────────────────────────────────────────────────────
        let payload = encode_binary_rows(&rows);
        self.response_with_payload(task, payload)
    }
}

/// A row key with its table qualifier removed (`a.attname` → `attname`).
fn bare_column(key: &str) -> &str {
    key.rsplit('.').next().unwrap_or(key)
}

/// The DISTINCT identity of `row` as projected: its column/value pairs by
/// canonical value identity, so `5`, `5.0` and `5.00` are one value and a
/// plain row and a native row of the same values are one row.
fn distinct_row_key(row: &[u8], projection: &[String]) -> crate::Result<Vec<String>> {
    let projected;
    let row = if projection.is_empty() {
        row
    } else {
        projected = project_row_by_names(row, projection)?;
        &projected
    };
    crate::util::value_identity::named_row_identity(row)
}

fn provider_decode_error(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::Codec {
        detail: format!("ProviderScan object row: {error}"),
    }
}

/// Decode a flat msgpack row array (the `encode_binary_rows` format) into
/// individual row byte vectors. Each element is a flat column map; its bytes are
/// returned verbatim so subsequent steps operate per-row without re-encoding.
/// Storage `{id, data}` wrappers are NOT handled here — they are flattened at
/// the Exchange-resolution boundary before reaching a relational operator.
fn decode_flat_row_array(bytes: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let (count, mut pos) = msgpack_scan::array_header(bytes, 0)
        .ok_or_else(|| provider_decode_error("expected row array"))?;
    let mut rows = Vec::new();
    for _ in 0..count {
        let end = msgpack_scan::skip_value(bytes, pos)
            .ok_or_else(|| provider_decode_error("truncated row array"))?;
        ObjectView::new(&bytes[pos..end])
            .map_err(provider_decode_error)?
            .decode()
            .map_err(provider_decode_error)?;
        rows.push(bytes[pos..end].to_vec());
        pos = end;
    }
    if pos != bytes.len() {
        return Err(provider_decode_error("trailing row array bytes"));
    }
    Ok(rows)
}

/// Project a single msgpack map row, keeping only the named columns.
///
/// Returns a new msgpack map containing only the fields whose name appears in
/// `projection`, preserving the original msgpack value bytes verbatim (no
/// decode). If a projection column is not present in the row it is silently
/// omitted. If `projection` is empty this function must not be called (the
/// caller is expected to skip projection for the empty case).
///
/// A row key matches a projection entry outright or once its table qualifier is
/// dropped — a join body emits one merged document per row whose columns keep
/// their `alias.column` prefix, so an exact-only match would project every
/// merged row down to nothing. This mirrors the qualified-then-bare lookup the
/// response shaper performs on the same rows.
fn project_row_by_names(row: &[u8], projection: &[String]) -> crate::Result<Vec<u8>> {
    let view = ObjectView::new(row).map_err(provider_decode_error)?;
    let mut entries = Vec::new();
    for field in view.fields() {
        let (key, value) = field.map_err(provider_decode_error)?;
        view.decode_value(value).map_err(provider_decode_error)?;
        if projection.iter().any(|p| p == key || p == bare_column(key)) {
            entries.push((key, value));
        }
    }
    let mut bytes = Vec::with_capacity(row.len());
    if view.format() == ObjectFormat::Native {
        bytes.extend_from_slice(&[0x92, 7]);
    }
    msgpack_scan::write_map_header(&mut bytes, entries.len());
    for (key, value) in entries {
        msgpack_scan::write_str(&mut bytes, key);
        bytes.extend_from_slice(value);
    }
    Ok(bytes)
}

#[cfg(test)]
mod native_tests {
    use super::*;
    use nodedb_types::Value;

    #[test]
    fn native_provider_projection_preserves_types_and_qualified_names() {
        let value = Value::Bytes(vec![0, 255]);
        let row = zerompk::to_msgpack_vec(&Value::Object(
            [
                ("a.payload".into(), value.clone()),
                ("a.other".into(), Value::Integer(3)),
            ]
            .into_iter()
            .collect(),
        ))
        .expect("native row");
        let projected = project_row_by_names(&row, &["payload".into()]).expect("project");
        let doc = nodedb_types::object_msgpack::decode_object(&projected).expect("object");
        assert_eq!(doc.get("a.payload"), Some(&value));
        assert!(doc.get("a.other").is_none());
        let payload = encode_binary_rows(std::slice::from_ref(&row));
        assert_eq!(decode_flat_row_array(&payload).expect("rows"), vec![row]);
    }

    #[test]
    fn distinct_keys_match_plain_and_native_shapes_without_losing_rich_types() {
        let plain = nodedb_types::json_to_msgpack(&serde_json::json!({"a.key": 42, "other": 1}))
            .expect("plain row");
        let native = zerompk::to_msgpack_vec(&Value::Object(
            [
                ("a.key".into(), Value::Integer(42)),
                ("other".into(), Value::Integer(2)),
            ]
            .into_iter()
            .collect(),
        ))
        .expect("native row");
        let projection = ["key".into()];
        assert_eq!(
            distinct_row_key(&plain, &projection).expect("plain key"),
            distinct_row_key(&native, &projection).expect("native key")
        );
        let uuid = zerompk::to_msgpack_vec(&Value::Object(
            [("key".into(), Value::Uuid("uuid".into()))]
                .into_iter()
                .collect(),
        ))
        .expect("uuid row");
        let text = zerompk::to_msgpack_vec(&Value::Object(
            [("key".into(), Value::String("uuid".into()))]
                .into_iter()
                .collect(),
        ))
        .expect("text row");
        assert_ne!(
            distinct_row_key(&uuid, &[]).expect("uuid key"),
            distinct_row_key(&text, &[]).expect("text key")
        );
    }

    #[test]
    fn provider_rows_reject_truncation_and_nonobjects() {
        for payload in [&[0x91][..], &[0x91, 1][..], &[0x90, 0xc0][..]] {
            assert!(decode_flat_row_array(payload).is_err());
        }
    }
}
