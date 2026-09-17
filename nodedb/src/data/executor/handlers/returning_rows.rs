// SPDX-License-Identifier: BUSL-1.1

//! Helper for projecting post-write documents into a `RowsPayload` msgpack
//! blob the Control Plane decodes into multi-column pgwire rows. The single
//! choke point where the RLS read policy applies to DML output — every
//! caller passes full pre-projection documents so the policy sees columns
//! `RETURNING` never mentions.

use super::{returning_doc, rls_eval};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::response_codec::RowsPayload;
use crate::data::executor::scan_normalize::{kv_row_to_doc, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;
use crate::util::rmpv_value::rmpv_to_value;
use nodedb_physical::physical_plan::{ReturningColumns, ReturningSpec};
use nodedb_types::columnar::StrictSchema;
use nodedb_types::{NativeCell, Value};

/// Rows a write path hands back to a `RETURNING` projection: the row's
/// client-visible identity paired with the exact bytes stored for it.
pub(in crate::data::executor) type StoredRow<'a> = (&'a RowIdentity, &'a [u8]);

impl CoreLoop {
    /// Build this task's `RETURNING` response from the rows it just stored.
    /// The single exit every insert-family handler uses, so the read gate and
    /// decode can never be applied on one path and skipped on another.
    pub(in crate::data::executor) fn stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        strict_schema: Option<&StrictSchema>,
        rows: &[StoredRow<'_>],
    ) -> Response {
        match build_stored_rows_payload(spec, rls_filters, strict_schema, rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// A KV row a write path hands back to a `RETURNING` projection: the raw key
/// bytes paired with the exact value bytes stored under them.
pub(in crate::data::executor) type KvStoredRow<'a> = (&'a [u8], &'a [u8]);

impl CoreLoop {
    /// Build a KV write's `RETURNING` response from the rows it just stored.
    /// Row shape comes from [`kv_row_to_doc`], the same helper KV scans use,
    /// so it's byte-identical to what `SELECT` on that key produces. A body
    /// that fails to decode fails the statement rather than substitute a
    /// bare `{key}` that hides an unreadable stored value.
    pub(in crate::data::executor) fn kv_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[KvStoredRow<'_>],
    ) -> Response {
        match kv_stored_rows_payload(spec, rls_filters, rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}

/// The `RowsPayload` blob a KV write's `RETURNING` clause projects. Split
/// from [`CoreLoop::kv_stored_returning_response`] so the resolve-before-
/// propose path can decide the same payload without holding a `Response`.
pub(in crate::data::executor) fn kv_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    rows: &[KvStoredRow<'_>],
) -> crate::Result<Vec<u8>> {
    let docs: Vec<Value> = rows
        .iter()
        .map(|(key, value)| {
            let (_key_str, body) = kv_row_to_doc(key, value);
            doc_format::decode_document_value(&body)
        })
        .collect::<crate::Result<Vec<_>>>()?;
    build_rows_payload(spec, rls_filters, &docs).map_err(|e| crate::Error::Internal {
        detail: format!("RETURNING encode: {e}"),
    })
}

impl CoreLoop {
    /// Build a columnar write's `RETURNING` response from the rows it stored.
    /// A columnar row is `Vec<Value>` in schema order, assembled by
    /// `row_values_to_object` — the same builder the WHERE evaluator and RLS
    /// write gate use, so output can't drift from what `SELECT` reports.
    pub(in crate::data::executor) fn columnar_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        schema: &nodedb_types::columnar::ColumnarSchema,
        rows: &[Vec<nodedb_types::Value>],
    ) -> Response {
        let docs: Vec<Value> = rows
            .iter()
            .map(|row| super::columnar_write::row_values_to_object(schema, row))
            .collect();
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

impl CoreLoop {
    /// Build a timeseries ingest's `RETURNING` response from the points it
    /// stored. `rows` are already `raw_scan::emit_memtable_rows_at` output, so
    /// a point renders exactly as `SELECT` would, `NaN`-as-NULL included; this
    /// only re-keys the shape, no per-cell decisions of its own.
    pub(in crate::data::executor) fn timeseries_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[rmpv::Value],
    ) -> Response {
        let docs: Vec<Value> = rows.iter().map(rmpv_to_value).collect();
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

impl CoreLoop {
    /// Build a vector-primary upsert's `RETURNING` response from the sidecar
    /// it stored, via [`sparse_row_to_doc`] against
    /// [`SparseBodyFormatRef::VectorSidecar`] — the same converter `SELECT`
    /// uses. The sidecar is `zerompk` TAGGED bytes an ordinary document
    /// decode misreads (`"alice"` comes back as `[4,"alice"]`), so the
    /// format must be this literal, not re-decided.
    pub(in crate::data::executor) fn vector_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        row_key: &nodedb_types::StorageKey,
        sidecar: &[u8],
    ) -> Response {
        let (_id, mp) = sparse_row_to_doc(row_key, sidecar, SparseBodyFormatRef::VectorSidecar);
        // An empty row set here would report "the write affected nothing" for a
        // write that did land, so an unreadable sidecar fails the statement.
        let docs: Vec<Value> = match doc_format::decode_document_value(&mp) {
            Ok(doc) => vec![doc],
            Err(e) => return self.response_error(task, e),
        };
        match build_rows_payload(spec, rls_filters, &docs) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// Project the STORED post-images of freshly written rows into a
/// `RowsPayload` — the write paths hold the exact bytes handed to storage,
/// so `RETURNING` reports what landed rather than echoing the submitted
/// body. `strict_schema` must be `Some` exactly when the collection stores
/// Binary Tuples; a decode failure fails the statement, never a bare `{id}`.
pub(in crate::data::executor) fn build_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    strict_schema: Option<&StrictSchema>,
    rows: &[StoredRow<'_>],
) -> crate::Result<Vec<u8>> {
    let docs: Vec<Value> = rows
        .iter()
        .map(|(doc_id, body)| returning_doc::from_stored(body, doc_id, strict_schema))
        .collect::<crate::Result<Vec<_>>>()?;
    build_rows_payload(spec, rls_filters, &docs)
}

/// Project documents per `spec` (Star = field name order, Named = spec
/// order, missing → `Value::Null`) into a `RowsPayload` msgpack blob.
/// `rls_filters` (the compiled read policy) is applied BEFORE projection — a
/// predicate often references a column `RETURNING` omits, so a
/// post-projection check would leak the row. Only the visible row set
/// shrinks; the affected count already counted the write.
pub(super) fn build_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    docs: &[Value],
) -> crate::Result<Vec<u8>> {
    let visible: Vec<&Value> = docs
        .iter()
        .filter(|doc| rls_eval::rls_check_value(rls_filters, doc))
        .collect();

    let (columns, source_names) = match &spec.columns {
        ReturningColumns::Star => {
            if visible.is_empty() {
                return encode_empty(Vec::new());
            }
            // Derive column names from the first doc's keys, sorted so the
            // shape is deterministic; both output and source names are
            // identical for `RETURNING *`.
            let mut cols: Vec<String> = match visible.first() {
                Some(Value::Object(obj)) => obj.keys().cloned().collect(),
                Some(_) | None => Vec::new(),
            };
            cols.sort_unstable();
            (cols.clone(), cols)
        }
        ReturningColumns::Named(items) => {
            let output_names: Vec<String> = items
                .iter()
                .map(|item| item.alias.clone().unwrap_or_else(|| item.name.clone()))
                .collect();
            let source_names: Vec<String> = items.iter().map(|item| item.name.clone()).collect();
            (output_names, source_names)
        }
    };

    let rows: Vec<Vec<NativeCell>> = visible
        .iter()
        .map(|doc| project_row(doc, &source_names))
        .collect();

    let payload = RowsPayload { columns, rows };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode: {e}"),
    })
}

fn encode_empty(columns: Vec<String>) -> crate::Result<Vec<u8>> {
    let payload = RowsPayload {
        columns,
        rows: Vec::new(),
    };
    zerompk::to_msgpack_vec(&payload).map_err(|e| crate::Error::Codec {
        detail: format!("RowsPayload encode empty: {e}"),
    })
}

/// Project a single document into one typed cell per source name.
///
/// A missing field is `Value::Null`; a document that is not an object has
/// every cell `Value::Null`.
fn project_row(doc: &Value, source_names: &[String]) -> Vec<NativeCell> {
    let fields = match doc {
        Value::Object(obj) => Some(obj),
        _ => None,
    };
    source_names
        .iter()
        .map(|name| {
            let cell = match fields.and_then(|obj| obj.get(name)) {
                Some(value) => value.clone(),
                None => Value::Null,
            };
            NativeCell(cell)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use nodedb_physical::physical_plan::ReturningItem;
    use serde_json::json;

    /// `owner = <value>`, the shape a compiled read policy lands in.
    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn named(cols: &[&str]) -> ReturningSpec {
        ReturningSpec {
            columns: ReturningColumns::Named(
                cols.iter()
                    .map(|c| ReturningItem {
                        name: (*c).to_string(),
                        alias: None,
                    })
                    .collect(),
            ),
        }
    }

    fn decode(payload: &[u8]) -> RowsPayload {
        zerompk::from_msgpack(payload).expect("decode RowsPayload")
    }

    fn cells(row: &[NativeCell]) -> Vec<Value> {
        row.iter().map(|c| c.0.clone()).collect()
    }

    fn docs() -> Vec<Value> {
        vec![
            Value::from(json!({"id": "r1", "owner": "alice", "note": "hidden"})),
            Value::from(json!({"id": "r2", "owner": "bob", "note": "shown"})),
        ]
    }

    #[test]
    fn empty_filters_return_every_row() {
        let payload = build_rows_payload(&named(&["id"]), &[], &docs()).expect("build");
        let rows = decode(&payload).rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn excluded_rows_are_dropped() {
        let payload =
            build_rows_payload(&named(&["id"]), &owner_policy("bob"), &docs()).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(cells(&decoded.rows[0]), vec![Value::String("r2".into())]);
    }

    /// The predicate names a column the projection omits — the filter must
    /// still see it, which it only can before projection.
    #[test]
    fn a_policy_column_outside_the_projection_still_filters() {
        let payload =
            build_rows_payload(&named(&["note"]), &owner_policy("bob"), &docs()).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["note".to_string()]);
        assert_eq!(decoded.rows.len(), 1);
        assert_eq!(cells(&decoded.rows[0]), vec![Value::String("shown".into())]);
    }

    /// `RETURNING *` derives its column list from the first VISIBLE row, so a
    /// hidden leading row cannot dictate the shape of the result.
    #[test]
    fn star_columns_come_from_the_first_visible_row() {
        let docs = vec![
            Value::from(json!({"id": "r1", "owner": "alice", "secret": "hidden"})),
            Value::from(json!({"id": "r2", "owner": "bob"})),
        ];
        let spec = ReturningSpec {
            columns: ReturningColumns::Star,
        };
        let payload = build_rows_payload(&spec, &owner_policy("bob"), &docs).expect("build");
        let decoded = decode(&payload);
        assert_eq!(decoded.columns, vec!["id".to_string(), "owner".to_string()]);
        assert_eq!(decoded.rows.len(), 1);
    }

    /// A malformed policy denies rather than passing rows through.
    #[test]
    fn a_corrupt_policy_returns_no_rows() {
        let payload = build_rows_payload(&named(&["id"]), &[0xFF, 0xFE], &docs()).expect("build");
        assert!(decode(&payload).rows.is_empty());
    }

    /// Cells keep their stored type: an instant stays an instant, a missing
    /// field is SQL NULL, a nested object stays nested.
    #[test]
    fn cells_keep_their_stored_type() {
        let at = nodedb_types::NdbDateTime::from_micros(1_583_402_400_000_000);
        let mut doc = std::collections::HashMap::new();
        doc.insert("at".to_string(), Value::NaiveDateTime(at));
        doc.insert("n".to_string(), Value::Integer(7));
        doc.insert(
            "meta".to_string(),
            Value::Object(std::collections::HashMap::from([(
                "k".to_string(),
                Value::Bool(true),
            )])),
        );
        let payload = build_rows_payload(
            &named(&["at", "n", "meta", "absent"]),
            &[],
            &[Value::Object(doc)],
        )
        .expect("build");
        let decoded = decode(&payload);
        assert_eq!(
            cells(&decoded.rows[0]),
            vec![
                Value::NaiveDateTime(at),
                Value::Integer(7),
                Value::Object(std::collections::HashMap::from([(
                    "k".to_string(),
                    Value::Bool(true),
                )])),
                Value::Null,
            ]
        );
    }

    /// A timeseries row's instant ext crosses into the payload typed.
    #[test]
    fn a_timeseries_instant_cell_stays_typed() {
        let at = nodedb_types::NdbDateTime::from_micros(42);
        let row = crate::util::rmpv_value::value_to_rmpv(&Value::Object(
            std::collections::HashMap::from([("t".to_string(), Value::DateTime(at))]),
        ));
        let docs = vec![rmpv_to_value(&row)];
        let payload = build_rows_payload(&named(&["t"]), &[], &docs).expect("build");
        assert_eq!(cells(&decode(&payload).rows[0]), vec![Value::DateTime(at)]);
    }
}
