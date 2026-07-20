// SPDX-License-Identifier: BUSL-1.1

//! Row-fetch stage for the document scan pipeline.
//!
//! This is the ONLY stage that differs between a current-time read and a
//! bitemporal `AS OF SYSTEM TIME` / `AS OF VALID TIME` / all-versions audit
//! read. Every post-fetch transform — sort, window functions, computed
//! columns, projection, `DISTINCT` — is shared downstream in
//! [`super::scan`], so temporal reads gain full parity with current-time reads
//! instead of routing through a stunted handler that dropped ordering,
//! computed columns and window functions.
//!
//! A fetch produces the raw rows plus the schema the downstream should decode
//! them with:
//! - **Current**: bodies in their stored encoding (Binary Tuple for strict,
//!   MessagePack/legacy-JSON for schemaless), paired with the collection's real
//!   strict schema; the downstream normalizes as needed.
//! - **AsOf / AllVersions**: bodies already normalized to MessagePack (with the
//!   synthetic `_ts_*` temporal columns injected for the audit case) so
//!   `effective_schema` is `None` and the shared sort/window/computed/projection
//!   pipeline operates on a uniform shape.

use tracing::warn;

use nodedb_types::columnar::schema::{
    BITEMPORAL_RESERVED_COLUMNS, StrictSchema, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL,
};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::filter_match::matches_with_resolved_schema;
use crate::data::executor::task::ExecutionTask;
use crate::data::executor::{doc_format, strict_format};

/// Which temporal slice of a document collection a scan fetches.
pub(in crate::data::executor) enum DocScanMode {
    /// Newest live version per document. Bitemporal collections read current
    /// state from the versioned store; plain collections from the live table.
    Current,
    /// Newest version per document visible at a system-time cutoff and/or a
    /// valid-time instant (`AS OF SYSTEM TIME` / `AS OF VALID TIME`).
    AsOf {
        system_as_of_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    },
    /// Every system-time version of every document (`AS OF SYSTEM TIME NULL`
    /// audit log), each row carrying the synthetic `_ts_*` temporal columns.
    AllVersions { valid_at_ms: Option<i64> },
}

impl DocScanMode {
    /// The current-time read is the only mode that folds this transaction's
    /// staging overlay onto the base result — temporal reads never see staged
    /// (current-version-only) writes.
    pub(in crate::data::executor) fn is_current(&self) -> bool {
        matches!(self, DocScanMode::Current)
    }
}

/// Borrowed inputs for [`CoreLoop::document_scan_fetch`].
pub(in crate::data::executor) struct DocFetchParams<'a> {
    pub collection: &'a str,
    pub mode: &'a DocScanMode,
    pub limit: usize,
    pub offset: usize,
    pub filter_predicates: &'a [ScanFilter],
    pub strict_schema: Option<&'a StrictSchema>,
}

/// Raw rows plus the schema the downstream should decode them with.
pub(in crate::data::executor) struct FetchedRows {
    pub rows: Vec<(String, Vec<u8>)>,
    pub effective_schema: Option<StrictSchema>,
}

impl CoreLoop {
    /// Fetch the raw rows for a document scan according to `mode`, feeding the
    /// shared downstream shaping pipeline in [`super::scan`].
    pub(in crate::data::executor) fn document_scan_fetch(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: DocFetchParams<'_>,
    ) -> crate::Result<FetchedRows> {
        let collection = params.collection;
        let limit = params.limit;
        let offset = params.offset;
        let filter_predicates = params.filter_predicates;
        let strict_schema = params.strict_schema;

        match params.mode {
            DocScanMode::Current => self.fetch_current(task, tid, &params),
            DocScanMode::AsOf {
                system_as_of_ms,
                valid_at_ms,
            } => {
                // `versioned_scan_as_of` returns each version's stored body
                // verbatim — strict bodies are Binary Tuples, schemaless bodies
                // may be legacy JSON. Normalize to standard MessagePack so the
                // shared sort/window/computed/projection pipeline (which scans
                // msgpack) operates uniformly, then hand it downstream with no
                // schema (bodies are already normalized).
                let predicate = |body: &[u8]| {
                    matches_with_resolved_schema(strict_schema, filter_predicates, body)
                };
                let scan_limit = offset.saturating_add(limit);
                let raw = self.sparse.versioned_scan_as_of(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id: task.request.database_id.as_u64(),
                        tenant: tid,
                        coll: collection,
                        sys_cutoff_ms: *system_as_of_ms,
                        valid_at_ms: *valid_at_ms,
                        limit: scan_limit,
                    },
                    &predicate,
                )?;
                let rows = raw
                    .into_iter()
                    .map(|(doc_id, body)| (doc_id, normalize_body(&body, strict_schema)))
                    .collect();
                Ok(FetchedRows {
                    rows,
                    effective_schema: None,
                })
            }
            DocScanMode::AllVersions { valid_at_ms } => {
                // Every system-time version of every document. Each version is
                // normalized to MessagePack and gets the synthetic `_ts_*`
                // temporal columns injected BEFORE the shared downstream runs,
                // so a user can `SELECT` / `ORDER BY` / project on them.
                let predicate = |body: &[u8]| {
                    matches_with_resolved_schema(strict_schema, filter_predicates, body)
                };
                let scan_limit = offset.saturating_add(limit);
                let raw = self.sparse.versioned_scan_all(
                    task.request.database_id.as_u64(),
                    tid,
                    collection,
                    *valid_at_ms,
                    scan_limit,
                    &predicate,
                )?;
                let mut rows: Vec<(String, Vec<u8>)> = Vec::with_capacity(raw.len());
                for row in raw {
                    let msgpack_body = match strict_schema {
                        Some(schema) => strict_audit_body(&row.body, schema)?,
                        None => row.body,
                    };
                    let with_ts = inject_temporal_columns(
                        &msgpack_body,
                        row.system_from_ms,
                        row.valid_from_ms,
                        row.valid_until_ms,
                    )?;
                    rows.push((row.doc_id, with_ts));
                }
                Ok(FetchedRows {
                    rows,
                    effective_schema: None,
                })
            }
        }
    }

    /// Newest live version per document (current-time read). Bitemporal
    /// collections read current state from the versioned store; plain
    /// collections from the live table with a `scan_collection` fallback.
    fn fetch_current(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: &DocFetchParams<'_>,
    ) -> crate::Result<FetchedRows> {
        let collection = params.collection;
        let limit = params.limit;
        let offset = params.offset;
        let filter_predicates = params.filter_predicates;
        let strict_schema = params.strict_schema;

        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let fetch_limit = crate::data::executor::handlers::scan_budget::fetch_limit_for(
            limit,
            offset,
            scan_budget_bytes,
        );
        let database_id = task.request.database_id.as_u64();
        let bitemporal = self.is_bitemporal(database_id, tid, collection);

        let matches = |value: &[u8]| -> bool {
            if filter_predicates.is_empty() {
                return true;
            }
            matches_with_resolved_schema(strict_schema, filter_predicates, value)
        };

        let rows = if filter_predicates.is_empty() {
            if bitemporal {
                self.sparse.versioned_scan_as_of(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        sys_cutoff_ms: None,
                        valid_at_ms: None,
                        limit: fetch_limit,
                    },
                    &|_| true,
                )?
            } else {
                let sparse_result =
                    self.sparse
                        .scan_documents(database_id, tid, collection, fetch_limit);
                match sparse_result {
                    Ok(docs) if docs.is_empty() => {
                        let fallback =
                            self.scan_collection(database_id, tid, collection, fetch_limit)?;
                        if !fallback.is_empty() {
                            warn!(
                                core = self.core_id,
                                %collection,
                                count = fallback.len(),
                                "document scan fallback to scan_collection"
                            );
                        }
                        fallback
                    }
                    other => other?,
                }
            }
        } else if strict_schema.is_some() {
            if bitemporal {
                self.sparse.versioned_scan_as_of(
                    crate::engine::sparse::btree_versioned::VersionedScanParams {
                        database_id,
                        tenant: tid,
                        coll: collection,
                        sys_cutoff_ms: None,
                        valid_at_ms: None,
                        limit: fetch_limit,
                    },
                    &matches,
                )?
            } else {
                self.sparse.scan_documents_filtered(
                    database_id,
                    tid,
                    collection,
                    fetch_limit,
                    &matches,
                )?
            }
        } else if bitemporal {
            self.sparse.versioned_scan_as_of(
                crate::engine::sparse::btree_versioned::VersionedScanParams {
                    database_id,
                    tenant: tid,
                    coll: collection,
                    sys_cutoff_ms: None,
                    valid_at_ms: None,
                    limit: fetch_limit,
                },
                &matches,
            )?
        } else {
            let sparse_result = self.sparse.scan_documents_filtered(
                database_id,
                tid,
                collection,
                fetch_limit,
                &matches,
            );
            match sparse_result {
                Ok(docs) if docs.is_empty() => self
                    .scan_collection(database_id, tid, collection, fetch_limit)?
                    .into_iter()
                    .filter(|(_, data)| matches(data))
                    .collect(),
                other => other?,
            }
        };

        Ok(FetchedRows {
            rows,
            effective_schema: strict_schema.cloned(),
        })
    }
}

/// Normalize a stored versioned body to standard MessagePack: strict Binary
/// Tuples via the schema, schemaless (possibly legacy-JSON) bodies via
/// [`doc_format::json_to_msgpack`] (which passes through already-msgpack maps).
fn normalize_body(body: &[u8], strict_schema: Option<&StrictSchema>) -> Vec<u8> {
    match strict_schema {
        Some(schema) => strict_format::binary_tuple_to_msgpack(body, schema)
            .unwrap_or_else(|| doc_format::json_to_msgpack(body)),
        None => doc_format::json_to_msgpack(body),
    }
}

/// Decode a strict row's Binary Tuple `body` into MessagePack via the
/// collection's schema, then strip the reserved bitemporal bookkeeping columns
/// (`__system_from_ms`, `__valid_from_ms`, `__valid_until_ms`) so the audit-log
/// output shape stays identical to the schemaless path: user columns plus the
/// synthetic temporal triple (injected by the caller via
/// [`inject_temporal_columns`]). The authoritative valid-time is taken from the
/// row's stored envelope (carried on `VersionedRow`), not from these slots, so
/// both Document engines surface identical temporal columns.
fn strict_audit_body(body: &[u8], schema: &StrictSchema) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;

    let msgpack = strict_format::binary_tuple_to_msgpack(body, schema).ok_or_else(|| {
        crate::Error::Serialization {
            format: "binary-tuple".into(),
            detail: "decode strict document body for audit-log scan".into(),
        }
    })?;
    let value =
        nodedb_types::value_from_msgpack(&msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode strict document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        other => {
            return Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("strict audit-log body decoded to non-object value: {other:?}"),
            });
        }
    };
    for reserved in BITEMPORAL_RESERVED_COLUMNS {
        obj.remove(reserved);
    }
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode stripped strict audit-log body: {e}"),
    })
}

/// Decode the MessagePack document body, insert/overwrite the three synthetic
/// user-facing audit temporal columns (`_ts_system`, `_ts_valid_from`,
/// `_ts_valid_until`) from the version's real stored temporal coordinates, and
/// re-encode. Valid-time is surfaced raw — `i64::MIN` / `i64::MAX` sentinels
/// mean "unbounded" (matching how columnar/timeseries emit their real Int64
/// temporal columns). Non-object bodies are wrapped in a fresh object carrying
/// only the temporal columns. The triple is uniform across both Document
/// engines and columnar/timeseries.
fn inject_temporal_columns(
    body: &[u8],
    system_from_ms: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;
    let value =
        nodedb_types::value_from_msgpack(body).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        _ => std::collections::HashMap::new(),
    };
    obj.insert(TS_SYSTEM.to_string(), Value::Integer(system_from_ms));
    obj.insert(TS_VALID_FROM.to_string(), Value::Integer(valid_from_ms));
    obj.insert(TS_VALID_UNTIL.to_string(), Value::Integer(valid_until_ms));
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode document body with audit temporal columns: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn obj(pairs: &[(&str, Value)]) -> Vec<u8> {
        let mut m = std::collections::HashMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        nodedb_types::value_to_msgpack(&Value::Object(m)).expect("encode object body")
    }

    fn decode(bytes: &[u8]) -> std::collections::HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(bytes).expect("decode") {
            Value::Object(m) => m,
            other => panic!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn inject_adds_temporal_columns_and_preserves_body_fields() {
        let body = obj(&[
            ("v", Value::Integer(1)),
            ("name", Value::String("alice".into())),
        ]);
        let out = inject_temporal_columns(&body, 1_700_000_000_123, 10, 20).unwrap();
        let m = decode(&out);
        assert_eq!(m.get("v"), Some(&Value::Integer(1)));
        assert_eq!(m.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(1_700_000_000_123)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(10)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(20)));
    }

    #[test]
    fn inject_overwrites_any_preexisting_temporal_columns() {
        // A document that happens to carry temporal fields of its own must not
        // shadow the version's true temporal coordinates in the audit output.
        let body = obj(&[
            (TS_SYSTEM, Value::Integer(-1)),
            (TS_VALID_FROM, Value::Integer(-2)),
            (TS_VALID_UNTIL, Value::Integer(-3)),
            ("v", Value::Integer(2)),
        ]);
        let out = inject_temporal_columns(&body, 999, 111, 222).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(999)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(111)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(222)));
        assert_eq!(m.get("v"), Some(&Value::Integer(2)));
    }

    #[test]
    fn inject_surfaces_unbounded_valid_time_sentinels() {
        let body = obj(&[("v", Value::Integer(1))]);
        let out = inject_temporal_columns(&body, 5, i64::MIN, i64::MAX).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(i64::MIN)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(i64::MAX)));
    }

    #[test]
    fn inject_wraps_non_object_body_in_fresh_object() {
        let body = nodedb_types::value_to_msgpack(&Value::Integer(42)).unwrap();
        let out = inject_temporal_columns(&body, 7, 8, 9).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(7)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(8)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(9)));
        assert_eq!(
            m.len(),
            3,
            "non-object body yields a fresh object carrying only the temporal columns"
        );
    }
}
