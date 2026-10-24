// SPDX-License-Identifier: BUSL-1.1

//! KV Scan handler and filter extraction.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvScanParams;
use crate::engine::kv::current_ms;

/// Parameters for the KV SCAN handler.
pub(in crate::data::executor) struct KvScanHandlerParams<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    pub match_pattern: Option<&'a str>,
    pub filters: &'a [u8],
    pub projection: &'a [String],
    pub computed_columns_bytes: &'a [u8],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub surrogate_ceiling: Option<u32>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_scan(
        &self,
        task: &ExecutionTask,
        params: KvScanHandlerParams<'_>,
    ) -> Response {
        let crs = self.crs_context();
        let KvScanHandlerParams {
            did,
            tid,
            collection,
            cursor,
            count,
            match_pattern,
            filters,
            projection,
            computed_columns_bytes,
            sort_keys,
            surrogate_ceiling,
        } = params;

        debug!(core = self.core_id, %collection, count, "kv scan");

        // Scan-quiesce gate: refuse new scans against a draining
        // collection so the purge handler can unlink on-disk files
        // without racing an in-flight reader.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let now_ms = current_ms();

        // A no-LIMIT SQL `SELECT * FROM <kv>` arrives as `count == usize::MAX`.
        // Bound the engine fetch to a row ceiling derived from the per-query
        // memory budget (+1 row to detect "more exist") so the materialized
        // `Vec` cannot grow to the whole collection. The RESP cursor
        // pagination path always carries a finite `count`, so it is unaffected.
        let scan_budget_bytes = self.query_tuning.max_scan_result_bytes;
        let unbounded = count == usize::MAX;
        let fetch_count = if unbounded {
            // KV scans have no row offset (pagination is cursor-based).
            scan_budget::fetch_limit_for(count, 0, scan_budget_bytes)
        } else {
            count
        };

        // A malformed filter payload is an error. Reading it as "no filter"
        // would return every row of the collection.
        let filter_predicates = match decode_scan_filters(filters, "kv scan filter") {
            Ok(predicates) => predicates,
            Err(e) => return self.response_error(task, e),
        };

        // Index pushdown for one equality conjunct. The post-scan filter
        // below still checks every predicate on every row.
        let probe = eq_index_probe(&filter_predicates);
        let (mut entries, _next_cursor) = self.kv_engine.scan(KvScanParams {
            database_id: did,
            tenant_id: tid,
            collection,
            cursor,
            count: fetch_count,
            now_ms,
            match_pattern,
            filter_field: probe.as_ref().map(|(field, _)| *field),
            filter_value: probe.as_ref().map(|(_, key)| key.as_slice()),
            surrogate_ceiling,
        });

        // Read-your-own-writes: fold this transaction's staged KV writes
        // into the base scan result before the shared filter/sort/encode
        // pipeline below, which then treats merged and base rows alike.
        // `matches` always accepts here -- the per-entry filter loop further
        // down re-applies `filter_predicates` uniformly to every row
        // (base or merged), so there is no need to duplicate that check
        // in the merge itself.
        if let Some(txn_id) = task.request.txn_id {
            let coll_key = (
                crate::types::DatabaseId::new(did),
                crate::types::TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_kv_overlay_into_scan(
                txn_id,
                &coll_key,
                &mut entries,
                &|_key: &[u8], _value: &[u8]| true,
            );
        }

        // Bound an unbounded (no-LIMIT) scan by the memory budget. Sum the raw
        // key+value bytes and surface a deterministic error if the result would
        // exceed the budget rather than silently truncating it.
        if unbounded {
            let total = entries.iter().fold(0usize, |acc, (k, v)| {
                acc.saturating_add(k.len()).saturating_add(v.len())
            });
            if scan_budget::budget_exceeded(total, scan_budget_bytes) {
                return self.response_error(task, ErrorCode::ResourcesExhausted);
            }
        }

        // Build results as raw msgpack — no serde_json::Value intermediary.
        let mut result_entries: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        for (k, v) in &entries {
            // The two storage shapes (msgpack map vs raw bytes) are resolved by
            // the one shared shaper, so this scan, the streaming/materializing
            // scans, and a write's `RETURNING` projection cannot disagree about
            // what a KV row looks like. A private copy of that logic in this
            // loop risks diverging from the others on the raw-bytes case.
            let (_key_str, entry_mp) = crate::data::executor::scan_normalize::kv_row_to_doc(k, v);

            // Apply filter predicates post-scan (already works on raw msgpack).
            if !filter_predicates.is_empty() {
                match crate::bridge::scan_filter::ScanFilter::all_match_binary(
                    &filter_predicates,
                    &entry_mp,
                    &crs,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        return self.response_error(task, ErrorCode::from(e));
                    }
                }
            }

            result_entries.push(entry_mp);
        }

        result_entries = match self.apply_kv_projection_and_computed(
            task,
            result_entries,
            projection,
            computed_columns_bytes,
        ) {
            Ok(rows) => rows,
            Err(resp) => return resp,
        };

        if !sort_keys.is_empty()
            && let Err(e) =
                super::super::sort_utils::sort_msgpack_rows(&mut result_entries, sort_keys, &crs)
        {
            return self.response_error(task, e);
        }

        // Build response as flat msgpack array — same format as document/columnar scan.
        // RESP SCAN handles cursor pagination at its own handler layer.
        let mut payload =
            Vec::with_capacity(result_entries.iter().map(|e| e.len()).sum::<usize>() + 64);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, result_entries.len());
        for entry in &result_entries {
            payload.extend_from_slice(entry);
        }
        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }

    /// Apply projection and computed columns to a kv scan's raw msgpack rows.
    ///
    /// Runs before sort so `ORDER BY` can name a computed alias. An empty
    /// projection with non-empty computed columns keeps the full row (kv has
    /// no fixed schema to project against) and appends the computed aliases;
    /// a non-empty projection keeps only the named columns plus the computed
    /// aliases. Returns `Err(Response)` on malformed computed-column bytes
    /// or a per-row evaluation error, ready to return directly to the caller.
    fn apply_kv_projection_and_computed(
        &self,
        task: &ExecutionTask,
        result_entries: Vec<Vec<u8>>,
        projection: &[String],
        computed_columns_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, Response> {
        let crs = self.crs_context();
        if projection.is_empty() && computed_columns_bytes.is_empty() {
            return Ok(result_entries);
        }

        let computed_cols: Vec<crate::bridge::expr_eval::ComputedColumn> =
            if computed_columns_bytes.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(computed_columns_bytes) {
                    Ok(cols) => cols,
                    Err(e) => {
                        return Err(self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("kv scan: malformed computed bytes: {e}"),
                            },
                        ));
                    }
                }
            };

        if projection.is_empty() {
            crate::data::executor::handlers::provider_scan_compute::apply_windows_and_computed(
                result_entries,
                &[],
                computed_columns_bytes,
                &crs,
            )
            .map_err(|e| self.response_error(task, e))
        } else {
            let mut projected_entries = Vec::with_capacity(result_entries.len());
            for entry in &result_entries {
                let row = crate::data::executor::handlers::document::read::projection::apply_projection_msgpack(
                    entry,
                    &computed_cols,
                    projection,
                    &crs,
                )
                .map_err(|e| self.response_error(task, e))?;
                projected_entries.push(row);
            }
            Ok(projected_entries)
        }
    }
}

/// The secondary-index probe for a decoded filter set: the field and index
/// key of the first top-level `field = <literal>` conjunct.
///
/// A KV index files each value under its coerced-equality bucket, so the
/// literal's key reaches every row the coerced `=` accepts: `5` reaches a
/// stored `5`, `'5'` and `5.0`, and a timestamp reaches the stored strings
/// naming its instant. The key can also reach rows the `=` refuses, and the
/// post-scan filter drops them. A literal that equals nothing, NULL
/// included, gives no probe.
pub(in crate::data::executor) fn eq_index_probe(filters: &[ScanFilter]) -> Option<(&str, Vec<u8>)> {
    filters.iter().find_map(|f| {
        let plain_eq = matches!(f.op, FilterOp::Eq)
            && !f.field.is_empty()
            && f.clauses.is_empty()
            && f.expr.is_none();
        if !plain_eq {
            return None;
        }
        crate::engine::kv::index::kv_probe_key(&f.value).map(|key| (f.field.as_str(), key))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::kv::index::kv_index_key;
    use crate::engine::kv::test_support::row_surrogate;
    use crate::engine::kv::{KvEngine, KvPutParams, RegisterIndexParams};
    use nodedb_types::Value;

    fn eq(field: &str, value: Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op: FilterOp::Eq,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    #[test]
    fn probe_uses_the_first_plain_equality() {
        let filters = [
            eq("age", Value::Integer(5)),
            eq("region", Value::String("us-east".into())),
        ];
        assert_eq!(
            eq_index_probe(&filters),
            Some(("age", kv_index_key(&Value::Integer(5))))
        );
    }

    /// Numeric, numeric-string, datetime and text literals all probe, each
    /// under the key its coerced equals are filed under.
    #[test]
    fn every_comparable_literal_probes() {
        let five = kv_index_key(&Value::Integer(5));
        assert_eq!(
            eq_index_probe(&[eq("n", Value::String("5".into()))]),
            Some(("n", five.clone()))
        );
        assert_eq!(
            eq_index_probe(&[eq("n", Value::Float(5.0))]),
            Some(("n", five))
        );
        assert_eq!(
            eq_index_probe(&[eq("t", Value::String("2024-01-02".into()))]),
            Some((
                "t",
                kv_index_key(&Value::String("2024-01-02T00:00:00".into()))
            ))
        );
        assert_eq!(
            eq_index_probe(&[eq("r", Value::String("us".into()))]),
            Some(("r", kv_index_key(&Value::String("us".into()))))
        );
    }

    /// `= NULL` matches no row, so it gives no probe.
    #[test]
    fn a_null_literal_gives_no_probe() {
        assert_eq!(eq_index_probe(&[eq("n", Value::Null)]), None);
    }

    const NOW: u64 = 1_000;

    /// An engine with collection `c` indexed on field `n`.
    fn engine_indexed_on_n() -> KvEngine {
        let mut engine = KvEngine::new(NOW, 16, 0.75, 4, 64, 1000, 1024);
        engine.register_index(RegisterIndexParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            field: "n",
            field_position: 0,
            backfill: false,
            now_ms: NOW,
        });
        engine
    }

    fn put(engine: &mut KvEngine, key: &[u8], n: Value) {
        let row = Value::Object(std::collections::HashMap::from([("n".to_string(), n)]));
        let body = nodedb_types::value_to_msgpack(&row).expect("encode row");
        engine
            .put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                key,
                value: &body,
                ttl_ms: 0,
                now_ms: NOW,
                surrogate: row_surrogate(key),
            })
            .expect("a bound row writes");
    }

    /// The rows the engine returns for `n = literal` when the scan takes the
    /// probe `eq_index_probe` gives, sorted by key. The engine applies no
    /// predicate itself: a scan that ignored the probe returns every row.
    fn probed(engine: &KvEngine, literal: Value) -> Vec<Vec<u8>> {
        let filters = [eq("n", literal)];
        let (field, key) = eq_index_probe(&filters).expect("the literal probes");
        let (rows, _) = engine.scan(KvScanParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            cursor: &[],
            count: usize::MAX,
            now_ms: NOW,
            match_pattern: None,
            filter_field: Some(field),
            filter_value: Some(&key),
            surrogate_ceiling: None,
        });
        let mut keys: Vec<Vec<u8>> = rows.into_iter().map(|(key, _)| key).collect();
        keys.sort();
        keys
    }

    fn keys(names: &[&str]) -> Vec<Vec<u8>> {
        names.iter().map(|name| name.as_bytes().to_vec()).collect()
    }

    /// An integer literal reaches the rows storing it as an integer, as a
    /// numeric string and as an equal float, and only those: the other rows
    /// stay out, so the scan read the index.
    #[test]
    fn an_int_literal_reaches_every_equal_stored_form_through_the_index() {
        let mut engine = engine_indexed_on_n();
        put(&mut engine, b"int", Value::Integer(5));
        put(&mut engine, b"text", Value::String("5".into()));
        put(&mut engine, b"float", Value::Float(5.0));
        put(&mut engine, b"other", Value::Integer(6));
        put(&mut engine, b"word", Value::String("five".into()));

        let expected = keys(&["float", "int", "text"]);
        assert_eq!(probed(&engine, Value::Integer(5)), expected);
        assert_eq!(probed(&engine, Value::String("5".into())), expected);
        assert_eq!(probed(&engine, Value::Float(5.0)), expected);
    }

    /// A datetime literal reaches the rows storing its instant, as a
    /// datetime or as text in either ISO-8601 spelling.
    #[test]
    fn a_datetime_literal_reaches_every_stored_form_of_its_instant() {
        let instant = nodedb_types::NdbDateTime::parse("2024-01-02T03:04:05").expect("instant");
        let mut engine = engine_indexed_on_n();
        put(&mut engine, b"dt", Value::DateTime(instant));
        put(
            &mut engine,
            b"iso",
            Value::String("2024-01-02T03:04:05".into()),
        );
        put(
            &mut engine,
            b"spaced",
            Value::String("2024-01-02 03:04:05".into()),
        );
        put(
            &mut engine,
            b"later",
            Value::String("2024-01-03T03:04:05".into()),
        );

        let expected = keys(&["dt", "iso", "spaced"]);
        assert_eq!(probed(&engine, Value::DateTime(instant)), expected);
        assert_eq!(
            probed(&engine, Value::String("2024-01-02T03:04:05".into())),
            expected
        );
    }

    /// An update re-files a row under its new value's key and a delete
    /// removes it, whatever form the value takes.
    #[test]
    fn updates_and_deletes_keep_the_index_consistent() {
        let five = Value::Integer(5);
        let seven = Value::Integer(7);
        let mut engine = engine_indexed_on_n();
        put(&mut engine, b"a", Value::Integer(5));
        put(&mut engine, b"b", Value::String("5".into()));
        put(&mut engine, b"c", Value::String("unrelated".into()));

        put(&mut engine, b"a", Value::Float(7.0));
        assert_eq!(probed(&engine, five.clone()), keys(&["b"]));
        assert_eq!(probed(&engine, seven.clone()), keys(&["a"]));

        put(&mut engine, b"b", Value::String("7".into()));
        assert!(engine.index_lookup_eq(0, 1, "c", "n", &five).is_empty());
        assert_eq!(probed(&engine, seven.clone()), keys(&["a", "b"]));

        assert_eq!(engine.delete(0, 1, "c", &[b"a".to_vec()], NOW), 1);
        assert_eq!(probed(&engine, seven.clone()), keys(&["b"]));
        assert_eq!(engine.delete(0, 1, "c", &[b"b".to_vec()], NOW), 1);
        assert!(engine.index_lookup_eq(0, 1, "c", "n", &seven).is_empty());
        assert_eq!(
            engine.index_lookup_eq(0, 1, "c", "n", &Value::String("unrelated".into())),
            keys(&["c"])
        );
    }

    /// A miss on a complete index returns no row: the engine scan applies no
    /// predicate, so a scan that read the table would return all three.
    #[test]
    fn a_miss_on_a_complete_index_returns_nothing_without_scanning() {
        let mut engine = engine_indexed_on_n();
        put(&mut engine, b"a", Value::Integer(5));
        put(&mut engine, b"b", Value::String("5".into()));
        put(&mut engine, b"c", Value::String("five".into()));

        assert!(probed(&engine, Value::Integer(9)).is_empty());
        assert!(probed(&engine, Value::String("nine".into())).is_empty());
    }

    /// The keys of the rows whose `n` a WHERE `n = literal` accepts, read
    /// through the scan path the handler takes: the probe, then the
    /// predicate on every row the engine returns.
    fn matching(engine: &KvEngine, literal: Value) -> Vec<Vec<u8>> {
        let filters = [eq("n", literal.clone())];
        let probe = eq_index_probe(&filters);
        let (rows, _) = engine.scan(KvScanParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            cursor: &[],
            count: usize::MAX,
            now_ms: NOW,
            match_pattern: None,
            filter_field: probe.as_ref().map(|(field, _)| *field),
            filter_value: probe.as_ref().map(|(_, key)| key.as_slice()),
            surrogate_ceiling: None,
        });
        let mut keys: Vec<Vec<u8>> = rows
            .into_iter()
            .filter(|(_, body)| match nodedb_types::value_from_msgpack(body) {
                Ok(Value::Object(map)) => map.get("n").is_some_and(|n| n.eq_coerced(&literal)),
                _ => false,
            })
            .map(|(key, _)| key)
            .collect();
        keys.sort();
        keys
    }

    /// An index registered with `backfill=false` over existing rows lacks
    /// them, so an equality reads the whole collection and finds the rows
    /// written before the index and after it.
    #[test]
    fn a_late_index_without_backfill_still_finds_older_rows() {
        let mut engine = KvEngine::new(NOW, 16, 0.75, 4, 64, 1000, 1024);
        put(&mut engine, b"old", Value::Integer(5));
        put(&mut engine, b"old_other", Value::Integer(6));
        engine.register_index(RegisterIndexParams {
            database_id: 0,
            tenant_id: 1,
            collection: "c",
            field: "n",
            field_position: 0,
            backfill: false,
            now_ms: NOW,
        });
        put(&mut engine, b"new", Value::String("5".into()));
        put(&mut engine, b"new_other", Value::Integer(7));

        assert_eq!(
            engine.index_lookup_eq(0, 1, "c", "n", &Value::Integer(5)),
            keys(&["new"]),
            "test premise: the index lacks the row written before it"
        );
        assert_eq!(matching(&engine, Value::Integer(5)), keys(&["new", "old"]));
        assert_eq!(matching(&engine, Value::Integer(6)), keys(&["old_other"]));
    }

    #[test]
    fn non_equality_or_grouped_filter_gives_no_probe() {
        let mut ne = eq("region", Value::String("us".into()));
        ne.op = FilterOp::Ne;
        let mut or = eq("region", Value::String("us".into()));
        or.clauses = vec![vec![eq("region", Value::String("eu".into()))]];
        assert_eq!(eq_index_probe(&[ne, or]), None);
    }
}
