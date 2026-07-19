// SPDX-License-Identifier: BUSL-1.1

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use nodedb_query::msgpack_scan;

use super::hash::{HashIndex, ProbeParams, probe_hash_index};
use super::params::HashJoinParams;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_hash_join(
        &mut self,
        p: HashJoinParams<'_>,
    ) -> Response {
        let HashJoinParams {
            join,
            tid,
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        } = p;

        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            left_alias = left_alias.unwrap_or(""),
            right_alias = right_alias.unwrap_or(""),
            keys = join.on.len(),
            %join.join_type,
            has_left_input = left_input.is_some(),
            "hash join"
        );

        // Derive a finite fetch-ceiling from the per-query byte budget so
        // the underlying KV scan never calls Vec::with_capacity(usize::MAX).
        // For an unbounded join (no SQL LIMIT) this evaluates to
        // budget_bytes / 16 + 1 (floored at 1000), which is tight enough to
        // prevent OOM but large enough not to silently truncate any real
        // workload. The post-materialisation byte-budget guards below are the
        // authoritative overflow check; this ceiling is only a pre-fetch hint.
        let budget = self.query_tuning.max_scan_result_bytes;
        let join_filters: Vec<crate::bridge::scan_filter::ScanFilter> =
            if join.join_filter_bytes.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(join.join_filter_bytes) {
                    Ok(filters) => filters,
                    Err(e) => {
                        return self.response_error(
                            join.task,
                            ErrorCode::Internal {
                                detail: format!("decode join ON filters: {e}"),
                            },
                        );
                    }
                }
            };
        let scan_limit =
            crate::data::executor::handlers::scan_budget::fetch_limit_for(usize::MAX, 0, budget);

        // Gating predicate for the memory-bounded (grace-hash spill) completion
        // path: BOTH sides must be plain local scans — no Exchange sub-plan and
        // no bitmap prefilter on either side. Captured before the bitmap
        // sub-plans below are consumed by `.map`.
        //
        // Declared deferral: spilling Exchange-supplied or bitmap-prefiltered
        // sides needs a streaming Exchange + streaming key-normalization that
        // does not exist yet (the rows for those sides are produced by
        // `execute_plan` / a prefiltered scan plan and decoded all at once).
        // Until that lands, those cases keep today's behavior exactly:
        // materialize each side and surface `ResourcesExhausted` on over-budget.
        let both_sides_local = left_input.is_none()
            && right_input.is_none()
            && left_bitmap.is_none()
            && right_bitmap.is_none()
            && join_filters.is_empty();

        // Evaluate bitmap sub-plans first. These prefilter the local scan for
        // each side, pushing surrogate exclusion into the document engine before
        // any msgpack decode occurs.
        let left_bm = left_bitmap.map(|sub_plan| {
            crate::data::executor::dispatch::bitmap::hashjoin_inline::run_bitmap_subplan(
                self, join.task, sub_plan,
            )
        });
        let right_bm = right_bitmap.map(|sub_plan| {
            crate::data::executor::dispatch::bitmap::hashjoin_inline::run_bitmap_subplan(
                self, join.task, sub_plan,
            )
        });

        // Memory-bounded completion path. Only when BOTH sides are plain local
        // scans can we stream them. For every both-local, NON-CROSS join this
        // returns `Some` and COMPLETES the join without ever surfacing
        // `ResourcesExhausted` for over-input-budget: the build side buffers
        // under budget then streams the probe in bounded batches against the
        // in-memory index, or — on crossing budget — spills to a grace-hash
        // partitioner that streams the probe side. It returns `None` ONLY for a
        // cross / keyless join (declared deferral: cross-join probe streaming is
        // a separate unit), in which case the caller falls through to the
        // unchanged in-memory path below (which handles the cartesian product).
        if both_sides_local
            && let Some(resp) = self.try_grace_hash_join(
                &join,
                tid,
                crate::data::executor::handlers::join::grace_drive::LocalJoinSides {
                    left_collection,
                    right_collection,
                    left_alias,
                    right_alias,
                },
                budget,
            )
        {
            return resp;
        }

        // Resolve the left side.
        //
        // When `left_input` is `Some` (e.g. a `ProviderScan` supplied by the
        // coordinator), we execute that sub-plan and use its rows directly.
        // The join keys from the plan may carry a collection-name prefix
        // (e.g. `"left_coll.field"`) that was resolved by the planner when the
        // input was still an Exchange child. After coordinator resolution the
        // rows arrive pre-flattened but the key names may still carry that
        // prefix. We strip the prefix with suffix matching so the probe-side
        // hash lookup finds the right field regardless of qualification.
        let (left_docs, left_key_strs) = if let Some(sub_plan) = left_input {
            let sub_response = self.execute_plan(join.task, sub_plan);
            let docs =
                match crate::data::executor::response_codec::decode_response_to_docs(&sub_response)
                {
                    Some(d) => d,
                    None => return sub_response,
                };

            // Resolve join keys: if a key is absent as-is, walk the first doc's
            // map looking for a field whose name ends with ".<key>" (i.e. a
            // collection-prefix–qualified key such as "orders.amount") and use
            // that fully-qualified name instead.
            let mut resolved: Vec<String> = join.on.iter().map(|(l, _)| l.clone()).collect();
            if let Some((_, first_doc)) = docs.first() {
                for key in &mut resolved {
                    if msgpack_scan::extract_field(first_doc, 0, key).is_none() {
                        let suffix = format!(".{key}");
                        if let Some((count, mut pos)) = msgpack_scan::map_header(first_doc, 0) {
                            let mut found: Option<String> = None;
                            for _ in 0..count {
                                if let Some(field_name) = msgpack_scan::read_str(first_doc, pos)
                                    && field_name.ends_with(&suffix)
                                {
                                    found = Some(field_name.to_string());
                                    break;
                                }
                                pos = match msgpack_scan::skip_value(first_doc, pos) {
                                    Some(p) => p,
                                    None => break,
                                };
                                pos = match msgpack_scan::skip_value(first_doc, pos) {
                                    Some(p) => p,
                                    None => break,
                                };
                            }
                            if let Some(resolved_key) = found {
                                *key = resolved_key;
                            }
                        }
                    }
                }
            }

            (docs, resolved)
        } else if let Some(bm) = left_bm {
            let docs = match crate::data::executor::dispatch::bitmap::hashjoin_inline::prefiltered_scan_plan(
                left_collection,
                scan_limit,
                bm,
            ) {
                Some(scan_plan) => {
                    let resp = self.execute_plan(join.task, &scan_plan);
                    // Forward a failing sub-plan response (e.g. ResourcesExhausted
                    // from the bitmap scan) instead of swallowing it to an empty
                    // Vec, which would silently return a zero-row join.
                    match crate::data::executor::response_codec::decode_response_to_docs(&resp) {
                        Some(d) => d,
                        None => return resp,
                    }
                }
                None => match self.scan_collection(join.task.request.database_id.as_u64(), tid, left_collection, scan_limit) {
                    Ok(d) => d,
                    Err(e) => {
                        return self.response_error(
                            join.task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                },
            };
            let keys = join.on.iter().map(|(l, _)| l.clone()).collect();
            (docs, keys)
        } else {
            let docs = match self.scan_collection(
                join.task.request.database_id.as_u64(),
                tid,
                left_collection,
                scan_limit,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        join.task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };
            let keys = join.on.iter().map(|(l, _)| l.clone()).collect();
            (docs, keys)
        };

        // Memory-budget guard on the hash-join probe side (left).
        //
        // Symmetric with the build-side guard below. The probe side is fully
        // materialised before the right side is scanned. We check it first so
        // that an over-budget left input surfaces the error immediately rather
        // than after also materialising the right side. A budget of 0 disables
        // the check (treated as unlimited), matching the scan-budget convention.
        if let Some(err) = self.join_side_over_budget(join.task, &left_docs, budget) {
            return err;
        }

        // Resolve the right side.
        let right_docs = if let Some(sub_plan) = right_input {
            let sub_response = self.execute_plan(join.task, sub_plan);
            match crate::data::executor::response_codec::decode_response_to_docs(&sub_response) {
                Some(docs) => docs,
                None => return sub_response,
            }
        } else if let Some(bm) = right_bm {
            match crate::data::executor::dispatch::bitmap::hashjoin_inline::prefiltered_scan_plan(
                right_collection,
                scan_limit,
                bm,
            ) {
                Some(scan_plan) => {
                    let resp = self.execute_plan(join.task, &scan_plan);
                    // Forward a failing sub-plan response (e.g. ResourcesExhausted
                    // from the bitmap scan) instead of swallowing it to an empty
                    // Vec, which would silently return a zero-row join.
                    match crate::data::executor::response_codec::decode_response_to_docs(&resp) {
                        Some(d) => d,
                        None => return resp,
                    }
                }
                None => match self.scan_collection(
                    join.task.request.database_id.as_u64(),
                    tid,
                    right_collection,
                    scan_limit,
                ) {
                    Ok(d) => d,
                    Err(e) => {
                        return self.response_error(
                            join.task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                },
            }
        } else {
            match self.scan_collection(
                join.task.request.database_id.as_u64(),
                tid,
                right_collection,
                scan_limit,
            ) {
                Ok(d) => d,
                Err(e) => {
                    return self.response_error(
                        join.task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        };

        let left_prefix = left_alias.unwrap_or(left_collection);
        let right_prefix = right_alias.unwrap_or(right_collection);

        let left_keys: Vec<&str> = left_key_strs.iter().map(|s| s.as_str()).collect();
        let right_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();

        // Memory-budget guard on the hash-join build side (right).
        //
        // The build side is fully materialised into `right_docs` before the
        // `HashIndex` is constructed. For a large build side this allocation
        // can OOM the TPC core. We check its byte total against the same
        // `max_scan_result_bytes` budget used by unbounded document/KV/columnar
        // scans. A budget of 0 disables the check (treated as unlimited),
        // matching the scan-budget convention.
        //
        // This is NOT a spill path — we do not drop or truncate rows.  We
        // surface a deterministic `ResourcesExhausted` error so the caller can
        // retry with a narrower predicate or explicit LIMIT.
        if let Some(err) = self.join_side_over_budget(join.task, &right_docs, budget) {
            return err;
        }

        let right_index = HashIndex::build(&right_docs, &right_keys);

        // Bound the emitted output.
        //
        // An explicit user `LIMIT n` (`join.limit != usize::MAX`) is honored
        // exactly — emit at most `n` rows, no further budget check. A no-LIMIT
        // join (`usize::MAX`) must NOT silently truncate at a default cap:
        // instead we bound its output by the per-query byte budget. We derive a
        // budget row-ceiling (`+1` so hitting it proves the output exceeds the
        // budget) and, if the probe fills it, surface a deterministic
        // `ResourcesExhausted` rather than dropping the excess rows. A budget
        // of 0 means "unlimited" → truly unbounded output.
        let (probe_limit, enforce_output_budget) = if join.limit != usize::MAX {
            (join.limit, false)
        } else if budget == 0 {
            (usize::MAX, false)
        } else {
            (
                crate::data::executor::handlers::scan_budget::fetch_limit_for(
                    usize::MAX,
                    0,
                    budget,
                ),
                true,
            )
        };

        let mut results = probe_hash_index(&ProbeParams {
            probe_docs: &left_docs,
            index: &right_index,
            index_docs: &right_docs,
            probe_keys: &left_keys,
            join_type: join.join_type,
            limit: probe_limit,
            probe_collection: left_prefix,
            index_collection: right_prefix,
            join_filters: &join_filters,
            emit_unmatched_right: true,
        });

        if enforce_output_budget && results.len() >= probe_limit {
            return self.response_error(join.task, ErrorCode::ResourcesExhausted);
        }

        if let Err(e) = join.filter_and_project(&mut results) {
            return self.response_error(
                join.task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(join.task, payload)
    }
}

#[cfg(test)]
mod tests {
    use crate::data::executor::handlers::scan_budget::{budget_exceeded, scan_bytes_exceeded};

    /// The budget guard helper (`scan_bytes_exceeded`) that backs the hash-join
    /// build-side memory check behaves correctly: it returns `true` only when the
    /// accumulated bytes exceed the budget, and treats a budget of 0 as unlimited.
    ///
    /// A full end-to-end test (build side over-budget → `ResourcesExhausted`
    /// response) requires a live `CoreLoop`, which is covered by the
    /// integration/cluster test suite. This unit test verifies the threshold
    /// function that gates the early return.
    #[test]
    fn build_side_budget_guard_helper_enforces_limit() {
        // One large row that exceeds the budget.
        let over_budget: Vec<(String, Vec<u8>)> = vec![
            ("id1".to_string(), vec![0u8; 600]),
            ("id2".to_string(), vec![0u8; 600]),
        ];
        // 603 + 603 = 1206 > 1024
        assert!(scan_bytes_exceeded(&over_budget, 1024));

        // Two small rows that fit within budget.
        let within_budget: Vec<(String, Vec<u8>)> = vec![
            ("id1".to_string(), vec![0u8; 100]),
            ("id2".to_string(), vec![0u8; 100]),
        ];
        // 103 + 103 = 206 <= 1024
        assert!(!scan_bytes_exceeded(&within_budget, 1024));
    }

    #[test]
    fn build_side_budget_zero_is_unlimited() {
        // A budget of 0 must disable the guard (matching the scan convention).
        let huge: Vec<(String, Vec<u8>)> = vec![("id".to_string(), vec![0u8; 1_000_000])];
        assert!(!scan_bytes_exceeded(&huge, 0));
        // Sanity: `budget_exceeded` itself also treats 0 as unlimited.
        assert!(!budget_exceeded(usize::MAX, 0));
    }

    #[test]
    fn build_side_budget_exactly_at_limit_is_allowed() {
        // Exactly on the budget boundary is NOT exceeded (strict >).
        // One row: value 1023 bytes + id "x" (1 byte) = 1024.
        let at_limit: Vec<(String, Vec<u8>)> = vec![("x".to_string(), vec![0u8; 1023])];
        assert!(!scan_bytes_exceeded(&at_limit, 1024));
    }
}
