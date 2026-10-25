// SPDX-License-Identifier: BUSL-1.1

//! Accumulate phase: stream `(doc_id, bytes)` rows through a spill-backed
//! GROUP BY accumulator and return the consolidated per-group `GroupState`
//! map (plus the optional sub-group map).
//!
//! Split out of `aggregate_over_docs` so the partial-state producer
//! (`PartialAggregateState`) can reuse the exact same accumulation without
//! finalizing — the byte-identical group-key encoding is what makes the
//! producer's emitted keys mergeable on the consumer side.

use std::collections::HashMap;

use nodedb_mem::{EngineId, ScopedMemory};

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use crate::data::executor::handlers::spill::groupby::GroupBySpiller;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_query::msgpack_scan;

/// Outer per-group accumulators plus the sub-group accumulators (keyed
/// `outer_key → sub_key → GroupState`). The sub-group map is empty when no
/// sub-aggregation was requested.
pub(in crate::data::executor) type AccumulatedGroups = (
    HashMap<String, GroupState>,
    HashMap<String, HashMap<String, GroupState>>,
);

/// Borrowed inputs to [`CoreLoop::accumulate_groups`]: the doc set, the outer
/// GROUP BY / aggregate specs, the WHERE-filter bytes, and the optional
/// sub-grouping specs.
pub(in crate::data::executor) struct AccumulateGroupsParams<'a> {
    pub docs: &'a [(String, Vec<u8>)],
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    /// Database this accumulation charges memory reservations against.
    pub database_id: DatabaseId,
    /// Tenant this accumulation charges memory reservations against.
    pub tenant_id: TenantId,
}

impl CoreLoop {
    /// Accumulate `docs` into a consolidated `HashMap<group_key, GroupState>`
    /// (and, when `sub_*` are non-empty, a nested sub-group map).
    ///
    /// This is the spill-feed half of `aggregate_over_docs`: WHERE filters and
    /// GROUP BY (plus optional sub-grouping via composite keys) are applied
    /// here; HAVING / ORDER BY / LIMIT / encoding are the caller's concern
    /// (`finalize_groups`).
    ///
    /// The group-key encoding (`msgpack_scan::build_group_key` → JSON-array
    /// string) is the same one the finalize split logic parses back, and the
    /// same one the distributed-shuffle producer/consumer rely on for
    /// byte-identical merge keys.
    pub(in crate::data::executor) fn accumulate_groups(
        &mut self,
        params: AccumulateGroupsParams<'_>,
    ) -> crate::Result<AccumulatedGroups> {
        let AccumulateGroupsParams {
            docs,
            group_by,
            aggregates,
            filters,
            sub_group_by,
            sub_aggregates,
            database_id,
            tenant_id,
        } = params;
        let filter_predicates =
            crate::bridge::scan_filter::decode_scan_filters(filters, "aggregate WHERE filter")?;

        let use_field_index = filter_predicates.len() + group_by.len() >= 2;
        let need_sub = !sub_group_by.is_empty() && !sub_aggregates.is_empty();
        // Sub-group keys are still plain column names; lift them to bare-column
        // specs so the sub-key uses the identical `build_group_key` encoding.
        let sub_specs: Vec<GroupKeySpec> = if need_sub {
            sub_group_by
                .iter()
                .map(|s| GroupKeySpec::column(s.as_str()))
                .collect()
        } else {
            Vec::new()
        };

        // Spill-to-disk GROUP BY accumulator.
        //
        // Sub-groups are flattened into the same spiller using composite keys:
        //   outer_key + '\x1F' + sub_key
        // U+001F (ASCII Unit Separator) cannot appear in JSON-encoded string
        // values, so the composite key is unambiguous.  At finalize time, keys
        // containing '\x1F' are split to reconstruct outer/sub structure.
        let spill_dir = self
            .data_dir
            .join("groupby-spill")
            .join(format!("core-{}", self.core_id));
        let cap = self.query_tuning.groupby_max_groups_in_mem;

        let memory = Some(ScopedMemory::new(
            self.governor.clone(),
            database_id,
            tenant_id,
            EngineId::Query,
        ));
        let mut spiller = GroupBySpiller::new(spill_dir, cap, memory)?;

        // Accumulate matching documents. Spill errors are fatal and surfaced
        // once accumulation stops — the first error breaks out of both loops.
        let mut spill_err: Option<crate::Error> = None;
        let chunk_size = self.query_tuning.aggregate_chunk_size;
        let crs = self.crs_context();

        for chunk in docs.chunks(chunk_size) {
            if spill_err.is_some() {
                break;
            }
            for (_, value) in chunk {
                // WHERE-filter evaluation is fatal to the whole accumulation
                // on a division/modulo-by-zero, exactly like a spill error
                // below — both break out via `spill_err`.
                let outer_key = if use_field_index {
                    let idx = msgpack_scan::FieldIndex::build(value, 0)
                        .unwrap_or_else(msgpack_scan::FieldIndex::empty);
                    match ScanFilter::all_match_binary_indexed(
                        &filter_predicates,
                        value,
                        &idx,
                        &crs,
                    ) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            spill_err = Some(crate::Error::from(e));
                            break;
                        }
                    }
                    match msgpack_scan::group_key::build_group_key_indexed(
                        value, group_by, &idx, &crs,
                    ) {
                        Ok(k) => k,
                        Err(e) => {
                            spill_err = Some(crate::Error::from(e));
                            break;
                        }
                    }
                } else {
                    match ScanFilter::all_match_binary(&filter_predicates, value, &crs) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            spill_err = Some(crate::Error::from(e));
                            break;
                        }
                    }
                    match msgpack_scan::build_group_key(value, group_by, &crs) {
                        Ok(k) => k,
                        Err(e) => {
                            spill_err = Some(crate::Error::from(e));
                            break;
                        }
                    }
                };

                if let Err(e) = spiller.feed(outer_key.clone(), aggregates, value, &crs) {
                    spill_err = Some(e);
                    break;
                }

                if need_sub {
                    let sub_key = match msgpack_scan::build_group_key(value, &sub_specs, &crs) {
                        Ok(k) => k,
                        Err(e) => {
                            spill_err = Some(crate::Error::from(e));
                            break;
                        }
                    };
                    // Composite key: outer + U+001F + sub.
                    let composite = format!("{outer_key}\x1F{sub_key}");
                    if let Err(e) = spiller.feed(composite, sub_aggregates, value, &crs) {
                        spill_err = Some(e);
                        break;
                    }
                }
            }
        }

        // Surface spill-level errors before proceeding.
        if let Some(e) = spill_err {
            return Err(e);
        }

        // Merge all spill runs into the consolidated map.
        let consolidated = spiller.finalize()?;

        // Separate outer groups from sub-group composite entries.
        let mut groups: HashMap<String, GroupState> = HashMap::new();
        // outer_key → sub_key → GroupState
        let mut sub_groups: HashMap<String, HashMap<String, GroupState>> = HashMap::new();

        for (key, state) in consolidated {
            if let Some(sep_pos) = key.find('\x1F') {
                // Sub-group composite key.
                let outer = key[..sep_pos].to_string();
                let sub = key[sep_pos + 1..].to_string();
                sub_groups.entry(outer).or_default().insert(sub, state);
            } else {
                groups.insert(key, state);
            }
        }

        Ok((groups, sub_groups))
    }
}
