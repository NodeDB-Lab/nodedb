// SPDX-License-Identifier: BUSL-1.1

//! Primary-key shadowing for clone reads.
//!
//! A source row whose primary key the clone target already holds is hidden,
//! whether or not a copy-up mapping or a KV tombstone records it. A target row
//! written before its mapping or tombstone (a copy-up that failed between the
//! two, or a materializer copy) then reads once.
//!
//! Both result sets reach this node in full, so the keys are compared on the
//! rows themselves. A document row's key is its identity under the rule
//! INSERT applies ([`RowIdentity::of_stored_row`]): the declared primary key's
//! body value, else the `id` body value, else the storage surrogate. The rule
//! reads only the row and the replicated collection descriptor, so every node
//! derives the same keys, with no surrogate binding and no RPC. A KV row
//! carries its key itself.
//!
//! Only a plain row scan qualifies. A projected, computed, or aggregated row
//! does not carry the full body, and its `id` can be a user column.

use std::collections::HashSet;

use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan, QueryOp};
use nodedb_query::msgpack_scan;
use nodedb_types::{RowIdentity, StorageKey};

use crate::control::server::response_shape::kv::apply_kv_wrap;

/// Which key a plain row scan's rows carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowScan {
    /// `{id, data}` rows: `id` is the storage key, `data` the full body.
    Document,
    /// Rows carrying their KV `key`.
    Kv,
}

/// The row shape of `plan` when it returns whole rows, else `None`.
///
/// Gather and post-process wrappers qualify only when they keep every row
/// whole: no projection, computed column, or window function.
pub(super) fn plain_row_scan(plan: &PhysicalPlan) -> Option<RowScan> {
    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(op)) => plain_row_scan(&op.child),
        PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            projection,
            computed_columns,
            window_functions,
            ..
        }) if projection.is_empty()
            && computed_columns.is_empty()
            && window_functions.is_empty() =>
        {
            plain_row_scan(input)
        }
        PhysicalPlan::Document(DocumentOp::Scan {
            projection,
            computed_columns,
            window_functions,
            ..
        }) if projection.is_empty()
            && computed_columns.is_empty()
            && window_functions.is_empty() =>
        {
            Some(RowScan::Document)
        }
        PhysicalPlan::Kv(KvOp::Get { .. } | KvOp::BatchGet { .. }) => Some(RowScan::Kv),
        PhysicalPlan::Kv(KvOp::Scan {
            projection,
            computed_columns,
            ..
        }) if projection.is_empty() && computed_columns.is_empty() => Some(RowScan::Kv),
        _ => None,
    }
}

/// Every row's key in a msgpack array of plain scan rows.
///
/// `plan` shapes a KV point read's bare row first. `declared_primary_key` is
/// the collection's declared key column, `None` for the default `id`.
pub(super) fn row_keys(
    shape: RowScan,
    plan: &PhysicalPlan,
    payload: &[u8],
    declared_primary_key: Option<&str>,
) -> crate::Result<HashSet<String>> {
    let wrapped = super::merge::wrap_single_map_as_array(apply_kv_wrap(plan, payload)?);
    let mut keys = HashSet::new();
    for_each_row(&wrapped, |row| {
        if let Some(key) = row_key(shape, row, declared_primary_key) {
            keys.insert(key);
        }
    });
    Ok(keys)
}

/// Drop every row of a msgpack array whose key is in `shadowed`. Returns the
/// payload unchanged when nothing is dropped, and `None` only when a non-empty
/// payload is not a msgpack array.
pub(super) fn drop_shadowed_rows(
    shape: RowScan,
    payload: &[u8],
    declared_primary_key: Option<&str>,
    shadowed: &HashSet<String>,
) -> Option<Vec<u8>> {
    if shadowed.is_empty() || payload.is_empty() {
        return Some(payload.to_vec());
    }
    let (count, _) = msgpack_scan::array_header(payload, 0)?;
    let mut kept: Vec<&[u8]> = Vec::with_capacity(count);
    let walked = for_each_row(payload, |row| {
        let hidden =
            row_key(shape, row, declared_primary_key).is_some_and(|k| shadowed.contains(&k));
        if !hidden {
            kept.push(row);
        }
    });
    if walked != count {
        return None;
    }
    if kept.len() == count {
        return Some(payload.to_vec());
    }
    let mut buf = Vec::with_capacity(payload.len());
    msgpack_scan::write_array_header(&mut buf, kept.len());
    for row in kept {
        buf.extend_from_slice(row);
    }
    Some(buf)
}

/// Call `visit` with each element of a msgpack array. Returns how many
/// elements were visited, fewer than the header count on a malformed payload.
fn for_each_row<'a>(payload: &'a [u8], mut visit: impl FnMut(&'a [u8])) -> usize {
    let Some((count, mut offset)) = msgpack_scan::array_header(payload, 0) else {
        return 0;
    };
    for visited in 0..count {
        let Some(next) = msgpack_scan::skip_value(payload, offset) else {
            return visited;
        };
        visit(&payload[offset..next]);
        offset = next;
    }
    count
}

/// One row's key. A document row without a parseable storage key, or a KV row
/// without `key`, has none and is never shadowed.
fn row_key(shape: RowScan, row: &[u8], declared_primary_key: Option<&str>) -> Option<String> {
    match shape {
        RowScan::Kv => msgpack_scan::extract_field(row, 0, "key")
            .and_then(|(start, _)| msgpack_scan::read_str(row, start))
            .map(str::to_string),
        RowScan::Document => {
            let storage_key = msgpack_scan::extract_field(row, 0, "id")
                .and_then(|(start, _)| msgpack_scan::read_str(row, start))
                .and_then(StorageKey::parse)?;
            let (start, end) = msgpack_scan::extract_field(row, 0, "data")?;
            Some(
                RowIdentity::of_stored_row(&row[start..end], declared_primary_key, storage_key)
                    .into_string(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{QualifiedCollection, Value};

    fn body(fields: &[(&str, &str)]) -> Vec<u8> {
        let obj = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), Value::String((*v).to_string())))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap()
    }

    /// `{id: <storage key>, data: <body>}` rows, as a plain document scan
    /// returns them.
    fn scan_rows(rows: &[(u32, Vec<u8>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        msgpack_scan::write_array_header(&mut buf, rows.len());
        for (surrogate, data) in rows {
            msgpack_scan::write_map_header(&mut buf, 2);
            msgpack_scan::write_kv_str(&mut buf, "id", &format!("{surrogate:08x}"));
            msgpack_scan::write_kv_raw(&mut buf, "data", data);
        }
        buf
    }

    fn doc_scan(projection: Vec<String>) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: QualifiedCollection::new(nodedb_types::DatabaseId::new(1025), "docs"),
            limit: usize::MAX,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection,
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        })
    }

    /// A target row with no copy-up mapping hides its source twin by primary
    /// key, with no surrogate binding anywhere: the two rows carry different
    /// surrogates. A source row the target does not hold stays.
    #[test]
    fn target_row_without_mapping_reads_once() {
        let plan = doc_scan(Vec::new());
        let shape = plain_row_scan(&plan).unwrap();
        let target = scan_rows(&[(50, body(&[("id", "d1"), ("content", "old1")]))]);
        let source = scan_rows(&[
            (10, body(&[("id", "d1"), ("content", "old1")])),
            (11, body(&[("id", "d2"), ("content", "old2")])),
        ]);

        let shadowed = row_keys(shape, &plan, &target, None).expect("keys");
        assert_eq!(shadowed, HashSet::from(["d1".to_string()]));
        let kept = drop_shadowed_rows(shape, &source, None, &shadowed).unwrap();
        assert_eq!(
            row_keys(shape, &plan, &kept, None).expect("keys"),
            HashSet::from(["d2".to_string()])
        );
    }

    /// A declared primary key column names the row, not `id`.
    #[test]
    fn declared_primary_key_names_the_row() {
        let plan = doc_scan(Vec::new());
        let target = scan_rows(&[(50, body(&[("sku", "a"), ("id", "x")]))]);
        let source = scan_rows(&[(10, body(&[("sku", "a"), ("id", "y")]))]);
        let shadowed = row_keys(RowScan::Document, &plan, &target, Some("sku")).expect("keys");
        let kept = drop_shadowed_rows(RowScan::Document, &source, Some("sku"), &shadowed).unwrap();
        assert!(
            row_keys(RowScan::Document, &plan, &kept, Some("sku"))
                .expect("keys")
                .is_empty()
        );
    }

    /// Projected rows are never shadowed: their `id` can be a user column.
    #[test]
    fn projected_scan_is_not_a_plain_row_scan() {
        assert_eq!(plain_row_scan(&doc_scan(vec!["id".into()])), None);
        assert_eq!(
            plain_row_scan(&doc_scan(Vec::new())),
            Some(RowScan::Document)
        );
    }

    #[test]
    fn kv_rows_shadow_by_key() {
        let mut target = Vec::new();
        msgpack_scan::write_array_header(&mut target, 1);
        target.extend_from_slice(&msgpack_scan::build_str_map(&[
            ("key", "k1"),
            ("value", "new"),
        ]));
        let mut source = Vec::new();
        msgpack_scan::write_array_header(&mut source, 2);
        source.extend_from_slice(&msgpack_scan::build_str_map(&[
            ("key", "k1"),
            ("value", "old"),
        ]));
        source.extend_from_slice(&msgpack_scan::build_str_map(&[
            ("key", "k2"),
            ("value", "old"),
        ]));

        let plan = doc_scan(Vec::new());
        let shadowed = row_keys(RowScan::Kv, &plan, &target, None).expect("keys");
        let kept = drop_shadowed_rows(RowScan::Kv, &source, None, &shadowed).unwrap();
        assert_eq!(
            row_keys(RowScan::Kv, &plan, &kept, None).expect("keys"),
            HashSet::from(["k2".to_string()])
        );
    }
}
