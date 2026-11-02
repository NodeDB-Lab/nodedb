// SPDX-License-Identifier: BUSL-1.1

//! The base rows of a KV scan at a past system-time cut, or of an
//! all-versions scan.
//!
//! A past cut reads every row visible there, deleted-since rows included.
//! The secondary indexes hold current values only, so the scan reads every
//! visible row and the handler's filter pass applies every predicate. An
//! all-versions scan reads every version of every row, oldest first per key,
//! and each emitted row carries `_ts_system`: the millisecond of the write
//! that set the version.

use nodedb_types::{SystemTimeScope, Value, ordinal_to_ms};

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::kv::KvVersionedScan;

/// The system-time column an all-versions row carries.
pub(in crate::data::executor) const TS_SYSTEM_COLUMN: &str = "_ts_system";

/// The base rows of a scan at a system-time scope: `(key, value)` pairs and,
/// for an all-versions scan, the `sys_from` ordinal of each row in the same
/// order. The ordinal list is empty for any other scope.
pub(in crate::data::executor) struct VersionedScanRows {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    pub sys_from: Vec<i64>,
}

/// Inputs of [`CoreLoop::kv_versioned_scan_rows`].
pub(in crate::data::executor) struct VersionedScanArgs<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    pub now_ms: u64,
    pub match_pattern: Option<&'a str>,
    pub surrogate_ceiling: Option<u32>,
    pub system_time: SystemTimeScope,
}

impl CoreLoop {
    /// Read the base rows of a scan at `args.system_time`.
    pub(in crate::data::executor) fn kv_versioned_scan_rows(
        &self,
        args: VersionedScanArgs<'_>,
    ) -> VersionedScanRows {
        let (rows, _next_cursor) = self.kv_engine.scan_at(KvVersionedScan {
            database_id: args.did,
            tenant_id: args.tid,
            collection: args.collection,
            cursor: args.cursor,
            count: args.count,
            now_ms: args.now_ms,
            match_pattern: args.match_pattern,
            surrogate_ceiling: args.surrogate_ceiling,
            system_time: args.system_time,
        });
        let all_versions = args.system_time.is_all_versions();
        let mut sys_from = Vec::with_capacity(if all_versions { rows.len() } else { 0 });
        let entries = rows
            .into_iter()
            .map(|row| {
                if all_versions {
                    sys_from.push(row.sys_from);
                }
                (row.key, row.value)
            })
            .collect();
        VersionedScanRows { entries, sys_from }
    }
}

/// `row` with `_ts_system` set to the millisecond of ordinal `sys_from`.
pub(in crate::data::executor) fn with_ts_system(
    row: &[u8],
    sys_from: i64,
) -> crate::Result<Vec<u8>> {
    let stamp =
        nodedb_types::value_to_msgpack(&Value::Integer(ordinal_to_ms(sys_from))).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("encode kv all-versions {TS_SYSTEM_COLUMN}: {e}"),
            }
        })?;
    Ok(nodedb_query::msgpack_scan::writer::merge_fields(
        row,
        &[(TS_SYSTEM_COLUMN, stamp.as_slice())],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_all_versions_row_carries_its_system_millisecond() {
        let row = nodedb_types::value_to_msgpack(&Value::Object(
            [("id".to_string(), Value::String("k".into()))]
                .into_iter()
                .collect(),
        ))
        .expect("encode row");
        let stamped = with_ts_system(&row, 42 * 1_000_000 + 7).expect("stamp");
        let Value::Object(fields) = nodedb_types::value_from_msgpack(&stamped).expect("decode")
        else {
            panic!("an object row stays an object");
        };
        assert_eq!(fields.get(TS_SYSTEM_COLUMN), Some(&Value::Integer(42)));
        assert_eq!(fields.get("id"), Some(&Value::String("k".into())));
    }
}
