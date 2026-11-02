// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated raw KV scan used by the clone materializer.
//!
//! Returns the engine's `(key, value)` pairs verbatim (no map wrapping or
//! key-injection — the materializer needs the raw stored value bytes to copy
//! them into the target) plus the next-cursor in a single response so the
//! caller can drive the scan to completion in O(N / count) round-trips. Each
//! row carries the ordinal of the version read and its absolute expiry: a
//! clone copy installs at exactly that version.

use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::current_ms;
use crate::engine::kv::{KvScanParams, KvVersionedScan};
use crate::types::KvVersionStamp;

/// One scanned KV row: its key, its stored value, and its version stamps.
type KvStampedRow = (Vec<u8>, Vec<u8>, KvVersionStamp);

/// One page of a materialize scan.
pub(in crate::data::executor) struct KvMaterializeScan<'a> {
    pub did: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub cursor: &'a [u8],
    pub count: usize,
    /// The cut the scan reads at. A cursor is valid only for the scope it
    /// came from.
    pub system_time: SystemTimeScope,
    /// The one key to read in place of a page after `cursor`.
    pub only_key: Option<&'a [u8]>,
}

impl CoreLoop {
    /// One page of raw `(key, value)` rows at the scan's system-time scope.
    /// `Current` and `AllVersions` scan the newest version of every live
    /// row. `AsOf` / `Pinned` scan every row visible at the cut.
    pub(in crate::data::executor) fn execute_kv_materialize_scan(
        &self,
        task: &ExecutionTask,
        params: KvMaterializeScan<'_>,
    ) -> Response {
        let KvMaterializeScan {
            did,
            tid,
            collection,
            cursor,
            count,
            system_time,
            only_key,
        } = params;
        // Quiesce gate: same contract as the standard scan so the purge
        // handler can drain readers safely.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let now_ms = current_ms();
        let stamped = |row: crate::engine::kv::KvVersionRow| -> KvStampedRow {
            let stamp = KvVersionStamp {
                sys_from: row.sys_from,
                expire_at_ms: row.expire_at_ms,
            };
            (row.key, row.value, stamp)
        };
        let (entries, next_cursor): (Vec<KvStampedRow>, Vec<u8>) =
            match (only_key, system_time.as_of_ordinal()) {
                (Some(key), _) => {
                    let row = self.kv_engine.version_of_key(
                        did,
                        tid,
                        collection,
                        key,
                        system_time,
                        now_ms,
                    );
                    (row.into_iter().map(stamped).collect(), Vec::new())
                }
                (None, Some(_)) => {
                    let (rows, next) = self.kv_engine.scan_at(KvVersionedScan {
                        database_id: did,
                        tenant_id: tid,
                        collection,
                        cursor,
                        count,
                        now_ms,
                        match_pattern: None,
                        surrogate_ceiling: None,
                        system_time,
                    });
                    (rows.into_iter().map(stamped).collect(), next)
                }
                (None, None) => {
                    let (pairs, next) = self.kv_engine.scan(KvScanParams {
                        database_id: did,
                        tenant_id: tid,
                        collection,
                        cursor,
                        count,
                        now_ms,
                        match_pattern: None,
                        filter_field: None,
                        filter_value: None,
                        surrogate_ceiling: None,
                    });
                    // The live row of each scanned key carries its stamps. The
                    // scan and the lookup run on this core with no write between.
                    let rows = pairs
                        .into_iter()
                        .filter_map(|(key, _)| {
                            self.kv_engine.version_of_key(
                                did,
                                tid,
                                collection,
                                &key,
                                SystemTimeScope::Current,
                                now_ms,
                            )
                        })
                        .map(stamped)
                        .collect();
                    (rows, next)
                }
            };

        // Encode response payload as msgpack:
        //   [next_cursor: bytes, entries: [[key, value, sys_from, expire_at_ms], ...]]
        let mut payload = Vec::with_capacity(
            entries
                .iter()
                .map(|(k, v, _)| k.len() + v.len() + 24)
                .sum::<usize>()
                + next_cursor.len()
                + 16,
        );
        nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
        write_bin(&mut payload, &next_cursor);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, entries.len());
        for (k, v, stamp) in &entries {
            nodedb_query::msgpack_scan::write_array_header(&mut payload, 4);
            write_bin(&mut payload, k);
            write_bin(&mut payload, v);
            stamp.write(&mut payload);
        }

        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }
}

/// Append a msgpack `bin` value (raw byte string) to `out`.
fn write_bin(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= u8::MAX as usize {
        out.push(0xc4);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xc5);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}
