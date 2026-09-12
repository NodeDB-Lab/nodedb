// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated document scan for the clone materializer. Returns
//! `(doc_id_hex, surrogate_u32, value_bytes)` triples plus a next-cursor.
//! `doc_id` is the hex-encoded surrogate; `value_bytes` is always standard
//! MessagePack — Binary Tuple and vector-primary sidecar sources are
//! transcoded here so consumers never re-decide the source format. `value_bytes`
//! also carries an `id` field via `sparse_row_to_doc`, so a Control-Plane
//! filter naming `id` sees the same identity the read paths produce.
//! Payload: `[next_cursor: bin, entries: [[doc_id, surrogate, value], ...]]`.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::scan_normalize::sparse_row_to_doc;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::{DOCUMENTS, KeyedTable, invalid_storage_key_err};
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Execute a cursor-paginated raw document scan for the clone materializer.
    pub(in crate::data::executor) fn execute_document_materialize_scan(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        cursor: &[u8],
        count: usize,
        _system_as_of_ms: Option<i64>,
    ) -> Response {
        // Quiesce gate: same contract as the standard scan.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let prefix = crate::engine::sparse::btree::coll_prefix(
            task.request.database_id.as_u64(),
            tid,
            collection,
        );
        let prefix_end = format!("{prefix}\u{ffff}");

        // Cursor is the last doc_id_hex seen; resume AFTER it.
        let range_start = if cursor.is_empty() {
            prefix.clone()
        } else {
            // cursor bytes are the UTF-8 doc_id_hex string; advance by one
            // character to make the scan exclusive.
            let cursor_str = String::from_utf8_lossy(cursor);
            format!("{prefix}{cursor_str}\x00")
        };

        let read_txn = match self.sparse.db().begin_read() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan begin_read: {e}"),
                    },
                );
            }
        };

        let table = match read_txn.open_table(DOCUMENTS) {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan open_table: {e}"),
                    },
                );
            }
        };

        let range = match table.range(range_start.as_str()..prefix_end.as_str()) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan range: {e}"),
                    },
                );
            }
        };

        // In-transaction callers need base ∪ overlay; since the overlay can
        // tombstone/supersede rows spanning pages, collect the whole base set
        // (ignoring the page cap) and return it un-paginated. Autocommit
        // callers (`txn_id == None`) keep cursor-paginated base-only behavior.
        let txn_id = task.request.txn_id;

        let mut entries: Vec<(StorageKey, Vec<u8>)> = Vec::with_capacity(count.min(256));
        let mut last_key: Option<StorageKey> = None;

        for row in range {
            if txn_id.is_none() && entries.len() >= count {
                break;
            }
            let row = match row {
                Ok(r) => r,
                Err(e) => {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("materialize_scan row: {e}"),
                        },
                    );
                }
            };
            let full_key = row.0.value();
            let rest = full_key.strip_prefix(&prefix).unwrap_or(full_key);
            let key = match StorageKey::parse(rest) {
                Some(key) => key,
                None => {
                    return self.response_error(
                        task,
                        invalid_storage_key_err(KeyedTable::Documents, collection, rest),
                    );
                }
            };
            let value = row.1.value().to_vec();

            last_key = Some(key);
            entries.push((key, value));
        }

        // Fold the staging overlay into the base set: a staged tombstone
        // hides its row, a staged put replaces or appends. The source ships
        // all rows unfiltered, so the merge predicate is collect-all.
        let next_cursor: Vec<u8> = if let Some(txn_id) = txn_id {
            let coll_key: (DatabaseId, TenantId, String) = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            self.merge_overlay_into_scan(txn_id, &coll_key, &mut entries, &|_, _| true);
            // The whole set is returned in one response; the scan is complete.
            Vec::new()
        } else if entries.len() < count {
            // Next-cursor is the last doc_id_hex seen; empty = scan complete.
            Vec::new()
        } else {
            last_key
                .map(|k| k.to_string().into_bytes())
                .unwrap_or_default()
        };

        // Normalize every body to standard msgpack and inject its `id` here —
        // the one place that owns the source format — so no consumer repeats
        // the decision or filters a row missing the identity its storage key
        // already carries.
        let body_format =
            self.sparse_body_format(task.request.database_id, TenantId::new(tid), collection);
        let format_ref = body_format.as_format_ref();
        for (key, value) in &mut entries {
            let (_, normalized) = sparse_row_to_doc(key, value, format_ref);
            *value = normalized;
        }

        // Encode response: [next_cursor: bin, entries: [[str, u32, bin], ...]]
        let mut payload = Vec::with_capacity(
            entries
                .iter()
                .map(|(_, v)| 8 + 4 + v.len() + 12)
                .sum::<usize>()
                + next_cursor.len()
                + 16,
        );
        nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
        write_bin(&mut payload, &next_cursor);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, entries.len());
        for (key, value) in &entries {
            nodedb_query::msgpack_scan::write_array_header(&mut payload, 3);
            write_str(&mut payload, key.to_string().as_bytes());
            write_u32(&mut payload, key.surrogate().as_u32());
            write_bin(&mut payload, value);
        }

        self.response_with_payload(task, payload)
    }
}

/// Append a msgpack `bin` value to `out`.
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

/// Append a msgpack `str` value to `out`.
fn write_str(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= 31 {
        out.push(0xa0 | len as u8);
    } else if len <= u8::MAX as usize {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Append a msgpack `u32` value to `out`.
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.push(0xce);
    out.extend_from_slice(&v.to_be_bytes());
}
