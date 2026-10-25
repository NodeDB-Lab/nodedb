// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated document scan for the clone materializer. Returns
//! `(doc_id_hex, surrogate_u32, value_bytes)` triples plus a next-cursor.
//! `doc_id` is the hex-encoded surrogate; `value_bytes` is always standard
//! MessagePack — Binary Tuple and vector-primary sidecar sources are
//! transcoded here so consumers never re-decide the source format. `value_bytes`
//! also carries the row identity under the collection's identity column via
//! `sparse_row_to_doc`, so a Control-Plane filter sees the same identity the
//! read paths produce. A `raw_bodies` scan adds no identity: the clone
//! materializer copies the stored contents, which a `HASH_CHAIN` link covers
//! and a strict schema accepts.
//! Payload: `[next_cursor: bin, entries: [[doc_id, surrogate, value], ...]]`.

use nodedb_types::StorageKey;
use redb::{ReadableDatabase, ReadableTable};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::SidecarRowShape;
use crate::data::executor::scan_normalize::{sparse_body_to_msgpack, sparse_row_to_doc};
use crate::data::executor::sparse_body_format::SparseBodyFormat;
use crate::data::executor::task::ExecutionTask;
use crate::engine::sparse::btree::{DOCUMENTS, KeyedTable, invalid_storage_key_err};
use crate::engine::sparse::btree_versioned::VersionedScanParams;
use crate::engine::sparse::scan_stop::never_stop;
use crate::types::{DatabaseId, TenantId};

/// One page of a document materialize scan: the fields of
/// `DocumentOp::MaterializeScan`, with the tenant it runs under.
pub(in crate::data::executor) struct DocumentMaterializeScan<'a> {
    pub tid: u64,
    pub collection: &'a str,
    /// The last `doc_id_hex` the previous page returned. Empty for the first
    /// page.
    pub cursor: &'a [u8],
    /// Rows per page outside a transaction.
    pub count: usize,
    /// The system-time ordinal cut the plan's millisecond converts to.
    /// `None` reads the current rows.
    pub system_as_of: Option<i64>,
    /// Copy the stored bodies as they are, with no `id` field added.
    pub raw_bodies: bool,
}

impl CoreLoop {
    /// Execute a cursor-paginated raw document scan for the clone materializer.
    pub(in crate::data::executor) fn execute_document_materialize_scan(
        &self,
        task: &ExecutionTask,
        scan: DocumentMaterializeScan<'_>,
    ) -> Response {
        let DocumentMaterializeScan {
            tid,
            collection,
            cursor,
            count,
            system_as_of,
            raw_bodies,
        } = scan;
        // Quiesce gate: same contract as the standard scan.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // In-transaction callers need base ∪ overlay; since the overlay can
        // tombstone/supersede rows spanning pages, collect the whole base set
        // (ignoring the page cap) and return it un-paginated. Autocommit
        // callers (`txn_id == None`) keep cursor-paginated base-only behavior.
        let txn_id = task.request.txn_id;
        let page = BasePage {
            database_id: task.request.database_id.as_u64(),
            tid,
            collection,
            cursor,
            limit: if txn_id.is_none() { count } else { usize::MAX },
        };

        // A bitemporal collection stores its rows as versions: the scan reads
        // each row's version as of `system_as_of`, or its current version
        // when unset. A system-time cut on any other collection is refused,
        // as the SQL planner refuses `FOR SYSTEM_TIME` on one.
        let base = if self.is_bitemporal(page.database_id, tid, collection) {
            self.materialize_versioned_rows(&page, system_as_of)
        } else if system_as_of.is_some() {
            Err(crate::bridge::envelope::ErrorCode::BadRequest {
                detail: format!(
                    "a materialize scan AS OF SYSTEM TIME requires a bitemporal collection; \
                     '{collection}' was not created WITH bitemporal = true"
                ),
            })
        } else {
            self.materialize_live_rows(&page)
        };
        let mut entries = match base {
            Ok(entries) => entries,
            Err(code) => return self.response_error(task, code),
        };
        let last_key = entries.last().map(|(key, _)| *key);

        // Fold the staging overlay into the base set: a staged tombstone
        // hides its row, a staged put replaces or appends. The source ships
        // all rows unfiltered, so the merge predicate is collect-all.
        let next_cursor: Vec<u8> = if let Some(txn_id) = txn_id {
            let coll_key: (DatabaseId, TenantId, String) = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            // A vector-primary row stages its vector with its sidecar; its
            // merge yields the stored sidecar, normalized below like base.
            let vector_primary = matches!(
                self.sparse_body_format(task.request.database_id, TenantId::new(tid), collection),
                SparseBodyFormat::VectorSidecar
            );
            if vector_primary {
                if let Err(e) = self.merge_vector_primary_overlay_into_scan(
                    txn_id,
                    &coll_key,
                    SidecarRowShape::Stored,
                    &mut entries,
                    &|_, _| true,
                ) {
                    return self.response_error(task, e);
                }
            } else {
                self.merge_overlay_into_scan(txn_id, &coll_key, &mut entries, &|_, _| true);
            }
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

        // Normalize every body to standard msgpack and inject its identity
        // here — the one place that owns the source format — so no consumer
        // repeats the decision or filters a row missing the identity its
        // storage key already carries.
        let body_format =
            self.sparse_body_format(task.request.database_id, TenantId::new(tid), collection);
        let format_ref = body_format.as_format_ref();
        let identity_column =
            self.identity_column(task.request.database_id.as_u64(), tid, collection);
        // A raw body goes out as stored, with no `id` added: the clone
        // materializer copies the exact stored contents.
        for (key, value) in &mut entries {
            *value = if raw_bodies {
                sparse_body_to_msgpack(value, format_ref).into_owned()
            } else {
                sparse_row_to_doc(key, value, format_ref, &identity_column).1
            };
        }
        // A read of a vector-primary row carries its vector under the vector
        // field, as a scan does: the page is a source a copy reads rows from.
        // The clone materializer's raw copy is the stored sidecar alone.
        if !raw_bodies
            && matches!(body_format, SparseBodyFormat::VectorSidecar)
            && let Some(field) =
                self.vector_primary_field(task.request.database_id.as_u64(), tid, collection)
        {
            let coll_key = (
                task.request.database_id,
                TenantId::new(tid),
                collection.to_string(),
            );
            for (key, value) in &mut entries {
                let with_vector = self
                    .current_primary_vector(txn_id, &coll_key, &field, key.surrogate())
                    .and_then(|vector| {
                        crate::data::executor::handlers::vector_row_column::with_vector_column(
                            value, &field, &vector,
                        )
                    });
                match with_vector {
                    Ok(row) => *value = row,
                    Err(e) => return self.response_error(task, e),
                }
            }
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

    /// One page of a current-only collection's rows, read from the live
    /// document table after the cursor.
    fn materialize_live_rows(&self, page: &BasePage<'_>) -> Result<BaseRows, ErrorCode> {
        let internal = |what: &str, e: &dyn std::fmt::Display| ErrorCode::Internal {
            detail: format!("materialize_scan {what}: {e}"),
        };
        let prefix =
            crate::engine::sparse::btree::coll_prefix(page.database_id, page.tid, page.collection);
        let prefix_end = format!("{prefix}\u{ffff}");
        // Cursor is the last doc_id_hex seen; `\x00` after it makes the scan
        // resume AFTER it.
        let range_start = if page.cursor.is_empty() {
            prefix.clone()
        } else {
            let cursor_str = String::from_utf8_lossy(page.cursor);
            format!("{prefix}{cursor_str}\x00")
        };

        let read_txn = self
            .sparse
            .db()
            .begin_read()
            .map_err(|e| internal("begin_read", &e))?;
        let table = read_txn
            .open_table(DOCUMENTS)
            .map_err(|e| internal("open_table", &e))?;
        let range = table
            .range(range_start.as_str()..prefix_end.as_str())
            .map_err(|e| internal("range", &e))?;

        let mut entries = Vec::with_capacity(page.limit.min(256));
        for row in range {
            if entries.len() >= page.limit {
                break;
            }
            let row = row.map_err(|e| internal("row", &e))?;
            let full_key = row.0.value();
            let rest = full_key.strip_prefix(&prefix).unwrap_or(full_key);
            let key = StorageKey::parse(rest).ok_or_else(|| {
                invalid_storage_key_err(KeyedTable::Documents, page.collection, rest)
            })?;
            entries.push((key, row.1.value().to_vec()));
        }
        Ok(entries)
    }

    /// One page of a bitemporal collection's rows after the cursor: each
    /// row's version as of `system_as_of`, its current version when unset.
    /// A row deleted before the cut, or created after it, is absent.
    fn materialize_versioned_rows(
        &self,
        page: &BasePage<'_>,
        system_as_of: Option<i64>,
    ) -> Result<BaseRows, ErrorCode> {
        let after = if page.cursor.is_empty() {
            None
        } else {
            let cursor_str = String::from_utf8_lossy(page.cursor);
            Some(StorageKey::parse(&cursor_str).ok_or_else(|| {
                invalid_storage_key_err(
                    KeyedTable::DocumentsVersioned,
                    page.collection,
                    &cursor_str,
                )
            })?)
        };
        self.sparse
            .versioned_scan_as_of_after(
                VersionedScanParams {
                    database_id: page.database_id,
                    tenant: page.tid,
                    coll: page.collection,
                    sys_cutoff: system_as_of,
                    valid_at_ms: None,
                    limit: page.limit,
                },
                after.as_ref(),
                &|_: &StorageKey, _: &[u8]| true,
                &never_stop,
            )
            .map_err(ErrorCode::from)
    }
}

/// The base rows of one page, in doc-id order.
type BaseRows = Vec<(StorageKey, Vec<u8>)>;

/// Where one page of base rows is read.
struct BasePage<'a> {
    database_id: u64,
    tid: u64,
    collection: &'a str,
    /// The last `doc_id_hex` the previous page returned. Empty for the first
    /// page.
    cursor: &'a [u8],
    /// Rows the page holds at most.
    limit: usize,
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

#[cfg(test)]
mod tests {
    use nodedb_query::msgpack_scan;
    use nodedb_types::Surrogate;

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::document::store::CollectionConfig;
    use crate::engine::sparse::btree_versioned::VersionedPut;

    /// The tenant `make_default_task` runs under.
    const TID: u64 = 1;
    const COLL: &str = "accounts";

    fn register(core: &mut CoreLoop, collection: &str, bitemporal: bool) {
        let mut config = CollectionConfig::new(collection);
        config.bitemporal = bitemporal;
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                collection.to_string(),
            ),
            config,
        );
    }

    /// Append the version of row `surrogate` that holds `state` from
    /// `sys_from` on.
    fn put_version(core: &CoreLoop, surrogate: u32, sys_from: i64, state: &str) {
        let doc_id = StorageKey::for_surrogate(Surrogate::new(surrogate));
        let body = nodedb_types::json_to_msgpack(&serde_json::json!({ "state": state }))
            .expect("encode body");
        core.sparse
            .versioned_put(VersionedPut {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant: TID,
                coll: COLL,
                doc_id: &doc_id,
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: &body,
            })
            .expect("versioned put");
    }

    fn scan(core: &CoreLoop, collection: &str, system_as_of: Option<i64>) -> Response {
        core.execute_document_materialize_scan(
            &make_default_task(),
            DocumentMaterializeScan {
                tid: TID,
                collection,
                cursor: &[],
                count: 16,
                system_as_of,
                raw_bodies: true,
            },
        )
    }

    /// The `state` field of every row one scan page returned, in order.
    fn states(response: &Response) -> Vec<String> {
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let payload: &[u8] = response.payload.as_ref();
        let (outer, mut off) = msgpack_scan::array_header(payload, 0).expect("page");
        assert_eq!(outer, 2);
        msgpack_scan::read_bin_advance(payload, &mut off).expect("next cursor");
        let (count, mut off) = msgpack_scan::array_header(payload, off).expect("entries");
        let mut states = Vec::new();
        for _ in 0..count {
            let (_, mut entry) = msgpack_scan::array_header(payload, off).expect("entry");
            msgpack_scan::read_str_advance(payload, &mut entry).expect("doc id");
            msgpack_scan::read_u32_advance(payload, &mut entry).expect("surrogate");
            let body = msgpack_scan::read_bin_advance(payload, &mut entry).expect("body");
            let doc = nodedb_types::json_from_msgpack(body).expect("decode body");
            states.push(doc["state"].as_str().expect("state field").to_string());
            off = entry;
        }
        states
    }

    #[test]
    fn a_row_updated_after_the_cut_scans_back_to_its_old_version() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        register(&mut core, COLL, true);
        put_version(&core, 7, 1_000, "old");
        put_version(&core, 7, 3_000, "new");

        assert_eq!(states(&scan(&core, COLL, Some(2_000))), ["old"]);
        assert_eq!(states(&scan(&core, COLL, None)), ["new"]);
        assert!(
            states(&scan(&core, COLL, Some(500))).is_empty(),
            "a row created after the cut is absent"
        );
    }

    /// Two versions written in one millisecond split at an ordinal cut
    /// between them, as edge versions split at that ordinal.
    #[test]
    fn versions_inside_one_millisecond_split_at_an_ordinal_cut() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        register(&mut core, COLL, true);
        let first = 1_700_000_000_123 * nodedb_types::NANOS_PER_MS + 1;
        put_version(&core, 7, first, "old");
        put_version(&core, 7, first + 2, "new");

        assert_eq!(states(&scan(&core, COLL, Some(first + 1))), ["old"]);
        assert_eq!(states(&scan(&core, COLL, Some(first + 2))), ["new"]);
    }

    #[test]
    fn a_system_time_cut_on_a_current_only_collection_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        register(&mut core, "plain", false);

        let response = scan(&core, "plain", Some(2_000));
        assert_eq!(response.status, Status::Error);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::BadRequest { .. })
        ));
    }
}
