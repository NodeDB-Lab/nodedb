// SPDX-License-Identifier: BUSL-1.1

//! KV serializer for transaction resolve.
//!
//! Turns the KV post-images a transaction staged into its overlay into the
//! engine-native WAL sub-record shapes the KV replay path decodes — the SAME
//! shapes the autocommit KV WAL path produces, so producer and replay share one
//! encoding:
//!
//! * A staged value ([`Staged::Put`]) → `RecordType::Put`, `encode_kv_put`'s
//!   `("kv_put", collection, key, value, ttl_ms, expire_at_ms, surrogate,
//!   declared)` with no declared columns: staging fitted the value. The
//!   overlay holds the resolved ABSOLUTE post-image (an atomic
//!   `Incr`/`Cas`/`GetSet` stages its computed value, never a delta), so resolve
//!   emits exactly that. When the overlay carries an absolute expiry
//!   ([`StagedTtl::ExpireAt`]) for the slot it travels verbatim so replay
//!   installs the exact instant instead of recomputing `now + ttl`; a `Persist`
//!   (or no TTL delta) emits `None`.
//! * A TTL-only slot (an `EXPIRE` / `PERSIST` of a row the transaction did
//!   not otherwise write) → `RecordType::Put`, `encode_kv_expire`'s
//!   `("kv_expire", collection, key, ttl_ms, expire_at_ms)` or
//!   `encode_kv_persist`'s `("kv_persist", collection, key)`.
//! * A staged tombstone ([`Staged::Tombstone`]) → `RecordType::Delete`,
//!   `("kv_delete", collection, [key])`.
//! * A staged TRUNCATE → `RecordType::Delete`, `encode_kv_truncate`'s
//!   `("kv_truncate", collection)`, emitted before the collection's row
//!   entries so replay wipes base first and then reinstalls the rows staged
//!   after the truncate ([`serialize_kv_truncate`]).
//!
//! ## `ttl_ms` in the redo payload
//!
//! The overlay stores only the resolved ABSOLUTE expiry instant, never the
//! original relative `ttl_ms`. Replay installs the absolute instant when the
//! record carries one and ignores the relative `ttl_ms`, so that slot is
//! vestigial in a redo sub-record and is set to `0` in every case.
//!
//! ## Determinism
//!
//! Entries emit in the overlay's merged identity order: the key-byte order
//! of each row's encoded identity (`kv_row_identity` of the KV key). The
//! order is a function of the staged rows alone, so two replicas resolving
//! the same transaction produce byte-identical redo ops, and the resolve
//! holds no row it has emitted.

use nodedb_wal::record::RecordType;

use super::sink::OpSink;
use crate::control::server::wal_dispatch_kv::encode::{
    encode_kv_expire, encode_kv_persist, encode_kv_put, encode_kv_truncate,
};
use crate::data::executor::handlers::transaction::overlay::{
    Staged, StagedSlot, StagedTtl, TxnOverlay,
};
use crate::data::executor::handlers::transaction::stage_write::unhex_key;
use crate::types::{DatabaseId, TenantId};
use crate::wal::RedoSubRecord;

/// The relative `ttl_ms` written into every KV redo sub-record. The absolute
/// expiry instant (when present) is authoritative on replay and the overlay
/// never retains the original relative TTL, so this slot is always `0`.
const RESOLVE_TTL_MS: u64 = 0;

/// Append the `kv_truncate` redo sub-record for a collection a staged
/// TRUNCATE wipes at COMMIT — the same record the autocommit KV truncate
/// appends.
pub(super) fn serialize_kv_truncate(collection: &str, ops: &mut dyn OpSink) -> crate::Result<()> {
    let payload = encode_kv_truncate(collection)?;
    ops.push(RedoSubRecord {
        record_type: RecordType::Delete as u32,
        payload,
    });
    Ok(())
}

/// Append the redo sub-records for every KV post-image staged in `overlay`
/// for `coll_key` to `ops`, in the overlay's identity order.
///
/// An `EXPIRE` or `PERSIST` of a row the transaction did not otherwise
/// write resolves to a `kv_expire` or `kv_persist` sub-record, the shapes
/// the autocommit TTL writes append. The install changes only the expiry of
/// the value the key holds when the record applies. A write committed
/// between stage and install keeps its value.
pub(super) fn serialize_kv_collection(
    overlay: &TxnOverlay,
    coll_key: &(DatabaseId, TenantId, String),
    collection: &str,
    ops: &mut dyn OpSink,
) -> crate::Result<()> {
    // A staged TRUNCATE hides every base row, so no TTL delta can target one.
    let ttl_only = !overlay.is_truncated(coll_key);
    for item in overlay.iter_slots_for_collection(coll_key, ttl_only) {
        let (doc_id, slot) = item?;
        let key = unhex_key(doc_id.as_str()).ok_or_else(|| crate::Error::Internal {
            detail: format!("kv resolve: overlay doc-id '{doc_id}' is not valid hex"),
        })?;
        let (surrogate, staged) = match slot {
            StagedSlot::Written { surrogate, staged } => (surrogate, staged),
            StagedSlot::TtlOnly(ttl) => {
                let payload = match ttl {
                    StagedTtl::ExpireAt(ms) => {
                        encode_kv_expire(collection, &key, RESOLVE_TTL_MS, ms)?
                    }
                    StagedTtl::Persist => encode_kv_persist(collection, &key)?,
                };
                ops.push(RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload,
                });
                continue;
            }
        };
        match &*staged {
            Staged::Put(value) => {
                let expire_at_ms = match overlay.get_ttl_by_doc_id(coll_key, &doc_id)? {
                    Some(StagedTtl::ExpireAt(ms)) => Some(ms),
                    Some(StagedTtl::Persist) | None => None,
                };
                // The overlay keys every staged row by surrogate and holds the
                // doc-id → surrogate map, so the redo record carries the same
                // identity the live write bound: the binding this entry was
                // read through.
                let payload = encode_kv_put(
                    collection,
                    &key,
                    value,
                    RESOLVE_TTL_MS,
                    expire_at_ms,
                    surrogate,
                    // Staging fitted the value under the plan's columns.
                    &[],
                )?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload,
                });
            }
            Staged::Tombstone => {
                let payload = zerompk::to_msgpack_vec(&("kv_delete", collection, vec![key]))
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("kv resolve delete: {e}"),
                    })?;
                ops.push(RedoSubRecord {
                    record_type: RecordType::Delete as u32,
                    payload,
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::spill::tiered::SpillSpace;
    use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;

    fn coll_key() -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(1), TenantId::new(1), "cache".to_string())
    }

    /// Stage the same writes into `overlay`: puts, overwrites, tombstones,
    /// a TTL on a put, and TTL-only rows.
    fn stage(overlay: &mut TxnOverlay) {
        let coll = coll_key();
        for n in 0..120u32 {
            let key = format!("k{n:03}");
            overlay
                .insert_put(
                    coll.clone(),
                    n + 1,
                    &kv_row_identity(key.as_bytes()),
                    vec![n as u8; 40],
                )
                .unwrap();
        }
        for n in (0..120u32).step_by(7) {
            let key = format!("k{n:03}");
            overlay
                .insert_put(
                    coll.clone(),
                    n + 1,
                    &kv_row_identity(key.as_bytes()),
                    vec![0xee; 12],
                )
                .unwrap();
        }
        for n in (3..120u32).step_by(11) {
            let key = format!("k{n:03}");
            overlay
                .insert_tombstone(coll.clone(), n + 1, &kv_row_identity(key.as_bytes()))
                .unwrap();
        }
        overlay
            .set_ttl(
                coll.clone(),
                9,
                &kv_row_identity(b"k008"),
                StagedTtl::ExpireAt(5_000),
            )
            .unwrap();
        overlay
            .set_ttl(
                coll.clone(),
                900,
                &kv_row_identity(b"base-only"),
                StagedTtl::Persist,
            )
            .unwrap();
        overlay
            .set_ttl(
                coll,
                901,
                &kv_row_identity(b"base-ttl"),
                StagedTtl::ExpireAt(7_000),
            )
            .unwrap();
    }

    fn resolve(overlay: &TxnOverlay) -> Vec<RedoSubRecord> {
        let mut ops: Vec<crate::wal::RedoSubRecord> = Vec::new();
        serialize_kv_collection(overlay, &coll_key(), "cache", &mut ops).unwrap();
        ops
    }

    /// An overlay that spilled after every write resolves to the same redo
    /// ops, in the same order, as one that held every write in RAM.
    #[test]
    fn a_spilled_overlay_resolves_like_an_in_memory_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut hot = TxnOverlay::new();
        let mut spilled = TxnOverlay::with_spill(SpillSpace::new(dir.path().join("t"), "doc", 0));
        stage(&mut hot);
        stage(&mut spilled);
        assert!(spilled.spilled_heap_bytes() > 0);
        assert_eq!(hot.spilled_heap_bytes(), 0);

        let expected = resolve(&hot);
        assert_eq!(expected.len(), 122, "120 rows plus two TTL-only rows");
        assert_eq!(resolve(&spilled), expected);
    }
}
