// SPDX-License-Identifier: BUSL-1.1

//! Replay arms for the absolute-overwrite KV record classes, `kv_put`,
//! `kv_batch_put`, and `kv_rewrite`.
//!
//! ## Why the record carries the surrogate
//!
//! Both shapes carry the row's stable cross-engine surrogate as a trailing
//! element, and replay restores it rather than binding `Surrogate::ZERO`. The
//! KV checkpoint persists real surrogates, so a zero here would leave one table
//! mixing checkpoint-restored rows that resolve through
//! `KvEngine::key_for_surrogate` with replayed rows that do not — and the
//! clone-snapshot visibility rule in `scan_ops` reads surrogate `0` as
//! unconditionally visible, so snapshot isolation would silently weaken after a
//! crash but not before.
//!
//! ## One shape per record class
//!
//! Each record class has exactly one shape, the one its encoder
//! (`wal_dispatch_kv::encode`) writes. A `kv_put` or `kv_batch_put` record
//! whose surrogate is `Surrogate::ZERO` matches no shape. A payload that opens
//! with a KV put discriminator but matches no shape is refused through
//! [`kv_put_family_discriminator`] and `replay_record_unapplied`, never
//! replayed unbound.
//!
//! A `kv_rewrite` record carries no surrogate: it replaces the value of a row
//! the table already holds, and the row keeps its bound identity.
//!
//! ## Absolute expiry
//!
//! When the record carries a resolved absolute instant it is installed
//! verbatim. Recomputing `now_ms + ttl_ms` at replay time would push every
//! expiry forward by the crash-to-restart delay.

use std::borrow::Cow;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::handlers::kv::declared_body::coerce_kv_body;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use nodedb_types::Surrogate;

/// Inputs shared by both arms, bundled so each stays under the
/// `too_many_arguments` clippy threshold.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct KvReplayRecord<'a> {
    pub payload: &'a [u8],
    pub tenant_id: u64,
    pub database_id: u64,
    pub now_ms: u64,
    pub record_lsn: u64,
}

impl CoreLoop {
    /// Replay a `kv_put` record. `None` when the payload is not one.
    ///
    /// `Some(0)` means the record was recognized and deliberately skipped (its
    /// collection is tombstoned, or a restored checkpoint already contains it).
    pub(in crate::data::executor) fn try_replay_kv_put(
        &mut self,
        rec: &KvReplayRecord<'_>,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<usize> {
        let KvReplayRecord {
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
        } = *rec;

        let (collection, key, value, ttl_ms, expire_at_ms, surrogate) = decode_kv_put(payload)?;

        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        if self.claim_for_validation() {
            return Some(0);
        }
        // The record holds the body the write supplied. The live apply
        // re-typed its declared numeric columns, so replay does too.
        let declared = self.declared_columns_of(database_id, tenant_id, &collection);
        let coerced = match coerce_kv_body(&value, declared) {
            Ok(Cow::Owned(bytes)) => Some(bytes),
            Ok(Cow::Borrowed(_)) => None,
            Err(e) => {
                self.replay_record_unapplied("kv", "put_declared", record_lsn, &e.to_string());
                return Some(0);
            }
        };
        let value = coerced.unwrap_or(value);
        if self.recording_redo_undo() {
            let prior =
                self.kv_engine
                    .entry_image(database_id, tenant_id, &collection, &key, now_ms);
            self.record_redo_undo([UndoEntry::KvPut {
                collection: collection.clone(),
                key: key.clone(),
                prior,
            }]);
        }

        let params = crate::engine::kv::KvPutParams {
            database_id,
            tenant_id,
            collection: &collection,
            key: &key,
            value: &value,
            ttl_ms,
            now_ms,
            surrogate,
        };
        // The prior value the put displaced is of no interest to replay.
        let written = match expire_at_ms {
            Some(instant) => self.kv_engine.put_with_absolute_expiry(params, instant),
            None => self.kv_engine.put(params),
        };
        if let Err(e) = written {
            self.replay_record_unapplied("kv", "put_identity", record_lsn, &e.to_string());
            return Some(0);
        }
        self.note_replay_write(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(1)
    }

    /// Replay a `kv_rewrite` record. `None` when the payload is not one.
    ///
    /// The row keeps its bound surrogate. A row the table no longer holds is
    /// not written: a rewrite never creates a row without identity.
    pub(in crate::data::executor) fn try_replay_kv_rewrite(
        &mut self,
        rec: &KvReplayRecord<'_>,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<usize> {
        let KvReplayRecord {
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
        } = *rec;

        let (collection, key, value, ttl_ms, expire_at_ms) = decode_kv_rewrite(payload)?;

        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        if self.claim_for_validation() {
            return Some(0);
        }
        if self.recording_redo_undo() {
            let prior =
                self.kv_engine
                    .entry_image(database_id, tenant_id, &collection, &key, now_ms);
            self.record_redo_undo([UndoEntry::KvPut {
                collection: collection.clone(),
                key: key.clone(),
                prior,
            }]);
        }
        let rewritten = self.kv_engine.rewrite_with_absolute_expiry(
            crate::engine::kv::KvRewriteParams {
                database_id,
                tenant_id,
                collection: &collection,
                key: &key,
                value: &value,
                ttl_ms,
                now_ms,
            },
            expire_at_ms,
        );
        self.note_replay_write(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(usize::from(rewritten.is_some()))
    }

    /// Replay a `kv_batch_put` record. `None` when the payload is not one.
    pub(in crate::data::executor) fn try_replay_kv_batch_put(
        &mut self,
        rec: &KvReplayRecord<'_>,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<usize> {
        let KvReplayRecord {
            payload,
            tenant_id,
            database_id,
            now_ms,
            record_lsn,
        } = *rec;

        let (collection, entries, ttl_ms, expire_at_ms, surrogates) = decode_kv_batch_put(payload)?;

        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }

        // `surrogates` is positional against `entries`. A record whose two
        // lengths disagree cannot be applied without guessing which row owns
        // which identity, so it aborts rather than binding the wrong one.
        if surrogates.len() != entries.len() {
            self.replay_record_unapplied(
                "kv",
                "batch_put_surrogates",
                record_lsn,
                &format!(
                    "kv_batch_put into '{collection}' carries {} entries but {} surrogates",
                    entries.len(),
                    surrogates.len()
                ),
            );
            return Some(0);
        }

        // Each entry holds the body the write supplied. The live apply
        // re-typed its declared numeric columns, so replay does too.
        let mut entries = entries;
        for (_, value) in entries.iter_mut() {
            let declared = self.declared_columns_of(database_id, tenant_id, &collection);
            let coerced = match coerce_kv_body(value, declared) {
                Ok(Cow::Owned(bytes)) => Some(bytes),
                Ok(Cow::Borrowed(_)) => None,
                Err(e) => {
                    self.replay_record_unapplied(
                        "kv",
                        "batch_put_declared",
                        record_lsn,
                        &e.to_string(),
                    );
                    return Some(0);
                }
            };
            if let Some(bytes) = coerced {
                *value = bytes;
            }
        }

        let params = crate::engine::kv::KvBatchPutParams {
            database_id,
            tenant_id,
            collection: &collection,
            entries: &entries,
            ttl_ms,
            now_ms,
            surrogates: &surrogates,
        };
        // The engine's own write count is redundant here: the caller counts the
        // entries it handed over.
        let written = match expire_at_ms {
            Some(instant) => self
                .kv_engine
                .batch_put_with_absolute_expiry(params, instant),
            None => self.kv_engine.batch_put(params),
        };
        if let Err(e) = written {
            self.replay_record_unapplied("kv", "batch_put_identity", record_lsn, &e.to_string());
            return Some(0);
        }
        for (entry_key, _entry_value) in &entries {
            self.note_replay_write(
                database_id,
                tenant_id,
                &collection,
                Some(KeyRepr::KvKey(Box::from(entry_key.as_slice()))),
                record_lsn,
            );
        }
        Some(entries.len())
    }
}

/// One `kv_rewrite` record's fields:
/// `(collection, key, value, ttl_ms, expire_at_ms)`.
type KvRewriteFields = (String, Vec<u8>, Vec<u8>, u64, u64);

fn decode_kv_rewrite(payload: &[u8]) -> Option<KvRewriteFields> {
    // ("kv_rewrite", collection, key, value, ttl_ms, expire_at_ms)
    let (disc, collection, key, value, ttl_ms, expire_at_ms) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(payload).ok()?;
    (disc == "kv_rewrite").then_some((collection, key, value, ttl_ms, expire_at_ms))
}

/// One `kv_put` record's fields.
type KvPutFields = (String, Vec<u8>, Vec<u8>, u64, Option<u64>, Surrogate);

fn decode_kv_put(payload: &[u8]) -> Option<KvPutFields> {
    // ("kv_put", collection, key, value, ttl_ms, expire_at_ms, surrogate)
    if let Ok((disc, collection, key, value, ttl_ms, expire_at_ms, surrogate)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(payload)
        && disc == "kv_put"
        && surrogate != 0
    {
        return Some((
            collection,
            key,
            value,
            ttl_ms,
            expire_at_ms,
            Surrogate::new(surrogate),
        ));
    }
    None
}

/// One `kv_batch_put` record's fields.
type KvBatchPutFields = (
    String,
    Vec<(Vec<u8>, Vec<u8>)>,
    u64,
    Option<u64>,
    Vec<Surrogate>,
);

fn decode_kv_batch_put(payload: &[u8]) -> Option<KvBatchPutFields> {
    // ("kv_batch_put", collection, entries, ttl_ms, expire_at_ms, surrogates)
    if let Ok((disc, collection, entries, ttl_ms, expire_at_ms, surrogates)) =
        zerompk::from_msgpack::<(
            &str,
            String,
            Vec<(Vec<u8>, Vec<u8>)>,
            u64,
            Option<u64>,
            Vec<u32>,
        )>(payload)
        && disc == "kv_batch_put"
        && !surrogates.contains(&0)
    {
        let surrogates = surrogates.into_iter().map(Surrogate::new).collect();
        return Some((collection, entries, ttl_ms, expire_at_ms, surrogates));
    }
    None
}

/// The KV put-family discriminators whose records carry a row's surrogate.
const KV_PUT_FAMILY: [&str; 4] = [
    "kv_put",
    "kv_batch_put",
    "kv_insert_on_conflict_update",
    "kv_rewrite",
];

/// The KV put-family discriminator `payload` opens with, when it is a
/// msgpack array whose first element names one. The KV replay pass asks this
/// only of a `Put` payload no KV arm decoded, so a match is a KV record of no
/// current shape.
pub(in crate::data::executor) fn kv_put_family_discriminator(
    payload: &[u8],
) -> Option<&'static str> {
    let nodedb_types::Value::Array(items) = nodedb_types::value_from_msgpack(payload).ok()? else {
        return None;
    };
    let Some(nodedb_types::Value::String(first)) = items.first() else {
        return None;
    };
    KV_PUT_FAMILY
        .into_iter()
        .find(|discriminator| *discriminator == first.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::wal_dispatch_kv::encode::{
        encode_kv_batch_put, encode_kv_put, encode_kv_rewrite,
    };

    #[test]
    fn current_kv_put_shape_round_trips_the_real_surrogate() {
        let payload = encode_kv_put("users", b"k1", b"v1", 0, None, 4242).expect("encode");
        let (collection, key, value, ttl_ms, expire_at_ms, surrogate) =
            decode_kv_put(&payload).expect("current shape decodes");
        assert_eq!(collection, "users");
        assert_eq!(key, b"k1");
        assert_eq!(value, b"v1");
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, None);
        assert_eq!(
            surrogate,
            Surrogate::new(4242),
            "a replayed row must keep the identity the live write bound, \
             not fall back to the always-visible zero"
        );
    }

    #[test]
    fn current_kv_put_shape_round_trips_the_absolute_expiry() {
        let payload = encode_kv_put("users", b"k1", b"v1", 5_000, Some(1_700_000_000_000), 7)
            .expect("encode");
        let (_, _, _, ttl_ms, expire_at_ms, surrogate) =
            decode_kv_put(&payload).expect("current shape decodes");
        assert_eq!(ttl_ms, 5_000);
        assert_eq!(expire_at_ms, Some(1_700_000_000_000));
        assert_eq!(surrogate, Surrogate::new(7));
    }

    /// The retired identity-less shapes decode as no KV record, and open
    /// with a KV put discriminator, so replay refuses them.
    #[test]
    fn retired_kv_put_shapes_are_refused_not_replayed_unbound() {
        let entries = vec![(b"k1".to_vec(), b"v1".to_vec())];
        let retired = [
            zerompk::to_msgpack_vec(&("kv_put", "users", b"k1", b"v1", 0u64)).expect("enc"),
            zerompk::to_msgpack_vec(&("kv_put", "users", b"k1", b"v1", 0u64, 99u64)).expect("enc"),
            zerompk::to_msgpack_vec(&("kv_batch_put", "users", &entries, 0u64)).expect("enc"),
        ];
        for payload in retired {
            assert!(decode_kv_put(&payload).is_none());
            assert!(decode_kv_batch_put(&payload).is_none());
            assert!(kv_put_family_discriminator(&payload).is_some());
        }
    }

    /// A put-family record whose surrogate is `0` matches no shape, and its
    /// discriminator makes replay refuse it as unapplied.
    #[test]
    fn unbound_kv_put_records_are_refused_not_replayed() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let put = encode_kv_put("users", b"k1", b"v1", 0, None, 0).expect("encode");
        let batch = encode_kv_batch_put("users", &entries, 0, None, &[3, 0]).expect("encode");
        assert!(decode_kv_put(&put).is_none());
        assert!(decode_kv_batch_put(&batch).is_none());
        assert_eq!(kv_put_family_discriminator(&put), Some("kv_put"));
        assert_eq!(kv_put_family_discriminator(&batch), Some("kv_batch_put"));
    }

    #[test]
    fn current_kv_batch_put_shape_round_trips_one_surrogate_per_entry() {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let payload = encode_kv_batch_put("users", &entries, 0, None, &[11, 12]).expect("encode");
        let (collection, decoded, ttl_ms, expire_at_ms, surrogates) =
            decode_kv_batch_put(&payload).expect("current shape decodes");
        assert_eq!(collection, "users");
        assert_eq!(decoded, entries);
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, None);
        assert_eq!(surrogates, vec![Surrogate::new(11), Surrogate::new(12)]);
    }

    /// A non-KV `Put` payload must fall through both arms so the document and
    /// graph decoders still get a chance at it.
    #[test]
    fn non_kv_payload_is_not_claimed() {
        let doc = zerompk::to_msgpack_vec(&("notes", "doc1", b"body".to_vec())).expect("enc");
        assert!(decode_kv_put(&doc).is_none());
        assert!(decode_kv_batch_put(&doc).is_none());
        assert!(kv_put_family_discriminator(&doc).is_none());
    }

    mod replay {
        use super::*;
        use crate::types::{DatabaseId, TenantId, VShardId};
        use crate::wal::manager::WalManager;
        use nodedb_wal::TombstoneSet;
        use std::sync::Arc;

        const TID: u64 = 1;

        struct CoreHarness {
            core: CoreLoop,
            _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
            _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
            _dir: tempfile::TempDir,
        }

        fn make_core() -> CoreHarness {
            use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
            use nodedb_bridge::buffer::RingBuffer;

            let dir = tempfile::tempdir().expect("tempdir");
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
            let core = CoreLoop::open(
                0,
                req_rx,
                resp_tx,
                dir.path(),
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            )
            .expect("open core");
            CoreHarness {
                core,
                _req_tx: req_tx,
                _resp_rx: resp_rx,
                _dir: dir,
            }
        }

        /// Append `payloads` as `Put` records and read them back the way boot
        /// does.
        fn wal_records(payloads: &[Vec<u8>]) -> (tempfile::TempDir, Vec<nodedb_wal::WalRecord>) {
            let dir = tempfile::tempdir().expect("wal tempdir");
            let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
            for payload in payloads {
                wal.appender(crate::wal::manager::NO_APPLY_KEY)
                    .with_event_source(crate::event::EventSource::User)
                    .append_put(
                        TenantId::new(TID),
                        VShardId::new(0),
                        DatabaseId::DEFAULT,
                        payload,
                    )
                    .expect("append");
            }
            wal.sync().expect("sync");
            let records = wal.replay().expect("replay read");
            (dir, records)
        }

        /// The defect: a replayed row bound to `Surrogate::ZERO` cannot be
        /// resolved by `key_for_surrogate`, and the clone-snapshot rule treats
        /// zero as unconditionally visible.
        #[test]
        fn replayed_row_carries_the_recorded_surrogate() {
            let payload = encode_kv_put("users", b"alice", b"body", 0, None, 4242).expect("encode");
            let (_dir, records) = wal_records(&[payload]);

            let mut h = make_core();
            h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

            assert_eq!(
                h.core.kv_engine.key_for_surrogate(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "users",
                    Surrogate::new(4242)
                ),
                Some(b"alice".to_vec()),
                "the replayed row must resolve through its real surrogate"
            );
        }

        #[test]
        fn replayed_batch_rows_carry_their_recorded_surrogates() {
            let entries = vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ];
            let payload = encode_kv_batch_put("carts", &entries, 0, None, &[7, 8]).expect("encode");
            let (_dir, records) = wal_records(&[payload]);

            let mut h = make_core();
            h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

            let did = DatabaseId::DEFAULT.as_u64();
            assert_eq!(
                h.core
                    .kv_engine
                    .key_for_surrogate(did, TID, "carts", Surrogate::new(7)),
                Some(b"k1".to_vec())
            );
            assert_eq!(
                h.core
                    .kv_engine
                    .key_for_surrogate(did, TID, "carts", Surrogate::new(8)),
                Some(b"k2".to_vec())
            );
        }

        /// A replayed `kv_rewrite` replaces the value and keeps the identity
        /// the row was bound under. A rewrite of an absent row writes nothing.
        #[test]
        fn replayed_rewrite_keeps_the_rows_identity() {
            let put = encode_kv_put("users", b"alice", b"old", 0, None, 77).expect("encode");
            let rewrite = encode_kv_rewrite("users", b"alice", b"new", 0, 0).expect("encode");
            let orphan = encode_kv_rewrite("users", b"bob", b"new", 0, 0).expect("encode");
            let (_dir, records) = wal_records(&[put, rewrite, orphan]);

            let mut h = make_core();
            h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

            let did = DatabaseId::DEFAULT.as_u64();
            assert_eq!(
                h.core
                    .kv_engine
                    .get_with_surrogate(did, TID, "users", b"alice", 0),
                Some((b"new".to_vec(), Surrogate::new(77))),
                "the rewrite replaces the value under the row's bound surrogate"
            );
            assert!(
                h.core
                    .kv_engine
                    .get_with_surrogate(did, TID, "users", b"bob", 0)
                    .is_none(),
                "a rewrite never creates a row without identity"
            );
        }

        /// Replaying the same retained tail a second time must land on
        /// identical state — the whole point of a crash-safe recovery pass.
        #[test]
        fn replaying_the_same_records_twice_is_a_no_op() {
            let payload = encode_kv_put("users", b"alice", b"body", 0, None, 9).expect("encode");
            let (_dir, records) = wal_records(&[payload]);

            let mut h = make_core();
            h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());
            let after_first = h.core.kv_engine.stats().total_entries;
            h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

            assert_eq!(
                h.core.kv_engine.stats().total_entries,
                after_first,
                "a second pass over the same records must not add rows"
            );
            assert_eq!(
                h.core.kv_engine.key_for_surrogate(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "users",
                    Surrogate::new(9)
                ),
                Some(b"alice".to_vec()),
                "and must not disturb the identity the first pass bound"
            );
        }
    }
}
