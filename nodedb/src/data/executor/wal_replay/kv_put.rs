// SPDX-License-Identifier: BUSL-1.1

//! Replay arms for the two absolute-overwrite KV record classes, `kv_put` and
//! `kv_batch_put`.
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
//! ## Pre-surrogate shapes
//!
//! Two shorter arities per record class predate the carried surrogate. zerompk
//! enforces a strict array length, so none of the three shapes can alias
//! another. A tail written before the upgrade can still be retained across it,
//! and a Data Plane core has no Control Plane catalog handle to recover the
//! identity from, so those rows replay unbound — exactly the state a
//! pre-upgrade restart left them in, never worse.
//!
//! ## Absolute expiry
//!
//! When the record carries a resolved absolute instant it is installed
//! verbatim. Recomputing `now_ms + ttl_ms` at replay time would push every
//! expiry forward by the crash-to-restart delay.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
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
        let _displaced = match expire_at_ms {
            Some(instant) => self.kv_engine.put_with_absolute_expiry(params, instant),
            None => self.kv_engine.put(params),
        };
        self.note_replay_write_lsn(
            database_id,
            tenant_id,
            &collection,
            Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
            record_lsn,
        );
        Some(1)
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
        let _written = match expire_at_ms {
            Some(instant) => self
                .kv_engine
                .batch_put_with_absolute_expiry(params, instant),
            None => self.kv_engine.batch_put(params),
        };
        for (entry_key, _entry_value) in &entries {
            self.note_replay_write_lsn(
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

/// One `kv_put` record's fields, normalized across the current and the two
/// pre-surrogate shapes.
type KvPutFields = (String, Vec<u8>, Vec<u8>, u64, Option<u64>, Surrogate);

fn decode_kv_put(payload: &[u8]) -> Option<KvPutFields> {
    // Current: ("kv_put", collection, key, value, ttl_ms, expire_at_ms, surrogate)
    if let Ok((disc, collection, key, value, ttl_ms, expire_at_ms, surrogate)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(payload)
        && disc == "kv_put"
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
    // Pre-surrogate, with absolute expiry.
    if let Ok((disc, collection, key, value, ttl_ms, expire_at_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(payload)
        && disc == "kv_put"
    {
        return Some((
            collection,
            key,
            value,
            ttl_ms,
            Some(expire_at_ms),
            Surrogate::ZERO,
        ));
    }
    // Pre-surrogate, no absolute expiry.
    if let Ok((disc, collection, key, value, ttl_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(payload)
        && disc == "kv_put"
    {
        return Some((collection, key, value, ttl_ms, None, Surrogate::ZERO));
    }
    None
}

/// One `kv_batch_put` record's fields, normalized across the current and the
/// two pre-surrogate shapes.
type KvBatchPutFields = (
    String,
    Vec<(Vec<u8>, Vec<u8>)>,
    u64,
    Option<u64>,
    Vec<Surrogate>,
);

fn decode_kv_batch_put(payload: &[u8]) -> Option<KvBatchPutFields> {
    // Current: ("kv_batch_put", collection, entries, ttl_ms, expire_at_ms, surrogates)
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
    {
        let surrogates = surrogates.into_iter().map(Surrogate::new).collect();
        return Some((collection, entries, ttl_ms, expire_at_ms, surrogates));
    }
    // Pre-surrogate, with absolute expiry.
    if let Ok((disc, collection, entries, ttl_ms, expire_at_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(payload)
        && disc == "kv_batch_put"
    {
        let surrogates = vec![Surrogate::ZERO; entries.len()];
        return Some((collection, entries, ttl_ms, Some(expire_at_ms), surrogates));
    }
    // Pre-surrogate, no absolute expiry.
    if let Ok((disc, collection, entries, ttl_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(payload)
        && disc == "kv_batch_put"
    {
        let surrogates = vec![Surrogate::ZERO; entries.len()];
        return Some((collection, entries, ttl_ms, None, surrogates));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::wal_dispatch_kv::encode::{encode_kv_batch_put, encode_kv_put};

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

    /// A tail written before the surrogate was carried must still replay, and
    /// must not be mistaken for the current shape.
    #[test]
    fn pre_surrogate_kv_put_shapes_still_decode_unbound() {
        let five = zerompk::to_msgpack_vec(&("kv_put", "users", b"k1", b"v1", 0u64)).expect("enc");
        let (_, _, _, ttl_ms, expire_at_ms, surrogate) =
            decode_kv_put(&five).expect("five-element shape decodes");
        assert_eq!(ttl_ms, 0);
        assert_eq!(expire_at_ms, None);
        assert_eq!(surrogate, Surrogate::ZERO);

        let six =
            zerompk::to_msgpack_vec(&("kv_put", "users", b"k1", b"v1", 0u64, 99u64)).expect("enc");
        let (_, _, _, _, expire_at_ms, surrogate) =
            decode_kv_put(&six).expect("six-element shape decodes");
        assert_eq!(expire_at_ms, Some(99));
        assert_eq!(surrogate, Surrogate::ZERO);
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

    #[test]
    fn pre_surrogate_kv_batch_put_shapes_still_decode_unbound() {
        let entries = vec![(b"k1".to_vec(), b"v1".to_vec())];
        let four =
            zerompk::to_msgpack_vec(&("kv_batch_put", "users", &entries, 0u64)).expect("enc");
        let (_, decoded, _, expire_at_ms, surrogates) =
            decode_kv_batch_put(&four).expect("four-element shape decodes");
        assert_eq!(decoded, entries);
        assert_eq!(expire_at_ms, None);
        assert_eq!(surrogates, vec![Surrogate::ZERO]);
    }

    /// A non-KV `Put` payload must fall through both arms so the document and
    /// graph decoders still get a chance at it.
    #[test]
    fn non_kv_payload_is_not_claimed() {
        let doc = zerompk::to_msgpack_vec(&("notes", "doc1", b"body".to_vec())).expect("enc");
        assert!(decode_kv_put(&doc).is_none());
        assert!(decode_kv_batch_put(&doc).is_none());
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
