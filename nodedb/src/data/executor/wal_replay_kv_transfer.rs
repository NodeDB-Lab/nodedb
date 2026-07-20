// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the atomic KV `Transfer` / `TransferItem` delta records.
//!
//! `wal_dispatch_kv/encode.rs` logs these as DELTA records (inputs, not post-images)
//! because the Control Plane cannot know the post-transfer values before
//! dispatch. Replay re-executes the same pure computation
//! (`compute_transfer`) and the same mutation sequence the live handlers in
//! `handlers/kv/transfer.rs` perform, against whatever state this core's KV
//! engine holds at this point in LSN-ordered replay.

use tracing::warn;

use super::core_loop::CoreLoop;
use super::handlers::kv::transfer_compute::{TransferError, compute_transfer};
use crate::data::executor::core_loop::write_index::KeyRepr;

/// Fields needed to replay one `kv_transfer` delta record.
pub(super) struct ReplayKvTransferParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub now_ms: u64,
    pub record_lsn: u64,
    pub collection: &'a str,
    pub source_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub field: &'a str,
    pub amount: f64,
    pub debit_surrogate: u32,
    pub credit_surrogate: u32,
}

/// Fields needed to replay one `kv_transfer_item` delta record.
pub(super) struct ReplayKvTransferItemParams<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub now_ms: u64,
    pub record_lsn: u64,
    pub source_collection: &'a str,
    pub dest_collection: &'a str,
    pub item_key: &'a [u8],
    pub dest_key: &'a [u8],
    pub surrogate: u32,
}

impl CoreLoop {
    /// Replay a `kv_transfer` delta: re-read source/dest, re-run
    /// `compute_transfer`, re-write both keys. Returns the number of `put`s
    /// applied (0 if the record was skipped).
    ///
    /// A source key absent from this core's KV engine (e.g. the collection
    /// was truncated by a later, already-replayed record that this record
    /// predates and the tombstone gate didn't catch, or the WAL was
    /// truncated/corrupted between the originating `kv_put` and this
    /// record) makes the transfer unresolvable. Mirroring how the sibling
    /// `kv_field_set` arm above is skipped outright when its precondition
    /// (an existing value) cannot be met, this logs a `warn` with the
    /// collection/keys and skips the record rather than panicking or
    /// fabricating a balance.
    pub(super) fn replay_kv_transfer(&mut self, p: ReplayKvTransferParams<'_>) -> usize {
        let Some(source_bytes) = self.kv_engine.get(
            p.database_id,
            p.tenant_id,
            p.collection,
            p.source_key,
            p.now_ms,
        ) else {
            warn!(
                core = self.core_id,
                collection = p.collection,
                source_key = %String::from_utf8_lossy(p.source_key),
                dest_key = %String::from_utf8_lossy(p.dest_key),
                "WAL kv_transfer replay: source key missing, skipping record"
            );
            return 0;
        };

        let dest_bytes = self
            .kv_engine
            .get(
                p.database_id,
                p.tenant_id,
                p.collection,
                p.dest_key,
                p.now_ms,
            )
            .unwrap_or_default();
        let dest_ref = if dest_bytes.is_empty() {
            None
        } else {
            Some(dest_bytes.as_slice())
        };

        let computed = match compute_transfer(&source_bytes, dest_ref, p.field, p.amount) {
            Ok(c) => c,
            Err(TransferError::TypeMismatch(detail)) => {
                warn!(
                    core = self.core_id,
                    collection = p.collection,
                    source_key = %String::from_utf8_lossy(p.source_key),
                    dest_key = %String::from_utf8_lossy(p.dest_key),
                    %detail,
                    "WAL kv_transfer replay: type mismatch, skipping record"
                );
                return 0;
            }
            Err(TransferError::InsufficientBalance { have, need }) => {
                warn!(
                    core = self.core_id,
                    collection = p.collection,
                    source_key = %String::from_utf8_lossy(p.source_key),
                    dest_key = %String::from_utf8_lossy(p.dest_key),
                    have,
                    need,
                    "WAL kv_transfer replay: insufficient balance, skipping record"
                );
                return 0;
            }
        };

        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: p.database_id,
            tenant_id: p.tenant_id,
            collection: p.collection,
            key: p.source_key,
            value: &computed.new_source,
            ttl_ms: 0,
            now_ms: p.now_ms,
            surrogate: nodedb_types::Surrogate::new(p.debit_surrogate),
        });
        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: p.database_id,
            tenant_id: p.tenant_id,
            collection: p.collection,
            key: p.dest_key,
            value: &computed.new_dest,
            ttl_ms: 0,
            now_ms: p.now_ms,
            surrogate: nodedb_types::Surrogate::new(p.credit_surrogate),
        });
        self.note_replay_write_lsn(
            p.database_id,
            p.tenant_id,
            p.collection,
            Some(KeyRepr::KvKey(Box::from(p.source_key))),
            p.record_lsn,
        );
        self.note_replay_write_lsn(
            p.database_id,
            p.tenant_id,
            p.collection,
            Some(KeyRepr::KvKey(Box::from(p.dest_key))),
            p.record_lsn,
        );
        2
    }

    /// Replay a `kv_transfer_item` delta: re-verify source ownership, then
    /// re-run the delete+insert pair. Returns `(puts, deletes)` applied.
    ///
    /// A missing source item (already moved, or its originating `kv_put`
    /// never reached this core) makes the move unresolvable — logs a `warn`
    /// and skips, matching the missing-source policy in
    /// [`Self::replay_kv_transfer`].
    pub(super) fn replay_kv_transfer_item(
        &mut self,
        p: ReplayKvTransferItemParams<'_>,
    ) -> (usize, usize) {
        let Some(item_data) = self.kv_engine.get(
            p.database_id,
            p.tenant_id,
            p.source_collection,
            p.item_key,
            p.now_ms,
        ) else {
            warn!(
                core = self.core_id,
                source_collection = p.source_collection,
                dest_collection = p.dest_collection,
                item_key = %String::from_utf8_lossy(p.item_key),
                "WAL kv_transfer_item replay: source item missing, skipping record"
            );
            return (0, 0);
        };

        self.kv_engine.delete(
            p.database_id,
            p.tenant_id,
            p.source_collection,
            &[p.item_key.to_vec()],
            p.now_ms,
        );
        self.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: p.database_id,
            tenant_id: p.tenant_id,
            collection: p.dest_collection,
            key: p.dest_key,
            value: &item_data,
            ttl_ms: 0,
            now_ms: p.now_ms,
            surrogate: nodedb_types::Surrogate::new(p.surrogate),
        });

        self.note_replay_write_lsn(
            p.database_id,
            p.tenant_id,
            p.source_collection,
            Some(KeyRepr::KvKey(Box::from(p.item_key))),
            p.record_lsn,
        );
        self.note_replay_write_lsn(
            p.database_id,
            p.tenant_id,
            p.dest_collection,
            Some(KeyRepr::KvKey(Box::from(p.dest_key))),
            p.record_lsn,
        );

        (1, 1)
    }

    /// Decode + tombstone-gate + replay one `kv_transfer` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_transfer`
    /// discriminator shape (caller tries the next candidate arm), otherwise
    /// `Some(puts_applied)` (0 if tombstoned or skipped).
    pub(super) fn try_replay_kv_transfer(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (
            disc,
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate,
            credit_surrogate,
        ) = zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, String, f64, u32, u32)>(
            payload,
        )
        .ok()?;
        if disc != "kv_transfer" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        Some(self.replay_kv_transfer(ReplayKvTransferParams {
            database_id,
            tenant_id,
            now_ms,
            record_lsn,
            collection: &collection,
            source_key: &source_key,
            dest_key: &dest_key,
            field: &field,
            amount,
            debit_surrogate,
            credit_surrogate,
        }))
    }

    /// Decode + skip-gate + replay one `kv_transfer_item` WAL record.
    ///
    /// Returns `None` when `payload` does not match the `kv_transfer_item`
    /// discriminator shape, otherwise `Some((puts, deletes))` applied (both
    /// 0 if skipped). The move touches two collections, so it is skipped whole
    /// if either side is gated out — matching the single-collection gate used
    /// for every other KV WAL arm, applied to both collections
    /// `execute_kv_transfer_item` authoritatively mutates (delete from source,
    /// insert into dest).
    ///
    /// Gating on either side is only sound because the two sides can never
    /// disagree. The tombstone half is per-collection, but the checkpoint half
    /// is engine-wide: a KV checkpoint publishes every collection at ONE LSN
    /// atomically, so this record is either below that LSN for both sides or
    /// above it for both. Were the checkpoint floor per-collection, a
    /// source-covered / dest-uncovered split would be unrepresentable here —
    /// skipping would drop the dest insert, applying would double-debit the
    /// source. See `kv_checkpoint.rs`.
    pub(super) fn try_replay_kv_transfer_item(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        now_ms: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<(usize, usize)> {
        let (disc, source_collection, dest_collection, item_key, dest_key, surrogate) =
            zerompk::from_msgpack::<(&str, String, String, Vec<u8>, Vec<u8>, u32)>(payload).ok()?;
        if disc != "kv_transfer_item" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &source_collection, record_lsn)
            || self.skip_kv_replay_record(tombstones, tenant_id, &dest_collection, record_lsn)
        {
            return Some((0, 0));
        }
        Some(self.replay_kv_transfer_item(ReplayKvTransferItemParams {
            database_id,
            tenant_id,
            now_ms,
            record_lsn,
            source_collection: &source_collection,
            dest_collection: &dest_collection,
            item_key: &item_key,
            dest_key: &dest_key,
            surrogate,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::Surrogate;
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;

    const TID: u64 = 1;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive replay directly and never tick the event loop.
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
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn kv_doc(field: &str, value: f64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ field: value })).expect("encode doc")
    }

    /// Append each plan through the **production autocommit WAL path**
    /// (`wal_append_if_write`), asserting every write plan produced a durable
    /// record (`Some(lsn)`) before reading the records back. This is the exact
    /// assertion that fails on the pre-fix code, where `Transfer` /
    /// `TransferItem` hit the read-only catch-all arm in
    /// `wal_dispatch_kv::wal_append_kv_op` and no record was ever written.
    fn append_via_autocommit(plans: &[PhysicalPlan]) -> Vec<nodedb_wal::WalRecord> {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        for plan in plans {
            let outcome = wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "kv Transfer/TransferItem autocommit writes must produce a durable WAL record"
            );
        }
        wal.sync().expect("wal sync");
        wal.replay().expect("wal replay read")
    }

    #[test]
    fn kv_transfer_survives_wal_replay_from_empty() {
        let put_alice = PhysicalPlan::Kv(KvOp::Put {
            collection: "accounts".into(),
            key: b"alice".to_vec(),
            value: kv_doc("balance", 100.0),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let put_bob = PhysicalPlan::Kv(KvOp::Put {
            collection: "accounts".into(),
            key: b"bob".to_vec(),
            value: kv_doc("balance", 10.0),
            ttl_ms: 0,
            surrogate: Surrogate::new(2),
        });
        let transfer = PhysicalPlan::Kv(KvOp::Transfer {
            collection: "accounts".into(),
            source_key: b"alice".to_vec(),
            dest_key: b"bob".to_vec(),
            field: "balance".into(),
            amount: 30.0,
            debit_surrogate: Surrogate::new(1),
            credit_surrogate: Surrogate::new(2),
        });

        // The `wal_append_kv_op` assertion the pre-fix code fails: `Transfer`
        // must yield `Some(lsn)`, not `None`.
        let records = append_via_autocommit(&[put_alice, put_bob, transfer]);

        // Replay into a fresh-from-empty KV engine (no checkpoint), exactly
        // how the KV engine recovers after a crash.
        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let now_ms = crate::engine::kv::current_ms();
        let alice = h
            .core
            .kv_engine
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "accounts",
                b"alice",
                now_ms,
            )
            .expect("alice survives replay");
        let bob = h
            .core
            .kv_engine
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                "accounts",
                b"bob",
                now_ms,
            )
            .expect("bob survives replay");

        assert_eq!(
            super::super::handlers::kv::transfer_compute::extract_numeric_field(&alice, "balance"),
            Some(70.0),
            "source balance must reflect the replayed transfer, not just the pre-transfer put"
        );
        assert_eq!(
            super::super::handlers::kv::transfer_compute::extract_numeric_field(&bob, "balance"),
            Some(40.0),
            "dest balance must reflect the replayed transfer"
        );
    }

    #[test]
    fn kv_transfer_item_survives_wal_replay_from_empty() {
        let put_item = PhysicalPlan::Kv(KvOp::Put {
            collection: "inventory".into(),
            key: b"sword_1".to_vec(),
            value: kv_doc("power", 5.0),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
        });
        let transfer_item = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "inventory".into(),
            dest_collection: "trades".into(),
            item_key: b"sword_1".to_vec(),
            dest_key: b"sword_1_moved".to_vec(),
            surrogate: Surrogate::new(7),
        });

        let records = append_via_autocommit(&[put_item, transfer_item]);

        let mut h = make_core();
        h.core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let now_ms = crate::engine::kv::current_ms();
        assert!(
            h.core
                .kv_engine
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "inventory",
                    b"sword_1",
                    now_ms
                )
                .is_none(),
            "item must be gone from the source collection after replay"
        );
        assert!(
            h.core
                .kv_engine
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    TID,
                    "trades",
                    b"sword_1_moved",
                    now_ms
                )
                .is_some(),
            "item must be present in the destination collection after replay"
        );
    }
}
