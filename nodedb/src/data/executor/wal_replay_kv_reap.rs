// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the KV `kv_reap` record a replicated TTL reap appends.
//!
//! A reap is a resolved KV write the data group's leader proposed once the
//! key's TTL passed. The record names the expiry it reaps. Replay runs in log
//! order, so the key holds at this point exactly what it held when the live
//! apply ran. Replay reaps it only while it still holds that expiry, as the
//! live apply did, and ends the version at the same instant: the last
//! ordinal of the expiry millisecond. Replay and every replica reach one
//! state.

use super::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::KeyRepr;

impl CoreLoop {
    /// Decode + tombstone-gate + replay one `kv_reap` WAL record.
    ///
    /// Returns `None` when `payload` is no `kv_reap` record (the caller tries
    /// the next arm), otherwise `Some(applied)`: `1` when the key was reaped,
    /// `0` when tombstoned or when the key no longer holds the expiry.
    pub(super) fn try_replay_kv_reap(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        let (disc, collection, key, expire_at_ms) =
            zerompk::from_msgpack::<(&str, String, Vec<u8>, u64)>(payload).ok()?;
        if disc != "kv_reap" {
            return None;
        }
        let tombstones = &tombstones.for_database(database_id);
        if self.skip_kv_replay_record(tombstones, tenant_id, &collection, record_lsn) {
            return Some(0);
        }
        if self.claim_for_validation() {
            return Some(0);
        }
        if self
            .kv_engine
            .reap_due(database_id, tenant_id, &collection, &key, expire_at_ms)
            .is_none()
        {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::{KvOp, KvResolvedMutation};
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;

    const TID: u64 = 1;

    fn collection() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "sessions")
    }

    fn resolved(mutations: Vec<KvResolvedMutation>) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::ResolvedWrite {
            mutations,
            response_payload: Vec::new(),
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        })
    }

    fn put(key: &[u8], expire_at_ms: u64, surrogate: u32) -> KvResolvedMutation {
        KvResolvedMutation::Put {
            collection: collection(),
            key: key.to_vec(),
            value: b"v".to_vec(),
            ttl_ms: 1,
            expire_at_ms,
            surrogate: Surrogate::new(surrogate),
            precondition: None,
        }
    }

    fn reap(key: &[u8], expire_at_ms: u64) -> KvResolvedMutation {
        KvResolvedMutation::Reap {
            collection: collection(),
            key: key.to_vec(),
            expire_at_ms,
        }
    }

    /// A replayed reap ends the key whose expiry it names, and leaves a key
    /// whose expiry a write replaced, as the live apply did.
    #[test]
    fn a_replayed_reap_repeats_the_live_verdict() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plans = [
            resolved(vec![put(b"due", 1_500, 1), put(b"kept", u64::MAX - 1, 2)]),
            resolved(vec![reap(b"due", 1_500), reap(b"kept", 1_500)]),
        ];
        for plan in &plans {
            let outcome = wal_append_if_write(
                &wal,
                TenantId::new(TID),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                plan,
            )
            .expect("wal append");
            assert!(outcome.lsn.is_some(), "a resolved write appends a record");
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let dir = tempfile::tempdir().expect("core tempdir");
        let (_req_tx, req_rx) = nodedb_bridge::buffer::RingBuffer::channel::<
            crate::bridge::dispatch::BridgeRequest,
        >(64);
        let (resp_tx, _resp_rx) = nodedb_bridge::buffer::RingBuffer::channel::<
            crate::bridge::dispatch::BridgeResponse,
        >(64);
        let mut core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        core.replay_kv_wal(&records, 1, &TombstoneSet::new());

        let get = |core: &CoreLoop, key: &[u8]| {
            core.kv_engine
                .get(DatabaseId::DEFAULT.as_u64(), TID, "sessions", key, 0)
        };
        assert_eq!(get(&core, b"due"), None, "the named expiry was reaped");
        assert_eq!(
            get(&core, b"kept"),
            Some(b"v".to_vec()),
            "a replaced expiry is kept"
        );
    }
}
