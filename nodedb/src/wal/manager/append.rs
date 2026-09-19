// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
    /// Internal: append a record of the given type to the WAL.
    pub(super) fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append(
                record_type as u32,
                tenant_id.as_u64(),
                vshard_id.as_u32(),
                database_id.as_u64(),
                payload,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }

    pub fn append_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Put, tid, vs, db, p)
    }

    pub fn append_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Delete, tid, vs, db, p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::record::{FtsIndexPayload, SyncSeqAdvancePayload};

    #[test]
    fn sync_seq_advance_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let lsn = wal
            .append_sync_seq_advance(0xCAFE_BABE_DEAD_BEEF, 7, 42, 1_000_000)
            .unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].header.record_type,
            RecordType::SyncSeqAdvance as u32
        );
        let payload = SyncSeqAdvancePayload::from_bytes(&records[0].payload).unwrap();
        assert_eq!(payload.producer_id, 0xCAFE_BABE_DEAD_BEEF);
        assert_eq!(payload.epoch, 7);
        assert_eq!(payload.stream_id, 42);
        assert_eq!(payload.seq, 1_000_000);
    }

    #[test]
    fn append_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(1);
        let v = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        let lsn1 = wal.append_put(t, v, db, b"key1=value1").unwrap();
        let lsn2 = wal.append_put(t, v, db, b"key2=value2").unwrap();
        let lsn3 = wal.append_delete(t, v, db, b"key1").unwrap();

        assert_eq!(lsn1, Lsn::new(1));
        assert_eq!(lsn2, Lsn::new(2));
        assert_eq!(lsn3, Lsn::new(3));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"key1=value1");
        assert_eq!(records[2].payload, b"key1");
    }

    #[test]
    fn append_transaction_redo_returns_monotonic_lsn() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(3);
        let v = VShardId::new(1);
        let db = DatabaseId::DEFAULT;

        let record = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![1, 2, 3],
            }],
            calvin_stamp: None,
        };

        let lsn1 = wal.append_transaction_redo(t, v, db, &record).unwrap();
        let lsn2 = wal.append_transaction_redo(t, v, db, &record).unwrap();
        let lsn3 = wal.append_transaction_redo(t, v, db, &record).unwrap();

        assert_eq!(lsn1, Lsn::new(1));
        assert_eq!(lsn2, Lsn::new(2));
        assert_eq!(lsn3, Lsn::new(3));
        assert!(lsn2 > lsn1);
        assert!(lsn3 > lsn2);

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0].header.record_type,
            RecordType::TransactionRedo as u32
        );
        let decoded = RedoRecord::from_bytes(&records[0].payload).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn crdt_delta_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(5);
        let v = VShardId::new(42);
        let db = DatabaseId::DEFAULT;

        let lsn = wal
            .append_crdt_delta(t, v, db, b"loro-delta-bytes")
            .unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.record_type, RecordType::CrdtDelta as u32);
        assert_eq!(records[0].header.tenant_id, 5);
        assert_eq!(records[0].header.vshard_id, 42);
        assert_eq!(records[0].payload, b"loro-delta-bytes");
    }

    #[test]
    fn fts_index_append_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_fts");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let payload = FtsIndexPayload::new(
            nodedb_types::sync::wire::SyncProvenance {
                producer_id: 0xDEAD_BEEF_CAFE_1234,
                epoch: 5,
                stream_id: 99,
                seq: 42,
            },
            "articles",
            "doc-abc",
            "hello world nodedb fts",
        );
        let bytes = payload.to_bytes().unwrap();

        let t = TenantId::new(2);
        let v = VShardId::new(7);
        let db = DatabaseId::DEFAULT;

        let lsn = wal.append_fts_index(t, v, db, &bytes).unwrap();
        assert_eq!(lsn, Lsn::new(1));

        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.record_type, RecordType::FtsIndex as u32);
        assert_eq!(records[0].header.tenant_id, 2);
        assert_eq!(records[0].header.vshard_id, 7);

        let decoded = FtsIndexPayload::from_bytes(&records[0].payload).unwrap();
        assert_eq!(decoded.provenance.producer_id, 0xDEAD_BEEF_CAFE_1234);
        assert_eq!(decoded.provenance.epoch, 5);
        assert_eq!(decoded.provenance.stream_id, 99);
        assert_eq!(decoded.provenance.seq, 42);
        assert_eq!(decoded.collection, "articles");
        assert_eq!(decoded.doc_id, "doc-abc");
        assert_eq!(decoded.text, "hello world nodedb fts");
    }
}
