// SPDX-License-Identifier: BUSL-1.1

//! The handle every WAL append goes through.
//!
//! A record's header carries an apply key: the idempotency key of the
//! replicated proposal whose apply appended it, [`NO_APPLY_KEY`] for a record
//! no proposal owns. The apply loop rebuilds which proposals applied from
//! these keys. The key is part of the append call: a caller names it when it
//! takes an appender, so no append can read a key another caller set and no
//! caller has a key to clear.

use nodedb_wal::RecordTarget;
use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// The apply key of a record no replicated proposal owns.
pub const NO_APPLY_KEY: u64 = 0;

/// Appends WAL records that all carry one apply key.
#[derive(Clone, Copy)]
pub struct WalAppender<'a> {
    wal: &'a WalManager,
    apply_key: u64,
}

impl WalManager {
    /// An appender whose records carry `apply_key`. A replicated proposal's
    /// apply passes the proposal's idempotency key. Every other append passes
    /// [`NO_APPLY_KEY`].
    pub fn appender(&self, apply_key: u64) -> WalAppender<'_> {
        WalAppender {
            wal: self,
            apply_key,
        }
    }
}

impl WalAppender<'_> {
    /// The apply key every record this appender writes carries.
    pub fn apply_key(&self) -> u64 {
        self.apply_key
    }

    /// Append one record of `record_type`.
    pub(super) fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append_keyed(
                RecordTarget {
                    record_type: record_type as u32,
                    tenant_id: tenant_id.as_u64(),
                    vshard_id: vshard_id.as_u32(),
                    database_id: database_id.as_u64(),
                },
                payload,
                self.apply_key,
            )
            .map_err(crate::Error::Wal)?;
        Ok(Lsn::new(lsn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_record_carries_the_key_of_the_appender_that_wrote_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let (t, v, db) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);

        let keyed = wal.appender(0xAB);
        keyed.append_put(t, v, db, b"keyed").expect("keyed append");
        wal.appender(NO_APPLY_KEY)
            .append_put(t, v, db, b"unkeyed")
            .expect("unkeyed append");
        keyed
            .append_put(t, v, db, b"keyed again")
            .expect("keyed append");
        wal.sync().expect("sync");

        let keys: Vec<u64> = wal
            .replay()
            .expect("replay")
            .iter()
            .map(|record| record.apply_key())
            .collect();
        assert_eq!(keys, vec![0xAB, NO_APPLY_KEY, 0xAB]);
    }
}
