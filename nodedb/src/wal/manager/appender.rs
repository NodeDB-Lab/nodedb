// SPDX-License-Identifier: BUSL-1.1

//! The handle every WAL append goes through.
//!
//! A record's header carries an apply key: the idempotency key of the
//! replicated proposal whose apply appended it, [`NO_APPLY_KEY`] for a record
//! no proposal owns. The apply loop rebuilds which proposals applied from
//! these keys. The key is part of the append call: a caller names it when it
//! takes an appender, so no append can read a key another caller set and no
//! caller has a key to clear.
//!
//! A row-write record (Put, Delete, TransactionRedo, graph node-label) also
//! carries the event source its write ran with, so WAL replay rebuilds the
//! event the live write emitted. The caller names the source with
//! [`WalAppender::with_event_source`]. A row-write append from an appender
//! with no source is refused: no row write is stored without one.

use std::sync::Mutex;

use nodedb_wal::RecordTarget;
use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// The apply key of a record no replicated proposal owns.
pub const NO_APPLY_KEY: u64 = 0;

/// One record a recording appender wrote: its LSN and the header fields
/// that place it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedAppend {
    pub lsn: Lsn,
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
}

/// Where a recording appender reports each record it writes. The report
/// runs while the WAL lock is held, so no reader sees the record before its
/// recorder does.
pub trait AppendSink: Sync {
    fn record(&self, append: RecordedAppend);
}

impl AppendSink for Mutex<Vec<RecordedAppend>> {
    fn record(&self, append: RecordedAppend) {
        self.lock().unwrap_or_else(|p| p.into_inner()).push(append);
    }
}

/// Appends WAL records that all carry one apply key, and row-write records
/// that carry one event source.
#[derive(Clone, Copy)]
pub struct WalAppender<'a> {
    wal: &'a WalManager,
    apply_key: u64,
    /// The event source code row-write records carry.
    /// `nodedb_wal::NO_EVENT_SOURCE` until the caller names one.
    event_source: u8,
    /// Collects every record this appender writes, when set.
    sink: Option<&'a dyn AppendSink>,
}

impl WalManager {
    /// An appender whose records carry `apply_key`. A replicated proposal's
    /// apply passes the proposal's idempotency key. Every other append passes
    /// [`NO_APPLY_KEY`].
    pub fn appender(&self, apply_key: u64) -> WalAppender<'_> {
        WalAppender {
            wal: self,
            apply_key,
            event_source: nodedb_wal::NO_EVENT_SOURCE,
            sink: None,
        }
    }

    /// An appender that also reports every record it writes to `sink`, in
    /// append order.
    pub fn recording_appender<'a>(
        &'a self,
        apply_key: u64,
        sink: &'a dyn AppendSink,
    ) -> WalAppender<'a> {
        WalAppender {
            wal: self,
            apply_key,
            event_source: nodedb_wal::NO_EVENT_SOURCE,
            sink: Some(sink),
        }
    }
}

impl WalAppender<'_> {
    /// The apply key every record this appender writes carries.
    pub fn apply_key(&self) -> u64 {
        self.apply_key
    }

    /// This appender, with row-write records carrying `source`.
    pub fn with_event_source(self, source: crate::event::EventSource) -> Self {
        Self {
            event_source: source.wal_code(),
            ..self
        }
    }

    /// Append one record of `record_type` that carries no row write.
    pub(super) fn append_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let target = RecordTarget {
            record_type: record_type as u32,
            tenant_id: tenant_id.as_u64(),
            vshard_id: vshard_id.as_u32(),
            database_id: database_id.as_u64(),
            event_source: nodedb_wal::NO_EVENT_SOURCE,
        };
        self.append_target(target, tenant_id, vshard_id, database_id, payload)
    }

    /// Append one row-write record of `record_type`, carrying this
    /// appender's event source. Refused when the caller named no source.
    pub(super) fn append_row_record(
        &self,
        record_type: RecordType,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        if self.event_source == nodedb_wal::NO_EVENT_SOURCE {
            return Err(crate::Error::Internal {
                detail: format!(
                    "WAL append: a {record_type:?} row-write record needs the event source \
                     of its write; the appender names none"
                ),
            });
        }
        let target = RecordTarget {
            record_type: record_type as u32,
            tenant_id: tenant_id.as_u64(),
            vshard_id: vshard_id.as_u32(),
            database_id: database_id.as_u64(),
            event_source: self.event_source,
        };
        self.append_target(target, tenant_id, vshard_id, database_id, payload)
    }

    fn append_target(
        &self,
        target: RecordTarget,
        tenant_id: TenantId,
        vshard_id: VShardId,
        database_id: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        let mut wal = self.wal.wal.lock().unwrap_or_else(|p| p.into_inner());
        let lsn = wal
            .append_keyed(target, payload, self.apply_key)
            .map_err(crate::Error::Wal)?;
        let lsn = Lsn::new(lsn);
        if let Some(sink) = self.sink {
            sink.record(RecordedAppend {
                lsn,
                tenant_id,
                vshard_id,
                database_id,
            });
        }
        drop(wal);
        Ok(lsn)
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

        let keyed = wal
            .appender(0xAB)
            .with_event_source(crate::event::EventSource::User);
        keyed.append_put(t, v, db, b"keyed").expect("keyed append");
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
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

    #[test]
    fn a_recording_appender_collects_every_lsn_it_writes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let (t, v, db) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let sink = Mutex::new(Vec::new());

        let recording = wal
            .recording_appender(NO_APPLY_KEY, &sink)
            .with_event_source(crate::event::EventSource::User);
        let first = recording.append_put(t, v, db, b"a").expect("append");
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(t, v, db, b"unrecorded")
            .expect("append");
        let second = recording.append_put(t, v, db, b"b").expect("append");

        let recorded: Vec<Lsn> = sink
            .lock()
            .expect("sink")
            .iter()
            .map(|record| record.lsn)
            .collect();
        assert_eq!(recorded, vec![first, second]);
    }
}
