// SPDX-License-Identifier: BUSL-1.1

//! The durable ledger of the CDC change-stream buffers.
//!
//! Routing an event pushes it into the buffer of every matching change
//! stream. Consumers read those buffers, so a push is the sink's effect. The
//! ledger keeps both halves on disk in one redb file:
//!
//! - **Events:** the retained events of every change-stream buffer.
//! - **Routed keys:** the key of every event routed since the watermark.
//!
//! Every flush writes the buffer changes and the routed keys in one
//! transaction, under the lock routing takes, so the persisted buffers and
//! keys always agree. At startup the buffers are restored first. A replayed
//! event whose key was persisted is already in a restored buffer and is not
//! routed again. An event routed after the last flush is in neither, and the
//! replay routes it once.
//!
//! Durable topics hydrate their own buffers, so the ledger holds change-stream
//! buffers only.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::event::cdc::router::BufferKey;
use crate::event::cdc::{CdcEvent, CdcOffset, CdcRouter};
use crate::event::types::WriteEvent;
use crate::event::watermark_tracker::WatermarkTracker;
use crate::types::DatabaseId;

use super::key::SinkEventKey;

/// Retained events, keyed by stream and position.
const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cdc_events");
/// Keys of the routed events.
const ROUTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cdc_routed");

fn storage(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("cdc ledger: {detail}"),
    }
}

fn serialization(detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "json".into(),
        detail: format!("cdc ledger event: {detail}"),
    }
}

/// The row prefix of one stream: database, tenant, then the length-prefixed
/// stream name.
fn stream_prefix((database_id, tenant_id, name): &BufferKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(20 + name.len());
    out.extend_from_slice(&database_id.as_u64().to_be_bytes());
    out.extend_from_slice(&tenant_id.to_be_bytes());
    let name_len = u32::try_from(name.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&name_len.to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    out
}

fn event_row(prefix: &[u8], position: CdcOffset) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + 16);
    out.extend_from_slice(prefix);
    out.extend_from_slice(&position.lsn.to_be_bytes());
    out.extend_from_slice(&position.sequence.to_be_bytes());
    out
}

/// The first and last row a stream can hold.
fn stream_bounds(key: &BufferKey) -> (Vec<u8>, Vec<u8>) {
    let prefix = stream_prefix(key);
    let mut last = prefix.clone();
    last.extend_from_slice(&[u8::MAX; 16]);
    (event_row(&prefix, CdcOffset::ZERO), last)
}

/// The stream a row belongs to.
fn row_stream(row: &[u8]) -> Option<BufferKey> {
    let database_id = u64::from_be_bytes(row.get(0..8)?.try_into().ok()?);
    let tenant_id = u64::from_be_bytes(row.get(8..16)?.try_into().ok()?);
    let name_len = u32::from_be_bytes(row.get(16..20)?.try_into().ok()?) as usize;
    let name = std::str::from_utf8(row.get(20..20usize.checked_add(name_len)?)?).ok()?;
    Some((DatabaseId::new(database_id), tenant_id, name.to_owned()))
}

#[derive(Default)]
struct Routed {
    keys: HashSet<SinkEventKey>,
    /// Routed since the last flush.
    pending: Vec<SinkEventKey>,
}

/// Durable record of the change-stream buffers and the events routed into
/// them.
pub struct CdcLedger {
    db: Database,
    routed: Mutex<Routed>,
}

impl CdcLedger {
    /// Open or create the ledger at `{dir}/cdc_ledger.redb`, loading the
    /// routed keys.
    pub fn open(dir: &Path) -> crate::Result<Self> {
        std::fs::create_dir_all(dir)
            .map_err(|e| storage(format!("create {}: {e}", dir.display())))?;
        let path = dir.join("cdc_ledger.redb");
        let db = Database::create(&path)
            .map_err(|e| storage(format!("open {}: {e}", path.display())))?;
        let txn = db.begin_write().map_err(storage)?;
        let mut keys = HashSet::new();
        {
            txn.open_table(EVENTS).map_err(storage)?;
            let table = txn.open_table(ROUTED).map_err(storage)?;
            for entry in table.iter().map_err(storage)? {
                let (row, _) = entry.map_err(storage)?;
                let key = SinkEventKey::from_bytes(row.value())
                    .ok_or_else(|| storage("a routed key did not decode"))?;
                keys.insert(key);
            }
        }
        txn.commit().map_err(storage)?;
        Ok(Self {
            db,
            routed: Mutex::new(Routed {
                keys,
                pending: Vec::new(),
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, Routed> {
        self.routed.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Route `event` unless an event with `key` was routed already. An event
    /// without a key reproduces no WAL record and is never replayed, so it
    /// always routes.
    pub fn route_once(
        &self,
        key: Option<&SinkEventKey>,
        router: &CdcRouter,
        event: &WriteEvent,
        watermark_tracker: &WatermarkTracker,
    ) {
        let mut routed = self.lock();
        if let Some(key) = key {
            if !routed.keys.insert(key.clone()) {
                return;
            }
            routed.pending.push(key.clone());
        }
        router.route_event(event, watermark_tracker);
    }

    /// Push every persisted event of a registered stream back into its
    /// buffer. Events of a stream that no longer exists are deleted.
    pub fn restore_into(&self, router: &CdcRouter) -> crate::Result<()> {
        let mut orphaned: HashSet<BufferKey> = HashSet::new();
        {
            let txn = self.db.begin_read().map_err(storage)?;
            let table = txn.open_table(EVENTS).map_err(storage)?;
            for entry in table.iter().map_err(storage)? {
                let (row, bytes) = entry.map_err(storage)?;
                let stream = row_stream(row.value())
                    .ok_or_else(|| storage("an event row did not decode"))?;
                let (database_id, tenant_id, name) = &stream;
                let Some(def) = router.registry().get(*database_id, *tenant_id, name) else {
                    orphaned.insert(stream);
                    continue;
                };
                let event: CdcEvent = sonic_rs::from_slice(bytes.value()).map_err(serialization)?;
                router
                    .ensure_buffer(*database_id, *tenant_id, name, &def.retention)
                    .push(event);
            }
        }
        if orphaned.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut table = txn.open_table(EVENTS).map_err(storage)?;
            for stream in &orphaned {
                let (first, last) = stream_bounds(stream);
                table
                    .retain_in(first.as_slice()..=last.as_slice(), |_, _| false)
                    .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)
    }

    /// Persist every buffer change and routed key since the last flush, in
    /// one transaction. On an error nothing is lost: the changes stay marked
    /// for the next flush.
    pub fn flush(&self, router: &CdcRouter) -> crate::Result<()> {
        let mut routed = self.lock();
        let removed = router.take_removed();
        let changed: Vec<(BufferKey, Arc<crate::event::cdc::buffer::StreamBuffer>)> = router
            .stream_buffers()
            .into_iter()
            .filter(|(_, buffer)| buffer.take_changed())
            .collect();
        if removed.is_empty() && changed.is_empty() && routed.pending.is_empty() {
            return Ok(());
        }
        let snapshots: Vec<(BufferKey, Vec<Arc<CdcEvent>>)> = changed
            .iter()
            .map(|(key, buffer)| (key.clone(), buffer.snapshot()))
            .collect();
        match self.write_changes(&removed, &snapshots, &routed.pending) {
            Ok(()) => {
                routed.pending.clear();
                Ok(())
            }
            Err(error) => {
                router.requeue_removed(removed);
                for (_, buffer) in &changed {
                    buffer.mark_changed();
                }
                Err(error)
            }
        }
    }

    fn write_changes(
        &self,
        removed: &[BufferKey],
        snapshots: &[(BufferKey, Vec<Arc<CdcEvent>>)],
        pending: &[SinkEventKey],
    ) -> crate::Result<()> {
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut events = txn.open_table(EVENTS).map_err(storage)?;
            for stream in removed {
                let (first, last) = stream_bounds(stream);
                events
                    .retain_in(first.as_slice()..=last.as_slice(), |_, _| false)
                    .map_err(storage)?;
            }
            for (stream, snapshot) in snapshots {
                let prefix = stream_prefix(stream);
                let retained: HashMap<Vec<u8>, &Arc<CdcEvent>> = snapshot
                    .iter()
                    .map(|event| (event_row(&prefix, event.position()), event))
                    .collect();
                let mut persisted: HashSet<Vec<u8>> = HashSet::new();
                let (first, last) = stream_bounds(stream);
                events
                    .retain_in(first.as_slice()..=last.as_slice(), |row, _| {
                        let keep = retained.contains_key(row);
                        if keep {
                            persisted.insert(row.to_vec());
                        }
                        keep
                    })
                    .map_err(storage)?;
                for (row, event) in &retained {
                    if persisted.contains(row) {
                        continue;
                    }
                    let bytes = sonic_rs::to_vec(event.as_ref()).map_err(serialization)?;
                    events
                        .insert(row.as_slice(), bytes.as_slice())
                        .map_err(storage)?;
                }
            }
            let mut keys = txn.open_table(ROUTED).map_err(storage)?;
            for key in pending {
                keys.insert(key.to_bytes().as_slice(), [].as_slice())
                    .map_err(storage)?;
            }
        }
        txn.commit().map_err(storage)
    }

    /// Drop the routed keys of `core` at or below `through`.
    pub fn prune(&self, core: u32, through: u64) -> crate::Result<()> {
        {
            let mut routed = self.lock();
            routed
                .keys
                .retain(|key| key.core != core || key.lsn > through);
            routed
                .pending
                .retain(|key| key.core != core || key.lsn > through);
        }
        let from = SinkEventKey::floor_bytes(core, 0);
        let to = SinkEventKey::floor_bytes(core, through.saturating_add(1));
        let txn = self.db.begin_write().map_err(storage)?;
        {
            let mut keys = txn.open_table(ROUTED).map_err(storage)?;
            keys.retain_in(from.as_slice()..to.as_slice(), |_, _| false)
                .map_err(storage)?;
        }
        txn.commit().map_err(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_names_its_stream_and_rows_of_a_stream_stay_in_its_bounds() {
        let stream: BufferKey = (DatabaseId::new(3), 7, "orders_stream".into());
        let prefix = stream_prefix(&stream);
        let row = event_row(&prefix, CdcOffset::new(42, 5));
        assert_eq!(row_stream(&row), Some(stream.clone()));
        let (first, last) = stream_bounds(&stream);
        assert!(first.as_slice() <= row.as_slice() && row.as_slice() <= last.as_slice());

        let other: BufferKey = (DatabaseId::new(3), 7, "orders_stream_2".into());
        let other_row = event_row(&stream_prefix(&other), CdcOffset::new(1, 0));
        assert!(
            !(first.as_slice() <= other_row.as_slice() && other_row.as_slice() <= last.as_slice())
        );
    }
}
