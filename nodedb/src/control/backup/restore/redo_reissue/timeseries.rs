// SPDX-License-Identifier: BUSL-1.1

//! Restored timeseries rows as redo units.
//!
//! A timeseries row has no key: an ingest appends and never matches a row
//! against stored ones, and every writer locks the collection whole. A
//! RESTORE re-issues each collection's current rows, the memtable and every
//! partition no TRUNCATE ended, in chunks, after the collection's series
//! catalog and every partition a TRUNCATE ended (`timeseries_partition`).
//! Each row keeps its backed-up
//! `_ts_system` ordinal. Each chunk is one unit: a
//! `timeseries_restore` sub-record its resolve turns into an ingest, or
//! drops when a TRUNCATE after the RESTORE's cut ends the rows. A RESTORE
//! never truncates the destination: its rows append beside the
//! destination's.

use std::collections::HashMap;

use nodedb_types::value::Value;
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, propose_replicated_entry};
use crate::types::{TenantId, TsFlushedCollectionBlob};
use crate::wal::{RedoSubRecord, TimeseriesRestoreRedo, TimeseriesSeriesCatalogRedo};

use super::super::target::{DatabaseTarget, RestoredName};
use super::super::timeseries_partition::ended_partition_redo;
use super::super::timeseries_reissue::decode_timeseries_restore_rows;
use super::commit::commit_collection;
use super::units::{CollectionUnits, RowUnit};

/// Most rows one restored chunk carries.
const ROWS_PER_CHUNK: usize = 1024;

/// The unit one sub-record of the collection `name` re-issues as: it locks
/// the collection whole.
fn collection_unit(name: &RestoredName, payload: Vec<u8>) -> RowUnit {
    let stored = name.stored.as_str().to_string();
    RowUnit {
        ops: vec![RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload,
        }],
        identities: Vec::new(),
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: Vec::new(),
        columnar_keys: Vec::new(),
        vector_rows: Vec::new(),
        timeseries: vec![stored],
    }
}

/// The unit one chunk of restored rows re-issues as.
fn chunk_unit(name: &RestoredName, rows: Vec<Value>) -> crate::Result<RowUnit> {
    let encoded =
        nodedb_types::value_to_msgpack(&Value::Array(rows)).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "restore reissue: encode timeseries rows for '{}': {e}",
                name.bare
            ),
        })?;
    let payload = TimeseriesRestoreRedo {
        collection: name.stored.as_str().to_string(),
        rows: encoded,
    }
    .encode()?;
    Ok(collection_unit(name, payload))
}

/// The unit that installs the backed-up series catalog `catalog` of `name`.
fn catalog_unit(name: &RestoredName, catalog: Vec<u8>) -> crate::Result<RowUnit> {
    let payload = TimeseriesSeriesCatalogRedo {
        collection: name.stored.as_str().to_string(),
        catalog,
    }
    .encode()?;
    Ok(collection_unit(name, payload))
}

/// The units that install every partition of `blob` a TRUNCATE ended, each
/// re-encoded as plaintext in this server's segment format.
fn ended_units(
    name: &RestoredName,
    blob: &TsFlushedCollectionBlob,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<Vec<RowUnit>> {
    let mut units = Vec::new();
    for part in &blob.partitions {
        let meta: nodedb_types::timeseries::PartitionMeta = zerompk::from_msgpack(&part.meta_bytes)
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: decode the meta of partition '{}' of '{}': {e}",
                    part.dir_name, name.bare
                ),
            })?;
        if meta.ended_at.is_none() {
            continue;
        }
        let redo = ended_partition_redo(&name.bare, name.stored.as_str(), part, meta, kek)?;
        units.push(collection_unit(name, redo.encode()?));
    }
    Ok(units)
}

/// The backed-up timeseries sections of one database, each keyed
/// `"{db}:{tid}:{collection}"`.
pub(in crate::control::backup::restore) struct BackedUpTimeseries {
    /// Memtable snapshots.
    pub memtables: Vec<(String, Vec<u8>)>,
    /// Flushed partitions, current and ended.
    pub flushed: Vec<TsFlushedCollectionBlob>,
    /// Series catalogs, in the `series.catalog` file encoding.
    pub catalogs: Vec<(String, Vec<u8>)>,
}

/// The units the current rows `rows` of one collection re-issue as.
fn chunk_units(name: &RestoredName, rows: Vec<Value>) -> crate::Result<Vec<RowUnit>> {
    let mut units = Vec::with_capacity(rows.len().div_ceil(ROWS_PER_CHUNK));
    let mut rows = rows.into_iter().peekable();
    while rows.peek().is_some() {
        let chunk: Vec<Value> = rows.by_ref().take(ROWS_PER_CHUNK).collect();
        units.push(chunk_unit(name, chunk)?);
    }
    Ok(units)
}

/// Decode and re-issue every restored timeseries collection of one
/// database, one Calvin transaction per batch of units. Each collection
/// installs its series catalog first, then every partition a TRUNCATE
/// ended, then its current rows. Returns the number of collections that
/// re-issued anything.
pub(in crate::control::backup::restore) async fn reissue_timeseries_rows(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    backed_up: BackedUpTimeseries,
) -> Result<usize, Error> {
    let BackedUpTimeseries {
        memtables,
        flushed,
        catalogs,
    } = backed_up;
    // An encrypted backed-up segment reads with the WAL encryption key.
    let kek = state.wal.encryption_key().cloned();
    let mut catalog_by_key: HashMap<String, Vec<u8>> = catalogs.into_iter().collect();
    let mut memtable_by_key: HashMap<String, Vec<u8>> = memtables.into_iter().collect();
    let mut keys_in_order: Vec<String> = Vec::new();
    let mut flushed_by_key: HashMap<String, TsFlushedCollectionBlob> = HashMap::new();
    for blob in flushed {
        keys_in_order.push(blob.collection_key.clone());
        flushed_by_key.insert(blob.collection_key.clone(), blob);
    }
    let mut unflushed: Vec<String> = memtable_by_key
        .keys()
        .chain(catalog_by_key.keys())
        .filter(|key| !flushed_by_key.contains_key(*key))
        .cloned()
        .collect();
    unflushed.sort();
    unflushed.dedup();
    keys_in_order.extend(unflushed);

    let empty = TsFlushedCollectionBlob::default();
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for key in keys_in_order {
        let name = target.resolve_scoped(&key, tenant_id)?;
        let memtable = memtable_by_key.remove(&key);
        let blob = flushed_by_key.get(&key).unwrap_or(&empty);
        let rows =
            decode_timeseries_restore_rows(&name.bare, memtable.as_deref(), blob, kek.as_ref())?;
        let mut units = Vec::new();
        if let Some(catalog) = catalog_by_key.remove(&key) {
            units.push(catalog_unit(&name, catalog)?);
        }
        units.extend(ended_units(&name, blob, kek.as_ref())?);
        units.extend(chunk_units(&name, rows)?);
        if !units.is_empty() {
            let collection = CollectionUnits::rows(target.dest, name.bare.clone(), units);
            commit_collection(state, tenant, target.mark(), collection).await?;
            reissued += 1;
        }
        // A RESTORE appends beside the destination's rows, so it installs no
        // aggregate state. A re-issue with no RESTORE fills an empty database.
        if target.restore_id == 0 && !blob.cagg_copy_image.is_empty() {
            install_aggregate_image(state, tenant_id, target, &name, &blob.cagg_copy_image).await?;
        }
    }
    Ok(reissued)
}

/// Put `image`, the aggregate state of the root `name`, into the root's data
/// group log. The rows it counts are ahead of it: each commit above returned
/// with its redo in that log, so every replica applies them first.
async fn install_aggregate_image(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    name: &RestoredName,
    image: &[u8],
) -> Result<(), Error> {
    let proposer = state.async_raft_proposer()?;
    let entry = ReplicatedEntry::new(
        tenant_id,
        target.dest.as_u64(),
        name.key(target.dest).vshard().as_u32(),
        ReplicatedWrite::TimeseriesAggregateImage {
            collection: name.stored.as_str().to_string(),
            image: image.to_vec(),
        },
    )
    .with_event_source(target.event_source);
    let deadline = StatementDeadline::of_statement(state).tokio_instant();
    propose_replicated_entry(state, proposer, entry, deadline).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rows split into chunks of at most `ROWS_PER_CHUNK`, each locking
    /// the collection whole.
    #[test]
    fn rows_chunk_and_lock_the_collection() {
        let target = DatabaseTarget {
            source: crate::types::DatabaseId::DEFAULT,
            dest: crate::types::DatabaseId::DEFAULT,
            restore_id: 1,
            restore_cut: i64::MAX,
            event_source: crate::event::EventSource::Restore,
        };
        let name = target.bare("cpu");
        let rows = vec![Value::Integer(1); ROWS_PER_CHUNK + 1];
        let units = chunk_units(&name, rows).expect("chunks");
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].timeseries, vec!["cpu".to_string()]);
        let redo = TimeseriesRestoreRedo::decode(&units[1].ops[0].payload).expect("restore");
        assert_eq!(redo.collection, "cpu");
    }
}
