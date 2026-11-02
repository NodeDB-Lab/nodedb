// SPDX-License-Identifier: BUSL-1.1

//! The durable series catalog of a timeseries collection.
//!
//! ## Why it exists
//!
//! Ingest gives each series an id from the collection's series catalog: the
//! hash of its metric and tags, rehashed past any slot another key holds.
//! Every row stores that id (`_ts_series`), and the last-value cache keys on
//! it. The id a key gets depends on the keys registered before it, so a
//! restart that rebuilds the catalog from replayed rows alone can give a
//! key another id than its flushed rows store.
//!
//! ## Where it lives
//!
//! The collection directory carries `series.catalog`. Every flush writes it
//! before the partition's commit point, so the catalog on disk names the
//! series of every row a partition holds. Restart replay resolves the rows
//! still in the WAL against it in log order, so every key gets the id it
//! got before the restart. A truncate writes the catalog it leaves, and its
//! undo writes the catalog it restores.

use std::path::Path;

use nodedb_types::timeseries::{SeriesCatalog, SeriesKey};

/// File name of the durable catalog in a collection directory. It does not
/// start with `ts-`, so the partition scan and the orphan sweep pass over it.
pub(in crate::data::executor) const TS_SERIES_CATALOG_FILE: &str = "series.catalog";

/// Leading byte of an encoded catalog file.
const TS_SERIES_CATALOG_FORMAT_VERSION: u8 = 1;

/// One registration of the catalog.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct DurableSeries {
    id: u64,
    metric: String,
    tags: Vec<(String, String)>,
    rehash_attempt: u64,
}

fn catalog_error(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".to_string(),
        detail: format!(
            "series catalog: failed to {action} {}: {detail}",
            path.display()
        ),
    }
}

/// The bytes of a catalog file holding `catalog`, its records sorted by id
/// so equal catalogs encode to equal bytes. A tenant snapshot carries the
/// same bytes.
pub(in crate::data::executor) fn encode_ts_series_catalog(
    catalog: &SeriesCatalog,
) -> crate::Result<Vec<u8>> {
    let mut records: Vec<DurableSeries> = catalog
        .entries()
        .map(|(id, key, rehash_attempt)| DurableSeries {
            id,
            metric: key.metric.clone(),
            tags: key.tags.clone(),
            rehash_attempt,
        })
        .collect();
    records.sort_unstable_by_key(|record| record.id);
    let mut bytes = vec![TS_SERIES_CATALOG_FORMAT_VERSION];
    let body = zerompk::to_msgpack_vec(&records)
        .map_err(|e| catalog_error(Path::new(TS_SERIES_CATALOG_FILE), "encode", &e))?;
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Write `catalog` into the collection directory `dir` durably: data
/// fsynced before the rename, `dir` fsynced after it.
pub(in crate::data::executor) fn write_ts_series_catalog(
    dir: &Path,
    catalog: &SeriesCatalog,
) -> crate::Result<()> {
    let bytes = encode_ts_series_catalog(catalog)?;
    nodedb_wal::segment::atomic_write_fsync(dir, TS_SERIES_CATALOG_FILE, &bytes)
        .map_err(|e| catalog_error(&dir.join(TS_SERIES_CATALOG_FILE), "write", &e))
}

/// Read the catalog in the collection directory `dir`. A missing file names
/// no series. A file that is present but does not decode, or that gives one
/// id two keys, is an error: the ids it holds are the ids stored rows carry.
pub(in crate::data::executor) fn read_ts_series_catalog(
    dir: &Path,
) -> crate::Result<Option<SeriesCatalog>> {
    let path = dir.join(TS_SERIES_CATALOG_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(catalog_error(&path, "read", &e)),
    };
    decode_ts_series_catalog(&bytes, &path).map(Some)
}

/// Decode the bytes [`encode_ts_series_catalog`] wrote. `path` names the
/// source in an error. Bytes that do not decode, or that give one id two
/// keys, are an error.
pub(in crate::data::executor) fn decode_ts_series_catalog(
    bytes: &[u8],
    path: &Path,
) -> crate::Result<SeriesCatalog> {
    let Some((&version, body)) = bytes.split_first() else {
        return Err(catalog_error(path, "decode", &"the file is empty"));
    };
    if version != TS_SERIES_CATALOG_FORMAT_VERSION {
        return Err(catalog_error(
            path,
            "decode",
            &format!("format version {version}, expected {TS_SERIES_CATALOG_FORMAT_VERSION}"),
        ));
    }
    let records: Vec<DurableSeries> =
        zerompk::from_msgpack(body).map_err(|e| catalog_error(path, "decode", &e))?;
    let mut catalog = SeriesCatalog::new();
    for record in records {
        let key = SeriesKey::new(record.metric, record.tags);
        if !catalog.install(record.id, key, record.rehash_attempt) {
            return Err(catalog_error(
                path,
                "validate",
                &format!("series id {} names two keys", record.id),
            ));
        }
    }
    Ok(catalog)
}

impl crate::data::executor::core_loop::CoreLoop {
    /// Write the series catalog of `key` into its collection directory. A
    /// collection with no catalog writes an empty one.
    pub(in crate::data::executor) fn persist_ts_series_catalog(
        &self,
        key: &super::stamp::TsCollectionKey,
    ) -> crate::Result<()> {
        let dir = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            key.0.as_u64(),
            key.1.as_u64(),
            &key.2,
        );
        if !dir.exists() {
            return Ok(());
        }
        let empty = SeriesCatalog::new();
        let catalog = self.ts_series_catalogs.get(key).unwrap_or(&empty);
        write_ts_series_catalog(&dir, catalog)
    }

    /// Install the catalog on disk for `key` under the one held in memory.
    /// A registration the memory catalog already gives its id to another key
    /// is an error: rows on disk carry the ids the file holds.
    pub(in crate::data::executor) fn load_ts_series_catalog(
        &mut self,
        key: &super::stamp::TsCollectionKey,
        dir: &Path,
    ) -> crate::Result<()> {
        let Some(stored) = read_ts_series_catalog(dir)? else {
            return Ok(());
        };
        let held = self.ts_series_catalogs.entry(key.clone()).or_default();
        for (id, series_key, attempt) in stored.entries() {
            if !held.install(id, series_key.clone(), attempt) {
                return Err(catalog_error(
                    &dir.join(TS_SERIES_CATALOG_FILE),
                    "install",
                    &format!("series id {id} names another key in memory"),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog written and read back keeps every id, key and attempt, so
    /// a key resolves to the id its stored rows carry.
    #[test]
    fn a_catalog_round_trips_its_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut catalog = SeriesCatalog::new();
        let a = SeriesKey::new("cpu", vec![("host".into(), "a".into())]);
        let b = SeriesKey::new("cpu", vec![("host".into(), "b".into())]);
        let id_a = catalog.resolve(&a);
        let id_b = catalog.resolve(&b);
        write_ts_series_catalog(dir.path(), &catalog).expect("write");
        let mut read = read_ts_series_catalog(dir.path())
            .expect("read")
            .expect("present");
        assert_eq!(read, catalog);
        assert_eq!(read.resolve(&b), id_b);
        assert_eq!(read.resolve(&a), id_a);
    }

    /// A follower that installs a snapshot gives every key the id the
    /// leader gave it, a rehashed one included, so the rows it installs and
    /// the rows it ingests later agree with the leader's.
    #[test]
    fn a_snapshot_install_keeps_the_leader_series_ids() {
        use crate::bridge::envelope::Status;
        use crate::data::executor::core_loop::tests::make_core_with_dir;
        use crate::data::executor::handlers::timeseries::cagg_test_support::{
            COLLECTION, TENANT, flush, ingest, task_at,
        };
        use crate::types::{DatabaseId, TenantId};

        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        let host_a = SeriesKey::new(COLLECTION, vec![("host".into(), "a".into())]);
        let leader_dir = tempfile::tempdir().expect("tempdir");
        let (mut leader, _tx, _rx) = make_core_with_dir(leader_dir.path());
        // Another key holds `host=a`'s natural slot, so the leader rehashes it.
        leader
            .ts_series_catalogs
            .entry(key.clone())
            .or_default()
            .install(host_a.to_series_id(0), SeriesKey::new("blocker", vec![]), 0);
        let rows = format!("{COLLECTION},host=a value=1.5 1700000000000000000\n");
        assert_eq!(ingest(&mut leader, &rows, 1), Status::Ok);
        flush(&mut leader);
        let leader_catalog = leader
            .ts_series_catalogs
            .get(&key)
            .expect("catalog")
            .clone();
        let rehashed = host_a.to_series_id(1);
        assert_eq!(
            leader_catalog
                .entries()
                .find(|(_, k, _)| **k == host_a)
                .map(|e| e.0),
            Some(rehashed)
        );

        let snapshot = leader.execute_create_tenant_snapshot(&task_at(2), TENANT, false);
        assert_eq!(snapshot.status, Status::Ok, "{:?}", snapshot.error_code);

        let follower_dir = tempfile::tempdir().expect("tempdir");
        let (mut follower, _tx2, _rx2) = make_core_with_dir(follower_dir.path());
        let installed = follower.execute_restore_tenant_snapshot(
            &task_at(3),
            TENANT,
            snapshot.payload.as_ref(),
            true,
            &[],
            &[],
        );
        assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
        assert_eq!(follower.ts_series_catalogs.get(&key), Some(&leader_catalog));

        // A row the follower ingests stores the leader's id for its key.
        let later = format!("{COLLECTION},host=a value=2.5 1700000001000000000\n");
        assert_eq!(ingest(&mut follower, &later, 4), Status::Ok);
        let memtable = follower.columnar_memtables.get(&key).expect("memtable");
        let idx = memtable.schema().ts_series_idx().expect("series column");
        let stored = match memtable.column(idx) {
            crate::engine::timeseries::columnar_memtable::ColumnData::Int64(ids) => ids.clone(),
            other => panic!("series column holds {other:?}"),
        };
        assert_eq!(
            stored.last().copied(),
            Some(i64::from_ne_bytes(rehashed.to_ne_bytes()))
        );
    }

    #[test]
    fn a_missing_file_names_no_series_and_a_bad_one_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(read_ts_series_catalog(dir.path()).expect("read").is_none());
        std::fs::write(dir.path().join(TS_SERIES_CATALOG_FILE), [9u8, 1]).expect("write");
        assert!(read_ts_series_catalog(dir.path()).is_err());
    }
}
