// SPDX-License-Identifier: BUSL-1.1

//! Install of a restored series catalog (`crate::wal::TimeseriesSeriesCatalogRedo`),
//! for restart replay and the committed-redo apply alike.
//!
//! The catalog merges into the destination's: every registration installs,
//! the series keys that collided in the backup included, so each restored
//! key resolves to the id its backed-up rows carry. A registration whose id
//! names another key here, or whose key is registered here under another id,
//! refuses the record: a restored row lands under an id its ended
//! partitions do not carry. The check reads only the catalog every replica
//! holds at this log position, so the refusal is final.
//!
//! The merge changes memory only. The next flush writes the catalog with the
//! partition it lands, and a restart replays this record ahead of the rows
//! it serves, installing each registration that agrees with the catalog the
//! boot load read. The undo restores the catalog the merge found.

use std::collections::HashMap;
use std::path::Path;

use nodedb_types::timeseries::{SeriesCatalog, SeriesId, SeriesKey};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::series_catalog::{
    TS_SERIES_CATALOG_FILE, decode_ts_series_catalog,
};
use crate::data::executor::timeseries_checkpoint::stamp::TsCollectionKey;
use crate::types::{DatabaseId, TenantId};
use crate::wal::TimeseriesSeriesCatalogRedo;

/// The first registration of `restored` that `held` gives another meaning:
/// its id names another key, or its key holds another id.
pub(in crate::data::executor) fn catalog_conflict(
    held: &SeriesCatalog,
    restored: &SeriesCatalog,
) -> Option<String> {
    let ids: HashMap<&SeriesKey, SeriesId> = held.entries().map(|(id, key, _)| (key, id)).collect();
    for (id, key, _) in restored.entries() {
        if let Some(other) = held.get(id)
            && other != key
        {
            return Some(format!("series id {id} names another series here"));
        }
        if let Some(other) = ids.get(key)
            && *other != id
        {
            return Some(format!(
                "a restored series holds id {id}, and id {other} here"
            ));
        }
    }
    None
}

impl CoreLoop {
    /// Apply the restored series catalog `redo` of the record at
    /// `record_lsn`. Returns whether it applied.
    pub(in crate::data::executor) fn replay_ts_series_catalog(
        &mut self,
        database_id: DatabaseId,
        tenant_id: u64,
        record_lsn: u64,
        redo: TimeseriesSeriesCatalogRedo,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        if tombstones.is_tombstoned(
            database_id.as_u64(),
            tenant_id,
            &redo.collection,
            record_lsn,
        ) {
            return false;
        }
        let restored =
            match decode_ts_series_catalog(&redo.catalog, Path::new(TS_SERIES_CATALOG_FILE)) {
                Ok(catalog) => catalog,
                Err(e) => {
                    self.replay_record_unapplied(
                        "timeseries",
                        "series_catalog",
                        record_lsn,
                        &e.to_string(),
                    );
                    return false;
                }
            };
        let key: TsCollectionKey = (database_id, TenantId::new(tenant_id), redo.collection);
        let empty = SeriesCatalog::new();
        let held = self.ts_series_catalogs.get(&key).unwrap_or(&empty);
        // A restart replays the record over the catalog the boot load read
        // back, which a later write can have changed. The catalog on disk
        // names the ids every flushed row carries, so the restart keeps it
        // and installs only the registrations that agree with it.
        if !self.applying_committed_redo() {
            let held = self.ts_series_catalogs.entry(key).or_default();
            for (id, series_key, attempt) in restored.entries() {
                held.install(id, series_key.clone(), attempt);
            }
            return true;
        }
        if let Some(conflict) = catalog_conflict(held, &restored) {
            self.replay_record_unapplied(
                "timeseries",
                "series_catalog",
                record_lsn,
                &format!("restored series catalog of '{}': {conflict}", key.2),
            );
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        if self.recording_redo_undo() {
            self.record_redo_ts_pre_image(key.0, key.1, &key.2);
        }
        let held = self.ts_series_catalogs.entry(key).or_default();
        for (id, series_key, attempt) in restored.entries() {
            // The conflict check above admits every registration.
            held.install(id, series_key.clone(), attempt);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(host: &str) -> SeriesKey {
        SeriesKey::new("cpu", vec![("host".into(), host.into())])
    }

    /// A catalog that agrees merges. One that gives an id or a key another
    /// meaning conflicts.
    #[test]
    fn a_conflicting_registration_is_named() {
        let mut held = SeriesCatalog::new();
        let a = held.resolve(&key("a"));
        let mut restored = SeriesCatalog::new();
        assert!(restored.install(a, key("a"), 0));
        let b = restored.resolve(&key("b"));
        assert_eq!(catalog_conflict(&held, &restored), None);

        let mut clash = SeriesCatalog::new();
        assert!(clash.install(a, key("z"), 0));
        assert!(catalog_conflict(&held, &clash).is_some());

        let mut moved = SeriesCatalog::new();
        assert!(moved.install(b, key("a"), 3));
        assert!(catalog_conflict(&held, &moved).is_some());
    }
}
