// SPDX-License-Identifier: BUSL-1.1

//! The write sets and boot verdicts every core's sparse store holds at boot.
//!
//! Boot reads them before any core opens its store, and drops each store's
//! handle before the cores start. A verdict decides its record whichever
//! core stored it, so a change of the core count keeps every verdict.

use std::path::{Path, PathBuf};

use std::collections::BTreeMap;

use crate::engine::sparse::btree::SparseEngine;
use crate::wal::redo::decode_stored_verdicts;
use crate::wal::{BootVerdictOutcome, NeverDurableRedos, WriteSetCapture};

/// The stored write sets of every core, and the stores that hold them.
pub struct StoredWriteSets {
    stores: Vec<SparseEngine>,
    /// Every stored write set, in origin order.
    pub captures: Vec<WriteSetCapture>,
    /// Every stored boot verdict of every core, by record LSN.
    pub verdicts: BTreeMap<u64, BootVerdictOutcome>,
}

impl StoredWriteSets {
    /// Read every core's sparse store under `data_dir`.
    pub fn read(data_dir: &Path) -> crate::Result<Self> {
        let mut stores = Vec::new();
        let mut captures = Vec::new();
        let mut verdicts = BTreeMap::new();
        for path in sparse_stores(data_dir)? {
            let store = SparseEngine::open(&path)?;
            for (_, bytes) in store.load_write_set_captures()? {
                captures.push(WriteSetCapture::from_bytes(&bytes)?);
            }
            verdicts.extend(decode_stored_verdicts(store.load_boot_verdicts()?)?);
            stores.push(store);
        }
        captures.sort_by_key(|capture| capture.origin);
        Ok(Self {
            stores,
            captures,
            verdicts,
        })
    }

    /// Hand each record `never_durable` judges the verdict any store holds
    /// for it, and drop, from every store, each verdict on a record no core
    /// judges: the WAL settled it. A store of a core this boot no longer
    /// runs is dropped from too.
    pub fn settle_verdicts(&self, never_durable: &mut NeverDurableRedos) -> crate::Result<()> {
        never_durable.attach_stored(&self.verdicts);
        let settled: Vec<u64> = self
            .verdicts
            .keys()
            .copied()
            .filter(|lsn| never_durable.judged(*lsn).is_none())
            .collect();
        for store in &self.stores {
            store.remove_boot_verdicts(&settled)?;
        }
        Ok(())
    }

    /// Drop every stored write set, durably.
    pub fn clear(&self) -> crate::Result<()> {
        for store in &self.stores {
            store.clear_write_set_captures()?;
        }
        Ok(())
    }
}

/// Every core's sparse store file under `data_dir`.
fn sparse_stores(data_dir: &Path) -> crate::Result<Vec<PathBuf>> {
    let first = crate::data::executor::snapshot::layout::sparse_store_path(data_dir, 0);
    let Some(dir) = first.parent() else {
        return Ok(Vec::new());
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let is_core_store = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("core-") && name.ends_with(".redb"));
        if is_core_store {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}
