// SPDX-License-Identifier: BUSL-1.1

//! The rows a set of indexed equalities all hold.
//!
//! A row stays only when every index holds it at the read's cut. Each
//! equality reads the versioned index at that cut, and the ids the first
//! equality names are narrowed by every further one before any row body is
//! read.

use std::collections::HashSet;

use nodedb_physical::physical_plan::IndexProbe;
use nodedb_types::StorageKey;

use crate::data::executor::core_loop::CoreLoop;

/// The equalities of one indexed fetch.
pub(super) struct IndexProbes<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    /// The first equality.
    pub path: &'a str,
    pub value: &'a str,
    /// Further equalities.
    pub also: &'a [IndexProbe],
    /// The system-time ordinal the read sees. `None` reads the current state.
    pub as_of: Option<i64>,
}

impl CoreLoop {
    /// The committed rows every equality of `probes` holds at its cut, in
    /// the first equality's order.
    pub(super) fn probe_index_ids(
        &self,
        probes: &IndexProbes<'_>,
    ) -> crate::Result<Vec<StorageKey>> {
        let mut ids = self.sparse.versioned_index_lookup_as_of(
            probes.database_id,
            probes.tid,
            probes.collection,
            probes.path,
            probes.value,
            probes.as_of,
        )?;
        for probe in probes.also {
            if ids.is_empty() {
                break;
            }
            let shared: HashSet<StorageKey> = self
                .sparse
                .versioned_index_lookup_as_of(
                    probes.database_id,
                    probes.tid,
                    probes.collection,
                    &probe.path,
                    &probe.value,
                    probes.as_of,
                )?
                .into_iter()
                .collect();
            ids.retain(|id| shared.contains(id));
        }
        Ok(ids)
    }
}
