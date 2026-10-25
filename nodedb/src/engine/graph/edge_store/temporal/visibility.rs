// SPDX-License-Identifier: BUSL-1.1

//! Which edge versions a TRUNCATE hides.
//!
//! A TRUNCATE of an edge collection records a cut: the ordinal of its Calvin
//! transaction. A cut hides every version of the collection applied below
//! it. Every version is applied at its Calvin transaction's ordinal. Most
//! versions take that ordinal as their `system_from` too. Two do not, and
//! `EDGE_APPLIED` records their applied ordinal under the version's forward
//! key:
//!
//! - a restored version keeps its historical `system_from`;
//! - a version raised above a stored version of the same edge, so it sorts
//!   after every earlier write of the edge, takes a `system_from` above its
//!   ordinal.
//!
//! A read at system time `as_of` consults the newest cut at or below
//! `as_of`, and resolves each edge to its newest version that cut leaves
//! visible. Cuts and applied ordinals are both sequence ordinals, so the
//! answer is the same whatever order a core applied the transactions in.

use std::collections::HashMap;

use redb::{
    ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition, WriteTransaction,
};

use super::keys::{EdgeRef, SYSTEM_TIME_WIDTH, edge_version_prefix, versioned_edge_key};
use crate::engine::graph::edge_store::store::redb_err;

/// Key shape every edge-store table shares: `(database, tenant, composite)`.
pub(in crate::engine::graph::edge_store) type StoreKey = (u64, u64, &'static str);

/// Cut table: `(db, tid, "{collection}\x00{cut:020}")` → `()`. One row per
/// TRUNCATE of the collection this store applied.
pub(in crate::engine::graph::edge_store) const EDGE_CUTS: TableDefinition<(u64, u64, &str), ()> =
    TableDefinition::new("edge_cuts");

/// Applied table: `(db, tid, forward versioned key)` → the ordinal the version
/// was applied at. Only a version applied at another ordinal than its
/// `system_from` has a row.
pub(in crate::engine::graph::edge_store) const EDGE_APPLIED: TableDefinition<
    (u64, u64, &str),
    i64,
> = TableDefinition::new("edge_applied");

/// The cut-table key of the cut at `cut` in `collection`.
pub(in crate::engine::graph::edge_store) fn cut_key(
    collection: &str,
    cut: i64,
) -> crate::Result<String> {
    if cut < 0 {
        return Err(crate::Error::BadRequest {
            detail: format!("edge cut of '{collection}' at negative ordinal {cut}"),
        });
    }
    Ok(format!(
        "{collection}\x00{cut:0width$}",
        width = SYSTEM_TIME_WIDTH
    ))
}

/// The prefix every cut key of `collection` starts with.
pub(in crate::engine::graph::edge_store) fn cut_prefix(collection: &str) -> String {
    format!("{collection}\x00")
}

/// The collection and ordinal a cut key names.
pub(in crate::engine::graph::edge_store) fn parse_cut_key(key: &str) -> Option<(&str, i64)> {
    let (collection, cut) = key.split_once('\x00')?;
    if cut.len() != SYSTEM_TIME_WIDTH {
        return None;
    }
    Some((collection, cut.parse().ok()?))
}

/// The cut and applied tables one read or write consults, with the cuts of
/// each collection it touched.
pub(in crate::engine::graph::edge_store) struct Visibility<C, A> {
    pub(in crate::engine::graph::edge_store) cuts: C,
    pub(in crate::engine::graph::edge_store) applied: A,
    loaded: HashMap<(u64, u64, String), Vec<i64>>,
}

/// The tables of a read transaction.
pub(in crate::engine::graph::edge_store) type ReadVisibility =
    Visibility<ReadOnlyTable<StoreKey, ()>, ReadOnlyTable<StoreKey, i64>>;

/// The tables of a write transaction. A writer inserts through them, since
/// a write transaction opens each table once.
pub(in crate::engine::graph::edge_store) type WriteVisibility<'txn> =
    Visibility<Table<'txn, StoreKey, ()>, Table<'txn, StoreKey, i64>>;

/// Open the cut and applied tables of `txn`.
pub(in crate::engine::graph::edge_store) fn read_visibility(
    txn: &ReadTransaction,
) -> crate::Result<ReadVisibility> {
    Ok(Visibility::new(
        txn.open_table(EDGE_CUTS)
            .map_err(|e| redb_err("open edge_cuts", e))?,
        txn.open_table(EDGE_APPLIED)
            .map_err(|e| redb_err("open edge_applied", e))?,
    ))
}

/// Open the cut and applied tables of `txn`.
pub(in crate::engine::graph::edge_store) fn write_visibility(
    txn: &WriteTransaction,
) -> crate::Result<WriteVisibility<'_>> {
    Ok(Visibility::new(
        txn.open_table(EDGE_CUTS)
            .map_err(|e| redb_err("open edge_cuts", e))?,
        txn.open_table(EDGE_APPLIED)
            .map_err(|e| redb_err("open edge_applied", e))?,
    ))
}

impl<C, A> Visibility<C, A>
where
    C: ReadableTable<StoreKey, ()>,
    A: ReadableTable<StoreKey, i64>,
{
    pub(in crate::engine::graph::edge_store) fn new(cuts: C, applied: A) -> Self {
        Self {
            cuts,
            applied,
            loaded: HashMap::new(),
        }
    }

    /// Every cut of `collection`, ascending.
    fn cuts_of(&mut self, db: u64, tid: u64, collection: &str) -> crate::Result<&[i64]> {
        let slot = (db, tid, collection.to_string());
        if !self.loaded.contains_key(&slot) {
            let prefix = cut_prefix(collection);
            let mut cuts = Vec::new();
            let range = self
                .cuts
                .range((db, tid, prefix.as_str())..)
                .map_err(|e| redb_err("edge cut range", e))?;
            for entry in range {
                let (key, _) = entry.map_err(|e| redb_err("edge cut iter", e))?;
                let (kd, kt, composite) = key.value();
                if kd != db || kt != tid || !composite.starts_with(&prefix) {
                    break;
                }
                if let Some((_, cut)) = parse_cut_key(composite) {
                    cuts.push(cut);
                }
            }
            self.loaded.insert(slot.clone(), cuts);
        }
        Ok(self
            .loaded
            .get(&slot)
            .map(Vec::as_slice)
            .unwrap_or_default())
    }

    /// The newest cut of `collection` at or below `as_of`.
    pub(in crate::engine::graph::edge_store) fn cut_at(
        &mut self,
        db: u64,
        tid: u64,
        collection: &str,
        as_of: i64,
    ) -> crate::Result<Option<i64>> {
        let cuts = self.cuts_of(db, tid, collection)?;
        let below = cuts.partition_point(|&cut| cut <= as_of);
        Ok(below.checked_sub(1).map(|i| cuts[i]))
    }

    /// The ordinal the version under `fwd_key` was applied at.
    pub(in crate::engine::graph::edge_store) fn applied_at(
        &self,
        db: u64,
        tid: u64,
        fwd_key: &str,
        system_from: i64,
    ) -> crate::Result<i64> {
        Ok(self
            .applied
            .get((db, tid, fwd_key))
            .map_err(|e| redb_err("read edge applied ordinal", e))?
            .map_or(system_from, |applied| applied.value()))
    }

    /// Whether the version of `collection` under `fwd_key`, at `system_from`,
    /// is hidden from a read at `as_of`: the newest cut at or below `as_of`
    /// is above the ordinal the version was applied at.
    pub(in crate::engine::graph::edge_store) fn hidden(
        &mut self,
        db: u64,
        tid: u64,
        collection: &str,
        fwd_key: &str,
        system_from: i64,
        as_of: i64,
    ) -> crate::Result<bool> {
        let Some(cut) = self.cut_at(db, tid, collection, as_of)? else {
            return Ok(false);
        };
        // A version raised above a stored one sits above its applied
        // ordinal, so the applied ordinal is read whatever the system time.
        Ok(self.applied_at(db, tid, fwd_key, system_from)? < cut)
    }

    /// Whether a read at `as_of` sees the version of `collection` under
    /// `fwd_key`, at `system_from`: the version is at or below `as_of`, and
    /// no TRUNCATE cut hides it.
    pub(in crate::engine::graph::edge_store) fn visible_at(
        &mut self,
        db: u64,
        tid: u64,
        collection: &str,
        fwd_key: &str,
        system_from: i64,
        as_of: i64,
    ) -> crate::Result<bool> {
        Ok(system_from <= as_of
            && !self.hidden(db, tid, collection, fwd_key, system_from, as_of)?)
    }
}

/// The version of `edge` a read at `as_of` resolves to: the newest at or
/// below `as_of` that the cuts leave visible, as `(system_from, bytes)`.
/// The bytes are a payload or a sentinel. `None` when no version qualifies.
pub(in crate::engine::graph::edge_store) fn visible_version<E, C, A>(
    edges: &E,
    visibility: &mut Visibility<C, A>,
    edge: &EdgeRef<'_>,
    as_of: i64,
) -> crate::Result<Option<(i64, Vec<u8>)>>
where
    E: ReadableTable<StoreKey, &'static [u8]>,
    C: ReadableTable<StoreKey, ()>,
    A: ReadableTable<StoreKey, i64>,
{
    let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
    let upper = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, as_of)?;
    let d = edge.db.as_u64();
    let t = edge.tid.as_u64();
    let range = edges
        .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
        .map_err(|e| redb_err("visible version range", e))?;
    for entry in range.rev() {
        let (key, value) = entry.map_err(|e| redb_err("visible version iter", e))?;
        let composite = key.value().2;
        let Some(system_from) = composite
            .strip_prefix(prefix.as_str())
            .and_then(|suffix| suffix.parse::<i64>().ok())
        else {
            continue;
        };
        if visibility.hidden(d, t, edge.collection, composite, system_from, as_of)? {
            continue;
        }
        return Ok(Some((system_from, value.value().to_vec())));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_key_round_trips_and_sorts_by_ordinal() {
        let early = cut_key("c", 90).unwrap();
        let late = cut_key("c", 1_000).unwrap();
        assert!(early < late);
        assert_eq!(parse_cut_key(&early), Some(("c", 90)));
        assert!(early.starts_with(&cut_prefix("c")));
        assert!(!early.starts_with(&cut_prefix("cc")));
        assert!(cut_key("c", -1).is_err());
    }
}
