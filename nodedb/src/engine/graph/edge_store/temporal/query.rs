// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal neighbor queries — prefix scan with Ceiling per base.
//!
//! `neighbors_out_as_of` / `neighbors_in_as_of` mirror the current-state
//! `EdgeStore::neighbors_out` / `neighbors_in` but honour:
//!
//! - `cutoff_ordinal: i64` — caps visibility to versions with
//!   `system_from` at or below this system-time ordinal. `i64::MAX` reads
//!   the current state.
//! - `valid_at_ms: Option<i64>` — each candidate's
//!   `[valid_from_ms, valid_until_ms)` must contain `valid_at_ms`.
//!   The Ceiling resolver walks to earlier versions if the latest one
//!   doesn't satisfy the predicate.

use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;

use nodedb_types::{DatabaseId, TenantId};

use super::super::store::{EDGES, Edge, EdgeStore, REVERSE_EDGES, redb_err};
use super::{EdgeRef, parse_versioned_edge_key};

/// Parameters shared by [`EdgeStore::neighbors_out_as_of`] and
/// [`EdgeStore::neighbors_in_as_of`]: the anchor node identity plus the
/// bitemporal visibility filters. `node` is the outbound source for the
/// `_out_` query and the inbound destination for the `_in_` query.
#[derive(Debug, Clone, Copy)]
pub struct NeighborsAsOfParams<'a> {
    pub db: u64,
    pub tid: TenantId,
    pub collection: &'a str,
    pub node: &'a str,
    pub label_filter: Option<&'a str>,
    /// The system-time ordinal visibility caps at. `i64::MAX` reads the
    /// current state.
    pub cutoff_ordinal: i64,
    pub valid_at_ms: Option<i64>,
}

impl EdgeStore {
    /// Bitemporal outbound neighbors. See module docs for semantics.
    pub fn neighbors_out_as_of(&self, params: NeighborsAsOfParams<'_>) -> crate::Result<Vec<Edge>> {
        let NeighborsAsOfParams {
            db,
            tid,
            collection,
            node: src,
            label_filter,
            cutoff_ordinal: cutoff,
            valid_at_ms,
        } = params;
        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{src}\x00{label}\x00"),
            None => format!("{collection}\x00{src}\x00"),
        };
        let t = tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Enumerate distinct `(label, dst)` bases with a qualifying version
        // at or below the cutoff. Latest-wins is enforced by the Ceiling
        // resolver below, so we only need the set of candidate bases here.
        let mut bases: HashMap<(String, String), ()> = HashMap::new();
        let range = table
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, _val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((_coll, _src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if sys > cutoff {
                continue;
            }
            bases.insert((label.to_string(), dst.to_string()), ());
        }
        drop(table);
        drop(read_txn);

        let mut edges = Vec::with_capacity(bases.len());
        for ((label, dst), _) in bases {
            let Some(props) = self.ceiling_resolve_edge(
                EdgeRef::new(DatabaseId::new(db), tid, collection, src, &label, &dst),
                cutoff,
                valid_at_ms,
            )?
            else {
                continue;
            };
            edges.push(Edge {
                collection: collection.to_string(),
                src_id: src.to_string(),
                label,
                dst_id: dst,
                properties: props,
            });
        }
        Ok(edges)
    }

    /// Bitemporal inbound neighbors. See module docs for semantics.
    pub fn neighbors_in_as_of(&self, params: NeighborsAsOfParams<'_>) -> crate::Result<Vec<Edge>> {
        let NeighborsAsOfParams {
            db,
            tid,
            collection,
            node: dst,
            label_filter,
            cutoff_ordinal: cutoff,
            valid_at_ms,
        } = params;
        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{dst}\x00{label}\x00"),
            None => format!("{collection}\x00{dst}\x00"),
        };
        let t = tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        // Distinct `(label, logical_src)` bases with a version at or below the
        // cutoff. The forward Ceiling below decides each one: it skips the
        // versions a TRUNCATE hides.
        let mut bases: HashMap<(String, String), ()> = HashMap::new();
        let range = table
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, _val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((_coll, _rev_dst, rev_label, rev_src, sys)) =
                parse_versioned_edge_key(composite)
            else {
                continue;
            };
            if sys > cutoff {
                continue;
            }
            bases.insert((rev_label.to_string(), rev_src.to_string()), ());
        }
        drop(table);
        drop(read_txn);

        let mut edges = Vec::with_capacity(bases.len());
        for ((label, src_id), _) in bases {
            let Some(props) = self.ceiling_resolve_edge(
                EdgeRef::new(DatabaseId::new(db), tid, collection, &src_id, &label, dst),
                cutoff,
                valid_at_ms,
            )?
            else {
                continue;
            };
            edges.push(Edge {
                collection: collection.to_string(),
                src_id,
                label,
                dst_id: dst.to_string(),
                properties: props,
            });
        }
        Ok(edges)
    }
}
