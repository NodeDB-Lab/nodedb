//! Bitemporal neighbor queries — prefix scan with Ceiling per base.
//!
//! `neighbors_out_as_of` / `neighbors_in_as_of` mirror the current-state
//! `EdgeStore::neighbors_out` / `neighbors_in` but honour:
//!
//! - `system_as_of_ms: Option<i64>` — caps visibility to versions with
//!   `system_from` at or before the given millisecond (via
//!   [`nodedb_types::ms_to_ordinal_upper`]). `None` means current state.
//! - `valid_at_ms: Option<i64>` — each candidate's
//!   `[valid_from_ms, valid_until_ms)` must contain `valid_at_ms`.
//!   The Ceiling resolver walks to earlier versions if the latest one
//!   doesn't satisfy the predicate.

use std::collections::HashMap;

use nodedb_types::{TenantId, ms_to_ordinal_upper};

use super::super::store::{EDGES, Edge, EdgeStore, REVERSE_EDGES, redb_err};
use super::{EdgeRef, is_sentinel, parse_versioned_edge_key};

impl EdgeStore {
    /// Bitemporal outbound neighbors. See module docs for semantics.
    pub fn neighbors_out_as_of(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label_filter: Option<&str>,
        system_as_of_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Vec<Edge>> {
        let cutoff = system_cutoff(system_as_of_ms);
        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{src}\x00{label}\x00"),
            None => format!("{collection}\x00{src}\x00"),
        };
        let t = tid.as_u32();

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
            .range((t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, _val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(&prefix) {
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
                EdgeRef::new(tid, collection, src, &label, &dst),
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
    pub fn neighbors_in_as_of(
        &self,
        tid: TenantId,
        collection: &str,
        dst: &str,
        label_filter: Option<&str>,
        system_as_of_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Vec<Edge>> {
        let cutoff = system_cutoff(system_as_of_ms);
        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{dst}\x00{label}\x00"),
            None => format!("{collection}\x00{dst}\x00"),
        };
        let t = tid.as_u32();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        // `(label, logical_src)` base → (latest_sys ≤ cutoff, sentinel flag).
        let mut latest: HashMap<(String, String), (i64, bool)> = HashMap::new();
        let range = table
            .range((t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(&prefix) {
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
            let is_sent = is_sentinel(val.value());
            latest
                .entry((rev_label.to_string(), rev_src.to_string()))
                .and_modify(|(cur, cur_sent)| {
                    if sys > *cur {
                        *cur = sys;
                        *cur_sent = is_sent;
                    }
                })
                .or_insert((sys, is_sent));
        }

        let mut edges = Vec::with_capacity(latest.len());
        for ((label, src_id), (_sys, is_sent)) in latest {
            if is_sent {
                continue;
            }
            let Some(props) = self.ceiling_resolve_edge(
                EdgeRef::new(tid, collection, &src_id, &label, dst),
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

/// Convert user-facing system-time millisecond cutoff to the internal HLC
/// ordinal upper-bound. `None` → `i64::MAX` (current state).
fn system_cutoff(system_as_of_ms: Option<i64>) -> i64 {
    match system_as_of_ms {
        Some(ms) => ms_to_ordinal_upper(ms),
        None => i64::MAX,
    }
}
