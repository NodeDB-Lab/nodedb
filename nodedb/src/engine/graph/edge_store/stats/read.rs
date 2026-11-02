// SPDX-License-Identifier: BUSL-1.1

//! Public read APIs: `collection_stats` and `tenant_stats`.
//!
//! ## Complexity
//!
//! - `as_of = None` (live snapshot): O(1) summary read + O(distinct_labels) label scan.
//!   A summary marked for scan (`ownership_version == 0`) or an absent summary falls
//!   back to a full EDGES prefix scan. The scan carries edge identities so the Control
//!   Plane can deduplicate dual-home replicas.
//!
//! - `as_of = Some(ts)`: falls back to a full EDGES prefix scan for the matching
//!   collection, materialising counts at the given system-time ordinal. O(edges-in-collection).
//!
//! ## Historical queries
//!
//! `as_of` accepts an optional system-time ordinal (not milliseconds — use
//! `nodedb_types::ms_to_ordinal_upper` to convert if needed). When present,
//! the function scans EDGES and counts only versions with `system_from <= ts`
//! whose latest qualifying version at `ts` is not a sentinel. This is the same
//! ceiling-resolution logic used by `ceiling_resolve_edge`, applied in aggregate.

use redb::{ReadableDatabase, ReadableTable};
use std::collections::HashMap;

use nodedb_types::TenantId;

use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, redb_err};
use crate::engine::graph::edge_store::temporal::keys::{is_sentinel, parse_versioned_edge_key};
use crate::engine::graph::edge_store::temporal::visibility::{ReadVisibility, read_visibility};

use super::table::{CollectionStats, GRAPH_STATS, LabelRow, SummaryRow, label_prefix, summary_key};

impl EdgeStore {
    /// Live or historical stats for a single `(tenant, collection)` pair.
    ///
    /// Returns zeros when no edges exist — not an error.
    ///
    /// When `as_of` is `None` the result is served from the persistent summary
    /// row (O(1) + O(distinct_labels)). A summary marked for scan, or an
    /// absent one, falls back to a full prefix scan of EDGES and returns
    /// logical identities for exact cross-core deduplication.
    ///
    /// When `as_of` is `Some(ordinal)` the function performs a full prefix scan
    /// of EDGES and materialises counts at that system-time ordinal.
    /// O(edges-in-collection).
    pub fn collection_stats(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        as_of: Option<i64>,
    ) -> crate::Result<CollectionStats> {
        match as_of {
            None => self.collection_stats_live(db, tid, collection),
            Some(ts) => self.collection_stats_historical(db, tid, collection, ts),
        }
    }

    /// Tenant-wide stats: one entry per collection that has any stat rows.
    ///
    /// With `as_of = None`: scans `GRAPH_STATS` summary rows for the tenant
    /// (O(collections-with-stats)), then does O(distinct_labels) label scan
    /// per collection. Summaries marked for scan, or absent ones, fall back to
    /// EDGES scans.
    ///
    /// With `as_of = Some(ts)`: scans all EDGES for the tenant once, grouping by
    /// collection and materialising counts at ordinal `ts`. O(total-edges-for-tenant).
    pub fn tenant_stats(
        &self,
        db: u64,
        tid: TenantId,
        as_of: Option<i64>,
    ) -> crate::Result<Vec<CollectionStats>> {
        match as_of {
            None => self.tenant_stats_live(db, tid),
            Some(ts) => self.tenant_stats_historical(db, tid, ts),
        }
    }

    // ── live paths ────────────────────────────────────────────────────────────

    fn collection_stats_live(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<CollectionStats> {
        let t = tid.as_u64();
        let skey = summary_key(collection);

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read (collection_stats)", e))?;
        let stats_table = read_txn
            .open_table(GRAPH_STATS)
            .map_err(|e| redb_err("open graph_stats (read)", e))?;

        // Decode inside a tight scope so the access guard's borrow of
        // `stats_table` ends before the table is moved. The guard borrows the
        // table it was read from, and both paths below drop the table early to
        // release the read transaction.
        let summary = {
            let guard = stats_table
                .get((db, t, skey.as_str()))
                .map_err(|e| redb_err("read summary", e))?;
            match guard {
                Some(bytes) => Some(SummaryRow::decode(bytes.value())?),
                None => None,
            }
        };

        if let Some(summary) = summary {
            if summary.ownership_version == 0 {
                drop(stats_table);
                drop(read_txn);
                return self.collection_stats_historical(db, tid, collection, i64::MAX);
            }
            let labels = read_labels_from_table(&stats_table, db, t, collection)?;
            return Ok(CollectionStats {
                collection: collection.to_string(),
                edge_count: summary.edge_count,
                distinct_node_count: summary.distinct_node_count,
                distinct_label_count: summary.distinct_label_count,
                labels,
                logical_edges: Vec::new(),
                exact_scan: false,
            });
        }
        drop(stats_table);
        drop(read_txn);

        // Summary row absent — check whether EDGES has entries for this collection.
        // If so, rebuild atomically; otherwise return zeros.
        let prefix = format!("{collection}\x00");
        let has_edges = self.collection_prefix_has_entries(db, tid, &prefix)?;
        if !has_edges {
            return Ok(CollectionStats::zero(collection.to_string()));
        }

        // A physical-edge scan is exact locally and carries logical identities
        // for cross-core union. Do not cache it as a summary: this store may be
        // the destination-home replica, and only request routing can establish
        // which physical copy owns global cardinality.
        self.collection_stats_historical(db, tid, collection, i64::MAX)
    }

    fn tenant_stats_live(&self, db: u64, tid: TenantId) -> crate::Result<Vec<CollectionStats>> {
        let t = tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read (tenant_stats)", e))?;
        let stats_table = read_txn
            .open_table(GRAPH_STATS)
            .map_err(|e| redb_err("open graph_stats (tenant read)", e))?;

        // Scan summary rows for this (database, tenant).
        let mut collections: Vec<String> = Vec::new();
        let range = stats_table
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("tenant_stats range", e))?;

        for entry in range {
            let (k, _v) = entry.map_err(|e| redb_err("tenant_stats iter", e))?;
            let (_kd, _kt, row_key) = k.value();
            // Only collect summary rows — they end with "\x00summary".
            if row_key.ends_with("\x00summary") {
                let collection = row_key.trim_end_matches("\x00summary").to_string();
                collections.push(collection);
            }
        }
        drop(stats_table);
        drop(read_txn);

        if collections.is_empty() {
            // No ownership-aware summary rows exist — scan EDGES and return
            // logical identities for cross-core deduplication.
            return self.tenant_stats_historical(db, tid, i64::MAX);
        }

        let mut result = Vec::with_capacity(collections.len());
        for coll in collections {
            let s = self.collection_stats_live(db, tid, &coll)?;
            result.push(s);
        }
        result.sort_by(|a, b| a.collection.cmp(&b.collection));
        Ok(result)
    }

    // ── historical paths ─────────────────────────────────────────────────────

    fn collection_stats_historical(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        as_of: i64,
    ) -> crate::Result<CollectionStats> {
        let prefix = format!("{collection}\x00");
        let t = tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read (historical)", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges (historical)", e))?;

        let mut visibility = read_visibility(&read_txn)?;

        let stats = materialise_collection_stats(
            collection,
            &edges,
            &mut visibility,
            db,
            t,
            &prefix,
            as_of,
        )?;
        Ok(stats)
    }

    fn tenant_stats_historical(
        &self,
        db: u64,
        tid: TenantId,
        as_of: i64,
    ) -> crate::Result<Vec<CollectionStats>> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read (tenant_stats_hist)", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges (tenant_stats_hist)", e))?;
        let mut visibility = read_visibility(&read_txn)?;

        // One pass across all (db, tenant) edges — collect per-collection accumulators.
        let mut per_coll: HashMap<String, CollectionAccum> = HashMap::new();

        let range = edges
            .range((db, t, "")..(db, t + 1, ""))
            .map_err(|e| redb_err("tenant_stats_hist range", e))?;

        for entry in range {
            let (k, v) = entry.map_err(|e| redb_err("tenant_stats_hist iter", e))?;
            let (_kd, _kt, composite) = k.value();
            let Some((coll, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
                continue;
            };
            if sys > as_of || visibility.hidden(db, t, coll, composite, sys, as_of)? {
                continue;
            }
            let bytes = v.value();
            let accum = per_coll.entry(coll.to_string()).or_default();
            let base = format!("{src}\x00{label}\x00{dst}");
            let entry = accum.bases.entry(base).or_default();
            // Keep the latest version at or before as_of.
            if sys > entry.latest_sys {
                entry.latest_sys = sys;
                entry.is_sentinel = is_sentinel(bytes);
                entry.label = label.to_string();
                entry.src = src.to_string();
                entry.dst = dst.to_string();
            }
        }

        let mut result = Vec::with_capacity(per_coll.len());
        for (coll, accum) in per_coll {
            let stats = accum.into_collection_stats(coll);
            result.push(stats);
        }
        result.sort_by(|a, b| a.collection.cmp(&b.collection));
        Ok(result)
    }

    // ── cold-start rebuild helpers ────────────────────────────────────────────

    fn collection_prefix_has_entries(
        &self,
        db: u64,
        tid: TenantId,
        prefix: &str,
    ) -> crate::Result<bool> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read (prefix check)", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges (prefix check)", e))?;
        let mut range = edges
            .range((db, t, prefix)..)
            .map_err(|e| redb_err("prefix range", e))?;
        match range.next() {
            None => Ok(false),
            Some(Err(e)) => Err(redb_err("prefix range first", e)),
            Some(Ok((k, _))) => {
                let (kd, kt, composite) = k.value();
                Ok(kd == db && kt == t && composite.starts_with(prefix))
            }
        }
    }
}

// ── free functions ────────────────────────────────────────────────────────────

fn read_labels_from_table(
    stats_table: &redb::ReadOnlyTable<(u64, u64, &str), &[u8]>,
    db: u64,
    t: u64,
    collection: &str,
) -> crate::Result<Vec<(String, u64)>> {
    let lp = label_prefix(collection);
    let range = stats_table
        .range((db, t, lp.as_str())..)
        .map_err(|e| redb_err("label scan range", e))?;

    let mut labels = Vec::new();
    for entry in range {
        let (k, v) = entry.map_err(|e| redb_err("label scan iter", e))?;
        let (kd, kt, row_key) = k.value();
        if kd != db || kt != t || !row_key.starts_with(&lp) {
            break;
        }
        let label_name = row_key[lp.len()..].to_string();
        let lrow = LabelRow::decode(v.value())?;
        labels.push((label_name, lrow.count));
    }
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(labels)
}

fn materialise_collection_stats(
    collection: &str,
    edges: &redb::ReadOnlyTable<(u64, u64, &str), &[u8]>,
    visibility: &mut ReadVisibility,
    db: u64,
    t: u64,
    prefix: &str,
    as_of: i64,
) -> crate::Result<CollectionStats> {
    // Per-base-key: track (latest_sys, is_sentinel, label, src, dst).
    struct BaseEntry {
        latest_sys: i64,
        is_sentinel: bool,
        label: String,
        src: String,
        dst: String,
    }

    let mut bases: HashMap<String, BaseEntry> = HashMap::new();

    let range = edges
        .range((db, t, prefix)..)
        .map_err(|e| redb_err("materialise range", e))?;

    for entry in range {
        let (k, v) = entry.map_err(|e| redb_err("materialise iter", e))?;
        let (kd, kt, composite) = k.value();
        if kd != db || kt != t || !composite.starts_with(prefix) {
            break;
        }
        let Some((_c, src, label, dst, sys)) = parse_versioned_edge_key(composite) else {
            continue;
        };
        if sys > as_of || visibility.hidden(db, t, collection, composite, sys, as_of)? {
            continue;
        }
        let base = format!("{src}\x00{label}\x00{dst}");
        let e = bases.entry(base).or_insert_with(|| BaseEntry {
            latest_sys: i64::MIN,
            is_sentinel: true,
            label: label.to_string(),
            src: src.to_string(),
            dst: dst.to_string(),
        });
        if sys > e.latest_sys {
            e.latest_sys = sys;
            e.is_sentinel = is_sentinel(v.value());
            e.label = label.to_string();
            e.src = src.to_string();
            e.dst = dst.to_string();
        }
    }

    let mut edge_count = 0u64;
    let mut label_counts: HashMap<String, u64> = HashMap::new();
    let mut node_refs: HashMap<String, u32> = HashMap::new();
    let mut logical_edges = Vec::new();

    for entry in bases.values() {
        if entry.is_sentinel {
            continue;
        }
        edge_count += 1;
        logical_edges.push((entry.src.clone(), entry.label.clone(), entry.dst.clone()));
        *label_counts.entry(entry.label.clone()).or_insert(0) += 1;
        *node_refs.entry(entry.src.clone()).or_insert(0) += 1;
        if entry.src != entry.dst {
            *node_refs.entry(entry.dst.clone()).or_insert(0) += 1;
        }
    }

    let distinct_node_count = node_refs.len() as u64;
    let distinct_label_count = label_counts.len() as u64;
    let mut labels: Vec<(String, u64)> = label_counts.into_iter().collect();
    labels.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(CollectionStats {
        collection: collection.to_string(),
        edge_count,
        distinct_node_count,
        distinct_label_count,
        labels,
        logical_edges,
        exact_scan: true,
    })
}

// ── per-collection accumulator used by tenant_stats_historical ────────────────

#[derive(Default)]
struct CollectionAccum {
    bases: HashMap<String, BaseVersionEntry>,
}

#[derive(Default)]
struct BaseVersionEntry {
    latest_sys: i64,
    is_sentinel: bool,
    label: String,
    src: String,
    dst: String,
}

impl CollectionAccum {
    fn into_collection_stats(self, collection: String) -> CollectionStats {
        let mut edge_count = 0u64;
        let mut label_counts: HashMap<String, u64> = HashMap::new();
        let mut node_refs: HashMap<String, u32> = HashMap::new();
        let mut logical_edges = Vec::new();

        for entry in self.bases.values() {
            if entry.is_sentinel {
                continue;
            }
            edge_count += 1;
            logical_edges.push((entry.src.clone(), entry.label.clone(), entry.dst.clone()));
            *label_counts.entry(entry.label.clone()).or_insert(0) += 1;
            *node_refs.entry(entry.src.clone()).or_insert(0) += 1;
            if entry.src != entry.dst {
                *node_refs.entry(entry.dst.clone()).or_insert(0) += 1;
            }
        }

        let distinct_node_count = node_refs.len() as u64;
        let distinct_label_count = label_counts.len() as u64;
        let mut labels: Vec<(String, u64)> = label_counts.into_iter().collect();
        labels.sort_by(|a, b| a.0.cmp(&b.0));

        CollectionStats {
            collection,
            edge_count,
            distinct_node_count,
            distinct_label_count,
            labels,
            logical_edges,
            exact_scan: true,
        }
    }
}
