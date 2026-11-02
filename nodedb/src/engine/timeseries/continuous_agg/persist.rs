// SPDX-License-Identifier: BUSL-1.1

//! Moving aggregate state between the manager and its on-disk image.
//!
//! An aggregate reads its image once, when it is unloaded: at boot, after a
//! CREATE, and after a reset. It takes no fold until then, so a fold never
//! lands on state the image is about to replace.
//!
//! A definition whose incarnation is `Hlc::ZERO` was never stamped by a
//! CREATE. Its state is never written to an image, and no image entry of
//! that incarnation is ever installed, so two such definitions never share
//! buckets.

use std::collections::HashSet;

use nodedb_types::Hlc;

use super::image::{CaggImage, CaggImageEntry, CaggImageEntryView, CaggImageView};
use super::manager::{AggKey, ContinuousAggregateManager, same_shape};
use super::partial::{ColumnLayout, PartialAggregate};
use super::refresh::{self, Buckets};
use super::watermark::WatermarkState;

impl ContinuousAggregateManager {
    /// Borrowed image of every loaded aggregate rooted at `root`, plus the
    /// entries of aggregates not registered here when the image was read.
    pub fn export_image(&self, database_id: u64, tenant_id: u64, root: &str) -> CaggImageView<'_> {
        let mut entries: Vec<CaggImageEntryView<'_>> = self
            .chain(database_id, tenant_id, root)
            .into_iter()
            .filter_map(|name| {
                let key = (database_id, tenant_id, name);
                if !self.loaded.contains(&key) {
                    return None;
                }
                let def = self.definitions.get(&key)?;
                if def.incarnation == Hlc::ZERO {
                    return None;
                }
                let wm = self.watermarks.get(&key).cloned().unwrap_or_default();
                let mut folded: Vec<&str> = self
                    .folded
                    .get(&key)
                    .map(|set| set.iter().map(String::as_str).collect())
                    .unwrap_or_default();
                folded.sort_unstable();
                let mut buckets: Vec<&PartialAggregate> = self
                    .materialized
                    .get(&key)
                    .map(|m| m.values().collect())
                    .unwrap_or_default();
                buckets
                    .sort_by(|a, b| (a.bucket_ts, &a.group_key).cmp(&(b.bucket_ts, &b.group_key)));
                Some(CaggImageEntryView {
                    def,
                    watermark_ts: wm.watermark_ts,
                    rows_aggregated: wm.rows_aggregated,
                    last_refresh_ms: wm.last_refresh_ms,
                    time_kind: self.time_kinds.get(&key).copied(),
                    folded,
                    buckets,
                    cut: self.cuts.get(&key),
                })
            })
            .collect();
        let root_key = (database_id, tenant_id, root.to_string());
        for entry in self.unclaimed.get(&root_key).into_iter().flatten() {
            if entries.iter().any(|e| e.def.name == entry.def.name) {
                continue;
            }
            let mut folded: Vec<&str> = entry.folded.iter().map(String::as_str).collect();
            folded.sort_unstable();
            entries.push(CaggImageEntryView {
                def: &entry.def,
                watermark_ts: entry.watermark_ts,
                rows_aggregated: entry.rows_aggregated,
                last_refresh_ms: entry.last_refresh_ms,
                time_kind: entry.time_kind,
                folded,
                buckets: entry.buckets.iter().collect(),
                cut: Some(&entry.cut),
            });
        }
        let orphans = self
            .orphan_cuts
            .get(&root_key)
            .map_or(&[][..], Vec::as_slice);
        CaggImageView {
            entries,
            orphans,
            raw_dropped_through: self.raw_dropped_through.get(&root_key).copied(),
            rewritten: self.rewritten_view(&root_key),
        }
    }

    /// Install `image` into every unloaded aggregate rooted at `root`, and
    /// mark each of them loaded. An entry installs only into the aggregate
    /// of its name whose definition has the same shape and incarnation. An
    /// unloaded aggregate with no such entry loads empty. An entry whose
    /// aggregate is not registered is kept for later image writes. An entry
    /// whose aggregate is registered with another shape or incarnation is
    /// dropped: that aggregate was re-created. An entry of incarnation
    /// `Hlc::ZERO` is dropped. The image's orphan cuts join the ones held
    /// here, and every cut whose aggregate is now loaded applies. The root
    /// is then marked for backfill, so the tick folds every partition the
    /// image does not name.
    pub fn install_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        image: CaggImage,
    ) {
        let unloaded: Vec<AggKey> = self
            .chain(database_id, tenant_id, root)
            .into_iter()
            .map(|name| (database_id, tenant_id, name))
            .filter(|key| !self.loaded.contains(key))
            .collect();
        let mut leftover = Vec::new();
        for entry in image.entries {
            if entry.def.incarnation == Hlc::ZERO {
                continue;
            }
            let key = (database_id, tenant_id, entry.def.name.clone());
            let installable = self
                .definitions
                .get(&key)
                .map(|def| unloaded.contains(&key) && same_shape(def, &entry.def));
            match installable {
                // Boot registered every aggregate the catalog holds: this
                // one was dropped.
                None if self.boot_registered => {}
                // Not registered here yet: carried until its tier arrives.
                None => leftover.push(entry),
                Some(true) => self.install_entry(&key, entry),
                Some(false) => {}
            }
        }
        let root_key = (database_id, tenant_id, root.to_string());
        if let Some(through) = image.raw_dropped_through {
            let held = self
                .raw_dropped_through
                .entry(root_key.clone())
                .or_insert(through);
            *held = (*held).max(through);
        }
        self.merge_rewritten(&root_key, image.rewritten);
        self.images_read.insert(root_key.clone());
        if leftover.is_empty() {
            self.unclaimed.remove(&root_key);
        } else {
            self.unclaimed.insert(root_key.clone(), leftover);
        }
        for key in unloaded {
            self.loaded.insert(key);
        }
        if !image.orphans.is_empty() {
            let held = self.orphan_cuts.entry(root_key.clone()).or_default();
            for orphan in image.orphans {
                if !held.iter().any(|h| h.cut == orphan.cut) {
                    held.push(orphan);
                }
            }
        }
        self.adopt_orphan_cuts(database_id, tenant_id, root);
        self.backfill_roots
            .insert((database_id, tenant_id, root.to_string()));
        self.dirty_images
            .insert((database_id, tenant_id, root.to_string()));
    }

    fn install_entry(&mut self, key: &AggKey, entry: CaggImageEntry) {
        let mut buckets = Buckets::new();
        for bucket in entry.buckets {
            buckets.insert((bucket.bucket_ts, bucket.group_key.clone()), bucket);
        }
        self.materialized.insert(key.clone(), buckets);
        self.folded
            .insert(key.clone(), entry.folded.into_iter().collect());
        self.watermarks.insert(
            key.clone(),
            WatermarkState {
                watermark_ts: entry.watermark_ts,
                rows_aggregated: entry.rows_aggregated,
                last_refresh_ms: entry.last_refresh_ms,
            },
        );
        if let Some(kind) = entry.time_kind {
            self.time_kinds.insert(key.clone(), kind);
        }
        if entry.cut.is_empty() {
            self.cuts.remove(key);
        } else {
            self.cuts.insert(key.clone(), entry.cut);
        }
    }

    /// Merge a snapshot's image into the loaded aggregates rooted at `root`.
    ///
    /// An entry merges only when its folded set shares no partition with
    /// the local one: the two then count disjoint rows, so the merged
    /// buckets equal one pass over both. An entry that overlaps is skipped,
    /// and the partitions it named that are present here fold from their
    /// rows. The entry's parked rows and cut ids join the local ones.
    /// Returns the names of the aggregates that took a merge.
    pub fn merge_snapshot_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        image: CaggImage,
    ) -> Vec<String> {
        let chain: HashSet<String> = self
            .chain(database_id, tenant_id, root)
            .into_iter()
            .collect();
        let mut merged = Vec::new();
        // The snapshot's root lost raw rows to retention the local root may
        // not have: a raw read of their range is incomplete here too.
        if let Some(through) = image.raw_dropped_through {
            let root_key = (database_id, tenant_id, root.to_string());
            let held = self
                .raw_dropped_through
                .entry(root_key.clone())
                .or_insert(through);
            *held = (*held).max(through);
            self.dirty_images.insert(root_key);
        }
        // The snapshot's aggregates were rewritten at its ordinals: a read
        // below them here finds the same buckets gone.
        if !image.rewritten.is_empty() {
            let root_key = (database_id, tenant_id, root.to_string());
            self.merge_rewritten(&root_key, image.rewritten);
            self.dirty_images.insert(root_key);
        }
        for entry in image.entries {
            if !chain.contains(&entry.def.name) || entry.def.incarnation == Hlc::ZERO {
                continue;
            }
            let key = (database_id, tenant_id, entry.def.name.clone());
            let Some(def) = self.definitions.get(&key) else {
                continue;
            };
            if !self.loaded.contains(&key) || !same_shape(def, &entry.def) {
                continue;
            }
            let layout = ColumnLayout::of(def);
            let local = self.folded.entry(key.clone()).or_default();
            if entry.folded.iter().any(|p| local.contains(p)) {
                continue;
            }
            local.extend(entry.folded);
            let mut delta = Buckets::new();
            for bucket in entry.buckets {
                delta.insert((bucket.bucket_ts, bucket.group_key.clone()), bucket);
            }
            refresh::merge_delta(
                self.materialized.entry(key.clone()).or_default(),
                &delta,
                &layout,
            );
            let wm = self.watermarks.entry(key.clone()).or_default();
            wm.advance(
                entry.watermark_ts,
                entry.rows_aggregated,
                entry.last_refresh_ms,
            );
            if let Some(kind) = entry.time_kind {
                self.time_kinds.entry(key.clone()).or_insert(kind);
            }
            self.merge_snapshot_cut(&key, entry.cut);
            merged.push(key.2);
        }
        if !merged.is_empty() {
            self.dirty_images
                .insert((database_id, tenant_id, root.to_string()));
        }
        merged
    }

    /// Join a snapshot entry's cut state to `key`'s. Its pending partitions
    /// that `key` has folded are already in its buckets.
    fn merge_snapshot_cut(&mut self, key: &AggKey, cut: super::cut::CutState) {
        if cut.is_empty() {
            return;
        }
        self.park_delta(key, &cut.parked, cut.parked_rows, cut.parked_max_ts);
        let folded = self.folded.get(key).cloned().unwrap_or_default();
        let local = self.cuts.entry(key.clone()).or_default();
        local.last_slot = local.last_slot.max(cut.last_slot);
        for id in cut.completed {
            if !local.completed.contains(&id) {
                local.completed.push_back(id);
            }
        }
        while local.completed.len() > super::cut::COMPLETED_CUTS_KEPT {
            local.completed.pop_front();
        }
        for id in cut.pending_ids {
            if !local.pending_ids.contains(&id) && !local.completed.contains(&id) {
                local.pending_ids.push(id);
            }
        }
        local
            .pending
            .extend(cut.pending.into_iter().filter(|p| !folded.contains(p)));
    }

    /// Mark every aggregate rooted at `root` loaded and stale, and keep its
    /// image as it is on disk. The image or a partition of `root` could not
    /// be read: no aggregate folds over state it cannot trust, and
    /// `SHOW CONTINUOUS AGGREGATES` reports each as stale. A reset of the
    /// root (truncate, clear) lifts the quarantine.
    pub fn quarantine_root(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        for name in self.chain(database_id, tenant_id, root) {
            let key = (database_id, tenant_id, name);
            if let Some(def) = self.definitions.get_mut(&key) {
                def.stale = true;
            }
            self.loaded.insert(key);
        }
        let root_key = (database_id, tenant_id, root.to_string());
        self.backfill_roots.remove(&root_key);
        self.dirty_images.remove(&root_key);
        self.quarantined.insert(root_key);
    }

    /// Whether `root` is quarantined: its image is never rewritten.
    pub fn is_quarantined(&self, database_id: u64, tenant_id: u64, root: &str) -> bool {
        self.quarantined
            .contains(&(database_id, tenant_id, root.to_string()))
    }

    /// Roots whose image does not hold the current state. The set is
    /// cleared; a root whose write fails goes back through
    /// [`Self::mark_image_dirty`].
    pub fn take_dirty_images(&mut self) -> Vec<AggKey> {
        let mut roots: Vec<AggKey> = self.dirty_images.drain().collect();
        roots.sort();
        roots
    }

    /// Whether `root`'s image was read into memory.
    pub fn image_read(&self, database_id: u64, tenant_id: u64, root: &str) -> bool {
        self.images_read
            .contains(&(database_id, tenant_id, root.to_string()))
    }

    /// Boot registered every aggregate the catalog holds. Drop the image
    /// entries held for aggregates that are not registered: they belong to
    /// dropped aggregates. Every image read from now on drops them too.
    pub fn finish_boot_registration(&mut self) {
        self.boot_registered = true;
        for root in std::mem::take(&mut self.unclaimed).into_keys() {
            self.dirty_images.insert(root);
        }
    }

    /// Mark the image of `root` dirty.
    pub fn mark_image_dirty(&mut self, database_id: u64, tenant_id: u64, root: &str) {
        self.dirty_images
            .insert((database_id, tenant_id, root.to_string()));
    }

    /// Whether `root`'s image is dirty.
    pub fn image_dirty(&self, database_id: u64, tenant_id: u64, root: &str) -> bool {
        self.dirty_images
            .contains(&(database_id, tenant_id, root.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::fold::FoldScope;
    use crate::engine::timeseries::continuous_agg::image::{decode_image, encode_image};
    use crate::engine::timeseries::continuous_agg::test_support::{
        T0, ad_hoc, exact_def, exact_rows, int_drain, loaded_manager, materialized,
    };

    fn round_trip(mgr: &ContinuousAggregateManager) -> CaggImage {
        let bytes = encode_image(&mgr.export_image(0, 1, "metrics")).expect("encode");
        decode_image(&bytes).expect("decode")
    }

    #[test]
    fn image_restores_buckets_and_folded_set() {
        let mut before = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        let rows = exact_rows();
        before.on_flush(0, 1, "metrics", "p1", &int_drain(&rows), T0);
        let image = round_trip(&before);

        let mut after = ContinuousAggregateManager::new();
        after.register(1, exact_def("v_1m", "metrics", "1m"));
        after.install_image(0, 1, "metrics", image);
        assert_eq!(materialized(&after, "v_1m"), ad_hoc(&rows, 60_000));
        assert!(!after.needs_fold(0, 1, "metrics", "p1", FoldScope::AtFlush));
        assert!(after.needs_fold(0, 1, "metrics", "p2", FoldScope::AtFlush));
    }

    #[test]
    fn recreated_aggregate_ignores_stale_image() {
        let mut before = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        before.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        let image = round_trip(&before);

        let mut recreated = exact_def("v_1m", "metrics", "1m");
        recreated.incarnation = nodedb_types::Hlc::new(7, 0);
        let mut after = ContinuousAggregateManager::new();
        after.register(1, recreated);
        after.install_image(0, 1, "metrics", image);
        assert!(
            after
                .get_materialized(0, 1, "v_1m")
                .expect("registered")
                .is_empty()
        );
        assert!(after.needs_fold(0, 1, "metrics", "p1", FoldScope::AtFlush));
    }

    #[test]
    fn snapshot_image_merges_only_disjoint_folds() {
        let rows = exact_rows();
        let mut source = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        source.on_flush(0, 1, "metrics", "old", &int_drain(&rows[..3]), T0);
        let snapshot = round_trip(&source);

        let mut target = loaded_manager(vec![exact_def("v_1m", "metrics", "1m")]);
        target.on_flush(0, 1, "metrics", "new", &int_drain(&rows[3..]), T0);
        assert_eq!(
            target.merge_snapshot_image(0, 1, "metrics", snapshot),
            vec!["v_1m"]
        );
        assert_eq!(materialized(&target, "v_1m"), ad_hoc(&rows, 60_000));

        let overlapping = round_trip(&source);
        assert!(
            target
                .merge_snapshot_image(0, 1, "metrics", overlapping)
                .is_empty()
        );
        assert_eq!(materialized(&target, "v_1m"), ad_hoc(&rows, 60_000));
    }

    #[test]
    fn quarantined_root_takes_no_fold() {
        let mut mgr = ContinuousAggregateManager::new();
        mgr.register(1, exact_def("v_1m", "metrics", "1m"));
        mgr.quarantine_root(0, 1, "metrics");
        assert!(mgr.get_definition(0, 1, "v_1m").is_some_and(|d| d.stale));
        assert!(
            mgr.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0)
                .is_empty()
        );
    }

    /// A tier registered after its root loaded the image still finds its
    /// entry: the image written in between carries it.
    #[test]
    fn late_registered_tier_finds_its_entry() {
        let rows = exact_rows();
        let mut before = loaded_manager(vec![
            exact_def("v_1m", "metrics", "1m"),
            exact_def("v_1h", "v_1m", "1h"),
        ]);
        before.on_flush(0, 1, "metrics", "p1", &int_drain(&rows), T0);
        let image = round_trip(&before);

        let mut after = ContinuousAggregateManager::new();
        after.register(1, exact_def("v_1m", "metrics", "1m"));
        after.install_image(0, 1, "metrics", image);
        let rewritten = round_trip(&after);
        assert_eq!(rewritten.entries.len(), 2, "the unclaimed entry is carried");

        after.register(1, exact_def("v_1h", "v_1m", "1h"));
        assert!(after.has_unloaded(0, 1, "metrics"));
        after.install_image(0, 1, "metrics", rewritten);
        assert_eq!(materialized(&after, "v_1h"), ad_hoc(&rows, 3_600_000));
        assert!(!after.needs_fold(0, 1, "metrics", "p1", FoldScope::AtFlush));
    }

    /// Once boot registered every aggregate the catalog holds, an entry of
    /// an aggregate that is not registered is dropped from the image.
    #[test]
    fn boot_registration_drops_entries_of_dropped_aggregates() {
        let mut before = loaded_manager(vec![
            exact_def("v_1m", "metrics", "1m"),
            exact_def("gone", "metrics", "1m"),
        ]);
        before.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        let image = round_trip(&before);

        let mut after = ContinuousAggregateManager::new();
        after.register(1, exact_def("v_1m", "metrics", "1m"));
        after.install_image(0, 1, "metrics", image);
        assert_eq!(
            round_trip(&after).entries.len(),
            2,
            "carried until boot registers"
        );

        after.take_dirty_images();
        after.finish_boot_registration();
        assert!(after.image_dirty(0, 1, "metrics"));
        let pruned = round_trip(&after);
        assert_eq!(pruned.entries.len(), 1);
        assert_eq!(pruned.entries[0].def.name, "v_1m");
    }

    /// An entry of incarnation `Hlc::ZERO` never installs.
    #[test]
    fn an_unstamped_entry_never_installs() {
        let mut unstamped = exact_def("v_1m", "metrics", "1m");
        unstamped.incarnation = Hlc::ZERO;
        let mut before = loaded_manager(vec![unstamped.clone()]);
        before.on_flush(0, 1, "metrics", "p1", &int_drain(&exact_rows()), T0);
        assert!(round_trip(&before).entries.is_empty(), "never written");

        let mut after = ContinuousAggregateManager::new();
        after.register(1, unstamped);
        after.install_image(0, 1, "metrics", round_trip(&before));
        assert!(
            after
                .get_materialized(0, 1, "v_1m")
                .expect("registered")
                .is_empty()
        );
    }
}
