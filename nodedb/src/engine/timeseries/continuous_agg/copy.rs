// SPDX-License-Identifier: BUSL-1.1

//! Aggregate state for a copy that re-issues a root's rows.
//!
//! A mirror's bootstrap writes each row anew, so the mirror's partitions mix rows
//! a source partition folded with rows it did not. No folded set of the source
//! can name them. So the state travels as a whole, and it covers every row:
//!
//! - The source adds each row an aggregate has not folded to the aggregate's
//!   state. A `Manual` or `Periodic` aggregate parks those rows, so its reads
//!   answer its last cut, and its next cut merges them. A flush-driven aggregate
//!   holds them in its buckets, as its source's reads count them.
//! - The copy flushes the root after the last re-issued row, and counts every
//!   partition it holds as folded. A later row lands in a later partition, which
//!   folds as it does on any replica.
//!
//! Buckets folded from partitions retention removed travel with the state. No
//! fold on the copy can rebuild them.

use super::cut::folds_at_cuts;
use super::image::{CaggImage, CaggImageEntry};
use super::manager::ContinuousAggregateManager;
use super::partial::ColumnLayout;
use super::refresh::{self, Buckets, RefreshResult, RefreshSource};
use super::rollup;
use crate::engine::timeseries::columnar_memtable::TimeKind;

impl CaggImageEntry {
    /// Add `rows` to this aggregate's state.
    fn absorb(&mut self, rows: &RefreshResult, time_kind: Option<TimeKind>) {
        if rows.rows_processed == 0 {
            return;
        }
        let layout = ColumnLayout::of(&self.def);
        if folds_at_cuts(&self.def.refresh_policy) {
            refresh::merge_delta(&mut self.cut.parked, &rows.delta, &layout);
            self.cut.parked_rows += rows.rows_processed;
            self.cut.parked_max_ts = self.cut.parked_max_ts.max(rows.max_ts);
        } else {
            let mut buckets: Buckets = std::mem::take(&mut self.buckets)
                .into_iter()
                .map(|bucket| ((bucket.bucket_ts, bucket.group_key.clone()), bucket))
                .collect();
            refresh::merge_delta(&mut buckets, &rows.delta, &layout);
            self.buckets = buckets.into_values().collect();
            self.watermark_ts = self.watermark_ts.max(rows.max_ts);
            self.rows_aggregated += rows.rows_processed;
        }
        self.time_kind = self.time_kind.or(time_kind);
    }
}

impl CaggImage {
    /// Remove what names a source partition: a copy's partitions have other
    /// names. Every applied cut has folded, so each pending cut completes. An
    /// orphan cut's aggregate holds these rows parked or in its buckets.
    pub fn seal_for_copy(&mut self) {
        self.orphans.clear();
        for entry in &mut self.entries {
            entry.folded.clear();
            entry.cut.complete_pending();
        }
    }
}

impl ContinuousAggregateManager {
    /// Add the root rows `src`, which `entry` has not folded, to `entry`. The
    /// rows roll down the aggregate's chain, as a partition fold does. An
    /// aggregate not registered here takes nothing.
    pub fn absorb_unfolded(
        &self,
        database_id: u64,
        tenant_id: u64,
        entry: &mut CaggImageEntry,
        src: &RefreshSource<'_>,
    ) {
        let Some(path) = self.chain_path(database_id, tenant_id, &entry.def.name) else {
            return;
        };
        let mut defs = path.iter().filter_map(|name| {
            self.definitions
                .get(&(database_id, tenant_id, name.clone()))
        });
        let Some(first) = defs.next() else {
            return;
        };
        let mut upstream = first;
        let mut rows = refresh::refresh_rows(first, src);
        for def in defs {
            rows.delta = rollup::rollup_delta(upstream, def, &rows.delta);
            upstream = def;
        }
        entry.absorb(&rows, src.time_kind());
    }

    /// Replace the state of every aggregate rooted at `root` with `image`, and
    /// count every partition in `live` as folded.
    ///
    /// A catalog apply registers an aggregate on its own schedule, so the image
    /// registers each definition it holds. A registration that arrives later
    /// finds the same shape and keeps this state.
    pub fn install_copy_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        mut image: CaggImage,
        live: &[String],
    ) {
        for entry in &mut image.entries {
            self.register(tenant_id, entry.def.clone());
            entry.folded = live.to_vec();
        }
        for name in self.chain(database_id, tenant_id, root) {
            self.loaded.remove(&(database_id, tenant_id, name));
        }
        self.install_image(database_id, tenant_id, root, image);
    }
}
