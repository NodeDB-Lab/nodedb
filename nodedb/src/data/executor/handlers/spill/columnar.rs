// SPDX-License-Identifier: BUSL-1.1

//! Spill-to-disk manager for the native columnar GROUP BY path.
//!
//! The columnar path uses `GroupKey = Vec<GroupKeyPart>` (integer/symbol IDs)
//! rather than the JSON-encoded string keys used by the schemaless path.
//! Both `GroupKey` and `Vec<AggAccum>` derive `serde::Serialize +
//! Deserialize`, so they serialize directly without intermediate string
//! encoding — no unwrap fallbacks, no double-serialization.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::data::executor::handlers::columnar_agg_support::{AggAccum, GroupKey};
use crate::types::{DatabaseId, TenantId};

use super::core::SpillCore;

/// Spill-to-disk manager for the columnar GROUP BY HashMap path.
pub(in crate::data::executor::handlers) struct ColumnarGroupBySpiller {
    core: SpillCore<GroupKey, Vec<AggAccum>>,
    in_mem: HashMap<GroupKey, Vec<AggAccum>>,
    cap: usize,
    governor: Arc<nodedb_mem::MemoryGovernor>,
    feed_counter: u64,
    /// Database this spiller is executing on behalf of.
    db: DatabaseId,
    /// Tenant this spiller is executing on behalf of.
    tenant: TenantId,
}

impl ColumnarGroupBySpiller {
    pub(in crate::data::executor::handlers) fn new(
        spill_dir: PathBuf,
        cap: usize,
        governor: Arc<nodedb_mem::MemoryGovernor>,
        db: DatabaseId,
        tenant: TenantId,
    ) -> crate::Result<Self> {
        Ok(Self {
            core: SpillCore::new(spill_dir)?,
            in_mem: HashMap::new(),
            cap: cap.max(1),
            governor,
            feed_counter: 0,
            db,
            tenant,
        })
    }

    /// Return a mutable reference to the in-memory group entry, spilling if
    /// necessary to make room.  The caller fills the returned accumulators.
    pub(in crate::data::executor::handlers) fn get_or_insert_with(
        &mut self,
        key: GroupKey,
        num_aggs: usize,
    ) -> crate::Result<&mut Vec<AggAccum>> {
        self.feed_counter += 1;

        if self.feed_counter.is_multiple_of(10_000) {
            let estimated_growth = std::mem::size_of::<AggAccum>() * num_aggs * 10_000;
            if self
                .governor
                .try_reserve(
                    self.db,
                    self.tenant,
                    nodedb_mem::EngineId::Query,
                    estimated_growth,
                )
                .is_err()
            {
                self.spill_current_run()?;
            }
        }

        if !self.in_mem.contains_key(&key) && self.in_mem.len() >= self.cap {
            self.spill_current_run()?;
        }

        Ok(self
            .in_mem
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::new()).collect()))
    }

    fn spill_current_run(&mut self) -> crate::Result<()> {
        if self.in_mem.is_empty() {
            return Ok(());
        }
        self.core.flush_run(self.in_mem.drain())?;
        Ok(())
    }

    /// Merge all spill runs and the remaining in-memory map into the
    /// consolidated output.
    pub(in crate::data::executor::handlers) fn finalize(
        mut self,
    ) -> crate::Result<HashMap<GroupKey, Vec<AggAccum>>> {
        self.core.merge(&mut self.in_mem, self.cap, |dst, src| {
            for (d, s) in dst.iter_mut().zip(src) {
                d.merge(s);
            }
        })
    }
}
