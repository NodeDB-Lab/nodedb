// SPDX-License-Identifier: BUSL-1.1

//! The system-time ordinal an array write stamps on its tile versions.
//!
//! Every array tile version is keyed by `(hilbert_prefix, ordinal)`. The
//! ordinal comes from the clock documents, edges and timeseries batches stamp
//! from, so one system-time cut splits array versions as it splits every
//! other engine's.
//!
//! The base ordinal is the Calvin transaction's, else the one the write's
//! WAL record carries (`ApplyScope::replicated_ordinal`): the replicated
//! entry's leader assigned it, or the local append reserved it, and replay
//! reads it from the record header. A Calvin epoch apply with neither
//! takes the last ordinal of the epoch's millisecond, which every replica
//! derives alike. Any other apply
//! takes a fresh ordinal of the shared clock, above every cut the clock
//! folded in. The base is raised one above the newest version the array
//! holds under the write's placement vShard when that version sits at or
//! above it, so a new version always sorts after every version of its cell.
//! A cell's versions all sit under its placement, and one placement's writes
//! apply in the same order on every replica, so every replica raises alike.
//! The stamp folds into the shared clock.
//!
//! The cells a RESTORE re-issued (`crate::wal::ArrayRestoreRedo`) carry the
//! backed-up stamp of every version, and the stamp of every version the
//! resolve set again. The install keeps each carried stamp, so an `AS OF`
//! read after the restore sees what it saw before the backup, and every
//! replica and replay installs the same versions. The highest carried stamp
//! folds into the shared clock.

use nodedb_array::types::ArrayId;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::bitemporal_time::raised_above;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};

impl CoreLoop {
    /// The system-time ordinal one write to `array_id` placed on `placement`
    /// stamps on every cell it lands (see the module doc).
    ///
    /// `Err` when the array is not open on this core.
    pub(in crate::data::executor) fn array_version_ordinal(
        &self,
        array_id: &ArrayId,
        placement: u32,
    ) -> crate::Result<i64> {
        let newest = self
            .array_engine
            .store(array_id)?
            .newest_version_stamp_at(placement);
        let base = match (
            self.apply_scope.calvin_txn_ordinal,
            self.apply_scope.replicated_ordinal,
            self.epoch_system_ms,
        ) {
            (Some(ordinal), _, _) | (None, Some(ordinal), _) => ordinal,
            (None, None, Some(epoch_ms)) => nodedb_types::ms_to_ordinal_upper(epoch_ms),
            (None, None, None) => self.hlc.next_ordinal(),
        };
        let ordinal = raised_above(base, newest);
        self.observe_bitemporal_ordinal(ordinal);
        Ok(ordinal)
    }

    /// Fold the newest version `array_id` holds into the shared clock, so a
    /// cut taken after the array opens covers every stored version. Called
    /// once a store opens. An array not open folds nothing.
    pub(in crate::data::executor) fn fold_array_versions(&self, array_id: &ArrayId) {
        if let Some(newest) = self
            .array_engine
            .store(array_id)
            .ok()
            .and_then(|store| store.newest_version_stamp())
        {
            self.observe_bitemporal_ordinal(newest);
        }
    }

    /// Fold the highest stamp of restored `puts` and `deletes` into the
    /// shared clock, so a later write stamps above every restored version.
    /// Each restored version keeps its carried stamp (see the module doc).
    pub(in crate::data::executor) fn fold_restored_array_stamps(
        &self,
        puts: &[ArrayPutCell],
        deletes: &[ArrayDeleteCell],
    ) {
        let highest = puts
            .iter()
            .map(|cell| cell.system_from_ms)
            .chain(deletes.iter().map(|cell| cell.system_from_ms))
            .max();
        if let Some(highest) = highest {
            self.observe_bitemporal_ordinal(highest);
        }
    }

    /// Stamp every put cell of a write placed on `placement` with this
    /// write's ordinal. Returns the ordinal.
    pub(in crate::data::executor) fn stamp_array_puts(
        &self,
        array_id: &ArrayId,
        placement: u32,
        cells: &mut [ArrayPutCell],
    ) -> crate::Result<i64> {
        let ordinal = self.array_version_ordinal(array_id, placement)?;
        for cell in cells {
            cell.system_from_ms = ordinal;
        }
        Ok(ordinal)
    }

    /// Stamp every tombstone or erasure of a write placed on `placement`
    /// with this write's ordinal. Returns the ordinal.
    pub(in crate::data::executor) fn stamp_array_deletes(
        &self,
        array_id: &ArrayId,
        placement: u32,
        cells: &mut [ArrayDeleteCell],
    ) -> crate::Result<i64> {
        let ordinal = self.array_version_ordinal(array_id, placement)?;
        for cell in cells {
            cell.system_from_ms = ordinal;
        }
        Ok(ordinal)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::{Surrogate, TenantId};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
        Arc::new(
            ArraySchemaBuilder::new("vs")
                .dim(DimSpec::new(
                    "x",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
                ))
                .attr(AttrSpec::new("v", AttrType::Int64, true))
                .tile_extents(vec![10])
                .build()
                .expect("schema"),
        )
    }

    fn cell(x: i64, stamp: i64) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(x)],
            surrogate: Surrogate::new(x as u32 + 1),
            system_from_ms: stamp,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    /// A write stamps a fresh ordinal of the shared clock, ignoring the
    /// carried value. A Calvin ordinal below a stored version rises one
    /// above it, so the new version sorts after the stored one.
    #[test]
    fn a_write_stamps_above_every_stored_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let aid = ArrayId::new(TenantId::new(1), "vs");
        core.array_engine
            .open_array(aid.clone(), schema(), 0x5)
            .expect("open");

        let mut first = vec![cell(1, 7)];
        let stamp = core.stamp_array_puts(&aid, 0, &mut first).expect("stamp");
        assert_ne!(stamp, 7, "the carried value never decides the version");
        assert_eq!(first[0].system_from_ms, stamp);
        core.array_engine
            .put_cells(&aid, 0, first, 1)
            .expect("put first");

        core.apply_scope.calvin_txn_ordinal = Some(stamp - 1_000);
        let mut second = vec![cell(2, 0)];
        let raised = core.stamp_array_puts(&aid, 0, &mut second).expect("stamp");
        assert_eq!(raised, stamp + 1);
        core.apply_scope.calvin_txn_ordinal = None;
        assert!(core.bitemporal_ordinal() > raised, "the stamp folded in");
    }

    /// A version under another placement never raises a write: each
    /// replica interleaves two placements' applies in its own order.
    #[test]
    fn a_write_raises_only_above_its_own_placement() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let aid = ArrayId::new(TenantId::new(1), "vs");
        core.array_engine
            .open_array(aid.clone(), schema(), 0x5)
            .expect("open");
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        core.array_engine
            .put_cells(&aid, 5, vec![cell(1, ahead)], 1)
            .expect("put on placement 5");

        let base = ahead - 1_000;
        core.apply_scope.replicated_ordinal = Some(base);
        let mut other = vec![cell(2, 0)];
        let stamp = core
            .stamp_array_puts(&aid, 6, &mut other)
            .expect("stamp on placement 6");
        assert_eq!(stamp, base, "placement 6 holds no version to raise above");
        let mut same = vec![cell(1, 0)];
        let raised = core
            .stamp_array_puts(&aid, 5, &mut same)
            .expect("stamp on placement 5");
        assert_eq!(
            raised,
            ahead + 1,
            "placement 5 raises above its own version"
        );
        core.apply_scope.replicated_ordinal = None;
    }

    /// Restored stamps fold into the clock: a later write stamps above the
    /// highest of them.
    #[test]
    fn restored_stamps_fold_into_the_clock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let aid = ArrayId::new(TenantId::new(1), "vs");
        core.array_engine
            .open_array(aid.clone(), schema(), 0x5)
            .expect("open");
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);

        let tombstones = vec![ArrayDeleteCell {
            coord: vec![CoordValue::Int64(1)],
            system_from_ms: ahead,
            erasure: false,
        }];
        core.fold_restored_array_stamps(&[cell(1, 70)], &tombstones);
        assert!(core.hlc.peek() >= ahead, "the highest stamp folded in");

        let mut live = vec![cell(3, 70)];
        core.stamp_array_puts(&aid, 0, &mut live).expect("stamp");
        assert!(live[0].system_from_ms > ahead, "a live write stamps fresh");
    }

    /// Opening an array whose versions sit ahead of the clock folds them in,
    /// so a later cut covers them.
    #[test]
    fn an_opened_array_folds_its_newest_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let aid = ArrayId::new(TenantId::new(1), "vs");
        core.array_engine
            .open_array(aid.clone(), schema(), 0x5)
            .expect("open");
        let ahead = core.hlc.peek().saturating_add(9_000_000_000_000);
        core.array_engine
            .put_cells(&aid, 0, vec![cell(1, ahead)], 1)
            .expect("put");
        core.fold_array_versions(&aid);
        assert!(core.hlc.peek() >= ahead);
    }
}
