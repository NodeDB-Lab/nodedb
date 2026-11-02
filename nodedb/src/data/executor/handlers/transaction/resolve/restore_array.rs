// SPDX-License-Identifier: BUSL-1.1

//! Resolve the restored cells of one array into their redo sub-record.
//!
//! A RESTORE re-issues every backed-up cell version at its own system time.
//! It is a write in the Calvin sequence, and it erases no history. A
//! destination coordinate can hold a version or a delete above the backup's
//! newest one: a write after the backup that `FORCE` overrode, or a delete
//! sequenced before the RESTORE. The resolve reads the destination at the
//! transaction's turn, before any version of the batch installs. For each
//! such coordinate it adds the backup's newest version once more, stamped
//! one above the destination's newest version and at least the
//! transaction's ordinal. A backed-up tombstone or erasure is not added again
//! when the destination already holds the coordinate absent. The current
//! read then answers the backup's state. A read at a cut below that stamp
//! answers as the destination held it. The sub-record carries every stamp,
//! so every replica and every replay installs the same versions.

use std::collections::BTreeMap;

use nodedb_array::types::coord::value::CoordValue;
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan, RestoredArrayCells};
use nodedb_wal::record::RecordType;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::wal_replay::ArrayOpen;
use crate::engine::array::store::ArrayStore;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::wal::{ArrayRestoreRedo, RedoSubRecord};

use super::restore_reassert::RestoreScope;

/// The newest backed-up version of one restored coordinate.
enum NewestCell<'a> {
    Put(&'a ArrayPutCell),
    Delete(&'a ArrayDeleteCell),
}

impl NewestCell<'_> {
    fn stamp(&self) -> i64 {
        match self {
            Self::Put(put) => put.system_from_ms,
            Self::Delete(delete) => delete.system_from_ms,
        }
    }

    fn coord(&self) -> &Vec<CoordValue> {
        match self {
            Self::Put(put) => &put.coord,
            Self::Delete(delete) => &delete.coord,
        }
    }
}

fn array_error(array: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Internal {
        detail: format!("restore resolve: read the destination cells of array '{array}': {e}"),
    }
}

fn encode_error(array: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore resolve: encode a coordinate of array '{array}': {e}"),
    }
}

/// Keep `candidate` in `newest` when it is the newest version of its
/// coordinate seen so far.
fn keep_newest<'a>(
    newest: &mut BTreeMap<Vec<u8>, NewestCell<'a>>,
    array: &str,
    candidate: NewestCell<'a>,
) -> crate::Result<()> {
    let key = zerompk::to_msgpack_vec(candidate.coord()).map_err(|e| encode_error(array, e))?;
    let newer = newest
        .get(&key)
        .is_none_or(|held| held.stamp() <= candidate.stamp());
    if newer {
        newest.insert(key, candidate);
    }
    Ok(())
}

/// The versions that set each coordinate of `redo` to its newest backed-up
/// version again, read against `store`, the destination (see the module
/// doc).
fn reasserted(
    store: &ArrayStore,
    redo: &ArrayRestoreRedo,
    scope: RestoreScope,
) -> crate::Result<(Vec<ArrayPutCell>, Vec<ArrayDeleteCell>)> {
    let array = redo.array_id.name.as_str();
    let mut newest: BTreeMap<Vec<u8>, NewestCell<'_>> = BTreeMap::new();
    for put in &redo.puts {
        keep_newest(&mut newest, array, NewestCell::Put(put))?;
    }
    for delete in &redo.deletes {
        keep_newest(&mut newest, array, NewestCell::Delete(delete))?;
    }
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    for version in newest.into_values() {
        let dest_newest = store
            .newest_cell_stamp(version.coord())
            .map_err(|e| array_error(array, e))?;
        let Some(stamp) = scope.reassert_stamp(version.stamp(), dest_newest) else {
            continue;
        };
        match version {
            NewestCell::Put(put) => puts.push(ArrayPutCell {
                system_from_ms: stamp,
                ..put.clone()
            }),
            NewestCell::Delete(delete) => {
                // A coordinate the destination already holds absent stays
                // as it is.
                let live = store
                    .contains_cell(&delete.coord)
                    .map_err(|e| array_error(array, e))?;
                if live {
                    deletes.push(ArrayDeleteCell {
                        system_from_ms: stamp,
                        ..delete.clone()
                    });
                }
            }
        }
    }
    Ok((puts, deletes))
}

impl CoreLoop {
    /// Open on this core every array a RESTORE batch among `plans` restores
    /// cells of, so the resolve reads its destination versions. An array
    /// this core has not opened since it booted opens from the catalog.
    ///
    /// `Err` when the catalog holds no such array, or it cannot be opened.
    pub(super) fn open_restored_arrays(&mut self, plans: &[PhysicalPlan]) -> crate::Result<()> {
        for plan in plans {
            let PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) = plan else {
                continue;
            };
            for cells in &batch.arrays {
                match self.ensure_array_open_for_replay(&cells.array_id)? {
                    ArrayOpen::Ready => {}
                    ArrayOpen::NoCatalogEntry => {
                        return Err(crate::Error::Internal {
                            detail: format!(
                                "restore resolve: array '{}' has no catalog entry on this node",
                                cells.array_id.name
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// The sub-record of the restored cells `cells`: every backed-up version,
    /// and the versions that set the backup's state again above the
    /// destination's newer ones.
    ///
    /// `Err` when the payload is no restored array, names another array
    /// than `cells`, or the array is not open on this core.
    pub(super) fn resolve_restored_array(
        &self,
        scope: RestoreScope,
        cells: &RestoredArrayCells,
    ) -> crate::Result<RedoSubRecord> {
        let mut redo =
            ArrayRestoreRedo::decode(&cells.redo).ok_or_else(|| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore resolve: the cells of array '{}' are no restored array",
                    cells.array_id.name
                ),
            })?;
        if redo.array_id != cells.array_id {
            return Err(crate::Error::Internal {
                detail: format!(
                    "restore resolve: a batch names array '{}' and carries the cells of '{}'",
                    cells.array_id.name, redo.array_id.name
                ),
            });
        }
        let store = self.array_engine.store(&redo.array_id)?;
        let (puts, deletes) = reasserted(store, &redo, scope)?;
        redo.puts.extend(puts);
        redo.deletes.extend(deletes);
        Ok(RedoSubRecord {
            record_type: RecordType::ArrayPut as u32,
            payload: redo.encode()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::{Surrogate, TenantId};

    use super::*;
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::types::DatabaseId;

    fn schema() -> Arc<nodedb_array::schema::ArraySchema> {
        Arc::new(
            ArraySchemaBuilder::new("grid")
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

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "grid")
    }

    fn put(x: i64, v: i64, stamp: i64) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x)],
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::new(x as u32 + 1),
            system_from_ms: stamp,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn tombstone(x: i64, stamp: i64) -> ArrayDeleteCell {
        ArrayDeleteCell {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: stamp,
            erasure: false,
        }
    }

    fn engine(dir: &std::path::Path) -> ArrayEngine {
        let mut engine =
            ArrayEngine::new(ArrayEngineConfig::new(dir.to_path_buf())).expect("engine");
        engine.open_array(aid(), schema(), 0x5).expect("open");
        engine
    }

    /// The scope of a batch at `applied` under no RESTORE cut.
    fn at(applied: i64) -> RestoreScope {
        RestoreScope {
            database_id: DatabaseId::DEFAULT,
            tid: 1,
            applied,
            cut: i64::MAX,
        }
    }

    fn redo(puts: Vec<ArrayPutCell>, deletes: Vec<ArrayDeleteCell>) -> ArrayRestoreRedo {
        ArrayRestoreRedo {
            array_id: aid(),
            vshard_id: 0,
            puts,
            deletes,
        }
    }

    /// A coordinate the destination deleted after the backup gets the
    /// backup's newest value once more, above the tombstone and at least the
    /// transaction's ordinal. A coordinate the destination holds nothing
    /// newer for gets nothing.
    #[test]
    fn a_newer_destination_tombstone_gets_the_backups_value_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut engine = engine(dir.path());
        engine
            .delete_cells(&aid(), 0, vec![tombstone(1, 9_000)], 1)
            .expect("delete");
        let store = engine.store(&aid()).expect("store");
        let restored = redo(
            vec![put(1, 6, 100), put(1, 7, 200), put(2, 8, 100)],
            Vec::new(),
        );
        let (puts, deletes) = reasserted(store, &restored, at(5_000)).expect("resolve");
        assert_eq!(puts, vec![put(1, 7, 9_001)]);
        assert!(deletes.is_empty());

        let (puts, _) = reasserted(store, &restored, at(20_000)).expect("resolve");
        assert_eq!(puts, vec![put(1, 7, 20_000)], "never below the ordinal");
    }

    /// A cell the destination wrote after the RESTORE's cut stays current:
    /// the RESTORE took effect before it.
    #[test]
    fn a_cell_written_after_the_restore_cut_stays_current() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut engine = engine(dir.path());
        engine
            .delete_cells(&aid(), 0, vec![tombstone(1, 9_000)], 1)
            .expect("delete");
        let store = engine.store(&aid()).expect("store");
        let restored = redo(vec![put(1, 7, 200)], Vec::new());
        let after_cut = RestoreScope {
            cut: 8_000,
            ..at(20_000)
        };
        let (puts, deletes) = reasserted(store, &restored, after_cut).expect("resolve");
        assert!(puts.is_empty(), "the later delete stays current");
        assert!(deletes.is_empty());

        let before_cut = RestoreScope {
            cut: 9_000,
            ..at(20_000)
        };
        let (puts, _) = reasserted(store, &restored, before_cut).expect("resolve");
        assert_eq!(
            puts,
            vec![put(1, 7, 20_000)],
            "an earlier delete is overwritten"
        );
    }

    /// A backed-up tombstone is set again above a newer live destination
    /// cell, and never over a coordinate the destination holds absent.
    #[test]
    fn a_backed_up_tombstone_is_set_again_only_over_a_live_cell() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut engine = engine(dir.path());
        engine
            .put_cells(&aid(), 0, vec![put(1, 9, 9_000)], 1)
            .expect("put");
        engine
            .delete_cells(&aid(), 0, vec![tombstone(2, 9_000)], 2)
            .expect("delete");
        let store = engine.store(&aid()).expect("store");
        let restored = redo(
            vec![put(1, 7, 100), put(2, 7, 100)],
            vec![tombstone(1, 200), tombstone(2, 200)],
        );
        let (puts, deletes) = reasserted(store, &restored, at(5_000)).expect("resolve");
        assert!(puts.is_empty());
        assert_eq!(deletes, vec![tombstone(1, 9_001)]);
    }

    /// A RESTORE after a destination delete: the resolve names the re-assert
    /// stamp in the record, and the install makes the backed-up value
    /// current. A read at the destination's tombstone still sees it deleted,
    /// and a read below it sees the backed-up value.
    #[test]
    fn a_restore_after_a_destination_delete_makes_the_cell_current() {
        use nodedb_array::query::ceiling::CeilingResult;
        use nodedb_wal::record::WalRecordArgs;
        use nodedb_wal::{TombstoneSet, WalRecord};

        use crate::control::array_catalog::ArrayCatalogEntry;
        use crate::data::executor::core_loop::tests::make_core_with_dir;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.array_catalog
            .write()
            .expect("catalog lock")
            .register(ArrayCatalogEntry {
                array_id: aid(),
                name: aid().name,
                schema_msgpack: zerompk::to_msgpack_vec(&*schema()).expect("encode schema"),
                schema_hash: 0x5,
                created_at_ms: 0,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
                modification_hlc: nodedb_types::Hlc::ZERO,
                incarnation: nodedb_types::Hlc::ZERO,
            })
            .expect("seed the catalog");
        // The destination held the backed-up value, then deleted the cell.
        core.array_engine
            .open_array(aid(), schema(), 0x5)
            .expect("open");
        core.array_engine
            .put_cells(&aid(), 0, vec![put(1, 7, 100)], 1)
            .expect("put");
        core.array_engine
            .delete_cells(&aid(), 0, vec![tombstone(1, 9_000)], 2)
            .expect("delete");

        let cells = RestoredArrayCells {
            array_id: aid(),
            redo: redo(vec![put(1, 7, 100)], Vec::new())
                .encode()
                .expect("encode"),
        };
        let scope = at(5_000);
        let sub = core.resolve_restored_array(scope, &cells).expect("resolve");
        let resolved = ArrayRestoreRedo::decode(&sub.payload).expect("decode");
        assert_eq!(resolved.puts, vec![put(1, 7, 100), put(1, 7, 9_001)]);

        let record = WalRecord::new(WalRecordArgs {
            record_type: sub.record_type,
            lsn: 3,
            tenant_id: 1,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            payload: sub.payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        core.replay_array_wal(&[record], 1, &TombstoneSet::new());

        let store = core.array_engine.store(&aid()).expect("store");
        let at = |cut: i64| {
            store
                .ceiling_for_coord(&[CoordValue::Int64(1)], cut, None)
                .expect("ceiling")
        };
        assert!(
            matches!(at(i64::MAX), CeilingResult::Live(ref cell) if cell.attrs == vec![CellValue::Int64(7)]),
            "the current read answers the backed-up value"
        );
        assert!(
            matches!(at(9_000), CeilingResult::Tombstoned),
            "history keeps the destination's tombstone"
        );
        assert!(matches!(at(8_999), CeilingResult::Live(_)));
    }
}
