// SPDX-License-Identifier: BUSL-1.1

//! Net changes of engines whose change events name a whole collection:
//! columnar and spatial collections, arrays and timeseries collections.
//!
//! Their autocommit statements publish one `*` event per kind of change, so
//! a transaction publishes one `*` entry per kind of net change its rows
//! hold, in insert, update, delete order. Each row still collapses to its own
//! net kind first: a row the transaction inserted and deleted adds no kind.

use std::collections::BTreeMap;

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{ArrayOp, PhysicalPlan, TimeseriesOp};

use super::super::columnar_image::ColumnarCollections;
use super::entry::ResolveScope;
use super::keyed::push_truncate;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::array::engine::ArrayEngineError;
use crate::types::TenantId;
use crate::wal::{EVERY_ROW, RedoRowChange, RedoRowKind};

/// Which kinds of net change one collection's rows hold.
#[derive(Default)]
struct KindsPresent {
    insert: bool,
    update: bool,
    delete: bool,
}

impl KindsPresent {
    fn note(&mut self, kind: RedoRowKind) {
        match kind {
            RedoRowKind::Insert => self.insert = true,
            RedoRowKind::Update => self.update = true,
            RedoRowKind::Delete => self.delete = true,
            RedoRowKind::NoChange => {}
        }
    }

    /// One `*` entry per kind present, in insert, update, delete order.
    fn push(self, collection: &str, changes: &mut Vec<RedoRowChange>) {
        let kinds = [
            (self.insert, RedoRowKind::Insert),
            (self.update, RedoRowKind::Update),
            (self.delete, RedoRowKind::Delete),
        ];
        for (present, kind) in kinds {
            if present {
                changes.push(RedoRowChange {
                    collection: collection.to_owned(),
                    row: EVERY_ROW.to_owned(),
                    kind,
                });
            }
        }
    }
}

impl CoreLoop {
    /// Columnar and spatial collections. A staged row displaced a committed
    /// row when the overlay recorded that row's primary key.
    pub(in crate::data::executor) fn columnar_row_changes(
        &self,
        scope: ResolveScope,
        collections: &ColumnarCollections,
        changes: &mut Vec<RedoRowChange>,
    ) {
        let Some(overlay) = self.txn_overlays.get(&scope.txn_id) else {
            return;
        };
        for collection in collections.keys() {
            let key = (
                scope.database_id,
                TenantId::new(scope.tid),
                collection.clone(),
            );
            let truncated = overlay.is_truncated(&key);
            push_truncate(collection, truncated, changes);
            let mut kinds = KindsPresent::default();
            for (surrogate, staged) in overlay.iter_for_collection(&key) {
                let existed = !truncated
                    && overlay
                        .base_pk(&key, surrogate)
                        .is_some_and(|pk| !pk.is_empty());
                kinds.note(RedoRowKind::net(existed, matches!(staged, Staged::Put(_))));
            }
            kinds.push(collection, changes);
        }
    }

    /// Arrays, named as every row change names its collection: qualified for
    /// the array's database. A cell existed when the array's committed state
    /// holds it. An array this core never opened holds no cell.
    pub(in crate::data::executor) fn array_row_changes(
        &self,
        scope: ResolveScope,
        plans: &[PhysicalPlan],
        changes: &mut Vec<RedoRowChange>,
    ) -> crate::Result<()> {
        let mut arrays: BTreeMap<String, &ArrayId> = BTreeMap::new();
        for plan in plans {
            if let PhysicalPlan::Array(
                ArrayOp::Put { array_id, .. } | ArrayOp::Delete { array_id, .. },
            ) = plan
            {
                let name =
                    nodedb_types::QualifiedCollection::new(array_id.database_id, &array_id.name);
                arrays.insert(name.as_str().to_owned(), array_id);
            }
        }
        let Some(overlay) = self.array_txn_overlays.get(&scope.txn_id) else {
            return Ok(());
        };
        for (name, array_id) in arrays {
            let mut kinds = KindsPresent::default();
            for (coord, _put) in overlay.staged_cells(array_id) {
                let existed = self.array_cell_exists(array_id, coord)?;
                kinds.note(RedoRowKind::net(existed, true));
            }
            for coord in overlay.tombstoned_coords(array_id) {
                let existed = self.array_cell_exists(array_id, coord)?;
                kinds.note(RedoRowKind::net(existed, false));
            }
            kinds.push(&name, changes);
        }
        Ok(())
    }

    /// Whether `array_id`'s committed state holds a live cell at `coord`.
    fn array_cell_exists(
        &self,
        array_id: &ArrayId,
        coord: &[nodedb_array::types::coord::value::CoordValue],
    ) -> crate::Result<bool> {
        match self.array_engine.contains_cell(array_id, coord) {
            Ok(present) => Ok(present),
            Err(ArrayEngineError::UnknownArray(_)) => Ok(false),
            Err(error) => Err(crate::Error::Internal {
                detail: format!("array resolve: cell lookup in '{}': {error}", array_id.name),
            }),
        }
    }
}

/// Timeseries collections. Timeseries rows are append-only: an ingest only
/// inserts rows and never updates or deletes one, so every ingest is an
/// insert. A TRUNCATE deletes every row. Entries follow plan order, one per
/// collection and kind.
pub(super) fn timeseries_row_changes(plans: &[PhysicalPlan], changes: &mut Vec<RedoRowChange>) {
    let mut seen: Vec<(String, RedoRowKind)> = Vec::new();
    for plan in plans {
        let (collection, kind) = match plan {
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
                (collection.to_string(), RedoRowKind::Insert)
            }
            PhysicalPlan::Timeseries(TimeseriesOp::Truncate { collection, .. }) => {
                (collection.to_string(), RedoRowKind::Delete)
            }
            _ => continue,
        };
        if seen.contains(&(collection.clone(), kind)) {
            continue;
        }
        seen.push((collection.clone(), kind));
        changes.push(RedoRowChange {
            collection,
            row: EVERY_ROW.to_owned(),
            kind,
        });
    }
}
