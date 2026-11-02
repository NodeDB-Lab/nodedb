// SPDX-License-Identifier: BUSL-1.1

//! The memory charge of plain columnar row-version history.
//!
//! Every plain columnar and spatial collection keeps row-version stamps and
//! per-key chains beside its segments (`MutationEngine::version_memory_bytes`).
//! Those bytes are charged to the governor's columnar budget under the
//! collection's database and tenant. The charge follows the bytes: every
//! columnar write, every committed redo that wrote a columnar collection,
//! boot replay, and every history purge recharge it. A collection that left
//! the core releases its charge.

use nodedb_mem::EngineId;
use nodedb_physical::physical_plan::ColumnarOp;

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};

/// One columnar collection on a core.
pub(in crate::data::executor) type ColumnarKey = (DatabaseId, TenantId, String);

impl super::CoreLoop {
    /// Bring the history charge of the collection a dispatched columnar
    /// write `op` names in line with its versions. A read changes nothing.
    pub(in crate::data::executor) fn recharge_columnar_written(
        &mut self,
        task: &ExecutionTask,
        op: &ColumnarOp,
    ) {
        let written = match op {
            ColumnarOp::Insert { collection, .. }
            | ColumnarOp::Update { collection, .. }
            | ColumnarOp::Delete { collection, .. }
            | ColumnarOp::ResolvedUpdate { collection, .. }
            | ColumnarOp::ResolvedDelete { collection, .. }
            | ColumnarOp::ResolveDml { collection, .. }
            | ColumnarOp::Truncate { collection, .. } => Some(collection),
            ColumnarOp::Scan { .. } | ColumnarOp::MaterializeScan { .. } => None,
        };
        if let Some(collection) = written {
            self.recharge_columnar_history(&(
                task.request.database_id,
                task.request.tenant_id,
                collection.as_str().to_string(),
            ));
        }
    }

    /// Bring `key`'s history charge in line with its stored versions. A
    /// collection no engine holds releases its charge.
    pub(in crate::data::executor) fn recharge_columnar_history(&mut self, key: &ColumnarKey) {
        let bytes = self
            .columnar_engines
            .get(key)
            .map_or(0, |engine| engine.version_memory_bytes());
        let charges = &mut self.maintenance.version_purge.columnar_history;
        if charges
            .get(key)
            .is_some_and(|(charged, _)| *charged == bytes)
        {
            return;
        }
        // Release the prior charge first, so a recharge never counts the
        // history twice.
        charges.remove(key);
        if bytes == 0 {
            return;
        }
        let token = self
            .governor
            .charge(key.0, key.1, EngineId::Columnar, bytes);
        self.maintenance
            .version_purge
            .columnar_history
            .insert(key.clone(), (bytes, token));
    }

    /// Recharge every columnar collection on this core, and release the
    /// charge of every collection that left it. Boot runs it once replay
    /// ends.
    pub(in crate::data) fn recharge_all_columnar_history(&mut self) {
        let mut keys: Vec<ColumnarKey> = self.columnar_engines.keys().cloned().collect();
        keys.extend(
            self.maintenance
                .version_purge
                .columnar_history
                .keys()
                .filter(|key| !self.columnar_engines.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>(),
        );
        for key in keys {
            self.recharge_columnar_history(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::MutationEngine;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    fn key() -> ColumnarKey {
        (DatabaseId::DEFAULT, TenantId::new(1), "hist".to_string())
    }

    fn charged(core: &super::super::CoreLoop) -> usize {
        core.maintenance
            .version_purge
            .columnar_history
            .get(&key())
            .map_or(0, |(bytes, _)| *bytes)
    }

    /// Overwrites raise the charge, a purge lowers it to what stays, and a
    /// dropped collection releases it.
    #[test]
    fn the_charge_follows_the_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("hist".to_string(), schema);
        for ordinal in 1..=20 {
            engine
                .insert(&[Value::Integer(1), Value::Integer(ordinal)], ordinal)
                .expect("overwrite");
        }
        core.columnar_engines.insert(key(), engine);
        core.recharge_columnar_history(&key());
        let grown = charged(&core);
        assert!(grown > 0);
        let allocated = core.governor.budget(EngineId::Columnar).allocated();
        assert!(allocated >= grown);

        core.purge_columnar_versions_below(key().0, key().1, "hist", 20);
        assert!(
            charged(&core) < grown,
            "the purge releases the removed versions"
        );

        core.columnar_engines.remove(&key());
        core.recharge_all_columnar_history();
        assert_eq!(
            charged(&core),
            0,
            "a dropped collection releases its charge"
        );
    }
}
