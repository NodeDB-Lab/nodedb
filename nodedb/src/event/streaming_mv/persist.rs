// SPDX-License-Identifier: BUSL-1.1

//! Streaming MV state persistence: periodic flush to redb + restore on startup.
//!
//! MvState is primarily in-memory for O(1) access. This module persists
//! snapshots to redb periodically (every 30s) so state survives restarts.
//! On startup, the registry restores state from the latest snapshot.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use super::registry::MvRegistry;
use super::state::GroupState;
use crate::types::DatabaseId;

/// redb table: "v2:{database_id}:{tenant_id}:{mv_name}" → MessagePack-serialized MvSnapshot.
const MV_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("mv_state");
/// redb table: the one row `APPLIED_ROW` → MessagePack list of applied
/// event keys, written in the same transaction as the view states.
const MV_APPLIED: TableDefinition<&str, &[u8]> = TableDefinition::new("mv_applied");
const APPLIED_ROW: &str = "applied";

/// Serialized MV state: Vec of (group_key, per-aggregate GroupState list).
pub type MvSnapshot = Vec<(String, Vec<GroupState>)>;

/// How often to persist MV state to redb.
const PERSIST_INTERVAL: Duration = Duration::from_secs(30);

fn state_key(database_id: DatabaseId, tenant_id: u64, mv_name: &str) -> String {
    format!("v2:{}:{tenant_id}:{mv_name}", database_id.as_u64())
}

/// Manages persistence of streaming MV state.
pub struct MvPersistence {
    db: Database,
    /// Generation of the applied keys last persisted. `u64::MAX` before the
    /// first flush.
    flushed_generation: std::sync::atomic::AtomicU64,
}

impl crate::storage::RedbBacked for MvPersistence {
    fn redb_database(&self) -> &redb::Database {
        &self.db
    }
}

impl MvPersistence {
    /// Open or create the MV state store.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("mv_state.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open mv_state db {}: {e}", path.display()),
        })?;

        // Ensure table exists.
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            txn.open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.open_table(MV_APPLIED)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        Ok(Self {
            db,
            flushed_generation: std::sync::atomic::AtomicU64::new(u64::MAX),
        })
    }

    /// Persist a single MV's state snapshot.
    pub fn save(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
        snapshot: &[(String, Vec<GroupState>)],
    ) -> crate::Result<()> {
        let key = state_key(database_id, tenant_id, mv_name);
        let bytes = zerompk::to_msgpack_vec(&snapshot.to_vec()).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("mv_state: {e}"),
            }
        })?;

        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;

        Ok(())
    }

    /// Load a persisted MV state snapshot.
    pub fn load(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
    ) -> crate::Result<Option<MvSnapshot>> {
        let key = state_key(database_id, tenant_id, mv_name);
        let txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_read: {e}"),
        })?;
        let table = txn
            .open_table(MV_STATE)
            .map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("open_table: {e}"),
            })?;

        match table.get(key.as_str()) {
            Ok(Some(guard)) => {
                let bytes: &[u8] = guard.value();
                let snapshot: MvSnapshot =
                    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("mv_state restore: {e}"),
                    })?;
                Ok(Some(snapshot))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("get mv_state: {e}"),
            }),
        }
    }

    /// Delete persisted state for a dropped MV.
    pub fn delete(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        mv_name: &str,
    ) -> crate::Result<()> {
        let key = state_key(database_id, tenant_id, mv_name);
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(MV_STATE)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            let _ = table.remove(key.as_str());
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Persist every view's state and the applied event keys in one redb
    /// transaction, holding off every apply meanwhile, so the persisted views
    /// and keys agree. Nothing is written when no event applied since the
    /// last flush.
    pub fn flush_all(&self, registry: &MvRegistry) -> crate::Result<()> {
        use std::sync::atomic::Ordering;
        let storage = |e: &dyn std::fmt::Display| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("mv flush: {e}"),
        };
        registry.applied().with_consistent(|keys, generation| {
            if self.flushed_generation.load(Ordering::Acquire) == generation {
                return Ok(());
            }
            let applied: Vec<Vec<u8>> = keys.iter().map(|key| key.to_bytes()).collect();
            let applied_bytes =
                zerompk::to_msgpack_vec(&applied).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("mv applied keys: {e}"),
                })?;
            let txn = self.db.begin_write().map_err(|e| storage(&e))?;
            {
                let mut states = txn.open_table(MV_STATE).map_err(|e| storage(&e))?;
                for mv_def in registry.list_all() {
                    let Some(state) =
                        registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name)
                    else {
                        continue;
                    };
                    let snapshot = state.snapshot();
                    if snapshot.is_empty() {
                        continue;
                    }
                    let bytes = zerompk::to_msgpack_vec(&snapshot).map_err(|e| {
                        crate::Error::Serialization {
                            format: "msgpack".into(),
                            detail: format!("mv_state: {e}"),
                        }
                    })?;
                    let key = state_key(mv_def.database_id, mv_def.tenant_id, &mv_def.name);
                    states
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(|e| storage(&e))?;
                }
                let mut applied_table = txn.open_table(MV_APPLIED).map_err(|e| storage(&e))?;
                applied_table
                    .insert(APPLIED_ROW, applied_bytes.as_slice())
                    .map_err(|e| storage(&e))?;
            }
            txn.commit().map_err(|e| storage(&e))?;
            self.flushed_generation.store(generation, Ordering::Release);
            Ok(())
        })
    }

    /// The applied event keys persisted with the view states.
    fn load_applied(&self) -> crate::Result<Vec<crate::event::sink_ledger::SinkEventKey>> {
        let storage = |e: &dyn std::fmt::Display| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("mv applied keys: {e}"),
        };
        let txn = self.db.begin_read().map_err(|e| storage(&e))?;
        let table = txn.open_table(MV_APPLIED).map_err(|e| storage(&e))?;
        let Some(guard) = table.get(APPLIED_ROW).map_err(|e| storage(&e))? else {
            return Ok(Vec::new());
        };
        let applied: Vec<Vec<u8>> =
            zerompk::from_msgpack(guard.value()).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("mv applied keys: {e}"),
            })?;
        applied
            .iter()
            .map(|bytes| {
                crate::event::sink_ledger::SinkEventKey::from_bytes(bytes).ok_or_else(|| {
                    crate::Error::Serialization {
                        format: "mv applied key".into(),
                        detail: "a persisted key did not decode".into(),
                    }
                })
            })
            .collect()
    }

    /// Restore all MV states and their applied event keys from redb into the
    /// registry. A view whose state cannot be read, or keys that cannot be
    /// read, fail the restore: applying events on top of a partial restore
    /// would count some of them twice.
    pub fn restore_all(&self, registry: &MvRegistry) -> crate::Result<()> {
        let mut restored = 0u32;
        for mv_def in registry.list_all() {
            match self.load(mv_def.database_id, mv_def.tenant_id, &mv_def.name) {
                Ok(Some(snapshot)) if !snapshot.is_empty() => {
                    if let Some(state) =
                        registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name)
                    {
                        state.restore(snapshot);
                        restored += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        if restored > 0 {
            info!(restored, "restored streaming MV states from redb");
        }
        registry.applied().restore(self.load_applied()?);
        Ok(())
    }
}

/// Spawn the background persistence task.
pub fn spawn_persist_task(
    persistence: Arc<MvPersistence>,
    registry: Arc<MvRegistry>,
    watermark_tracker: Arc<crate::event::watermark_tracker::WatermarkTracker>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!("MV persistence task started");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(PERSIST_INTERVAL) => {
                    // Finalize time buckets using the global watermark event_time.
                    // This is min(per_partition_event_times) — the wall-clock time
                    // below which ALL partitions have advanced. Groups with
                    // latest_event_time < this value will receive no more events.
                    let cutoff = watermark_tracker.global_watermark_event_time();

                    if cutoff > 0 {
                        let mut total_finalized = 0u32;
                        for mv_def in registry.list_all() {
                            if let Some(state) = registry.get_state(mv_def.database_id, mv_def.tenant_id, &mv_def.name) {
                                total_finalized += state.finalize_buckets(cutoff);
                            }
                        }
                        if total_finalized > 0 {
                            registry.applied().touch();
                            debug!(
                                finalized = total_finalized,
                                cutoff_ms = cutoff,
                                "MV time buckets finalized via global watermark event_time"
                            );
                        }
                    }

                    // Persist state to redb.
                    match persistence.flush_all(&registry) {
                        Ok(()) => trace!("MV state flushed to redb"),
                        Err(e) => warn!(error = %e, "failed to persist MV state"),
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        if let Err(e) = persistence.flush_all(&registry) {
                            warn!(error = %e, "failed to persist MV state on shutdown");
                        }
                        debug!("MV persistence task: final flush on shutdown");
                        return;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::event::streaming_mv::state::{AggInput, MvState};
    use crate::event::streaming_mv::types::{AggDef, AggFunction};
    use nodedb_types::Value;

    /// A group state that took `values` for `func`.
    fn state_of(func: AggFunction, values: &[i64]) -> GroupState {
        let mut state = GroupState::default();
        for v in values {
            state.update(func, &AggInput::Value(Value::Integer(*v)));
        }
        state
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();

        let snapshot = vec![
            (
                "INSERT".to_string(),
                vec![state_of(AggFunction::Sum, &[10, 20, 30, 40])],
            ),
            (
                "UPDATE".to_string(),
                vec![state_of(AggFunction::Sum, &[5, 15])],
            ),
        ];

        persist
            .save(DatabaseId::new(1), 1, "order_stats", &snapshot)
            .unwrap();

        let loaded = persist
            .load(DatabaseId::new(1), 1, "order_stats")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, snapshot);
    }

    /// Exact state survives a save, a fresh open of the store, and a restore:
    /// the restored view reads the same exact values it held before.
    #[test]
    fn restart_round_trip_keeps_exact_values() {
        const ABOVE: i64 = 9_007_199_254_740_993;
        const AT: i64 = 9_007_199_254_740_992;
        let aggregates = vec![
            AggDef {
                output_name: "s".into(),
                function: AggFunction::Sum,
                input_expr: "v".into(),
            },
            AggDef {
                output_name: "lo".into(),
                function: AggFunction::Min,
                input_expr: "v".into(),
            },
            AggDef {
                output_name: "hi".into(),
                function: AggFunction::Max,
                input_expr: "v".into(),
            },
            AggDef {
                output_name: "mean".into(),
                function: AggFunction::Avg,
                input_expr: "v".into(),
            },
        ];
        let before = MvState::new("m".into(), Vec::new(), aggregates.clone());
        for v in [
            Value::Integer(ABOVE),
            Value::Integer(AT),
            Value::Integer(1_700_000_000_000_000_001),
            Value::from_u64(u64::MAX),
        ] {
            let input = AggInput::Value(v);
            before.update_with_time("", &vec![input; 4], 1);
        }
        let expected = before.read_results().unwrap();

        let dir = tempfile::tempdir().unwrap();
        {
            let persist = MvPersistence::open(dir.path()).unwrap();
            persist
                .save(DatabaseId::new(1), 1, "m", &before.snapshot())
                .unwrap();
        }
        let persist = MvPersistence::open(dir.path()).unwrap();
        let after = MvState::new("m".into(), Vec::new(), aggregates);
        after.restore(persist.load(DatabaseId::new(1), 1, "m").unwrap().unwrap());

        assert_eq!(after.read_results().unwrap(), expected);
        assert_eq!(
            expected[0].1[0].1,
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                i128::from(ABOVE)
                    + i128::from(AT)
                    + 1_700_000_000_000_000_001
                    + i128::from(u64::MAX),
                0
            ))
        );
        assert_eq!(expected[0].1[1].1, Value::Integer(AT));
        assert_eq!(expected[0].1[2].1, Value::from_u64(u64::MAX));
    }

    #[test]
    fn load_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();
        assert!(
            persist
                .load(DatabaseId::new(1), 1, "nonexistent")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn delete_removes_state() {
        let dir = tempfile::tempdir().unwrap();
        let persist = MvPersistence::open(dir.path()).unwrap();

        let snapshot = vec![("k".to_string(), vec![GroupState::default()])];
        persist
            .save(DatabaseId::new(1), 1, "mv1", &snapshot)
            .unwrap();
        persist.delete(DatabaseId::new(1), 1, "mv1").unwrap();
        assert!(
            persist
                .load(DatabaseId::new(1), 1, "mv1")
                .unwrap()
                .is_none()
        );
    }
}
