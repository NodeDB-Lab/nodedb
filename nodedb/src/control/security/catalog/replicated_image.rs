// SPDX-License-Identifier: BUSL-1.1

//! The `_system.*` tables metadata Raft group 0 replicates, as raw rows.
//!
//! A group 0 snapshot carries exactly these tables. [`REPLICATED_TABLES`]
//! lists every table a committed metadata entry writes: the tables
//! `CatalogEntry::apply_to` writes (except `wal_tombstones` and the
//! surrogate bindings, below), the metadata-group host tables, join
//! tokens, enrollment pre-authorizations, the surrogate and database-id
//! watermarks with their reserve cursors, the sync producer registry, the
//! cluster restore points, and the scheduled backup marks.
//!
//! Every other bootstrap table stays out of the image. Each one is written
//! only by this node, or travels with a data-group snapshot:
//! - `audit_log`: this node's audit trail, numbered by a node-local sequence.
//! - `blacklist`, `orgs`, `org_members`, `scopes`: written only by the node
//!   that ran the statement. Group 0 never applies them.
//! - `lockout_state`: this node's failed-login counters.
//! - `wal_tombstones`: purge boundaries in this node's WAL LSN space. Every
//!   node records its own when it reclaims a collection. A replicated
//!   `RecordWalTombstone` (backup restore) also writes one, but another
//!   node's rows move this node's replay boundaries.
//! - `tenant_group_marks`, `tenant_group_restore_marks`: data-group write
//!   marks, carried by data-group snapshots.
//! - `calvin_applied`: this node's Calvin apply ledger.
//! - `redo_snapshot_owed`: the data-group snapshot installs this node's
//!   replicas owe.
//! - `l2_cleanup_queue`, `pending_reclaim`, `pending_history_compaction`:
//!   work this node still owes its own storage.
//! - `surrogate_pk_v3`, `surrogate_pk_rev_v3`: PK bindings of stored rows,
//!   carried by data-group snapshots. A purge apply deletes a collection's
//!   bindings; the install's reclaim of that collection does the same here.
//! - `crdt_signing_keys`, `crdt_signing_root_metadata`: key material derived
//!   from this node's WAL.
//! - `topic_messages`: this node's published-message buffer.
//! - `topic_publish_marks`: the committed-message marks of this node's
//!   topic log, moved with each append.
//! - `mirror_collection_map`, `mirror_lag`: state of a mirror stream, not of
//!   group 0.
//! - `move_tenant_journal`: the journal of the node coordinating a move.

use redb::{
    Key, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition, TableError, Value,
    WriteTransaction,
};

use super::tables::*;
use super::types::{SystemCatalog, catalog_err};

/// Raw rows of one table: `(key bytes, value bytes)` in key order.
pub type RawRows = Vec<(Vec<u8>, Vec<u8>)>;

/// One replicated table: a stable label and thunks that read or replace its
/// rows without naming its key and value types.
pub(super) struct ReplicatedTable {
    pub(super) label: &'static str,
    dump: fn(&ReadTransaction) -> crate::Result<RawRows>,
    /// The rows as the write transaction sees them, for a merge.
    current: fn(&WriteTransaction) -> crate::Result<RawRows>,
    replace: fn(&WriteTransaction, &RawRows) -> crate::Result<()>,
}

fn dump_rows<K: Key + 'static, V: Value + 'static>(
    txn: &ReadTransaction,
    def: TableDefinition<K, V>,
) -> crate::Result<RawRows> {
    let table = match txn.open_table(def) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(catalog_err("replicated image: open table", e)),
    };
    let mut rows = Vec::new();
    for item in table
        .iter()
        .map_err(|e| catalog_err("replicated image: iterate table", e))?
    {
        let (key, value) = item.map_err(|e| catalog_err("replicated image: read row", e))?;
        let key = key.value();
        let value = value.value();
        rows.push((
            K::as_bytes(&key).as_ref().to_vec(),
            V::as_bytes(&value).as_ref().to_vec(),
        ));
    }
    Ok(rows)
}

fn current_rows<K: Key + 'static, V: Value + 'static>(
    txn: &WriteTransaction,
    def: TableDefinition<K, V>,
) -> crate::Result<RawRows> {
    let table = txn
        .open_table(def)
        .map_err(|e| catalog_err("replicated image: open table", e))?;
    let mut rows = Vec::new();
    for item in table
        .iter()
        .map_err(|e| catalog_err("replicated image: iterate table", e))?
    {
        let (key, value) = item.map_err(|e| catalog_err("replicated image: read row", e))?;
        let key = key.value();
        let value = value.value();
        rows.push((
            K::as_bytes(&key).as_ref().to_vec(),
            V::as_bytes(&value).as_ref().to_vec(),
        ));
    }
    Ok(rows)
}

fn replace_rows<K: Key + 'static, V: Value + 'static>(
    txn: &WriteTransaction,
    def: TableDefinition<K, V>,
    rows: &RawRows,
) -> crate::Result<()> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| catalog_err("replicated image: open table", e))?;
    table
        .retain(|_, _| false)
        .map_err(|e| catalog_err("replicated image: clear table", e))?;
    for (key, value) in rows {
        table
            .insert(K::from_bytes(key), V::from_bytes(value))
            .map_err(|e| catalog_err("replicated image: insert row", e))?;
    }
    Ok(())
}

macro_rules! replicated_tables {
    ($($label:literal => $def:expr),+ $(,)?) => {
        &[$(
            ReplicatedTable {
                label: $label,
                dump: |txn| dump_rows(txn, $def),
                current: |txn| current_rows(txn, $def),
                replace: |txn, rows| replace_rows(txn, $def, rows),
            }
        ),+]
    };
}

/// Every table group 0 replicates. Labels match the bootstrap registry.
pub(super) const REPLICATED_TABLES: &[ReplicatedTable] = replicated_tables![
    // ── Auth / tenancy ──
    "users" => USERS,
    "api_keys" => API_KEYS,
    "roles" => ROLES,
    "permissions" => PERMISSIONS,
    "owners" => OWNERS,
    "tenants" => TENANTS,
    "tenant_id_hwm" => super::tenant_id_hwm::TENANT_ID_HWM,
    "auth_users" => AUTH_USERS,
    "scope_grants" => SCOPE_GRANTS,
    "scope_quotas" => SCOPE_QUOTAS,
    "oidc_providers" => OIDC_PROVIDERS,
    // ── Collections ──
    "collections" => COLLECTIONS,
    "metadata" => METADATA,
    "column_stats" => COLUMN_STATS,
    "vector_model_metadata" => VECTOR_MODEL_METADATA,
    "vector_index_params" => VECTOR_INDEX_PARAMS,
    "index_registry" => INDEX_REGISTRY,
    "checkpoints" => CHECKPOINTS,
    "crdt_compaction_points" => super::crdt_compaction_points::CRDT_COMPACTION_POINTS,
    // ── Metadata-group host state ──
    "metadata_leases" => super::metadata_host::leases::METADATA_LEASES,
    "metadata_drains" => super::metadata_host::drains::METADATA_DRAINS,
    "metadata_host_scalars" => super::metadata_host::scalars::METADATA_HOST_SCALARS,
    "pending_ddl" => super::metadata_host::ddl::PENDING_DDL,
    "pending_leave_cleanup" => super::pending_leave_cleanup::PENDING_LEAVE_CLEANUP,
    // ── Replicated watermarks ──
    "surrogate_hwm" => super::surrogate_hwm::SURROGATE_HWM,
    "surrogate_reserve_index" => super::surrogate_hwm::SURROGATE_RESERVE_INDEX,
    "database_hwm" => DATABASE_HWM,
    // ── Sync producers, join tokens, enrollment ──
    "sync_producer_hwm" => super::sync_producer::SYNC_PRODUCER_HWM,
    "sync_producers" => super::sync_producer::SYNC_PRODUCERS,
    "sync_peer_bindings" => super::sync_producer::SYNC_PEER_BINDINGS,
    "join_token_states" => super::sync_producer::JOIN_TOKEN_STATES,
    "enrollment_preauthorizations" => super::sync_producer::ENROLLMENT_PREAUTHORIZATIONS,
    // ── DDL objects ──
    "materialized_views" => MATERIALIZED_VIEWS,
    "continuous_aggregates" => CONTINUOUS_AGGREGATES,
    "functions" => FUNCTIONS,
    "procedures" => PROCEDURES,
    "triggers" => TRIGGERS,
    "arrays" => ARRAYS,
    "dependencies" => DEPENDENCIES,
    "sequences" => SEQUENCES,
    "sequence_state" => SEQUENCE_STATE,
    "synonym_groups" => SYNONYM_GROUPS,
    "custom_types" => CUSTOM_TYPES,
    "custom_type_oid_hwm" => super::custom_type_oid_hwm::CUSTOM_TYPE_OID_HWM,
    "wasm_modules" => WASM_MODULES,
    "rls_policies" => super::rls::RLS_POLICIES,
    "redaction_policies" => super::redaction::REDACTION_POLICIES,
    // ── Event Plane definitions ──
    "change_streams" => CHANGE_STREAMS,
    "consumer_groups" => CONSUMER_GROUPS,
    "schedules" => SCHEDULES,
    "retention_policies" => RETENTION_POLICIES,
    "alert_rules" => ALERT_RULES,
    "topics_ep" => TOPICS_EP,
    "streaming_mvs" => STREAMING_MVS,
    // ── Databases and quotas ──
    "databases" => DATABASES,
    "databases_by_name" => DATABASES_BY_NAME,
    "database_grants" => DATABASE_GRANTS,
    "database_quotas" => DATABASE_QUOTAS,
    "tenant_quotas" => TENANT_QUOTAS,
    // ── Clone copy-on-write ──
    "clone_copyups" => CLONE_COPYUPS,
    "clone_tombstones" => CLONE_TOMBSTONES,
    "clone_kv_tombstones" => CLONE_KV_TOMBSTONES,
    "clone_lineage" => CLONE_LINEAGE,
    "clone_source_drains" => super::clone_source_drains::CLONE_SOURCE_DRAINS,
    // ── Cluster restore points ──
    "restore_points" => super::restore_points::RESTORE_POINTS,
    // ── Scheduled backups ──
    "backup_schedule_marks" => super::backup_schedule_marks::BACKUP_SCHEDULE_MARKS,
];

/// The labels of every replicated table, in image order.
pub fn replicated_table_labels() -> Vec<&'static str> {
    REPLICATED_TABLES.iter().map(|table| table.label).collect()
}

/// A read transaction on the system catalog, held to dump the replicated
/// tables at one commit point.
pub struct ReplicatedCatalogRead {
    txn: ReadTransaction,
}

impl ReplicatedCatalogRead {
    /// Read every replicated table: `(label, rows)` in image order.
    pub fn dump(&self) -> crate::Result<Vec<(String, RawRows)>> {
        REPLICATED_TABLES
            .iter()
            .map(|table| Ok((table.label.to_string(), (table.dump)(&self.txn)?)))
            .collect()
    }
}

impl SystemCatalog {
    /// Open a read transaction for [`ReplicatedCatalogRead::dump`]. The dump
    /// sees the catalog as of this call, whatever commits after it.
    pub fn begin_replicated_read(&self) -> crate::Result<ReplicatedCatalogRead> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("replicated image: begin read", e))?;
        Ok(ReplicatedCatalogRead { txn })
    }

    /// Replace every replicated table with `tables`, in one write
    /// transaction.
    ///
    /// A table that also takes this node's own writes merges instead (see
    /// [`super::replicated_image_merge`]): a counter or a fencing epoch never
    /// moves down.
    ///
    /// `tables` must name each replicated table exactly once. A missing,
    /// repeated, or unknown label fails before anything is written: the image
    /// came from a build with a different table set.
    pub fn replace_replicated_tables(&self, tables: &[(String, RawRows)]) -> crate::Result<()> {
        if tables.len() != REPLICATED_TABLES.len() {
            return Err(crate::Error::Internal {
                detail: format!(
                    "metadata image carries {} tables; this build replicates {}",
                    tables.len(),
                    REPLICATED_TABLES.len()
                ),
            });
        }
        let mut ordered: Vec<(&ReplicatedTable, &RawRows)> = Vec::with_capacity(tables.len());
        for table in REPLICATED_TABLES {
            let matches: Vec<&RawRows> = tables
                .iter()
                .filter(|(label, _)| label == table.label)
                .map(|(_, rows)| rows)
                .collect();
            match matches.as_slice() {
                [rows] => ordered.push((table, rows)),
                _ => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "metadata image must carry table '{}' exactly once, found {}",
                            table.label,
                            matches.len()
                        ),
                    });
                }
            }
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("replicated image: begin write", e))?;
        for (table, rows) in ordered {
            match super::replicated_image_merge::merge_for(table.label) {
                Some(merge) => {
                    let local = (table.current)(&txn)?;
                    (table.replace)(&txn, &merge(&local, rows)?)?;
                }
                None => (table.replace)(&txn, rows)?,
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("replicated image: commit", e))?;
        self.reload_event_definitions()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_types::DatabaseId;

    /// Every replicated table is a bootstrap table, and no label repeats.
    #[test]
    fn replicated_tables_are_bootstrap_tables() {
        let bootstrap: Vec<&str> = super::super::bootstrap_tables::BOOTSTRAP_TABLES
            .iter()
            .map(|table| table.label)
            .collect();
        let labels = replicated_table_labels();
        for label in &labels {
            assert!(
                bootstrap.contains(label),
                "{label} is not a bootstrap table"
            );
        }
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "a replicated label repeats");
    }

    /// A dump replaces another catalog's tables exactly: rows it lacks are
    /// removed, rows it holds are written.
    #[test]
    fn dump_then_replace_copies_the_replicated_tables() {
        let source = SystemCatalog::open_in_memory().unwrap();
        let target = SystemCatalog::open_in_memory().unwrap();
        source
            .put_collection(
                DatabaseId::DEFAULT,
                &StoredCollection::stamped_for_test(1, "kept", "admin"),
            )
            .unwrap();
        target
            .put_collection(
                DatabaseId::DEFAULT,
                &StoredCollection::stamped_for_test(1, "stale", "admin"),
            )
            .unwrap();

        let image = source.begin_replicated_read().unwrap().dump().unwrap();
        target.replace_replicated_tables(&image).unwrap();

        assert!(
            target
                .get_collection(DatabaseId::DEFAULT, 1, "kept")
                .unwrap()
                .is_some()
        );
        assert!(
            target
                .get_collection(DatabaseId::DEFAULT, 1, "stale")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            target.begin_replicated_read().unwrap().dump().unwrap(),
            image
        );
    }

    /// A counter this node advanced past the image keeps its local value,
    /// and one the image advanced further takes the image's.
    #[test]
    fn replace_never_moves_a_local_counter_down() {
        let source = SystemCatalog::open_in_memory().unwrap();
        let target = SystemCatalog::open_in_memory().unwrap();
        source.put_surrogate_hwm(10).unwrap();
        source.save_next_user_id(40).unwrap();
        target.put_surrogate_hwm(25).unwrap();
        target.save_next_user_id(7).unwrap();

        let image = source.begin_replicated_read().unwrap().dump().unwrap();
        target.replace_replicated_tables(&image).unwrap();

        assert_eq!(target.get_surrogate_hwm().unwrap(), 25);
        assert_eq!(target.load_next_user_id().unwrap(), 40);
    }

    /// An image with a missing table is refused before any write.
    #[test]
    fn an_incomplete_image_is_refused() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let mut image = catalog.begin_replicated_read().unwrap().dump().unwrap();
        image.pop();
        assert!(catalog.replace_replicated_tables(&image).is_err());
    }
}
