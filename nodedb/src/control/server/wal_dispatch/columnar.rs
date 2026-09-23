// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Columnar(ColumnarOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::ColumnarOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Append the WAL record for a single `ColumnarOp`, returning the allocated LSN
/// for the write variants (`Some`) or `None` for the scan variants, which carry
/// no durable per-write effect.
///
/// The match over [`ColumnarOp`] is **exhaustive** (`wildcard_enum_match_arm`
/// is denied), so a future write variant cannot silently become non-durable.
pub(super) fn wal_append_columnar_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &ColumnarOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        ColumnarOp::Insert {
            collection,
            payload,
            format: _,
            intent,
            on_conflict_updates,
            surrogates,
            schema_bytes: _,
            provenance,
            wal_lsn: _,
            // The compiled RLS predicate is a per-request authorization input,
            // not part of the row image being made durable, so it is not
            // written to the log and replay does not re-evaluate it.
            rls_write_check: _,
            // Likewise per-request, and about the response rather than the row:
            // a projection and its read gate shape what one caller is shown,
            // which is not something replay reconstructs.
            returning: _,
            rls_filters: _,
        } => {
            // Encode a map-shaped `ColumnarWalRecord` carrying the per-row
            // cross-engine surrogates and the insert's conflict policy, so
            // replay restores the exact same identity and decides each
            // existing key the way the live insert did. `surrogates` is index-aligned with the
            // rows in `payload`. The map shape is distinct from the legacy
            // 4-tuple array, so old on-disk records still decode via the
            // replay fallback path.
            let wal_payload = super::timeseries::encode_columnar_batch_payload(
                super::timeseries::ColumnarBatchRecord {
                    collection: collection.as_str(),
                    payload,
                    provenance: provenance.as_ref(),
                    surrogates,
                    conflict_policy: &crate::wal::ColumnarConflictPolicy {
                        intent: *intent,
                        on_conflict_updates: on_conflict_updates.clone(),
                    },
                },
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        ColumnarOp::Update {
            collection,
            filters,
            updates,
            rls_write_check: _,
        } => {
            // Predicate UPDATE has no row post-image at append time (the
            // matching set is only known once the Data Plane scans current
            // state), so the durable record carries the predicate itself;
            // replay re-executes it through the same live handler. See
            // `encode_columnar_dml_payload` for the record shape and the
            // idempotence constraint on replay ordering.
            let wal_payload = super::timeseries::encode_columnar_dml_payload(
                collection.as_str(),
                true,
                filters,
                updates,
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        ColumnarOp::Delete {
            collection,
            filters,
            rls_write_check: _,
        } => {
            // Mirrors the `Update` arm above; delete is idempotent (mark +
            // remove from PK index), so unlike update it tolerates a
            // hypothetical double-apply, but replay still runs it exactly
            // once by construction.
            let wal_payload = super::timeseries::encode_columnar_dml_payload(
                collection.as_str(),
                false,
                filters,
                &[],
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        ColumnarOp::ResolvedUpdate {
            collection,
            rows,
            rls_write_check: _,
        } => {
            // Rows the Control Plane already resolved and decided against the
            // write policy, so the durable record carries the exact images,
            // never a predicate: replay has no writing identity to decide one
            // against.
            let wal_payload = super::timeseries::encode_columnar_resolved_dml_payload(
                collection.as_str(),
                true,
                rows,
                &[],
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        ColumnarOp::ResolvedDelete {
            collection,
            pks,
            rls_write_check: _,
        } => {
            let wal_payload = super::timeseries::encode_columnar_resolved_dml_payload(
                collection.as_str(),
                false,
                &[],
                pks,
            )?;
            Some(wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        // `restart_identity` is applied by the Control Plane after dispatch,
        // against the sequence store; the Data-Plane record carries only what
        // replay re-applies.
        ColumnarOp::Truncate {
            collection,
            restart_identity: _,
        } => {
            let wal_payload =
                super::timeseries::encode_columnar_truncate_payload(collection.as_str())?;
            Some(wal.append_columnar_truncate(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation
        // here. `ResolveDml` mutates nothing; it only reports the row set a
        // predicate DML would touch, so it appends no durable record.
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => None,
    };
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, PhysicalPlan};

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn has_record_of_type(wal: &WalManager, record_type: nodedb_wal::record::RecordType) -> bool {
        wal.sync().expect("sync wal");
        wal.replay().expect("read wal").into_iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type()) == Some(record_type)
        })
    }

    #[test]
    fn insert_appends_timeseries_batch_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: vec![1, 2, 3],
            format: "msgpack".to_string(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: vec![],
            surrogates: vec![],
            schema_bytes: vec![],
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: vec![],
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Insert must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::TimeseriesBatch
        ));
    }

    #[test]
    fn truncate_appends_columnar_truncate_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            restart_identity: true,
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Truncate must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::ColumnarTruncate
        ));
    }

    #[test]
    fn scan_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            projection: vec![],
            limit: 10,
            filters: vec![],
            rls_filters: vec![],
            sort_keys: vec![],
            system_time: Default::default(),
            valid_at_ms: None,
            prefilter: None,
            computed_columns: vec![],
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "Scan must produce no durable LSN");
    }
}
