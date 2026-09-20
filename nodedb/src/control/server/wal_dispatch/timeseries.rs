// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch + payload encoders for the columnar-family engines
//! (`PhysicalPlan::Timeseries` and the columnar batch/DML records).

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::control::security::credential::CredentialStore;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Append the WAL record for a `TimeseriesOp`: LSN for ingest, `None` for
/// `Scan` or a `wal=false` collection. `credentials` is threaded through
/// solely for the per-collection bypass check.
pub(super) fn wal_append_timeseries_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &TimeseriesOp,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format: _,
            provenance,
            ..
        } => {
            // WAL bypass: skip WAL if collection has wal=false in timeseries_config.
            if let Some(creds) = credentials
                && let Ok(Some(coll)) = creds.catalog().get_collection(
                    database_id,
                    tenant_id.as_u64(),
                    collection.as_str(),
                )
                && let Some(config) = coll.get_timeseries_config()
                && config.get("wal").and_then(|v| v.as_str()) == Some("false")
            {
                // WAL bypassed — acceptable data loss of last flush interval on crash.
                None
            } else {
                // Provenance appended last; older 3-element decoders ignore it via arity fallback.
                let wal_payload = encode_timeseries_batch_payload(
                    collection.as_str(),
                    payload,
                    provenance.as_ref(),
                )?;
                Some(wal.append_timeseries_batch(
                    tenant_id,
                    vshard_id,
                    database_id,
                    &wal_payload,
                )?)
            }
        }
        // `restart_identity` is applied by the Control Plane after dispatch,
        // against the sequence store; the Data-Plane record carries only what
        // replay re-applies.
        TimeseriesOp::Truncate {
            collection,
            restart_identity: _,
        } => {
            let wal_payload = encode_columnar_truncate_payload(collection.as_str())?;
            Some(wal.append_timeseries_truncate(tenant_id, vshard_id, database_id, &wal_payload)?)
        }
        // Reads / read-only resolve pass — no engine mutation here.
        TimeseriesOp::Scan { .. } | TimeseriesOp::ResolveIngest(_) => None,
    };
    Ok(appended)
}

/// Encode the payload of a `ColumnarTruncate` / `TimeseriesTruncate` WAL
/// record: the collection name only. The record type names the engine.
pub(crate) fn encode_columnar_truncate_payload(collection: &str) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarTruncateWalRecord {
        collection: collection.to_string(),
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar truncate: {e}"),
    })
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a timeseries ingest.
/// Produces the legacy 4-element tuple `("timeseries", collection, payload,
/// provenance)`. New transaction redo must use [`encode_timeseries_batch_payload_with_format`].
pub(crate) fn encode_timeseries_batch_payload(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&("timeseries", collection, payload, provenance)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch: {e}"),
        }
    })
}

/// Encode the format-preserving 5-element timeseries WAL/redo tuple. The format
/// field is required because payload bytes alone can't distinguish ILP from row MessagePack.
pub(crate) fn encode_timeseries_batch_payload_with_format(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    format: &str,
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&("timeseries", collection, payload, provenance, format)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal timeseries batch with format: {e}"),
        }
    })
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar batch.
/// Produces the map-shaped `ColumnarWalRecord` (`kind = "columnar"`), distinct
/// from the timeseries tuple so `decode_batch_record` routes correctly.
pub(crate) fn encode_columnar_batch_payload(
    collection: &str,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    surrogates: &[nodedb_types::Surrogate],
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: collection.to_string(),
        payload: payload.to_vec(),
        provenance: provenance.cloned(),
        surrogates: surrogates.to_vec(),
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar batch: {e}"),
    })
}

/// Stable routing and collection scope for a timeseries WAL append. Keeps these
/// fields together so callers can't mix the authenticated scope with another's payload.
pub(crate) struct TimeseriesWalAppendContext<'a> {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
}

/// Append a timeseries batch to WAL and return the assigned LSN. Used by the ILP
/// listener and sync handler for dedup tracking and `flush_wal_lsn`.
/// Returns `None` if WAL is bypassed.
pub(crate) fn wal_append_timeseries(
    wal: &WalManager,
    context: TimeseriesWalAppendContext<'_>,
    payload: &[u8],
    provenance: Option<&nodedb_types::sync::wire::SyncProvenance>,
    credentials: Option<&CredentialStore>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let TimeseriesWalAppendContext {
        tenant_id,
        vshard_id,
        database_id,
        collection,
    } = context;
    // WAL bypass check.
    if let Some(creds) = credentials
        && let Ok(Some(coll)) =
            creds
                .catalog()
                .get_collection(database_id, tenant_id.as_u64(), collection)
        && let Some(config) = coll.get_timeseries_config()
        && config.get("wal").and_then(|v| v.as_str()) == Some("false")
    {
        return Ok(None);
    }

    let wal_payload = encode_timeseries_batch_payload(collection, payload, provenance)?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar
/// predicate DML. Produces `ColumnarDmlWalRecord` carrying the predicate and
/// field assignments — not row post-images, since matches are re-scanned at apply.
pub(crate) fn encode_columnar_dml_payload(
    collection: &str,
    is_update: bool,
    filters: &[u8],
    updates: &[(String, Vec<u8>)],
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarDmlWalRecord {
        kind: "columnar_dml".to_string(),
        collection: collection.to_string(),
        is_update,
        filters: filters.to_vec(),
        updates: updates.to_vec(),
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar dml: {e}"),
    })
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar
/// resolved-row-set DML. Produces `ColumnarResolvedDmlWalRecord` carrying concrete
/// row images already resolved — never a predicate. `updates` empty for a delete row.
pub(crate) fn encode_columnar_resolved_dml_payload(
    collection: &str,
    is_update: bool,
    rows: &[(nodedb_types::Value, Vec<nodedb_types::Value>)],
    pks: &[nodedb_types::Value],
) -> crate::Result<Vec<u8>> {
    let to_msgpack = |v: &nodedb_types::Value| {
        nodedb_types::value_to_msgpack(v).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal columnar resolved dml row: {e}"),
        })
    };
    let wal_rows = if is_update {
        rows.iter()
            .map(|(pk, new_row)| {
                Ok(nodedb_types::columnar::ColumnarResolvedDmlWalRow {
                    pk_msgpack: to_msgpack(pk)?,
                    new_row_msgpack: to_msgpack(&nodedb_types::Value::Array(new_row.clone()))?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?
    } else {
        pks.iter()
            .map(|pk| {
                Ok(nodedb_types::columnar::ColumnarResolvedDmlWalRow {
                    pk_msgpack: to_msgpack(pk)?,
                    new_row_msgpack: Vec::new(),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?
    };
    let record = nodedb_types::columnar::ColumnarResolvedDmlWalRecord {
        kind: "columnar_resolved_dml".to_string(),
        collection: collection.to_string(),
        is_update,
        rows: wal_rows,
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar resolved dml: {e}"),
    })
}

/// Record-level fields for a columnar WAL append. Groups collection identity,
/// row payload, provenance, and surrogates, reducing [`wal_append_columnar`]'s argument count.
pub struct ColumnarWalAppendArgs<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. Pass an empty
    /// slice when the caller does not carry surrogate identity (e.g. the
    /// sync/CRDT path).
    pub surrogates: &'a [nodedb_types::Surrogate],
}

/// Append a columnar batch to WAL and return the assigned LSN. Mirrors
/// `wal_append_timeseries` but encodes `ColumnarWalRecord` so replay restores
/// per-row surrogates. Always returns `Some` — columnar has no `wal=false`.
pub fn wal_append_columnar(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: ColumnarWalAppendArgs<'_>,
) -> crate::Result<Option<nodedb_types::Lsn>> {
    let ColumnarWalAppendArgs {
        collection,
        payload,
        provenance,
        surrogates,
    } = args;
    let wal_payload = encode_columnar_batch_payload(collection, payload, provenance, surrogates)?;
    let lsn = wal.append_timeseries_batch(tenant_id, vshard_id, database_id, &wal_payload)?;
    Ok(Some(lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::PhysicalPlan;

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
    fn ingest_appends_timeseries_batch_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: vec![1, 2, 3],
            format: "samples".to_string(),
            wal_lsn: None,
            surrogates: vec![],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: vec![],
        });

        // No credentials => no WAL bypass; the ingest must produce a record.
        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "Ingest must produce a durable LSN");
        assert!(has_record_of_type(
            &wal,
            nodedb_wal::record::RecordType::TimeseriesBatch
        ));
    }

    #[test]
    fn truncate_appends_timeseries_truncate_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            restart_identity: false,
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
            nodedb_wal::record::RecordType::TimeseriesTruncate
        ));
    }

    #[test]
    fn scan_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            time_range: (0, i64::MAX),
            projection: vec![],
            limit: 10,
            filters: vec![],
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: vec![],
            aggregates: vec![],
            gap_fill: String::new(),
            computed_columns: vec![],
            rls_filters: vec![],
            system_time: Default::default(),
            valid_at_ms: None,
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
