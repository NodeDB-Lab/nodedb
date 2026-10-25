// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch + payload encoders for the columnar-family engines
//! (`PhysicalPlan::Timeseries` and the columnar batch/DML records).

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::TimeseriesOp;

use crate::control::security::credential::CredentialStore;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

/// Inputs of [`wal_append_timeseries_op`].
pub(super) struct TimeseriesAppend<'a> {
    pub wal: WalAppender<'a>,
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub op: &'a TimeseriesOp,
    /// Threaded through solely for the per-collection `wal=false` bypass.
    pub credentials: Option<&'a CredentialStore>,
    /// The instant decided elsewhere for the ingest's untimed rows (see
    /// `WalAppendRequest::now_override`).
    pub now_override: Option<u64>,
}

/// What [`wal_append_timeseries_op`] appended and resolved.
pub(super) struct TimeseriesAppendOutcome {
    /// LSN for an ingest or truncate, `None` for a scan. A `wal=false` ingest
    /// carries its `ProposalApplied` marker's LSN inside a replicated
    /// proposal's apply, and `None` outside one.
    pub lsn: Option<Lsn>,
    /// The instant an ingest's untimed rows take. `Some` for every ingest,
    /// WAL-bypassed or not, so the live apply stores the same rows restart
    /// replay does.
    pub resolved_now_ms: Option<u64>,
}

/// Append the WAL record for a `TimeseriesOp`.
///
/// An ingest resolves its stamp here, once: `now_override` when it was
/// decided elsewhere (a replicated entry's leader stamp), else the
/// millisecond of a fresh ordinal of this node's system-time clock. It is
/// the `_ts_system` of the batch's bitemporal rows and the default timestamp
/// of its untimed rows. The record carries it, and the live apply receives
/// it as `resolved_now_ms`.
pub(super) fn wal_append_timeseries_op(
    append: TimeseriesAppend<'_>,
) -> crate::Result<TimeseriesAppendOutcome> {
    let TimeseriesAppend {
        wal,
        tenant_id,
        vshard_id,
        database_id,
        op,
        credentials,
        now_override,
    } = append;
    let outcome = match op {
        TimeseriesOp::Ingest {
            collection,
            payload,
            format,
            provenance,
            ..
        } => {
            let now_ms = now_override.unwrap_or_else(|| wal.ordinal_now_ms());
            // WAL bypass: skip WAL if collection has wal=false in timeseries_config.
            let bypassed = credentials.is_some_and(|creds| {
                matches!(
                    creds.catalog().get_collection(
                        database_id,
                        tenant_id.as_u64(),
                        collection.as_str(),
                    ),
                    Ok(Some(coll)) if coll
                        .get_timeseries_config()
                        .is_some_and(|config| {
                            config.get("wal").and_then(|v| v.as_str()) == Some("false")
                        })
                )
            });
            let lsn = if bypassed {
                // WAL bypassed: the rows since the last flush are lost on a
                // crash. A replicated proposal's apply still records its key,
                // in a payload-free marker that stands in for the forward
                // record, so a second committed copy of the proposal is
                // skipped after a restart. Outside a proposal's apply nothing
                // is appended.
                wal.append_proposal_applied(tenant_id, vshard_id, database_id)?
            } else {
                let wal_payload = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
                    collection: collection.as_str(),
                    payload,
                    provenance: provenance.as_ref(),
                    format,
                    default_timestamp_ms: i64::try_from(now_ms).unwrap_or(i64::MAX),
                })?;
                Some(wal.append_timeseries_batch(
                    tenant_id,
                    vshard_id,
                    database_id,
                    &wal_payload,
                )?)
            };
            TimeseriesAppendOutcome {
                lsn,
                resolved_now_ms: Some(now_ms),
            }
        }
        // `restart_identity` is applied by the Control Plane after dispatch,
        // against the sequence store; the Data-Plane record carries only what
        // replay re-applies.
        TimeseriesOp::Truncate {
            collection,
            restart_identity: _,
        } => {
            let wal_payload = encode_columnar_truncate_payload(collection.as_str(), None, false)?;
            TimeseriesAppendOutcome {
                lsn: Some(wal.append_timeseries_truncate(
                    tenant_id,
                    vshard_id,
                    database_id,
                    &wal_payload,
                )?),
                resolved_now_ms: None,
            }
        }
        // A refresh cut writes no row, and every replica applies it at its
        // log position, so no forward record replays it: a cut replayed after
        // later records would cut at different rows. A replicated proposal's
        // apply records its key in a payload-free marker, so a second
        // committed copy is skipped after a restart. The Data Plane skips a
        // repeated `refresh_id` on its own as well.
        TimeseriesOp::RefreshAggregate { .. } => TimeseriesAppendOutcome {
            lsn: wal.append_proposal_applied(tenant_id, vshard_id, database_id)?,
            resolved_now_ms: None,
        },
        // Reads / read-only resolve pass, and partition maintenance, which
        // changes no row — no engine mutation here.
        TimeseriesOp::Scan { .. }
        | TimeseriesOp::ResolveIngest(_)
        | TimeseriesOp::Partitions { .. }
        | TimeseriesOp::ReadAggregate { .. } => TimeseriesAppendOutcome {
            lsn: None,
            resolved_now_ms: None,
        },
    };
    Ok(outcome)
}

/// Encode the payload of a `ColumnarTruncate` / `TimeseriesTruncate` WAL
/// record. The record type names the engine. `system_from` and
/// `erase_history` are those of `ColumnarOp::Truncate`: a timeseries truncate
/// passes `None` and `false`.
pub(crate) fn encode_columnar_truncate_payload(
    collection: &str,
    system_from: Option<i64>,
    erase_history: bool,
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarTruncateWalRecord {
        collection: collection.to_string(),
        system_from,
        erase_history,
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar truncate: {e}"),
    })
}

/// One timeseries ingest's `TimeseriesBatch` WAL record.
pub(crate) struct TimeseriesIngestRecord<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
    /// The payload's ingest format: payload bytes alone cannot distinguish ILP
    /// from row MessagePack.
    pub format: &'a str,
    /// The timestamp, in epoch milliseconds, of every row that carries none.
    pub default_timestamp_ms: i64,
}

/// Encode a timeseries ingest's `TimeseriesBatch` WAL record or redo
/// sub-record: the six-element tuple `("timeseries", collection, payload,
/// provenance, format, default_timestamp_ms)`. Every timeseries ingest record
/// takes this shape. Replay stamps untimed rows with the carried instant, so
/// they store the rows the live apply stored.
pub(crate) fn encode_timeseries_ingest_payload(
    record: TimeseriesIngestRecord<'_>,
) -> crate::Result<Vec<u8>> {
    let TimeseriesIngestRecord {
        collection,
        payload,
        provenance,
        format,
        default_timestamp_ms,
    } = record;
    zerompk::to_msgpack_vec(&(
        "timeseries",
        collection,
        payload,
        provenance,
        format,
        default_timestamp_ms,
    ))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal timeseries ingest: {e}"),
    })
}

/// One columnar insert's `TimeseriesBatch` WAL record.
pub(crate) struct ColumnarBatchRecord<'a> {
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates, index-aligned with the rows in `payload`.
    pub surrogates: &'a [nodedb_types::Surrogate],
    /// What a row whose primary key already exists does.
    pub conflict_policy: &'a crate::wal::ColumnarConflictPolicy,
    /// System-time ordinal of the first row of a bitemporal insert.
    pub system_from: Option<i64>,
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar batch.
/// Produces the map-shaped `ColumnarWalRecord` (`kind = "columnar"`), distinct
/// from the timeseries tuple so `decode_batch_record` routes correctly. The
/// record carries the insert's conflict policy, so replay skips, merges or
/// replaces each row exactly as the live insert did.
pub(crate) fn encode_columnar_batch_payload(
    record: ColumnarBatchRecord<'_>,
) -> crate::Result<Vec<u8>> {
    let ColumnarBatchRecord {
        collection,
        payload,
        provenance,
        surrogates,
        conflict_policy,
        system_from,
    } = record;
    let record = nodedb_types::columnar::ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: collection.to_string(),
        payload: payload.to_vec(),
        provenance: provenance.cloned(),
        surrogates: surrogates.to_vec(),
        conflict_policy: conflict_policy.encode()?,
        system_from,
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar batch: {e}"),
    })
}

/// Encode the payload of a `TimeseriesBatch` WAL record for a columnar
/// predicate DML. Produces `ColumnarDmlWalRecord` carrying the predicate and
/// field assignments — not row post-images, since matches are re-scanned at apply.
pub(crate) fn encode_columnar_dml_payload(
    collection: &str,
    is_update: bool,
    filters: &[u8],
    updates: &[(String, nodedb_physical::physical_plan::UpdateValue)],
    system_from: Option<i64>,
) -> crate::Result<Vec<u8>> {
    let record = nodedb_types::columnar::ColumnarDmlWalRecord {
        kind: "columnar_dml".to_string(),
        collection: collection.to_string(),
        is_update,
        filters: filters.to_vec(),
        updates: crate::wal::encode_columnar_dml_updates(updates)?,
        system_from,
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
    system_from: Option<i64>,
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
        system_from,
    };
    zerompk::to_msgpack_vec(&record).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal columnar resolved dml: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
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
    fn an_ingest_record_carries_the_instant_its_untimed_rows_take() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=1".to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: vec![],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: vec![],
        });

        let outcome = super::super::wal_append(super::super::WalAppendRequest {
            wal: wal.appender(NO_APPLY_KEY),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: Some(1_700_000_000_123),
        })
        .expect("append");

        assert_eq!(outcome.resolved_now_ms, Some(1_700_000_000_123));
        wal.sync().expect("sync wal");
        let record = wal
            .replay()
            .expect("read wal")
            .into_iter()
            .find(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(nodedb_wal::record::RecordType::TimeseriesBatch)
            })
            .expect("ingest record");
        let decoded = crate::wal::decode_batch_record(&record.payload).expect("decode");
        assert_eq!(decoded.default_timestamp_ms, Some(1_700_000_000_123));
        assert_eq!(decoded.format.as_deref(), Some("ilp"));
    }

    /// A `wal=false` ingest appends no batch record. Under a proposal's apply
    /// key it appends a `ProposalApplied` marker instead and returns the
    /// marker's LSN as the write's LSN, so the funnel's durability barrier,
    /// which waits on that LSN, makes the marker durable before the ack.
    #[test]
    fn a_wal_bypassed_ingest_under_an_apply_key_returns_its_marker_lsn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let credentials = CredentialStore::new().expect("in-memory credential store");
        let mut collection = crate::control::security::catalog::StoredCollection::stamped_for_test(
            1, "metrics", "owner",
        );
        collection.timeseries_config = Some(r#"{"wal":"false"}"#.to_string());
        credentials
            .catalog()
            .put_collection(DatabaseId::DEFAULT, &collection)
            .expect("store collection");
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=1".to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: vec![],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            returning: None,
            rls_filters: vec![],
        });
        let append = |apply_key: u64| {
            super::super::wal_append(super::super::WalAppendRequest {
                wal: wal.appender(apply_key),
                event_source: crate::event::EventSource::User,
                tenant_id: TenantId::new(1),
                vshard_id: VShardId::new(0),
                database_id: DatabaseId::DEFAULT,
                plan: &plan,
                credentials: Some(&credentials),
                now_override: None,
            })
            .expect("append")
        };

        assert_eq!(
            append(NO_APPLY_KEY).lsn,
            None,
            "outside a proposal's apply nothing is appended"
        );
        let marker_lsn = append(0xAB)
            .lsn
            .expect("the marker's LSN is the write's LSN");

        wal.sync().expect("sync wal");
        let records = wal.replay().expect("read wal");
        assert_eq!(records.len(), 1, "only the marker reaches the WAL");
        assert_eq!(records[0].header.lsn, marker_lsn.as_u64());
        assert_eq!(
            nodedb_wal::record::RecordType::from_raw(records[0].logical_record_type()),
            Some(nodedb_wal::record::RecordType::ProposalApplied)
        );
        assert_eq!(records[0].apply_key(), 0xAB);
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
            having: vec![],
            tiering: None,
            output_columns: Vec::new(),
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
