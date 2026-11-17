// SPDX-License-Identifier: BUSL-1.1

//! How a resolved timeseries batch maps onto the live memtable schema.
//!
//! The live schema at an install is the schema in force at the install's
//! log position. Every replica applies the same log in the same order, and a
//! restart seeds the memtable with its flushed schema, so every replica holds
//! the same live schema at that position. A row whose value conflicts with
//! its column's type in that schema is therefore rejected by every replica
//! alike.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnValue, ColumnarSchema};
use crate::engine::timeseries::resolved_ingest::{
    ResolvedTsBatch, ResolvedTsRow, empty_value, value_fits,
};
use crate::types::{DatabaseId, TenantId};

pub(super) type CollKey = (DatabaseId, TenantId, String);

/// How a batch's rows map onto the live schema.
#[derive(Clone, Copy)]
pub(super) enum SchemaFit {
    /// The live schema evolves to the batch's schema: every row's values land
    /// in order.
    Exact,
    /// A concurrent write changed the schema: values land by column name.
    ByName,
}

impl CoreLoop {
    /// How `batch` maps onto the live memtable of `key`. A collection with no
    /// memtable takes the batch's schema whole.
    pub(super) fn schema_fit(&self, key: &CollKey, batch: &ResolvedTsBatch) -> SchemaFit {
        fit_against(
            self.columnar_memtables.get(key).map(|mt| mt.schema()),
            batch,
        )
    }

    /// The schema a fresh memtable takes for `batch`: its columns, with the
    /// declared column codecs where the declaration names the column.
    pub(super) fn resolved_memtable_schema(
        &self,
        key: &CollKey,
        batch: &ResolvedTsBatch,
    ) -> ColumnarSchema {
        let declared = self.declared_ts_memtable_schema(key.0, key.1, &key.2);
        let codecs = batch
            .columns
            .iter()
            .map(|(name, _)| {
                declared
                    .as_ref()
                    .and_then(|schema| {
                        schema
                            .columns
                            .iter()
                            .position(|(declared_name, _)| declared_name == name)
                            .and_then(|idx| schema.codecs.get(idx).cloned())
                    })
                    .unwrap_or(nodedb_codec::ColumnCodec::Auto)
            })
            .collect();
        ColumnarSchema {
            columns: batch.columns.clone(),
            timestamp_idx: usize::try_from(batch.timestamp_idx).unwrap_or(0),
            codecs,
        }
    }
}

/// How `batch` maps onto the live schema `live`. A collection with no live
/// schema takes the batch's schema whole.
pub(super) fn fit_against(live: Option<&ColumnarSchema>, batch: &ResolvedTsBatch) -> SchemaFit {
    let Some(live) = live else {
        return SchemaFit::Exact;
    };
    if live.timestamp_idx as u64 != batch.timestamp_idx {
        return SchemaFit::ByName;
    }
    let mut prospective = live.columns.clone();
    for (name, column_type) in &batch.columns {
        if !prospective.iter().any(|(existing, _)| existing == name) {
            prospective.push((name.clone(), *column_type));
        }
    }
    if prospective == batch.columns {
        SchemaFit::Exact
    } else {
        SchemaFit::ByName
    }
}

/// Each row's values in `schema`'s column order under `fit`. A row whose
/// value conflicts with its column's type in `schema` has `None`.
pub(super) fn landing_values(
    fit: SchemaFit,
    schema: &ColumnarSchema,
    batch: &ResolvedTsBatch,
) -> Vec<Option<Vec<ColumnValue>>> {
    batch
        .rows
        .iter()
        .map(|row| match fit {
            SchemaFit::Exact => Some(row.values.clone()),
            SchemaFit::ByName => values_by_name(schema, &batch.columns, row),
        })
        .collect()
}

/// `row`'s values rearranged into `schema`'s column order by name. A column
/// the row does not name takes its empty value in `schema`. `None` when a
/// value the row names does not fit its column's type.
fn values_by_name(
    schema: &ColumnarSchema,
    columns: &[(String, ColumnType)],
    row: &ResolvedTsRow,
) -> Option<Vec<ColumnValue>> {
    let mut values = Vec::with_capacity(schema.columns.len());
    for (idx, (name, column_type)) in schema.columns.iter().enumerate() {
        let named = columns
            .iter()
            .position(|(batch_name, _)| batch_name == name)
            .filter(|position| {
                !row.absent
                    .iter()
                    .any(|absent| *absent as usize == *position)
            })
            .and_then(|position| row.values.get(position));
        let value = match named {
            Some(value) if value_fits(value, *column_type) => value.clone(),
            Some(_) => return None,
            None if idx == schema.timestamp_idx => ColumnValue::Timestamp(row.timestamp_ms),
            None => empty_value(*column_type),
        };
        values.push(value);
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::TimeKind;

    fn live() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("extra".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    fn batch_columns() -> Vec<(String, ColumnType)> {
        vec![
            ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
            ("value".into(), ColumnType::Float64),
            ("extra".into(), ColumnType::Symbol),
        ]
    }

    fn row(extra: &str, absent: Vec<u32>) -> ResolvedTsRow {
        ResolvedTsRow {
            line: 0,
            timestamp_ms: 1_000,
            values: vec![
                ColumnValue::Timestamp(1_000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol(extra.into()),
            ],
            absent,
            image: Vec::new(),
        }
    }

    #[test]
    fn a_named_value_of_another_type_conflicts() {
        assert!(values_by_name(&live(), &batch_columns(), &row("text", Vec::new())).is_none());
    }

    #[test]
    fn an_absent_column_takes_the_live_empty_value() {
        let values = values_by_name(&live(), &batch_columns(), &row("", vec![2]))
            .expect("an absent column never conflicts");
        assert_eq!(values.get(2), Some(&ColumnValue::Null));
        assert_eq!(values.get(1), Some(&ColumnValue::Float64(1.0)));
    }
}

#[cfg(test)]
mod install_tests {
    use super::super::ingest_resolved_test_support::{
        COLLECTION, TENANT, drained_images, install_committed, key, resolve, resolve_lines,
        stored_row, task_at, wire_events,
    };
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    /// A line whose field conflicts with its column's type is rejected at
    /// resolve. The batch carries the count and the reason, and the install
    /// reports it as the ingest's `rejected` count.
    #[test]
    fn a_type_conflicting_line_is_reported_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let batch = resolve_lines(
            &core,
            &[
                "metrics value=1 1000000000",
                "metrics value=\"text\" 2000000000",
            ],
            1_000,
        );
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rejected, 1);
        assert!(
            batch.first_rejection.is_some(),
            "the rejection names its reason"
        );

        let response = install_committed(&mut core, &batch, 1);
        assert_eq!(response.status, Status::Ok);
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(1));
        assert_eq!(body["rejected"], serde_json::json!(1));
    }

    /// Two ingests resolve against the same schema and give a new column
    /// conflicting types. The first installs. The second is a committed
    /// entry: its conflicting row is rejected and counted, its other row
    /// lands with the column empty, the core stays up, and its one event
    /// equals the row it stored.
    #[test]
    fn a_committed_row_that_conflicts_is_rejected_and_the_rest_land() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        let second = resolve_lines(
            &core,
            &[
                "metrics extra=\"text\" 2000000000",
                "metrics value=2 3000000000",
            ],
            2_000,
        );
        assert_eq!(second.rows.len(), 2, "the resolve saw no conflict");

        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(drained_images(&mut events), vec![stored_row(&core, 0)]);

        let response = install_committed(&mut core, &second, 2);
        assert_eq!(response.status, Status::Ok);
        assert!(!core.is_fail_stopped());
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(1));
        assert_eq!(body["rejected"], serde_json::json!(1));
        assert_eq!(
            core.columnar_memtables.get(&key()).map(|mt| mt.row_count()),
            Some(2)
        );
        let stored = stored_row(&core, 1);
        assert_eq!(drained_images(&mut events), vec![stored.clone()]);
        let rmpv::Value::Map(fields) = stored else {
            panic!("a stored row is a map");
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key.as_str() == Some(name))
                .map(|(_, value)| value.clone())
        };
        assert_eq!(field("value"), Some(rmpv::Value::F64(2.0)));
        assert!(
            matches!(field("extra"), Some(rmpv::Value::Nil)),
            "a column the row does not name is empty, not a conflict"
        );
    }

    /// A flush drains the memtable and keeps its schema on disk. A restart
    /// seeds the memtable with it, so a committed row that conflicts with
    /// the flushed schema is rejected after the restart as before it.
    #[test]
    fn a_restart_keeps_the_flushed_schema_for_conflicts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        core.flush_ts_collection(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            COLLECTION,
            1_000,
        )
        .expect("flush");
        drop(core);

        let (mut restarted, _tx, _rx) = make_core_with_dir(dir.path());
        restarted.load_ts_registries().expect("load registries");
        let second = resolve(&restarted, "metrics extra=\"text\" 2000000000", 2_000);
        assert_eq!(second.rejected, 1, "the resolve sees the flushed schema");

        // A proposer that never held the column resolves the row whole.
        let unseeded = {
            let scratch = tempfile::tempdir().expect("tempdir");
            let (other, _tx, _rx) = make_core_with_dir(scratch.path());
            resolve(&other, "metrics extra=\"text\" 2000000000", 2_000)
        };
        assert_eq!(unseeded.rows.len(), 1);
        let response = install_committed(&mut restarted, &unseeded, 2);
        assert_eq!(response.status, Status::Ok);
        assert!(!restarted.is_fail_stopped());
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
        assert_eq!(body["accepted"], serde_json::json!(0));
        assert_eq!(body["rejected"], serde_json::json!(1));
    }

    /// The ring drops the events of a committed by-name install that
    /// rejected a row. WAL catch-up rebuilds them from the record and the
    /// install's outcome: one event per landed row, with its stored image,
    /// and none for the rejected row.
    #[test]
    fn catch_up_after_an_apply_time_rejection_rebuilds_only_landed_rows() {
        use crate::control::server::wal_dispatch::{
            TimeseriesIngestRecord, encode_timeseries_ingest_payload,
        };
        use crate::engine::timeseries::install_outcome::{TsOutcomeIndex, outcome_dir};
        use crate::event::wal_replay_scope::{ReplayScope, RowSources};

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut events = wire_events(&mut core);

        let first = resolve(&core, "metrics extra=1.5 1000000000", 1_000);
        let second = resolve_lines(
            &core,
            &[
                "metrics extra=\"text\" 2000000000",
                "metrics value=2 3000000000",
            ],
            2_000,
        );
        assert_eq!(install_committed(&mut core, &first, 1).status, Status::Ok);
        assert_eq!(install_committed(&mut core, &second, 2).status, Status::Ok);
        assert_eq!(
            drained_images(&mut events).len(),
            2,
            "one live event per landed row"
        );

        // The ring lost both events. Catch-up reads the logged record.
        let logged = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            collection: COLLECTION,
            payload: &second.to_bytes().expect("encode batch"),
            provenance: None,
            format: RESOLVED_INGEST_FORMAT,
            default_timestamp_ms: second.now_ms,
        })
        .expect("encode record");
        let outcomes =
            TsOutcomeIndex::load(&outcome_dir(dir.path(), core.core_id)).expect("load outcomes");
        let scope = ReplayScope {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            lsn: Lsn::new(2),
            sources: RowSources::uniform(crate::event::EventSource::User),
            commit_hlc: None,
        };
        let mut sequence = 0;
        let rebuilt = crate::event::wal_replay_timeseries::replayed_timeseries_events(
            &logged,
            &scope,
            &outcomes,
            &mut sequence,
        );
        let images: Vec<rmpv::Value> = rebuilt
            .iter()
            .map(|event| {
                let image = event.new_value.as_deref().expect("insert image");
                crate::util::bounded_msgpack::read_value(image).expect("decode image")
            })
            .collect();
        assert_eq!(images, vec![stored_row(&core, 1)]);
    }

    /// A follower catches up by a snapshot taken after the leader flushed a
    /// column's first rows. It installs the leader's schema, so a committed
    /// row that conflicts with it is rejected on the follower as on the
    /// leader, before and after the follower restarts.
    #[test]
    fn a_follower_restored_from_a_snapshot_rejects_as_the_leader_does() {
        let leader_dir = tempfile::tempdir().expect("tempdir");
        let (mut leader, _tx, _rx) = make_core_with_dir(leader_dir.path());
        let first = resolve(&leader, "metrics extra=1.5 1000000000", 1_000);
        assert_eq!(install_committed(&mut leader, &first, 1).status, Status::Ok);
        leader
            .flush_ts_collection(
                TenantId::new(TENANT),
                DatabaseId::DEFAULT,
                COLLECTION,
                1_000,
            )
            .expect("flush");
        let snapshot = leader.execute_create_tenant_snapshot(&task_at(1), TENANT, false);
        assert_eq!(snapshot.status, Status::Ok, "{:?}", snapshot.error_code);

        let follower_dir = tempfile::tempdir().expect("tempdir");
        let (mut follower, _ftx, _frx) = make_core_with_dir(follower_dir.path());
        let restored = follower.execute_restore_tenant_snapshot(
            &task_at(1),
            TENANT,
            snapshot.payload.as_bytes(),
            true,
            &[],
            &[],
        );
        assert_eq!(restored.status, Status::Ok, "{:?}", restored.error_code);

        // A proposer that never held the column resolves the row whole.
        let conflicting = {
            let scratch = tempfile::tempdir().expect("tempdir");
            let (other, _otx, _orx) = make_core_with_dir(scratch.path());
            resolve(&other, "metrics extra=\"text\" 2000000000", 2_000)
        };
        let rejected = |core: &mut CoreLoop| {
            let response = install_committed(core, &conflicting, 2);
            assert_eq!(response.status, Status::Ok);
            let json = crate::data::executor::response_codec::decode_payload_to_json(
                response.payload.as_bytes(),
            );
            let body: serde_json::Value = sonic_rs::from_str(&json).expect("decode response");
            body["rejected"].clone()
        };
        assert_eq!(rejected(&mut leader), serde_json::json!(1));
        assert_eq!(rejected(&mut follower), serde_json::json!(1));
        drop(follower);
        let (mut restarted, _rtx, _rrx) = make_core_with_dir(follower_dir.path());
        restarted.load_ts_registries().expect("load registries");
        assert_eq!(rejected(&mut restarted), serde_json::json!(1));
    }
}
