// SPDX-License-Identifier: BUSL-1.1

//! Timeseries write events rebuilt from a WAL record.
//!
//! A timeseries ingest resolves before its record is appended. The record, a
//! standalone `TimeseriesBatch` or a redo record's sub-record, carries the
//! resolved rows. When some Event Plane consumer read the collection at
//! resolve, each row carries its image, and the install emitted one Insert
//! per landed row. This rebuilds the same events from the same bytes: the
//! collection, `RowId::Batch`, the row image, the record's LSN and commit
//! HLC. The caller numbers them per record, as the producer numbered the
//! install's events, so a rebuilt event names the event the ring dropped.
//!
//! A record that carries no resolved rows, or whose batch emitted no events,
//! rebuilds nothing.
//!
//! An install that stored by column name wrote its outcome
//! (`install_outcome`): the rows that landed and how each reads. Such a
//! batch rebuilds one Insert per landed row, with its stored image. Every
//! other batch stored each row as it carries it.

use std::sync::Arc;

use tracing::warn;

use crate::engine::timeseries::install_outcome::TsOutcomeIndex;
use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, ResolvedTsBatch};
use crate::event::types::{RecordPosition, RowId, WriteEvent, WriteOp};
use crate::event::wal_replay_scope::ReplayScope;

/// The Insert events one `TimeseriesBatch` payload carries, in row order.
/// `outcomes` holds the install outcomes of the core that installed it.
pub(crate) fn replayed_timeseries_events(
    payload: &[u8],
    scope: &ReplayScope,
    outcomes: &TsOutcomeIndex,
    sequence: &mut u64,
) -> Vec<WriteEvent> {
    let Ok(record) = crate::wal::decode_batch_record(payload) else {
        warn!(
            lsn = scope.lsn.as_u64(),
            "WAL replay: a TimeseriesBatch payload matched no record shape; no event rebuilt"
        );
        return Vec::new();
    };
    if record.kind.as_deref() != Some("timeseries")
        || record.format.as_deref() != Some(RESOLVED_INGEST_FORMAT)
    {
        return Vec::new();
    }
    let batch = match ResolvedTsBatch::from_bytes(&record.payload) {
        Ok(batch) => batch,
        Err(error) => {
            warn!(
                lsn = scope.lsn.as_u64(),
                error = %error,
                "WAL replay: a resolved timeseries batch did not decode; no event rebuilt"
            );
            return Vec::new();
        }
    };
    if !batch.emits_events {
        return Vec::new();
    }
    // The images of the rows the install stored: its outcome's when it
    // wrote one, else every row's own.
    let images: Vec<Vec<u8>> = match outcomes.lookup(scope.lsn.as_u64(), &record.collection, &batch)
    {
        Ok(Some(outcome)) => outcome.images,
        Ok(None) => batch.rows.into_iter().map(|row| row.image).collect(),
        Err(error) => {
            warn!(
                lsn = scope.lsn.as_u64(),
                error = %error,
                "WAL replay: a timeseries install outcome did not read; no event rebuilt"
            );
            return Vec::new();
        }
    };
    let collection: Arc<str> = Arc::from(record.collection.as_str());
    let mut events = Vec::with_capacity(images.len());
    for image in images {
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(image.as_slice()));
        *sequence += 1;
        events.push(WriteEvent {
            sequence: *sequence,
            collection: Arc::clone(&collection),
            op: WriteOp::Insert,
            row_id: RowId::Batch,
            lsn: scope.lsn,
            record: Some(RecordPosition::first(scope.lsn)),
            database_id: scope.database_id,
            tenant_id: scope.tenant_id,
            vshard_id: scope.vshard_id,
            source: scope.sources.other,
            new_value: Some(Arc::from(image)),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc: scope.commit_hlc,
            image_fault: None,
        });
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::wal_dispatch::{
        TimeseriesIngestRecord, encode_timeseries_ingest_payload,
    };
    use crate::engine::timeseries::columnar_memtable::{ColumnType, ColumnValue, TimeKind};
    use crate::engine::timeseries::resolved_ingest::{ResolvedTsRow, TsDriftPolicy};
    use crate::event::EventSource;
    use crate::event::wal_replay_scope::RowSources;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    const COMMIT_HLC: u64 = 7_000_000;

    fn scope() -> ReplayScope {
        ReplayScope {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            lsn: Lsn::new(42),
            sources: RowSources::committed_redo(EventSource::User),
            commit_hlc: Some(COMMIT_HLC),
        }
    }

    fn image(value: f64) -> Vec<u8> {
        let row = rmpv::Value::Map(vec![(rmpv::Value::from("value"), rmpv::Value::from(value))]);
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, &row).expect("encode image");
        bytes
    }

    fn record(emits_events: bool, images: &[Vec<u8>]) -> Vec<u8> {
        let batch = ResolvedTsBatch {
            measurement: "metrics".into(),
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            drift: TsDriftPolicy::Refuse,
            now_ms: 1_000,
            resolved_bytes: 64,
            emits_events,
            rows: images
                .iter()
                .enumerate()
                .map(|(i, image)| ResolvedTsRow {
                    line: i as u64,
                    tags: Vec::new(),
                    timestamp_ms: 1_000 + i as i64,
                    values: vec![
                        ColumnValue::Timestamp(1_000 + i as i64),
                        ColumnValue::Float64(i as f64),
                    ],
                    absent: Vec::new(),
                    image: image.clone(),
                })
                .collect(),
            rejected: 0,
            first_rejection: None,
        };
        let bytes = batch.to_bytes().expect("encode batch");
        encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection: "metrics",
            payload: &bytes,
            provenance: None,
            format: RESOLVED_INGEST_FORMAT,
            default_timestamp_ms: 1_000,
        })
        .expect("encode record")
    }

    #[test]
    fn a_resolved_batch_with_events_rebuilds_one_insert_per_row() {
        let rows = vec![image(1.0), image(2.0)];
        let mut sequence = 0;
        let events = replayed_timeseries_events(
            &record(true, &rows),
            &scope(),
            &TsOutcomeIndex::default(),
            &mut sequence,
        );

        assert_eq!(events.len(), 2);
        assert_eq!(sequence, 2);
        for (event, row) in events.iter().zip(&rows) {
            assert_eq!(event.collection.as_ref(), "metrics");
            assert_eq!(event.op, WriteOp::Insert);
            assert_eq!(event.row_id, RowId::Batch);
            assert_eq!(event.record, Some(RecordPosition::first(Lsn::new(42))));
            assert_eq!(event.commit_hlc, Some(COMMIT_HLC));
            assert_eq!(event.source, EventSource::User);
            assert_eq!(event.new_value.as_deref(), Some(row.as_slice()));
        }
    }

    /// An install that stored by name wrote its outcome. Catch-up rebuilds
    /// one event per landed row, with the image the install stored, and none
    /// for a rejected row.
    #[test]
    fn a_batch_with_an_install_outcome_rebuilds_only_its_landed_rows() {
        use crate::engine::timeseries::install_outcome::{
            TsInstallOutcome, batch_digest, write_install_outcome,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let payload = record(true, &[image(1.0), image(2.0)]);
        let logged = crate::wal::decode_batch_record(&payload).expect("decode record");
        let batch = ResolvedTsBatch::from_bytes(&logged.payload).expect("decode batch");
        let stored = image(9.0);
        write_install_outcome(
            dir.path(),
            42,
            TsInstallOutcome {
                collection: "metrics".into(),
                digest: batch_digest(&batch).expect("digest"),
                landed: vec![1],
                images: vec![stored.clone()],
            },
        )
        .expect("write outcome");
        let outcomes = TsOutcomeIndex::load(dir.path()).expect("load outcomes");

        let mut sequence = 0;
        let events = replayed_timeseries_events(&payload, &scope(), &outcomes, &mut sequence);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].new_value.as_deref(), Some(stored.as_slice()));
    }

    #[test]
    fn a_resolved_batch_that_emitted_no_events_rebuilds_nothing() {
        let mut sequence = 0;
        let events = replayed_timeseries_events(
            &record(false, &[Vec::new()]),
            &scope(),
            &TsOutcomeIndex::default(),
            &mut sequence,
        );
        assert!(events.is_empty());
        assert_eq!(sequence, 0);
    }

    #[test]
    fn an_unresolved_ingest_record_rebuilds_nothing() {
        let payload = encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection: "metrics",
            payload: b"metrics value=1",
            provenance: None,
            format: "ilp",
            default_timestamp_ms: 1_000,
        })
        .expect("encode record");
        let mut sequence = 0;
        assert!(
            replayed_timeseries_events(
                &payload,
                &scope(),
                &TsOutcomeIndex::default(),
                &mut sequence
            )
            .is_empty()
        );
        assert_eq!(sequence, 0);
    }
}
