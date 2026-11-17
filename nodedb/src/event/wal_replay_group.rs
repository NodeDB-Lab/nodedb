// SPDX-License-Identifier: BUSL-1.1

//! Event Plane replay of a write's `WriteGroup` records.
//!
//! The records of one write group carry the rows one write stored. Every
//! event the write emitted live names the group's origin LSN as its record
//! position: the origin is the LSN the write ran at on its core. So every
//! event rebuilt from any record of the group names the origin too, and the
//! delivery guard refuses a rebuilt copy of an event the ring already
//! delivered, and the reverse.

use nodedb_wal::record::RecordType;

use crate::engine::timeseries::install_outcome::TsOutcomeIndex;
use crate::event::types::{RecordPosition, WriteEvent};
use crate::event::wal_replay::{apply_committed_row_metadata, single_row_events};
use crate::event::wal_replay_kind::row_kind;
use crate::event::wal_replay_scope::{ReplayScope, RowSources};
use crate::types::Lsn;

/// The events of one `WriteGroup` record, each at the group's origin.
///
/// Malformed payloads return errors without publishing partial events.
pub(super) fn group_record_events(
    payload: &[u8],
    scope: &ReplayScope,
    outcomes: &TsOutcomeIndex,
    sequence: &mut u64,
) -> crate::Result<Vec<WriteEvent>> {
    let group = crate::wal::WriteGroupRecord::from_bytes(payload)?;
    let mut next_sequence = *sequence;
    let origin = Lsn::new(group.group.origin_at(scope.lsn.as_u64()));
    // A continuation of a committed transaction record rebuilds its rows as
    // the record's own rows rebuild: committed sources and net kinds.
    let scope = match &group.redo {
        Some(_) => ReplayScope {
            sources: RowSources::committed_redo(scope.sources.other),
            ..*scope
        },
        None => *scope,
    };
    let mut events = Vec::new();
    for mut sub in group.ops {
        if let Some(installed) =
            crate::wal::redo::installed_document::InstalledDocumentRedo::<u32>::decode(
                &sub.payload,
            )?
        {
            sub.payload = installed.resolve(&group.schemas)?.encode()?;
        }
        let Some(record_type) = RecordType::from_raw(sub.record_type) else {
            continue;
        };
        if let Some(kind) = row_kind(record_type) {
            events.extend(single_row_events(
                kind,
                &sub.payload,
                &scope,
                outcomes,
                &mut next_sequence,
            )?);
        }
    }
    if let Some(continued) = &group.redo {
        apply_committed_row_metadata(
            &mut events,
            &crate::wal::RowSourceIndex::new(&continued.row_sources),
            &continued.row_changes,
            &scope,
        );
    }
    for event in &mut events {
        event.record = Some(RecordPosition::first(origin));
    }
    *sequence = next_sequence;
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::EventSource;
    use crate::event::wal_replay_scope::RowSources;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::{RedoSubRecord, WriteGroup, WriteGroupRecord};

    fn scope(lsn: u64) -> ReplayScope {
        ReplayScope {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            lsn: Lsn::new(lsn),
            sources: RowSources::uniform(EventSource::User),
            commit_hlc: None,
        }
    }

    fn doc_put(id: &str) -> RedoSubRecord {
        let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&("orders", id, b"v".to_vec(), prov, 7u32))
                .expect("encode"),
        }
    }

    /// A part's events name the origin, so they match the live events of the
    /// write, which ran at the origin's LSN.
    #[test]
    fn a_part_rebuilds_its_events_at_the_origin() {
        let record = WriteGroupRecord {
            schemas: Vec::new(),
            group: WriteGroup::part_of(40, 1, 1),
            ops: vec![doc_put("o1"), doc_put("o2")],
            redo: None,
        };
        let mut sequence = 0;
        let events = group_record_events(
            &record.to_bytes().expect("encode"),
            &scope(55),
            &TsOutcomeIndex::default(),
            &mut sequence,
        )
        .expect("events");
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|e| e.record == Some(RecordPosition::first(Lsn::new(40))))
        );
        assert!(events.iter().all(|e| e.source == EventSource::User));
    }

    fn part_events(origin: u64, lsn: u64, part: u32, ids: &[&str]) -> Vec<WriteEvent> {
        let record = WriteGroupRecord {
            schemas: Vec::new(),
            group: WriteGroup::part_of(origin, part, 2),
            ops: ids.iter().copied().map(doc_put).collect(),
            redo: None,
        };
        let mut sequence = 0;
        group_record_events(
            &record.to_bytes().expect("encode"),
            &scope(lsn),
            &TsOutcomeIndex::default(),
            &mut sequence,
        )
        .expect("events")
    }

    /// A crash after the ring delivered a write's events, and before the
    /// consumer persisted its watermark, makes catch-up rebuild the events
    /// from the write's parts. Each rebuilt copy names the position and
    /// occurrence of its live copy, even with another write's record between
    /// the parts, so the delivery guard refuses every one.
    #[test]
    fn rebuilt_part_events_are_refused_after_their_live_copies() {
        use crate::event::consumer::delivery::DeliveryGuard;
        use crate::event::record_numbering::RecordNumbering;

        // Write 40 stores o1, o2 and o1 again, in two parts. A part of write
        // 35 stores x, and lands between them.
        let mut rebuilt = part_events(40, 41, 1, &["o1", "o2"]);
        rebuilt.extend(part_events(35, 42, 2, &["x"]));
        rebuilt.extend(part_events(40, 43, 2, &["o1"]));
        let mut catch_up = RecordNumbering::new();
        for event in &mut rebuilt {
            catch_up.stamp(event);
        }

        // The producer numbers each write's events as it emits them.
        let mut live: Vec<WriteEvent> = rebuilt.clone();
        live.sort_by_key(|event| event.record.map(|r| r.lsn));
        let mut producer = RecordNumbering::new();
        for event in &mut live {
            producer.stamp(event);
        }

        let mut guard = DeliveryGuard::new(Lsn::new(30));
        assert!(live.iter().all(|event| guard.admit(event)));
        assert!(
            rebuilt.iter().all(|event| !guard.admit(event)),
            "no rebuilt event publishes twice"
        );
        let occurrences: Vec<u32> = rebuilt
            .iter()
            .map(|event| event.record.map_or(u32::MAX, |r| r.occurrence))
            .collect();
        assert_eq!(occurrences, [0, 0, 0, 1]);
    }

    #[test]
    fn an_opening_record_is_its_own_origin() {
        let record = WriteGroupRecord {
            schemas: Vec::new(),
            group: WriteGroup::OPENING,
            ops: vec![doc_put("o1")],
            redo: None,
        };
        let mut sequence = 0;
        let events = group_record_events(
            &record.to_bytes().expect("encode"),
            &scope(12),
            &TsOutcomeIndex::default(),
            &mut sequence,
        )
        .expect("events");
        assert_eq!(events[0].record, Some(RecordPosition::first(Lsn::new(12))));
    }
    #[test]
    fn installed_group_events_preserve_public_bytes_and_origin() {
        use crate::bridge::envelope::RowVersion;
        use crate::types::installed_document::InstalledDocument;
        use crate::wal::redo::installed_document::InstalledDocumentRedo;
        use nodedb_physical::physical_plan::StorageMode;
        let canonical = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(
            std::collections::HashMap::from([("n".to_string(), nodedb_types::Value::Integer(7))]),
        ))
        .expect("canonical");
        let event_new_value =
            nodedb_types::json_to_msgpack(&serde_json::json!({"n": 7})).expect("event");
        assert_ne!(canonical, event_new_value);
        let installed = InstalledDocumentRedo {
            collection: "orders".into(),
            document_id: "o1".into(),
            surrogate: 7,
            event: crate::types::document_event::DocumentPutEvent {
                new_value: event_new_value.clone(),
                old_value: Some(
                    nodedb_types::json_to_msgpack(&serde_json::json!({"n": 3})).expect("prior"),
                ),
            },
            image: InstalledDocument {
                canonical: canonical.clone(),
                stored: vec![0x91, 7],
                version: RowVersion {
                    sys_from: 12 * nodedb_types::NANOS_PER_MS,
                    valid_from_ms: 3,
                    valid_until_ms: 8,
                },
                schema: 0u32,
                chain_head: None,
            },
        };
        let mut record = WriteGroupRecord {
            schemas: vec![StorageMode::Schemaless],
            group: WriteGroup::part_of(40, 1, 1),
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: installed.encode().expect("installed"),
            }],
            redo: None,
        };
        let mut sequence = 0;
        let events = group_record_events(
            &record.to_bytes().expect("group"),
            &scope(55),
            &TsOutcomeIndex::default(),
            &mut sequence,
        )
        .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, crate::event::WriteOp::Update);
        assert_eq!(
            events[0].new_value.as_deref(),
            Some(event_new_value.as_slice())
        );
        assert_eq!(
            events[0].old_value.as_deref(),
            installed.event.old_value.as_deref()
        );
        assert_eq!(events[0].record, Some(RecordPosition::first(Lsn::new(40))));
        assert_eq!(events[0].system_time_ms, Some(12));
        assert_eq!(events[0].valid_time_ms, Some(3));
        record.schemas.clear();
        record.ops.insert(0, doc_put("before"));
        let prior_sequence = sequence;
        assert!(
            group_record_events(
                &record.to_bytes().expect("group"),
                &scope(55),
                &TsOutcomeIndex::default(),
                &mut sequence
            )
            .is_err()
        );
        assert_eq!(
            sequence, prior_sequence,
            "a malformed later subrecord publishes no earlier event numbers"
        );
    }
}
