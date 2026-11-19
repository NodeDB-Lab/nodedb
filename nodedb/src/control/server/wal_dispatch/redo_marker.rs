// SPDX-License-Identifier: BUSL-1.1

//! The WAL payload of a committed redo's `TransactionRedo` record.
//!
//! A whole record's WAL copy carries the materialized-sum targets its
//! install folds with: the plan's `sum_targets`, the field the Data Plane
//! install reads. The fold rows become durable only with the install's
//! stored write set, and a crash can cut that write set after this record is
//! durable. Boot then folds the record from the targets its WAL copy holds,
//! the same targets on every replica and at every restart. It carries the
//! plan's `pins` the same way, so boot judges the record under the config
//! content the proposer pinned.
//!
//! A stream marker's plan carries the row changes the Control-Plane change
//! stream publishes. The chunk records hold the same changes, and every
//! whole-WAL reader rebuilds the marker from them. So the marker's WAL record
//! holds its reference alone, and stays one small record however many rows
//! the stream changes. Its sum targets and pins ride the stream content's
//! header, which the install reads them from.

use std::borrow::Cow;

use nodedb_physical::physical_plan::{RedoPins, RedoSumTargets};

use crate::wal::{RedoRecord, RedoStreamRef};

/// What the WAL record of a committed redo holds.
pub(super) struct RedoWalPayload<'a> {
    pub bytes: Cow<'a, [u8]>,
    /// The redo is a stream marker.
    pub stream_marker: bool,
}

/// The bytes the WAL record of the redo `redo` holds. `sum_targets` is the
/// resolution the install of `redo` folds with. `pins` is the config
/// content its validate is judged under.
pub(super) fn redo_wal_payload<'a>(
    redo: &'a [u8],
    sum_targets: &[RedoSumTargets],
    pins: &RedoPins,
) -> crate::Result<RedoWalPayload<'a>> {
    let mut record = RedoRecord::from_bytes(redo)?;
    if RedoStreamRef::of_marker(&record)?.is_some() {
        let bytes = if record.row_changes.is_empty() {
            Cow::Borrowed(redo)
        } else {
            record.row_changes = Vec::new();
            Cow::Owned(record.to_bytes()?)
        };
        return Ok(RedoWalPayload {
            bytes,
            stream_marker: true,
        });
    }
    let bytes = if record.sum_targets.as_slice() == sum_targets && record.pins == *pins {
        Cow::Borrowed(redo)
    } else {
        record.sum_targets = sum_targets.to_vec();
        record.pins = pins.clone();
        Cow::Owned(record.to_bytes()?)
    };
    Ok(RedoWalPayload {
        bytes,
        stream_marker: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{RedoRowChange, RedoRowKind, RedoStreamId};
    use nodedb_physical::physical_plan::ResolvedSumTarget;

    fn reference() -> RedoStreamRef {
        RedoStreamRef {
            stream: RedoStreamId::Session {
                vshard: 1,
                idempotency_key: 2,
            },
            len: 0,
            chunks: Vec::new(),
            publish_position: None,
            entry: None,
            stamp_floor: None,
        }
    }

    fn inline() -> RedoRecord {
        RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: vec![RedoRowChange {
                collection: "orders".into(),
                row: "o1".into(),
                kind: RedoRowKind::Insert,
            }],
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        }
    }

    fn targets() -> Vec<RedoSumTargets> {
        vec![RedoSumTargets {
            collection: "orders".into(),
            resolved: vec![ResolvedSumTarget::new(
                "balances",
                "a1",
                nodedb_types::Surrogate::new(7),
            )],
        }]
    }

    #[test]
    fn a_markers_wal_record_drops_its_row_changes() {
        let change = RedoRowChange {
            collection: "orders".into(),
            row: "o1".into(),
            kind: RedoRowKind::Insert,
        };
        let marker = reference()
            .marker(None, None, vec![change])
            .expect("marker")
            .to_bytes()
            .expect("encode");
        let payload = redo_wal_payload(&marker, &targets(), &pins()).expect("payload");
        assert!(payload.stream_marker);
        let stored = RedoRecord::from_bytes(&payload.bytes).expect("decode");
        assert!(stored.row_changes.is_empty());
        assert!(
            stored.sum_targets.is_empty(),
            "a marker's targets ride its content header"
        );
        assert!(
            stored.pins.is_empty(),
            "a marker's pins ride its content header"
        );
        assert_eq!(
            RedoStreamRef::of_marker(&stored).expect("ref"),
            Some(reference())
        );
    }

    #[test]
    fn an_inline_redo_with_no_targets_is_stored_verbatim() {
        let inline = inline().to_bytes().expect("encode");
        let payload = redo_wal_payload(&inline, &[], &RedoPins::default()).expect("payload");
        assert!(!payload.stream_marker);
        assert!(matches!(payload.bytes, Cow::Borrowed(_)));
    }

    fn pins() -> RedoPins {
        RedoPins {
            configs: vec![std::sync::Arc::new(
                nodedb_physical::physical_plan::ConfigPin {
                    key: nodedb_physical::physical_plan::ConfigPinKey {
                        database_id: nodedb_types::DatabaseId::DEFAULT,
                        tenant_id: nodedb_types::TenantId::new(1),
                        collection: "orders".into(),
                    },
                    digest: 31,
                    wire: vec![0x91, 0x01].into(),
                },
            )],
            crs: Some(std::sync::Arc::new(
                nodedb_physical::physical_plan::CrsPin {
                    digest: 32,
                    bytes: vec![0x90].into(),
                },
            )),
        }
    }

    /// A whole record's WAL copy carries the targets its install folds with
    /// and the pins its validate is judged under, and decodes to them.
    #[test]
    fn an_inline_redo_carries_its_install_targets_and_pins() {
        let record = inline();
        let bytes = record.to_bytes().expect("encode");
        let stored = RedoRecord::from_bytes(
            &redo_wal_payload(&bytes, &targets(), &pins())
                .expect("payload")
                .bytes,
        )
        .expect("decode");
        assert_eq!(stored.sum_targets, targets());
        assert_eq!(stored.pins, pins());
        assert_eq!(stored.ops, record.ops);
        assert_eq!(stored.row_changes, record.row_changes);
    }
}
