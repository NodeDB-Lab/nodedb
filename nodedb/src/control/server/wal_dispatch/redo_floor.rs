// SPDX-License-Identifier: BUSL-1.1

//! Raise a session commit's redo stamps to the ordinal its record stamps,
//! before the record is appended.
//!
//! A session commit's resolve stamps its versions from the core clock, and
//! no reservation covers those stamps. The record's ordinal is reserved from
//! the moment the leader's write gate draws it until the record installed
//! on the leader. Every version the record installs takes at least that
//! ordinal (`RedoRecord::raise_stamps_to`), so no read cut closes between a
//! version's stamp and its install.
//!
//! A Calvin slice stamps the transaction's ordinal, and a RESTORE keeps each
//! version's historical system time: neither is raised.

use nodedb_physical::physical_plan::{MetaOp, RedoOrigin};

use crate::bridge::envelope::PhysicalPlan;
use crate::wal::{RedoRecord, RedoStreamRef};

/// Raise the stamps of `plan`'s redo to `ordinal` when `plan` installs a
/// session commit's redo. Every other plan stays as it is.
///
/// The funnel calls it once the write's ordinal is fixed and before the
/// write's WAL record exists, so the record holds the raised stamps.
///
/// A whole record's bytes also take the plan's `sum_targets` and `pins`.
/// So the WAL append stores the bytes as they are, with no second encode.
///
/// `Err` when the redo does not decode or re-encode.
pub fn floor_redo_stamps(plan: &mut PhysicalPlan, ordinal: Option<i64>) -> crate::Result<()> {
    let Some(floor) = ordinal else {
        return Ok(());
    };
    let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo,
        sum_targets,
        origin: RedoOrigin::Commit,
        calvin: None,
        pins,
        ..
    }) = plan
    else {
        return Ok(());
    };
    let mut record = RedoRecord::from_bytes(redo)?;
    if record.calvin_stamp.is_some() {
        return Ok(());
    }
    // A stream marker's versions sit in its chunks: the reference carries
    // the floor, and every install and replay raises them to it.
    if let Some(mut reference) = RedoStreamRef::of_marker(&record)? {
        reference.stamp_floor = Some(floor);
        reference.store_in(&mut record)?;
        *redo = record.to_bytes()?;
        return Ok(());
    }
    let raised = record.raise_stamps_to(floor)?;
    // A whole record's WAL copy holds the targets and pins its install uses.
    // Writing them in this encode lets the WAL append store these bytes.
    let folds = record.sum_targets != *sum_targets || record.pins != *pins;
    if folds {
        record.sum_targets = sum_targets.clone();
        record.pins = pins.clone();
    }
    if raised || folds {
        *redo = record.to_bytes()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::RecordType;

    use super::*;
    use crate::wal::{CalvinStamp, DocumentPutRedo, RedoSubRecord};

    fn redo_plan(origin: RedoOrigin, calvin_stamp: Option<CalvinStamp>) -> PhysicalPlan {
        let none: Option<SyncProvenance> = None;
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: zerompk::to_msgpack_vec(&(
                    "c",
                    "d1",
                    vec![1u8],
                    none,
                    3u32,
                    100i64,
                    i64::MIN,
                    i64::MAX,
                ))
                .expect("encode put"),
            }],
            calvin_stamp,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        }
        .to_bytes()
        .expect("encode redo");
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo,
            collections: vec!["c".into()],
            sum_targets: Vec::new(),
            origin,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: None,
            pins: Default::default(),
        })
    }

    fn put_stamp(plan: &PhysicalPlan) -> i64 {
        let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { redo, .. }) = plan else {
            panic!("a redo install");
        };
        let record = RedoRecord::from_bytes(redo).expect("decode redo");
        DocumentPutRedo::decode(&record.ops[0].payload)
            .and_then(|put| put.stamp)
            .expect("stamped put")
            .sys_from
    }

    #[test]
    fn a_session_commit_takes_its_record_ordinal() {
        let mut plan = redo_plan(RedoOrigin::Commit, None);
        floor_redo_stamps(&mut plan, Some(500)).expect("floor");
        assert_eq!(put_stamp(&plan), 500);
    }

    #[test]
    fn a_restore_a_calvin_record_and_an_unstamped_write_keep_their_stamps() {
        let calvin = CalvinStamp {
            epoch: 1,
            position: 0,
            vshard_id: 0,
            mirror_database: None,
        };
        for (mut plan, ordinal) in [
            (redo_plan(RedoOrigin::Restore, None), Some(500)),
            (redo_plan(RedoOrigin::Commit, Some(calvin)), Some(500)),
            (redo_plan(RedoOrigin::Commit, None), None),
        ] {
            floor_redo_stamps(&mut plan, ordinal).expect("floor");
            assert_eq!(put_stamp(&plan), 100);
        }
    }

    /// A floored session commit's bytes hold the plan's pins, so the WAL
    /// append stores them as they are.
    #[test]
    fn a_floored_commit_holds_its_pins_for_the_wal_append() {
        use std::borrow::Cow;

        use nodedb_physical::physical_plan::{CrsPin, RedoPins};

        let pins = RedoPins {
            configs: Vec::new(),
            crs: Some(std::sync::Arc::new(CrsPin {
                digest: 41,
                bytes: vec![0x90].into(),
            })),
        };
        let mut plan = redo_plan(RedoOrigin::Commit, None);
        let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { pins: held, .. }) = &mut plan else {
            panic!("a redo install");
        };
        *held = pins.clone();
        floor_redo_stamps(&mut plan, Some(500)).expect("floor");
        let PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo, sum_targets, ..
        }) = &plan
        else {
            panic!("a redo install");
        };
        let payload =
            super::super::redo_marker::redo_wal_payload(redo, sum_targets, &pins).expect("payload");
        assert!(matches!(payload.bytes, Cow::Borrowed(_)));
        assert_eq!(RedoRecord::from_bytes(redo).expect("decode").pins, pins);
        assert_eq!(put_stamp(&plan), 500);
    }
}
