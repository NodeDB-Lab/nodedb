// SPDX-License-Identifier: BUSL-1.1

//! Restored sync-frame text as redo units.
//!
//! A backup carries every stored sync-frame text version of a tenant: the
//! `(field, text)` pairs a Lite client's `FtsIndex` frame delivered, keyed
//! by collection, row surrogate and system-time ordinal. An empty version
//! is a tombstone.
//!
//! Each row re-issues as one unit: one `FtsIndex` sub-record per frame
//! version and one `FtsDelete` sub-record per tombstone, in ordinal order,
//! each carrying its source ordinal. Every replica stores each version at
//! that ordinal. A version indexes the collection's declared fields only
//! when the destination holds no document version of the row, the rule a
//! text-index rebuild and a snapshot restore apply. Rows go first, so a
//! restored body is in place before its frames.
//!
//! A version below the row's newest stored frame is history only. The
//! transaction's resolve sets the row's newest backed-up version again
//! above a newer destination frame at or below the RESTORE's cut, as it
//! does for a document row. A destination frame above the cut stays
//! current.
//!
//! A row re-issues under the destination's surrogate of the key the backup
//! carries for its backed-up surrogate. The collection's home assigns it. A
//! row with no carried key takes a fresh surrogate, or re-issues nothing
//! when it holds tombstones only.

use std::collections::{BTreeMap, HashMap};

use nodedb_physical::physical_plan::RestoredSyncedText;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{StorageKey, Surrogate};
use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload, RecordType};

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{SyncedTextEntry, TenantId};
use crate::wal::RedoSubRecord;

use super::super::bind_conflicts::CarriedBind;
use super::super::target::{DatabaseTarget, RestoredName};
use super::commit::commit_collection;
use super::row_keys::{RowKey, destination_keys};
use super::units::{CollectionUnits, RowUnit};

/// Every frame version of one row, `(ordinal, fields)`, under its
/// backed-up surrogate.
type RowFrames = BTreeMap<u32, Versions>;

/// Group `frames` by the collection the source stored them under, then by
/// row, each row's versions in ordinal order. Every frame must name
/// `tenant_id` and the source database of `target`.
fn group_frames(
    tenant_id: u64,
    target: DatabaseTarget,
    frames: Vec<SyncedTextEntry>,
) -> crate::Result<BTreeMap<String, RowFrames>> {
    let mut grouped: BTreeMap<String, RowFrames> = BTreeMap::new();
    for frame in frames {
        if frame.tenant_id != tenant_id || frame.database_id != target.source.as_u64() {
            return Err(Error::Serialization {
                format: "backup".into(),
                detail: format!(
                    "restore: synced text of '{}' names tenant {} in database {}, but sits \
                     with tenant {tenant_id} in database {}",
                    frame.collection,
                    frame.tenant_id,
                    frame.database_id,
                    target.source.as_u64()
                ),
            });
        }
        grouped
            .entry(frame.collection)
            .or_default()
            .entry(frame.surrogate)
            .or_default()
            .push((frame.ordinal, frame.fields));
    }
    for rows in grouped.values_mut() {
        for versions in rows.values_mut() {
            versions.sort_by_key(|(ordinal, _)| *ordinal);
        }
    }
    Ok(grouped)
}

fn encode_error(e: nodedb_wal::WalError) -> Error {
    Error::Serialization {
        format: "wal".into(),
        detail: format!("restore redo: encode synced text: {e}"),
    }
}

/// The sub-record of one frame version of the row `doc_id` at `ordinal`:
/// `FtsIndex` for a version with pairs, `FtsDelete` for a tombstone.
fn frame_op(
    stored: &str,
    doc_id: &str,
    ordinal: i64,
    fields: Vec<(String, String)>,
) -> crate::Result<RedoSubRecord> {
    // A restored frame was judged when it was first delivered. The local
    // producer passes the sync gate.
    let provenance = SyncProvenance::default();
    let (record_type, payload) = if fields.is_empty() {
        let payload = FtsDeletePayload::new(provenance, stored, Some(doc_id.to_string()))
            .at_sys_from(ordinal);
        (RecordType::FtsDelete, payload.to_bytes())
    } else {
        let payload = FtsIndexPayload::new(provenance, stored, doc_id, fields).at_sys_from(ordinal);
        (RecordType::FtsIndex, payload.to_bytes())
    };
    Ok(RedoSubRecord {
        record_type: record_type as u32,
        payload: payload.map_err(encode_error)?,
    })
}

/// Every version of one row, `(ordinal, fields)`, in ordinal order.
type Versions = Vec<(i64, Vec<(String, String)>)>;

/// The key each frame row of `rows` binds, with its versions, in backed-up
/// surrogate order.
///
/// `pks` maps a backed-up surrogate to the key the backup carries for it: a
/// surrogate bind, or the identity of a backed-up document stored under it.
/// A row with no carried key is [`RowKey::Keyless`]. A keyless row whose
/// every version is a tombstone matches no read at any system time, so it
/// re-issues nothing.
fn row_keys(rows: RowFrames, pks: &HashMap<u32, &[u8]>) -> Vec<(RowKey, Versions)> {
    rows.into_iter()
        .filter_map(|(backed_up, versions)| match pks.get(&backed_up) {
            Some(pk) => Some((RowKey::Carried(pk.to_vec()), versions)),
            None if versions.iter().all(|(_, fields)| fields.is_empty()) => None,
            None => Some((RowKey::Keyless, versions)),
        })
        .collect()
}

/// The unit one restored row's frames re-issue as, under `surrogate` in the
/// destination. `pk` is the key the row binds to `surrogate`.
fn row_unit(
    name: &RestoredName,
    surrogate: Surrogate,
    pk: &[u8],
    versions: Vec<(i64, Vec<(String, String)>)>,
) -> crate::Result<RowUnit> {
    let stored = name.stored.as_str().to_string();
    let doc_id = StorageKey::for_surrogate(surrogate).to_string();
    let mut ops = Vec::with_capacity(versions.len());
    for (ordinal, fields) in versions {
        ops.push(frame_op(&stored, &doc_id, ordinal, fields)?);
    }
    Ok(RowUnit {
        ops,
        identities: vec![CarriedIdentity {
            collection: name.bare.clone(),
            pk_bytes: pk.to_vec(),
            surrogate,
        }],
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: Vec::new(),
        columnar_keys: Vec::new(),
        vector_rows: Vec::new(),
        synced_text: vec![RestoredSyncedText {
            collection: stored,
            surrogate: surrogate.as_u32(),
        }],
        timeseries: Vec::new(),
    })
}

/// Re-issue every restored sync-frame text version of one database, one
/// Calvin transaction per batch of rows. `carried` names the key of each
/// backed-up surrogate: the backup's surrogate binds and the identities of
/// its documents. Returns the frame versions re-issued.
pub(in crate::control::backup::restore) async fn reissue_synced_text(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    frames: Vec<SyncedTextEntry>,
    carried: &[CarriedBind],
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
    let mut reissued = 0usize;
    for (source_stored, rows) in group_frames(tenant_id, target, frames)? {
        let name = target.resolve(&source_stored)?;

        // The key of each frame row, and the destination's surrogate of
        // each, in one batch at the home.
        let pks: HashMap<u32, &[u8]> = carried
            .iter()
            .filter(|bind| bind.collection == name.bare)
            .map(|bind| (bind.surrogate, bind.pk.as_slice()))
            .collect();
        let (keys, versions): (Vec<RowKey>, Vec<Versions>) =
            row_keys(rows, &pks).into_iter().unzip();
        if keys.is_empty() {
            continue;
        }
        let bound =
            destination_keys(state, name.key(target.dest), tenant, &name.bare, &keys).await?;

        let mut units = Vec::with_capacity(bound.len());
        for ((surrogate, pk), versions) in bound.into_iter().zip(versions) {
            reissued += versions.len();
            units.push(row_unit(&name, surrogate, &pk, versions)?);
        }
        let collection = CollectionUnits::rows(target.dest, name.bare.clone(), units);
        commit_collection(state, tenant, target.mark(), collection).await?;
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> DatabaseTarget {
        DatabaseTarget {
            source: crate::types::DatabaseId::DEFAULT,
            dest: crate::types::DatabaseId::DEFAULT,
            restore_id: 1,
            restore_cut: i64::MAX,
            event_source: crate::event::EventSource::Restore,
        }
    }

    fn frame(surrogate: u32, ordinal: i64, text: &str) -> SyncedTextEntry {
        SyncedTextEntry {
            database_id: crate::types::DatabaseId::DEFAULT.as_u64(),
            tenant_id: 7,
            collection: "notes".to_string(),
            surrogate,
            ordinal,
            fields: if text.is_empty() {
                Vec::new()
            } else {
                vec![("body".to_string(), text.to_string())]
            },
        }
    }

    /// Each row keeps every frame version, in ordinal order.
    #[test]
    fn frames_group_by_row_in_ordinal_order() {
        let grouped = group_frames(
            7,
            target(),
            vec![frame(3, 20, ""), frame(3, 10, "alpha"), frame(4, 5, "beta")],
        )
        .expect("group");
        let notes = &grouped["notes"];
        let ordinals: Vec<i64> = notes[&3].iter().map(|(o, _)| *o).collect();
        assert_eq!(ordinals, vec![10, 20]);
        assert_eq!(notes[&4].len(), 1);
        assert!(group_frames(8, target(), vec![frame(3, 10, "x")]).is_err());
    }

    /// A row with a carried key binds that key. A keyless row with live text
    /// is kept for a fresh surrogate. A keyless row of tombstones only
    /// re-issues nothing.
    #[test]
    fn rows_split_by_carried_key_and_keyless_tombstones_drop() {
        let grouped = group_frames(
            7,
            target(),
            vec![
                frame(3, 10, "alpha"),
                frame(4, 5, "beta"),
                frame(4, 9, ""),
                frame(5, 7, ""),
            ],
        )
        .expect("group");
        let rows = grouped.into_values().next().expect("notes");
        let pks: HashMap<u32, &[u8]> = [(3, b"n3".as_slice())].into_iter().collect();
        let keyed = row_keys(rows, &pks);
        let keys: Vec<&RowKey> = keyed.iter().map(|(key, _)| key).collect();
        assert_eq!(
            keys,
            [&RowKey::Carried(b"n3".to_vec()), &RowKey::Keyless],
            "row 5 holds tombstones only"
        );
        assert_eq!(keyed[1].1.len(), 2, "row 4 keeps every version");
    }

    /// A row's unit carries one sub-record per version at its source
    /// ordinal, a tombstone as `FtsDelete`, and locks the row's surrogate.
    #[test]
    fn a_row_unit_keeps_each_version_at_its_ordinal() {
        let name = target().bare("notes");
        let versions = vec![
            (10, vec![("body".to_string(), "alpha".to_string())]),
            (20, Vec::new()),
        ];
        let unit = row_unit(&name, Surrogate::new(9), b"n1", versions).expect("unit");
        assert_eq!(unit.ops.len(), 2);
        assert_eq!(unit.ops[0].record_type, RecordType::FtsIndex as u32);
        let index = FtsIndexPayload::from_bytes(&unit.ops[0].payload).expect("index");
        assert_eq!(index.sys_from, Some(10));
        assert_eq!(
            index.doc_id,
            StorageKey::for_surrogate(Surrogate::new(9)).to_string()
        );
        assert_eq!(unit.ops[1].record_type, RecordType::FtsDelete as u32);
        let delete = FtsDeletePayload::from_bytes(&unit.ops[1].payload).expect("delete");
        assert_eq!(delete.sys_from, Some(20));
        assert_eq!(unit.synced_text[0].surrogate, 9);
        assert_eq!(unit.identities[0].surrogate, Surrogate::new(9));
    }
}
