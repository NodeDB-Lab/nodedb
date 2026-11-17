// SPDX-License-Identifier: BUSL-1.1

//! The newest backed-up sync-frame version a RESTORE sets again above the
//! destination's newer frames.
//!
//! A restored row's frame versions install at their source ordinals. A
//! version below the row's newest stored frame is history only. A
//! destination frame above the backup's newest one, at or below the
//! RESTORE's cut, holds the row's current text. The resolve adds the
//! backup's newest frame once more, stamped above it (`restore_reassert`).
//! A destination frame above the cut is a write that followed the RESTORE.
//! It stays current, as a document version above the cut does.
//!
//! A backed-up tombstone is set again only over a live destination frame.
//! A row the destination already holds absent stays as it is.

use std::collections::BTreeMap;

use nodedb_types::{Surrogate, TenantId};
use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload, RecordType};

use crate::data::executor::core_loop::CoreLoop;
use crate::wal::RedoSubRecord;

use super::restore_reassert::RestoreScope;

/// One restored frame version: a frame with pairs, or a tombstone.
pub(super) enum RestoredFrame {
    Index(FtsIndexPayload),
    Delete(FtsDeletePayload),
}

impl RestoredFrame {
    /// The restored frame version `payload` carries. `None` for a record
    /// that is no frame version at a source ordinal.
    pub(super) fn decode(record_type: u32, payload: &[u8]) -> crate::Result<Option<Self>> {
        let frame = match RecordType::from_raw(record_type) {
            Some(RecordType::FtsIndex) => Self::Index(FtsIndexPayload::from_bytes(payload)?),
            Some(RecordType::FtsDelete) => Self::Delete(FtsDeletePayload::from_bytes(payload)?),
            _ => return Ok(None),
        };
        Ok((frame.sys_from().is_some() && frame.doc_id().is_some()).then_some(frame))
    }

    fn sys_from(&self) -> Option<i64> {
        match self {
            Self::Index(index) => index.sys_from,
            Self::Delete(delete) => delete.sys_from,
        }
    }

    fn collection(&self) -> &str {
        match self {
            Self::Index(index) => &index.collection,
            Self::Delete(delete) => &delete.collection,
        }
    }

    fn doc_id(&self) -> Option<&str> {
        match self {
            Self::Index(index) => Some(&index.doc_id),
            Self::Delete(delete) => delete.doc_id.as_deref(),
        }
    }

    /// The sub-record of this version at `stamp`.
    fn at(self, stamp: i64) -> crate::Result<RedoSubRecord> {
        let (record_type, payload) = match self {
            Self::Index(index) => (RecordType::FtsIndex, index.at_sys_from(stamp).to_bytes()?),
            Self::Delete(delete) => (RecordType::FtsDelete, delete.at_sys_from(stamp).to_bytes()?),
        };
        Ok(RedoSubRecord {
            record_type: record_type as u32,
            payload,
        })
    }
}

/// The newest restored frame version of each row in one batch, keyed by
/// `(collection, doc_id)`.
#[derive(Default)]
pub(super) struct NewestFrames {
    rows: BTreeMap<(String, String), RestoredFrame>,
}

impl NewestFrames {
    /// Keep `frame` when it is its row's newest version so far.
    pub(super) fn note(&mut self, frame: RestoredFrame) {
        let Some(doc_id) = frame.doc_id() else {
            return;
        };
        let key = (frame.collection().to_string(), doc_id.to_string());
        let newer = self
            .rows
            .get(&key)
            .is_none_or(|held| held.sys_from() <= frame.sys_from());
        if newer {
            self.rows.insert(key, frame);
        }
    }
}

/// The surrogate a frame's hex `doc_id` names, as the frame's apply reads it.
fn frame_surrogate(collection: &str, doc_id: &str) -> crate::Result<Surrogate> {
    u32::from_str_radix(doc_id, 16)
        .map(Surrogate::new)
        .map_err(|e| crate::Error::Serialization {
            format: "wal".into(),
            detail: format!(
                "restore resolve: frame of '{collection}' names doc_id '{doc_id}', not a hex \
                 surrogate: {e}"
            ),
        })
}

impl CoreLoop {
    /// The versions that set each row of `newest` to its newest backed-up
    /// frame again, for every row the destination holds a newer frame of
    /// at or below the RESTORE's cut.
    pub(super) fn reassert_restored_frames(
        &self,
        scope: RestoreScope,
        newest: NewestFrames,
    ) -> crate::Result<Vec<RedoSubRecord>> {
        let mut out = Vec::new();
        for ((collection, doc_id), frame) in newest.rows {
            let Some(backup_newest) = frame.sys_from() else {
                continue;
            };
            let surrogate = frame_surrogate(&collection, &doc_id)?;
            let head = self.inverted.synced_text_head(
                scope.database_id.as_u64(),
                TenantId::new(scope.tid),
                &collection,
                surrogate,
            )?;
            let Some(stamp) = scope.reassert_stamp(backup_newest, head.map(|h| h.ordinal)) else {
                continue;
            };
            if matches!(frame, RestoredFrame::Delete(_)) && head.is_some_and(|h| h.tombstone) {
                continue;
            }
            out.push(frame.at(stamp)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{DatabaseId, StorageKey};

    use super::super::restore_changes::RestoreChanges;
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::fts_sync::SyncedFrame;

    const TID: u64 = 1;

    fn declare_notes(core: &mut CoreLoop) {
        let mut config = crate::engine::document::store::CollectionConfig::new("notes");
        config.text_fields =
            nodedb_physical::physical_plan::TextFieldSet::all_readable(vec!["body".to_string()]);
        core.doc_configs.insert(
            (DatabaseId::DEFAULT, TenantId::new(TID), "notes".to_string()),
            config,
        );
    }

    fn body(text: &str) -> Vec<(String, String)> {
        vec![("body".to_string(), text.to_string())]
    }

    fn hits(core: &CoreLoop, query: &str) -> Vec<u32> {
        let mut ids: Vec<u32> = core
            .inverted
            .search(
                DatabaseId::DEFAULT.as_u64(),
                TenantId::new(TID),
                "notes",
                nodedb_fts::FtsSearchParams {
                    query,
                    top_k: 10,
                    fuzzy_enabled: false,
                    mode: nodedb_fts::QueryMode::And,
                    prefilter: None,
                },
            )
            .expect("search")
            .into_iter()
            .map(|r| r.doc_id.as_u32())
            .collect();
        ids.sort_unstable();
        ids
    }

    fn backed_up(surrogate: u32, ordinal: i64, text: &str) -> RedoSubRecord {
        let doc_id = StorageKey::for_surrogate(Surrogate::new(surrogate)).to_string();
        let payload = FtsIndexPayload::new(SyncProvenance::default(), "notes", doc_id, body(text))
            .at_sys_from(ordinal)
            .to_bytes()
            .expect("encode");
        RedoSubRecord {
            record_type: RecordType::FtsIndex as u32,
            payload,
        }
    }

    /// A destination frame written after the backup and before the cut
    /// loses to the backup's frame: the backup's text is the newest version
    /// and the live postings. Its own text stays readable below the stamp.
    /// A destination frame above the cut stays current.
    #[test]
    fn a_newer_destination_frame_loses_to_the_backups_frame() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        declare_notes(&mut core);
        let task = make_default_task();
        let db = DatabaseId::DEFAULT.as_u64();
        core.write_synced_text(
            db,
            TID,
            "notes",
            Surrogate::new(5),
            &body("destination"),
            2_500,
        )
        .expect("destination frame of row 5");
        core.write_synced_text(db, TID, "notes", Surrogate::new(6), &body("later"), 3_500)
            .expect("destination frame of row 6");

        let scope = RestoreScope {
            database_id: DatabaseId::DEFAULT,
            tid: TID,
            applied: 5_000,
            cut: 3_000,
        };
        let ops = vec![backed_up(5, 10, "backup"), backed_up(6, 10, "older")];
        let mut changes = RestoreChanges::default();
        let ops = core
            .reassert_restored_rows(scope, ops, &mut changes)
            .expect("resolve");
        assert_eq!(ops.len(), 3, "only row 5 is set again");
        let again = FtsIndexPayload::from_bytes(&ops[2].payload).expect("decode");
        assert_eq!(again.sys_from, Some(5_000));
        assert_eq!(again.fields, body("backup"));

        for op in &ops {
            let frame = FtsIndexPayload::from_bytes(&op.payload).expect("decode");
            let surrogate = frame_surrogate("notes", &frame.doc_id).expect("surrogate");
            let response = core.execute_fts_index_doc_at(
                &task,
                TID,
                SyncedFrame {
                    collection: "notes",
                    surrogate,
                    fields: &frame.fields,
                    sys_from: frame.sys_from,
                },
                None,
            );
            assert_eq!(response.status, Status::Ok);
        }

        let head = core
            .inverted
            .synced_text_head(db, TenantId::new(TID), "notes", Surrogate::new(5))
            .expect("head")
            .expect("row 5 holds frames");
        assert_eq!(head.ordinal, 5_000);
        assert_eq!(hits(&core, "backup"), vec![5]);
        assert!(hits(&core, "destination").is_empty());
        assert_eq!(hits(&core, "later"), vec![6], "a write after the cut stays");
        assert!(hits(&core, "older").is_empty());

        let at_3000 = core
            .inverted
            .text_cut_view(db, TenantId::new(TID), "notes".into(), 3_000)
            .expect("cut view");
        let readers: Vec<u32> = at_3000.docs.iter().map(|d| d.doc_id.as_u32()).collect();
        assert_eq!(
            readers,
            vec![5],
            "row 5 reads the destination text below the stamp"
        );
    }
}
