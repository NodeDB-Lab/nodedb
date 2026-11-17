// SPDX-License-Identifier: BUSL-1.1

//! Checked write-note admission and infallible postcommit publication.

use super::super::CoreLoop;
use super::super::newest_ordinals::cache::SliceKey;
use super::super::newest_ordinals::collection_vshard;
use super::{KeyRepr, WriteStamp};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, VShardId};

pub(in crate::data::executor) struct PreparedSurrogateNote {
    database: DatabaseId,
    tenant: TenantId,
    collection: String,
    home: VShardId,
    surrogate: u32,
    stamp: WriteStamp,
}

impl CoreLoop {
    /// Check routing while the row transaction can still roll back.
    pub(in crate::data::executor) fn prepare_surrogate_write(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        surrogate: u32,
    ) -> crate::Result<Option<PreparedSurrogateNote>> {
        let Some(stamp) = self.task_write_stamp(task) else {
            return Ok(None);
        };
        let home = collection_vshard(task.request.database_id, collection)?;
        Ok(Some(PreparedSurrogateNote {
            database: task.request.database_id,
            tenant: TenantId::new(tid),
            collection: collection.to_owned(),
            home,
            surrogate,
            stamp,
        }))
    }

    /// Publish a checked note without fallible storage reads after commit.
    pub(in crate::data::executor) fn publish_surrogate_write(
        &mut self,
        note: Option<PreparedSurrogateNote>,
        ordinal: i64,
    ) {
        let Some(note) = note else {
            return;
        };
        if let Some(scope) = self.redo_apply.scope.as_mut() {
            scope.defer_write(
                note.database,
                note.tenant,
                &note.collection,
                Some(KeyRepr::Surrogate(note.surrogate)),
                note.stamp,
            );
            return;
        }
        self.newest_ordinals.raise(
            &SliceKey {
                vshard: note.home,
                db: note.database,
                tenant: note.tenant,
                collection: note.collection.into_boxed_str(),
            },
            ordinal,
        );
        self.publish_applied(note.stamp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::write_note::tests::{make_core, wal_task};
    use crate::types::Lsn;

    #[test]
    fn prepared_note_raises_known_cache_and_preserves_newer_versions() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let task = wal_task(7);
        let home = collection_vshard(DatabaseId::DEFAULT, "docs").unwrap();
        let key = SliceKey {
            vshard: home,
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("docs"),
        };
        core.newest_ordinals.set_slice(key.clone(), Some(50));
        core.newest_ordinals.set_vshard(home, Some(50));
        let note = core.prepare_surrogate_write(&task, 1, "docs", 1).unwrap();
        core.publish_surrogate_write(note, 20);
        assert_eq!(core.newest_ordinals.slice(&key), Some(Some(50)));
        assert_eq!(core.newest_ordinals.vshard(home), Some(Some(50)));
        assert_eq!(core.watermark, Lsn::new(7));
        let note = core.prepare_surrogate_write(&task, 1, "docs", 1).unwrap();
        core.publish_surrogate_write(note, 80);
        assert_eq!(core.newest_ordinals.slice(&key), Some(Some(80)));
        assert_eq!(core.newest_ordinals.vshard(home), Some(Some(80)));
    }

    #[test]
    fn prepared_note_leaves_unknown_cache_unknown() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let task = wal_task(7);
        let note = core.prepare_surrogate_write(&task, 1, "docs", 1).unwrap();
        core.publish_surrogate_write(note, 80);
        let home = collection_vshard(DatabaseId::DEFAULT, "docs").unwrap();
        assert_eq!(core.newest_ordinals.vshard(home), None);
    }

    #[test]
    fn prepared_note_checks_collection_database_before_publication() {
        let (core, _tx, _rx, _dir) = make_core();
        let task = wal_task(7);
        assert!(core.prepare_surrogate_write(&task, 1, "1/docs", 1).is_err());
        assert_eq!(core.watermark, Lsn::new(0));
    }
}
