// SPDX-License-Identifier: BUSL-1.1

//! Resolve one restored columnar key into the versions its install writes.
//!
//! The resolve reads the destination's newest ordinal for the key's primary
//! key at the transaction's turn, and keeps the versions `restore_key_plan`
//! chooses. A key whose current state is set again keeps one version: the
//! backup's newest row at the stamp, open when the backup holds the key live
//! and `[at, at)` when it holds the key absent.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::wal_replay_columnar_restore::restored_row_values;
use crate::types::TenantId;
use crate::wal::{ColumnarRestoreRedo, ColumnarRestoreVersion, RedoRowKind, RedoSubRecord};

use super::restore_changes::RestoreChanges;
use super::restore_key_plan::{BackedUpSpan, KeyRestore, plan_key};
use super::restore_reassert::RestoreScope;

/// Rewrite `redo` to the versions `plan` installs. `None` when nothing
/// installs.
pub(super) fn apply_plan(
    mut redo: ColumnarRestoreRedo,
    plan: KeyRestore,
) -> Option<ColumnarRestoreRedo> {
    match plan {
        KeyRestore::Keep => None,
        KeyRestore::Versions { from } => {
            redo.versions.drain(..from.min(redo.versions.len()));
            (!redo.versions.is_empty()).then_some(redo)
        }
        KeyRestore::SetAgain { at } => {
            let newest = redo.versions.pop()?;
            redo.versions = vec![ColumnarRestoreVersion {
                row: newest.row,
                sys_from: at,
                sys_to: newest.sys_to.map(|_| at),
            }];
            Some(redo)
        }
    }
}

/// The spans of `redo`'s versions, in order.
fn spans(redo: &ColumnarRestoreRedo) -> Vec<BackedUpSpan> {
    redo.versions
        .iter()
        .map(|version| BackedUpSpan {
            sys_from: version.sys_from,
            sys_to: version.sys_to,
        })
        .collect()
}

impl CoreLoop {
    /// The sub-record that installs the restored columnar key `redo`, or
    /// `None` when the destination keeps its own state.
    ///
    /// `Err` when the key's newest row does not decode against the
    /// destination's schema.
    pub(super) fn resolve_restored_columnar_key(
        &self,
        scope: RestoreScope,
        record_type: u32,
        redo: ColumnarRestoreRedo,
        changes: &mut RestoreChanges,
    ) -> crate::Result<Option<RedoSubRecord>> {
        let key = (
            scope.database_id,
            TenantId::new(scope.tid),
            redo.collection.clone(),
        );
        let (dest_newest, dest_live) = match (self.columnar_engines.get(&key), redo.versions.last())
        {
            (Some(engine), Some(newest)) => {
                let values = restored_row_values(engine.schema(), &newest.row, &redo.collection)?;
                let pk = engine
                    .encode_pk_from_row(&values)
                    .map_err(crate::Error::from)?;
                engine.key_newest_ordinal(&pk)
            }
            _ => (None, false),
        };
        let plan = plan_key(
            &spans(&redo),
            dest_newest,
            dest_live,
            scope.applied,
            scope.cut,
        );
        let Some(redo) = apply_plan(redo, plan) else {
            return Ok(None);
        };
        let ends_live = redo
            .versions
            .last()
            .is_some_and(|version| version.sys_to.is_none());
        changes.note_grouped(&redo.collection, RedoRowKind::net(dest_live, ends_live));
        Ok(Some(RedoSubRecord {
            record_type,
            payload: redo.encode()?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redo() -> ColumnarRestoreRedo {
        let version = |row: u8, sys_from, sys_to| ColumnarRestoreVersion {
            row: vec![row],
            sys_from,
            sys_to,
        };
        ColumnarRestoreRedo {
            collection: "metrics".into(),
            schema_bytes: Vec::new(),
            surrogate: 4,
            versions: vec![version(1, 10, Some(20)), version(2, 20, None)],
        }
    }

    /// A client write after the cut survives: nothing installs. A
    /// destination at or below the cut gets the backup's versions above it,
    /// or the backup's state again above it.
    #[test]
    fn the_plan_picks_the_versions_the_install_writes() {
        assert_eq!(apply_plan(redo(), KeyRestore::Keep), None);
        let tail = apply_plan(redo(), KeyRestore::Versions { from: 1 }).expect("tail");
        assert_eq!(
            tail.versions
                .iter()
                .map(|v| (v.sys_from, v.sys_to))
                .collect::<Vec<_>>(),
            vec![(20, None)]
        );
        let again = apply_plan(redo(), KeyRestore::SetAgain { at: 500 }).expect("again");
        assert_eq!(again.versions.len(), 1);
        assert_eq!(again.versions[0].row, vec![2]);
        assert_eq!(
            (again.versions[0].sys_from, again.versions[0].sys_to),
            (500, None)
        );
    }

    /// A key the backup ends absent is set again as `[at, at)`.
    #[test]
    fn an_absent_key_is_set_again_as_a_zero_width_version() {
        let mut ended = redo();
        ended.versions.pop();
        let again = apply_plan(ended, KeyRestore::SetAgain { at: 500 }).expect("again");
        assert_eq!(
            (again.versions[0].sys_from, again.versions[0].sys_to),
            (500, Some(500))
        );
    }

    /// Versions at or below the cut are re-asserted; a write after it
    /// survives (`plan_key` against the batch's spans).
    #[test]
    fn a_restored_key_follows_the_cut_rule() {
        let s = spans(&redo());
        assert_eq!(
            plan_key(&s, Some(3_001), true, 5_000, 3_000),
            KeyRestore::Keep
        );
        assert_eq!(
            plan_key(&s, Some(2_500), true, 5_000, 3_000),
            KeyRestore::SetAgain { at: 5_000 }
        );
    }

    /// Against a real engine: a client write after the cut keeps the key,
    /// and a destination version at or below it gets the backup's row again.
    #[test]
    fn a_columnar_key_against_the_destination_engine() {
        use nodedb_columnar::MutationEngine;
        use nodedb_columnar::pk_index::encode_pk;
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        use nodedb_types::value::Value;

        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("schema");
        let row = |v: i64| vec![Value::Integer(1), Value::Integer(v)];
        let pk = encode_pk(&Value::Integer(1));
        let backup = [BackedUpSpan {
            sys_from: 10,
            sys_to: None,
        }];
        let cut = 3_000;

        // A client write after the cut survives.
        let mut after = MutationEngine::new("m".into(), schema.clone());
        after
            .install_version(&row(7), None, 3_500, None)
            .expect("write");
        let (newest, live) = after.key_newest_ordinal(&pk);
        assert_eq!(
            plan_key(&backup, newest, live, 5_000, cut),
            KeyRestore::Keep
        );

        // A write at or below the cut is set again to the backup's row.
        let mut before = MutationEngine::new("m".into(), schema);
        before
            .install_version(&row(7), None, 2_500, None)
            .expect("write");
        let (newest, live) = before.key_newest_ordinal(&pk);
        let KeyRestore::SetAgain { at } = plan_key(&backup, newest, live, 5_000, cut) else {
            panic!("the backup's row is set again");
        };
        before
            .install_version(&row(42), None, at, None)
            .expect("set again");
        let current = before
            .scan_memtable_versions(nodedb_columnar::VersionCut::Current)
            .map(|r| r.map(|(_, _, values)| values))
            .collect::<Result<Vec<_>, _>>()
            .expect("read");
        assert_eq!(current, vec![row(42)]);
    }
}
