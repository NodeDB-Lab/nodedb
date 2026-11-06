// SPDX-License-Identifier: BUSL-1.1

//! One scheduled backup run: `BACKUP DATABASE` to a new envelope under the
//! target, then `keep` retention under the target.

use std::sync::Arc;

use super::blocking::off_runtime;
use super::envelopes::{apply_keep, envelope_name};
use crate::config::server::{BackupScheduleSettings, BackupStorageSettings};
use crate::control::backup::database::{backup_database, database_tenants};
use crate::control::backup::store::BackupObject;
use crate::control::state::SharedState;

/// What one run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupRun {
    pub envelope_uri: String,
    pub bytes: u64,
    /// Envelopes `keep` retention deleted.
    pub deleted: u64,
}

/// Back up `schedule.database` to the envelope named for
/// `scheduled_unix_ms`, then apply `keep`.
///
/// `scheduled_unix_ms` is the scheduled minute, not the clock at run time.
/// Two runs of one minute, such as a run repeated after a leader change,
/// write the same object.
pub async fn run_scheduled_backup(
    state: &Arc<SharedState>,
    schedule: &BackupScheduleSettings,
    scheduled_unix_ms: u64,
) -> crate::Result<BackupRun> {
    let (database_id, tenants) = {
        let (state, database) = (Arc::clone(state), schedule.database.clone());
        off_runtime("scheduled backup database lookup", move || {
            let database_id = state
                .credentials
                .catalog()
                .get_database_id_by_name(&database)?
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!(
                        "scheduled backup: database '{database}' does not exist; fix or \
                         remove its backup.schedule entry"
                    ),
                })?;
            let tenants = database_tenants(&state, database_id)?;
            Ok((database_id, tenants))
        })
        .await?
    };
    let bytes = backup_database(state, database_id, &schedule.database, &tenants).await?;
    // A former coordinator whose lease lapsed during the capture writes no
    // envelope.
    crate::event::scheduler::coordinator::ensure_system_coordinator(state)?;
    // The record is durable before the envelope exists, and a failed record fails the run.
    // The audit sync runs on the blocking pool. The failed minute stays due.
    let intent = format!(
        "scheduled BACKUP DATABASE {} TO '{}'",
        schedule.database, schedule.target
    );
    let intent_state = Arc::clone(state);
    off_runtime("scheduled backup audit intent", move || {
        intent_state.audit_record_strict(
            crate::control::security::audit::AuditEvent::ActionIntent,
            None,
            "_system_scheduler",
            &intent,
        )
    })
    .await?;
    let run = write_and_retain(
        schedule,
        state.backup_storage.as_deref(),
        scheduled_unix_ms,
        bytes,
    )
    .await?;
    // The audit append is durable, so it runs on the blocking pool.
    let detail = format!(
        "scheduled BACKUP DATABASE {} TO '{}' wrote {} bytes, deleted {} old envelopes",
        schedule.database, run.envelope_uri, run.bytes, run.deleted
    );
    let audit_state = Arc::clone(state);
    off_runtime("scheduled backup audit record", move || {
        audit_state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            None,
            "_system_scheduler",
            &detail,
        );
        Ok(())
    })
    .await?;
    Ok(run)
}

/// Write `bytes` as the envelope of `scheduled_unix_ms` under the target,
/// replacing an envelope of the same minute, then delete the oldest
/// envelopes of the database beyond `keep`.
///
/// A retention error comes after the write: the new envelope is kept, and
/// the next run retries retention.
pub async fn write_and_retain(
    schedule: &BackupScheduleSettings,
    storage: Option<&BackupStorageSettings>,
    scheduled_unix_ms: u64,
    bytes: Vec<u8>,
) -> crate::Result<BackupRun> {
    let prefix = schedule.target_prefix();
    let envelope_uri = format!(
        "{prefix}/{}",
        envelope_name(&schedule.database, scheduled_unix_ms)
    );
    let envelope = resolve(envelope_uri.clone(), storage).await?;
    let size = bytes.len() as u64;
    envelope.put(bytes).await?;

    let target = resolve(prefix.to_string(), storage).await?;
    let deleted = apply_keep(
        target.store(),
        target.path(),
        &schedule.database,
        schedule.keep,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "backup".into(),
        detail: format!(
            "scheduled backup wrote '{envelope_uri}', but keep retention under \
                 '{prefix}' did not finish: {e}"
        ),
    })?;
    Ok(BackupRun {
        envelope_uri,
        bytes: size,
        deleted,
    })
}

/// Resolve `uri` on the blocking pool: a `file://` URI opens its local root.
async fn resolve(
    uri: String,
    storage: Option<&BackupStorageSettings>,
) -> crate::Result<BackupObject> {
    let storage = storage.cloned();
    off_runtime("backup URI resolve", move || {
        BackupObject::resolve(&uri, storage.as_ref()).map_err(crate::Error::from)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::backup::schedule::envelopes::list_envelopes;

    fn schedule(root: &std::path::Path, keep: u64) -> BackupScheduleSettings {
        BackupScheduleSettings {
            database: "sales".into(),
            target: format!("file://{}/nightly/", root.display()),
            cron: "0 3 * * *".into(),
            keep,
        }
    }

    fn storage(root: &std::path::Path) -> BackupStorageSettings {
        BackupStorageSettings {
            local_root: Some(root.to_path_buf()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn each_run_writes_an_envelope_and_keep_removes_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let (schedule, storage) = (schedule(&root, 2), storage(&root));

        let mut deleted = Vec::new();
        for (at, body) in [(1_000, b"one"), (2_000, b"two"), (3_000, b"thr")] {
            let run = write_and_retain(&schedule, Some(&storage), at, body.to_vec())
                .await
                .unwrap();
            assert!(run.envelope_uri.ends_with(&envelope_name("sales", at)));
            assert_eq!(run.bytes, 3);
            deleted.push(run.deleted);
        }
        assert_eq!(deleted, [0, 0, 1]);

        let nightly = root.join("nightly");
        assert!(!nightly.join(envelope_name("sales", 1_000)).exists());
        assert_eq!(
            std::fs::read(nightly.join(envelope_name("sales", 3_000))).unwrap(),
            b"thr"
        );
        let target = BackupObject::resolve(schedule.target_prefix(), Some(&storage)).unwrap();
        let left: Vec<u64> = list_envelopes(target.store(), target.path(), "sales")
            .await
            .unwrap()
            .into_iter()
            .map(|envelope| envelope.at_unix_ms)
            .collect();
        assert_eq!(left, [2_000, 3_000]);
    }

    #[tokio::test]
    async fn a_repeated_minute_rewrites_its_own_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let (schedule, storage) = (schedule(&root, 5), storage(&root));
        for body in [b"first", b"again"] {
            let run = write_and_retain(&schedule, Some(&storage), 60_000, body.to_vec())
                .await
                .unwrap();
            assert_eq!(run.deleted, 0);
        }
        let nightly = root.join("nightly");
        assert_eq!(std::fs::read_dir(&nightly).unwrap().count(), 1);
        assert_eq!(
            std::fs::read(nightly.join(envelope_name("sales", 60_000))).unwrap(),
            b"again"
        );
    }

    #[tokio::test]
    async fn a_target_outside_the_local_root_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let schedule = schedule(&elsewhere.path().canonicalize().unwrap(), 1);
        assert!(
            write_and_retain(&schedule, Some(&storage(&root)), 1, vec![1])
                .await
                .is_err()
        );
        assert!(std::fs::read_dir(&root).unwrap().next().is_none());
    }
}
