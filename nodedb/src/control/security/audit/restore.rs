// SPDX-License-Identifier: BUSL-1.1

//! Boot-time rebuild of the in-memory audit log from the catalog and the audit WAL.
//!
//! Every recorded entry reaches the audit WAL before its record call returns.
//! A crash can leave WAL entries that no flush committed.
//! Boot commits those entries to the catalog before the server takes traffic.
//! The log then continues the seq counter and the hash chain from the newest persisted entry.

use std::ops::ControlFlow;
use std::sync::Arc;

use tracing::{info, warn};

use super::entry::{AuditEntry, hash_entry};
use super::flush_control::DEFAULT_FLUSH_BOUND;
use super::log::AuditLog;
use super::wal_sink::AuditWalSink;
use crate::control::security::catalog::SystemCatalog;
use crate::wal::WalManager;

/// Commit unflushed audit WAL entries to the catalog and build the live audit log.
///
/// The returned log appends every new entry to `wal`.
pub fn restore_audit_log(
    catalog: &SystemCatalog,
    wal: &Arc<WalManager>,
) -> crate::Result<AuditLog> {
    let persisted_max = catalog.load_audit_max_seq()?;

    // WAL order equals seq order, so a crash during these commits leaves a prefix committed.
    // The next boot starts above that prefix.
    let mut chunk: Vec<AuditEntry> = Vec::with_capacity(DEFAULT_FLUSH_BOUND);
    let mut committed = 0usize;
    wal.scan_audit_entries(|_data_lsn, bytes| {
        match zerompk::from_msgpack::<AuditEntry>(bytes) {
            Ok(entry) if entry.seq > persisted_max => chunk.push(entry),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "skipping malformed audit WAL record"),
        }
        if chunk.len() >= DEFAULT_FLUSH_BOUND {
            catalog.append_audit_entries(&chunk)?;
            committed += chunk.len();
            chunk.clear();
        }
        Ok(ControlFlow::Continue(()))
    })?;
    catalog.append_audit_entries(&chunk)?;
    committed += chunk.len();
    if committed > 0 {
        info!(
            count = committed,
            "committed unflushed audit WAL entries to the catalog"
        );
    }
    // The catalog now holds every WAL record, so the WAL drops them all.
    wal.truncate_audit_through(wal.audit_last_lsn()?)?;

    let mut log = AuditLog::new(DEFAULT_FLUSH_BOUND);
    if let Some(newest) = catalog.load_audit_newest()? {
        log.set_next_seq(newest.seq.saturating_add(1));
        log.set_last_hash(hash_entry(&newest));
    }
    log.set_wal_sink(Arc::clone(wal) as Arc<dyn AuditWalSink>);
    Ok(log)
}

/// Test support: an audit log whose WAL sink is a fresh on-disk WAL in `dir`.
#[cfg(test)]
pub(crate) fn audit_with_test_wal(
    dir: &std::path::Path,
) -> (Arc<std::sync::Mutex<AuditLog>>, Arc<WalManager>) {
    let wal = Arc::new(
        WalManager::open_for_testing(&dir.join("audit-site.wal")).expect("open audit site WAL"),
    );
    let mut log = AuditLog::new(DEFAULT_FLUSH_BOUND);
    log.set_wal_sink(Arc::clone(&wal) as Arc<dyn AuditWalSink>);
    (Arc::new(std::sync::Mutex::new(log)), wal)
}

/// Test support: the entries boot restores from `wal` after a crash that lost every buffer.
///
/// The fresh catalog holds nothing, so only the WAL can supply the entries.
#[cfg(test)]
pub(crate) fn replay_after_crash(wal: &Arc<WalManager>) -> Vec<AuditEntry> {
    let catalog = SystemCatalog::open_in_memory().expect("open fresh catalog");
    restore_audit_log(&catalog, wal).expect("replay audit WAL");
    catalog
        .load_recent_audit_entries(usize::MAX, |_| true)
        .expect("read replayed entries")
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::control::security::audit::emitter::{
        ArcAuditEmitter, AuditEmitContext, AuditEmitter,
    };
    use crate::control::security::audit::{AuditEvent, verify_chain};

    #[test]
    fn unflushed_entries_survive_a_crash_and_the_chain_continues() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().join("restore.wal");
        let catalog_path = dir.path().join("system.redb");

        let first_life_seqs: Vec<u64> = {
            let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("open WAL"));
            let catalog = SystemCatalog::open(&catalog_path).expect("open catalog");
            let audit = Arc::new(Mutex::new(
                restore_audit_log(&catalog, &wal).expect("first boot"),
            ));
            ArcAuditEmitter(Arc::clone(&audit)).emit(
                AuditEvent::AuthzDenied,
                "emitter",
                "denied",
                AuditEmitContext::new(None, "u1", "alice"),
            );
            let mut log = audit.lock().expect("audit lock");
            log.record(AuditEvent::AdminAction, None, "direct", "record");
            let seqs = log.all().iter().map(|e| e.seq).collect();
            // No flush runs: dropping everything here is the crash.
            drop(log);
            seqs
        };

        let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("reopen WAL"));
        let catalog = SystemCatalog::open(&catalog_path).expect("reopen catalog");
        let mut log = restore_audit_log(&catalog, &wal).expect("second boot");
        let persisted: Vec<u64> = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read catalog")
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(
            persisted, first_life_seqs,
            "every entry must survive the crash"
        );

        log.record(AuditEvent::AdminAction, None, "direct", "after restart");
        let mut entries = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read catalog");
        entries.extend(log.all().iter().cloned());
        let report = verify_chain(&entries, 1, u64::MAX, log.last_hash());
        assert_eq!(
            report.broken_at, None,
            "the chain must continue across the restart"
        );
    }

    #[test]
    fn emitter_entry_survives_a_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (audit, wal) = audit_with_test_wal(dir.path());
        ArcAuditEmitter(Arc::clone(&audit)).emit(
            AuditEvent::PermissionDenied,
            "emitter_site",
            "denied",
            AuditEmitContext::new(None, "u7", "bob"),
        );
        drop(audit);
        let replayed = replay_after_crash(&wal);
        assert!(
            replayed
                .iter()
                .any(|e| e.source == "emitter_site" && e.auth_user_id == "u7"),
            "the emitter entry must replay with its auth fields: {replayed:?}"
        );
    }
}
