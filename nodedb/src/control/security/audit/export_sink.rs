// SPDX-License-Identifier: BUSL-1.1

//! Export target every recorded audit entry is handed to, whichever path recorded it.

use super::entry::AuditEntry;
use crate::control::security::siem::SiemExporter;

/// Export target for recorded audit entries.
///
/// `AuditLog` copies an entry for export only while [`Self::is_active`] holds.
/// So an inactive sink costs one check per entry and no clone.
/// The copies reach [`Self::export`] after the audit lock is released.
pub trait AuditExportSink: Send + Sync {
    /// Whether the sink takes entries now.
    fn is_active(&self) -> bool;
    /// Take one recorded entry.
    fn export(&self, entry: AuditEntry);
}

impl AuditExportSink for SiemExporter {
    fn is_active(&self) -> bool {
        self.is_configured()
    }

    fn export(&self, entry: AuditEntry) {
        if entry.event.is_auth_event() {
            self.push_auth(entry);
        } else {
            self.push_audit(entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::control::security::audit::emitter::{
        ArcAuditEmitter, AuditEmitContext, AuditEmitter,
    };
    use crate::control::security::audit::{AuditEvent, AuditLog, lock_audit_log};
    use crate::control::security::siem::SiemConfig;

    fn configured_siem() -> Arc<SiemExporter> {
        Arc::new(SiemExporter::strict_for_test(SiemConfig {
            destinations: vec!["splunk".into()],
            ..SiemConfig::default()
        }))
    }

    fn audit_with(siem: &Arc<SiemExporter>) -> Arc<Mutex<AuditLog>> {
        let audit = Arc::new(Mutex::new(AuditLog::new(100)));
        lock_audit_log(&audit).set_export_sink(Arc::clone(siem) as Arc<dyn AuditExportSink>);
        audit
    }

    #[test]
    fn direct_site_entries_reach_siem() {
        let siem = configured_siem();
        let audit = audit_with(&siem);

        ArcAuditEmitter(Arc::clone(&audit)).emit(
            AuditEvent::PermissionDenied,
            "emitter_site",
            "denied",
            AuditEmitContext::new(None, "u1", "erin"),
        );
        lock_audit_log(&audit).record(AuditEvent::SessionRevoked, None, "bus_site", "revoked");

        let mut sources: Vec<String> = siem.drain_audit().into_iter().map(|e| e.source).collect();
        sources.extend(siem.drain_auth().into_iter().map(|e| e.source));
        sources.sort();
        assert_eq!(
            sources,
            vec!["bus_site".to_string(), "emitter_site".to_string()]
        );
    }

    #[test]
    fn dormant_siem_takes_no_copy() {
        let siem = Arc::new(SiemExporter::strict_for_test(SiemConfig::default()));
        let audit = audit_with(&siem);
        lock_audit_log(&audit).record(AuditEvent::AdminAction, None, "src", "dormant");
        assert!(siem.drain_audit().is_empty());
        assert!(
            audit
                .lock()
                .expect("audit")
                .take_export_pending()
                .0
                .is_empty()
        );
    }
}
