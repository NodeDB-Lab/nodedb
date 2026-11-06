// SPDX-License-Identifier: BUSL-1.1

//! Test fixture: the descriptor of a mirror database.

use nodedb_types::{AuditDmlMode, DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};

use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};

/// A mirroring database `id` named `name` whose link holds `status`.
pub(crate) fn mirror_descriptor(id: u64, name: &str, status: MirrorStatus) -> DatabaseDescriptor {
    DatabaseDescriptor {
        id: DatabaseId::new(id),
        name: name.to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn: 0,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: "source".to_string(),
            source_database: DatabaseId::new(id),
            source_database_name: "source_db".to_string(),
            mode: MirrorMode::Async,
            status,
        }),
        audit_dml: AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    }
}
