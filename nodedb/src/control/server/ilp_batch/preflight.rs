// SPDX-License-Identifier: BUSL-1.1

//! Canonical grouping and authorization preflight for ILP batches.
//!
//! Parses and authorizes every unique collection in a batch before any
//! quota accounting, task construction, sequencer submission, or catalog
//! projection work runs.

use std::collections::BTreeMap;

use crate::control::security::audit::AuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::types::DatabaseId;

/// Preflighted raw ILP lines for one canonical measurement.
///
/// `raw_lines` preserve physical source order; map iteration canonicalizes
/// measurement order. `catalog_fields` is a rebuildable control-plane projection
/// of the authoritative timeseries-engine schema.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct IlpMeasurementBatch {
    pub(super) measurement: String,
    pub(super) raw_lines: Vec<String>,
    pub(super) catalog_fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IlpPreflightFailure {
    Parse,
    PermissionDenied,
}

/// Parse and authorize every unique collection before quota accounting, task
/// construction, sequencer submission, or catalog projection work.
///
/// A `BTreeMap` gives canonical deterministic authorization/dispatch order,
/// while appending each original raw line preserves source order within a
/// measurement group.
pub(super) fn preflight_ilp_batch(
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    batch: &str,
    permissions: &crate::control::security::permission::PermissionStore,
    roles: &crate::control::security::role::RoleStore,
    audit: &dyn AuditEmitter,
) -> Result<Vec<IlpMeasurementBatch>, IlpPreflightFailure> {
    let parsed = crate::engine::timeseries::ilp::parse_batch(batch)
        .map_err(|_| IlpPreflightFailure::Parse)?;
    if parsed.lines().is_empty() {
        return Err(IlpPreflightFailure::Parse);
    }

    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for line in parsed.lines() {
        grouped
            .entry(line.measurement.to_string())
            .or_default()
            .push(line.raw.to_owned());
    }

    let mut groups = Vec::with_capacity(grouped.len());
    for (measurement, raw_lines) in grouped {
        authorize_collection(
            identity,
            database_id,
            &measurement,
            Permission::Write,
            permissions,
            roles,
            audit,
        )
        .map_err(|_| IlpPreflightFailure::PermissionDenied)?;
        let grouped_source = raw_lines.join("\n");
        let parsed_group = crate::engine::timeseries::ilp::parse_batch(&grouped_source)
            .map_err(|_| IlpPreflightFailure::Parse)?;
        let schema = crate::engine::timeseries::ilp_ingest::infer_schema(parsed_group.lines());
        let catalog_fields = schema
            .columns
            .iter()
            .map(|(name, ty)| (name.clone(), ty.ddl_type_name().to_owned()))
            .collect();
        groups.push(IlpMeasurementBatch {
            measurement,
            raw_lines,
            catalog_fields,
        });
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::{IlpPreflightFailure, preflight_ilp_batch};

    use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
    use crate::control::security::audit::{
        AuditEmitContext, AuditEmitter, AuditEvent, NoopAuditEmitter,
    };
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Permission,
    };
    use crate::control::security::permission::PermissionStore;
    use crate::control::security::role::RoleStore;
    use crate::types::{DatabaseId, TenantId};
    use std::sync::Mutex;

    #[derive(Clone, Debug)]
    struct CapturedAuditEvent {
        event: AuditEvent,
        source: String,
        auth_user_id: String,
        auth_user_name: String,
        tenant_id: Option<TenantId>,
    }

    struct ContextCapturingEmitter {
        events: Mutex<Vec<CapturedAuditEvent>>,
    }

    impl ContextCapturingEmitter {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }

        fn recorded(&self) -> Vec<CapturedAuditEvent> {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl AuditEmitter for ContextCapturingEmitter {
        fn emit(
            &self,
            event: AuditEvent,
            source: &str,
            _detail: &str,
            context: AuditEmitContext<'_>,
        ) {
            let mut events = self
                .events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            events.push(CapturedAuditEvent {
                event,
                source: source.into(),
                auth_user_id: context.auth_user_id.into(),
                auth_user_name: context.auth_user_name.into(),
                tenant_id: context.tenant_id,
            });
        }
    }

    fn identity(database_id: DatabaseId) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "ingester",
            TenantId::new(9),
            AuthMethod::ApiKey,
            Vec::new(),
            Some(database_id),
            DatabaseSet::Some(smallvec::smallvec![database_id]),
        )
    }

    fn grant_write(permissions: &PermissionStore, collection: &str) {
        let target = format!("collection:9:{collection}");
        permissions
            .grant(&target, "user:ingester", Permission::Write, "admin", None)
            .expect("in-memory grant succeeds");
    }

    fn preflight(
        batch: &str,
        permissions: &PermissionStore,
    ) -> Result<Vec<super::IlpMeasurementBatch>, IlpPreflightFailure> {
        preflight_ilp_batch(
            &identity(DatabaseId::new(7)),
            DatabaseId::new(7),
            batch,
            permissions,
            &RoleStore::new(),
            &NoopAuditEmitter,
        )
    }

    #[test]
    fn groups_two_measurements_in_canonical_order_and_preserves_source_order() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        grant_write(&permissions, "mem");

        let groups = preflight("mem value=1i\ncpu value=2i\nmem value=3i\n", &permissions)
            .expect("all measurements are writable");

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].measurement, "cpu");
        assert_eq!(groups[0].raw_lines, vec!["cpu value=2i"]);
        assert_eq!(groups[1].measurement, "mem");
        assert_eq!(groups[1].raw_lines, vec!["mem value=1i", "mem value=3i"]);
    }

    #[test]
    fn comments_blanks_and_escaped_measurements_use_canonical_grouping() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu load");

        let groups = preflight(
            "# comment\n\n cpu\\ load value=1i\ncpu\\ load value=2i\n",
            &permissions,
        )
        .expect("escaped measurement is writable");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].measurement, "cpu load");
        assert_eq!(
            groups[0].raw_lines,
            vec![" cpu\\ load value=1i", "cpu\\ load value=2i"]
        );
    }

    #[test]
    fn empty_or_comment_only_batch_is_rejected_before_quota_or_dispatch() {
        let permissions = PermissionStore::new();

        assert_eq!(
            preflight(" \n# comment\n", &permissions),
            Err(IlpPreflightFailure::Parse)
        );
    }

    #[test]
    fn malformed_batch_fails_before_any_measurement_can_be_authorized_or_dispatched() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");

        assert_eq!(
            preflight(
                "cpu value=1i\nthis is not valid ILP trailing\n",
                &permissions
            ),
            Err(IlpPreflightFailure::Parse)
        );
    }

    #[test]
    fn second_ungranted_collection_rejects_before_authorized_work_can_run() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let mut authorized_work_runs = 0;

        if preflight("cpu value=1i\nmem value=2i\n", &permissions).is_ok() {
            authorized_work_runs += 1;
        }

        assert_eq!(authorized_work_runs, 0);
    }

    #[test]
    fn denied_batch_emits_one_audit_event_for_its_first_canonical_denial() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let audit = CapturingEmitter::new();

        assert_eq!(
            preflight_ilp_batch(
                &identity(DatabaseId::new(7)),
                DatabaseId::new(7),
                "cpu value=1i\nmem value=2i\n",
                &permissions,
                &RoleStore::new(),
                &audit,
            ),
            Err(IlpPreflightFailure::PermissionDenied)
        );
        assert_eq!(audit.recorded().len(), 1);
    }

    #[test]
    fn read_only_collection_is_not_sufficient_for_ilp_ingest() {
        // ILP is write-only; read access is not applicable to ingestion.
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:9:cpu",
                "user:ingester",
                Permission::Read,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");

        assert_eq!(
            preflight("cpu value=1i\n", &permissions),
            Err(IlpPreflightFailure::PermissionDenied)
        );
    }

    #[test]
    fn system_audit_log_measurement_is_denied_to_regular_ingester() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "_system.audit_log");
        let audit = ContextCapturingEmitter::new();

        assert_eq!(
            preflight_ilp_batch(
                &identity(DatabaseId::new(7)),
                DatabaseId::new(7),
                "_system.audit_log value=1i\n",
                &permissions,
                &RoleStore::new(),
                &audit,
            ),
            Err(IlpPreflightFailure::PermissionDenied)
        );
        let events = audit.recorded();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, AuditEvent::PermissionDenied);
        assert_eq!(events[0].source, "ingester");
        assert_eq!(events[0].auth_user_id, "7");
        assert_eq!(events[0].auth_user_name, "ingester");
        assert_eq!(events[0].tenant_id, Some(TenantId::new(9)));
    }

    #[test]
    fn tenant_ten_write_grant_does_not_authorize_tenant_nine_ilp_ingest() {
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:10:cpu",
                "user:ingester",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant succeeds");

        assert_eq!(
            preflight("cpu value=1i\n", &permissions),
            Err(IlpPreflightFailure::PermissionDenied)
        );
    }

    #[test]
    fn non_default_database_is_used_for_collection_authorization() {
        let permissions = PermissionStore::new();
        grant_write(&permissions, "cpu");
        let database_id = DatabaseId::new(7);
        let roles = RoleStore::new();

        // Database selection is handshake-bound; ILP payload does not select it.
        assert_eq!(
            preflight_ilp_batch(
                &identity(DatabaseId::DEFAULT),
                database_id,
                "cpu value=1i\n",
                &permissions,
                &roles,
                &NoopAuditEmitter,
            ),
            Err(IlpPreflightFailure::PermissionDenied)
        );
        let groups = preflight_ilp_batch(
            &identity(database_id),
            database_id,
            "cpu value=1i\n",
            &permissions,
            &roles,
            &NoopAuditEmitter,
        )
        .expect("the explicitly bound non-default database is authorized");

        assert_eq!(groups[0].measurement, "cpu");
    }
}
