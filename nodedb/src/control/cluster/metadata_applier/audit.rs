// SPDX-License-Identifier: BUSL-1.1

//! Audit and CA-trust helpers for `MetadataCommitApplier`.

use crate::control::catalog_entry;
use crate::control::state::SharedState;

/// Apply a `MetadataEntry::CaTrustChange` on the host side: write or
/// delete `tls/ca.d/<fp>.crt`, emit an [`AuditEvent::CertRotation`]
/// record, and log for the operator. Hot-reload of the rustls config
/// picks up the new trust set on the next connection; the overlap
/// window guarantees existing connections keep working through the
/// rotation.
pub(super) fn apply_ca_trust_change(
    shared: &SharedState,
    add: Option<&[u8]>,
    remove: Option<&[u8; 32]>,
    raft_index: u64,
) {
    use crate::control::cluster::tls::{TLS_SUBDIR, remove_trusted_ca, write_trusted_ca};
    use crate::control::security::audit::{AuditAuth, AuditEvent};

    let tls_dir = shared.data_dir.join(TLS_SUBDIR);

    let mut added_fp: Option<[u8; 32]> = None;
    if let Some(der) = add {
        match write_trusted_ca(&tls_dir, der) {
            Ok(fp) => {
                added_fp = Some(fp);
                tracing::info!(
                    fingerprint = %nodedb_cluster::ca_fingerprint_hex(&fp),
                    raft_index,
                    "cluster CA trust: added overlap anchor"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, raft_index, "ca trust add failed");
            }
        }
    }
    if let Some(fp) = remove {
        match remove_trusted_ca(&tls_dir, fp) {
            Ok(()) => {
                tracing::info!(
                    fingerprint = %nodedb_cluster::ca_fingerprint_hex(fp),
                    raft_index,
                    "cluster CA trust: removed overlap anchor"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, raft_index, "ca trust remove failed");
            }
        }
    }

    let detail = sonic_rs::to_string(&sonic_rs::json!({
        "raft_index": raft_index,
        "added_fingerprint": added_fp.map(|fp| nodedb_cluster::ca_fingerprint_hex(&fp)),
        "removed_fingerprint": remove.map(nodedb_cluster::ca_fingerprint_hex),
    }))
    .unwrap_or_default();
    if let Ok(mut log) = shared.audit.lock() {
        log.record_with_auth(
            AuditEvent::CertRotation,
            None,
            None,
            "metadata_group",
            &detail,
            &AuditAuth::default(),
        );
    }
}

/// Emit a [`AuditEvent::DdlChange`] record describing one applied
/// `CatalogEntry`. Kept as a free function so the applier stays the
/// orchestrator and the formatting lives next to the audit log.
pub(super) fn emit_ddl_audit(
    shared: &SharedState,
    raft_index: u64,
    stamped: &catalog_entry::CatalogEntry,
    audit: Option<&(String, String, String)>,
) {
    use crate::control::security::audit::{AuditAuth, AuditEvent, DdlAuditDetail};
    use crate::control::security::catalog::StoredCollection;

    let (descriptor_name, version_after, hlc) = describe_entry(stamped);
    let version_before = version_after.saturating_sub(1);

    let (user_id, user_name, sql) = match audit {
        Some((uid, uname, sql)) => (uid.clone(), uname.clone(), sql.clone()),
        None => (String::new(), String::new(), String::new()),
    };

    let detail = DdlAuditDetail {
        descriptor_kind: stamped.kind().to_string(),
        descriptor_name,
        version_before,
        version_after,
        hlc,
        raft_index,
        sql_statement: sql,
    };
    let detail_json = sonic_rs::to_string(&detail).unwrap_or_else(|_| String::new());

    // `tenant_id` on the audit entry: the authoritative tenant for
    // most descriptor types is available on the `Stored*` value, but
    // extracting it per-variant would bloat this helper. Leave it
    // `None` at this layer — consumers that care route by
    // `descriptor_kind` + `descriptor_name`.
    let _ = std::any::type_name::<StoredCollection>();

    let mut log = match shared.audit.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.record_with_auth(
        AuditEvent::DdlChange,
        None,
        None,
        "metadata_group",
        &detail_json,
        &AuditAuth {
            user_id,
            user_name,
            session_id: String::new(),
        },
    );
}

/// Return `(descriptor_name, version_after, hlc_string)` for a
/// stamped `CatalogEntry`. Delete* variants return `version_after = 0`
/// since the object is being removed.
pub(super) fn describe_entry(e: &catalog_entry::CatalogEntry) -> (String, u64, String) {
    use catalog_entry::CatalogEntry as E;
    match e {
        E::PutCollection(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::PutCollectionIfAbsent(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::DeactivateCollection { name, .. } => (name.clone(), 0, String::new()),
        E::PurgeCollection { name, .. } => (name.clone(), 0, String::new()),
        E::RecordWalTombstone { collection, .. } => (collection.clone(), 0, String::new()),
        E::PutSequence(s) => (
            s.name.clone(),
            s.descriptor_version,
            format!("{:?}", s.modification_hlc),
        ),
        E::DeleteSequence { name, .. } => (name.clone(), 0, String::new()),
        E::PutSequenceState(s) => (s.name.clone(), 0, String::new()),
        E::PutTrigger(t) => (
            t.name.clone(),
            t.descriptor_version,
            format!("{:?}", t.modification_hlc),
        ),
        E::DeleteTrigger { name, .. } => (name.clone(), 0, String::new()),
        E::PutFunction(f) => (
            f.name.clone(),
            f.descriptor_version,
            format!("{:?}", f.modification_hlc),
        ),
        E::DeleteFunction { name, .. } => (name.clone(), 0, String::new()),
        E::PutProcedure(p) => (
            p.name.clone(),
            p.descriptor_version,
            format!("{:?}", p.modification_hlc),
        ),
        E::DeleteProcedure { name, .. } => (name.clone(), 0, String::new()),
        E::PutSchedule(s) => (s.name.clone(), 0, String::new()),
        E::DeleteSchedule { name, .. } => (name.clone(), 0, String::new()),
        E::PutChangeStream(cs) => (cs.name.clone(), 0, String::new()),
        E::DeleteChangeStream { name, .. } => (name.clone(), 0, String::new()),
        E::PutUser(u) => (u.username.clone(), 0, String::new()),
        E::DropUser { username, .. } => (username.clone(), 0, String::new()),
        E::PutRole(r) => (r.name.clone(), 0, String::new()),
        E::DeleteRole { name, .. } => (name.clone(), 0, String::new()),
        E::PutApiKey(k) => (k.key_id.clone(), 0, String::new()),
        E::RevokeApiKey { key_id, .. } => (key_id.clone(), 0, String::new()),
        E::PutMaterializedView(m) => (m.name.clone(), 0, String::new()),
        E::DeleteMaterializedView { name, .. } => (name.clone(), 0, String::new()),
        E::PutContinuousAggregate(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::DeleteContinuousAggregate { name, .. } => (name.clone(), 0, String::new()),
        E::PutTenant(t) => (t.name.clone(), 0, String::new()),
        E::PutTenantWithAdmin { tenant, admin } => (tenant.name.clone(), 0, admin.username.clone()),
        E::DeleteTenant { tenant_id, .. } => (tenant_id.to_string(), 0, String::new()),
        E::PutRlsPolicy(p) => (p.name.clone(), 0, String::new()),
        E::DeleteRlsPolicy { name, .. } => (name.clone(), 0, String::new()),
        E::PutPermission(p) => (
            format!("{}@{}:{}", p.grantee, p.target, p.permission),
            0,
            String::new(),
        ),
        E::DeletePermission {
            target,
            grantee,
            permission,
        } => (format!("{grantee}@{target}:{permission}"), 0, String::new()),
        E::PutOwner(o) => (o.object_name.clone(), 0, String::new()),
        E::DeleteOwner { object_name, .. } => (object_name.clone(), 0, String::new()),
        E::PutSynonymGroup(g) => (g.name.clone(), 0, String::new()),
        E::DeleteSynonymGroup { name, .. } => (name.clone(), 0, String::new()),
        E::PutCustomType(t) => (t.name.clone(), 0, String::new()),
        E::DeleteCustomType { name, .. } => (name.clone(), 0, String::new()),
        E::PutDatabase(d) => (d.name.clone(), 0, String::new()),
        E::DeleteDatabase { db_id } => (db_id.to_string(), 0, String::new()),
        E::PutDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => (
            format!("db:{db_id}:user:{user_id}:{privilege}"),
            0,
            String::new(),
        ),
        E::DeleteDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => (
            format!("db:{db_id}:user:{user_id}:{privilege}"),
            0,
            String::new(),
        ),
        E::CloneDatabase {
            target_descriptor, ..
        } => (target_descriptor.name.clone(), 0, String::new()),
        E::MoveTenantCutover { tenant_id, .. } => (format!("tenant:{tenant_id}"), 0, String::new()),
        E::PutOidcProvider(p) => (p.provider_name.clone(), 0, String::new()),
        E::DeleteOidcProvider { name } => (name.clone(), 0, String::new()),
    }
}
