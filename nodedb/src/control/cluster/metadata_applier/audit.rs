// SPDX-License-Identifier: BUSL-1.1

//! Audit and CA-trust helpers for `MetadataCommitApplier`.

use crate::control::catalog_entry;
use crate::control::state::SharedState;

use super::audit_describe::describe_entry;

/// Apply a `MetadataEntry::CaTrustChange` on the host side: write or
/// delete `tls/ca.d/<fp>.crt`, emit an [`AuditEvent::CertRotation`]
/// record, and log for the operator. Hot-reload of the rustls config
/// picks up the new trust set on the next connection; the overlap
/// window guarantees existing connections keep working through the
/// rotation.
///
/// A failed write or remove returns `Err`, so the entry is re-delivered.
/// Both are idempotent: a rewrite replaces the same file, and removing an
/// absent file succeeds.
pub(super) fn apply_ca_trust_change(
    shared: &SharedState,
    add: Option<&[u8]>,
    remove: Option<&[u8; 32]>,
    raft_index: u64,
) -> crate::Result<()> {
    use crate::control::cluster::tls::{TLS_SUBDIR, remove_trusted_ca, write_trusted_ca};
    use crate::control::security::audit::{AuditAuth, AuditEvent};

    let tls_dir = shared.data_dir.join(TLS_SUBDIR);

    let mut added_fp: Option<[u8; 32]> = None;
    if let Some(der) = add {
        let fp = write_trusted_ca(&tls_dir, der)?;
        added_fp = Some(fp);
        tracing::info!(
            fingerprint = %nodedb_cluster::ca_fingerprint_hex(&fp),
            raft_index,
            "cluster CA trust: added overlap anchor"
        );
    }
    if let Some(fp) = remove {
        remove_trusted_ca(&tls_dir, fp)?;
        tracing::info!(
            fingerprint = %nodedb_cluster::ca_fingerprint_hex(fp),
            raft_index,
            "cluster CA trust: removed overlap anchor"
        );
    }

    let detail = sonic_rs::to_string(&sonic_rs::json!({
        "raft_index": raft_index,
        "added_fingerprint": added_fp.map(|fp| nodedb_cluster::ca_fingerprint_hex(&fp)),
        "removed_fingerprint": remove.map(nodedb_cluster::ca_fingerprint_hex),
    }))
    .unwrap_or_default();
    crate::control::security::audit::record_concurrent(
        &shared.audit,
        crate::control::security::audit::AuditRecordInput {
            event: AuditEvent::CertRotation,
            tenant_id: None,
            database_id: None,
            source: "metadata_group",
            detail: &detail,
            auth: &AuditAuth::default(),
        },
    );
    Ok(())
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

    // A consumer offset commit and a backup schedule mark are progress, not
    // a schema change.
    if stamped.is_progress_mark() {
        return;
    }

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
    // extracting it per-variant will bloat this helper. Leave it
    // `None` at this layer — consumers that care route by
    // `descriptor_kind` + `descriptor_name`.
    let _ = std::any::type_name::<StoredCollection>();

    // Hand the record off rather than taking the audit mutex here.
    //
    // This runs on the Raft metadata apply loop — the single thread that
    // applies committed entries for group 0. Blocking it on a process-wide
    // mutex lets audit-log contention stall EVERY metadata apply, and with it
    // collection materialization, DDL, and any proposer waiting on the applied
    // index. That is a liveness bug rather than a slow path: one contended
    // acquisition here was measured parking the loop past a proposer's whole
    // apply wait, so peer `CollectionSchema` announces never materialized and
    // the engine writes that followed them were rejected as unknown
    // collections.
    //
    // The record's payload is fully built above and needs nothing further from
    // the apply loop, so deferring the emit keeps the compliance row (no silent
    // drop) while letting the loop advance. Hash-chain integrity is unaffected:
    // `stage_batch` allocates the sequence number under the same mutex, so
    // chain order follows lock-acquisition order exactly as it does for every
    // other audit writer.
    let audit = std::sync::Arc::clone(&shared.audit);
    let emit = move || {
        let auth = AuditAuth {
            user_id,
            user_name,
            session_id: String::new(),
        };
        crate::control::security::audit::record_concurrent(
            &audit,
            crate::control::security::audit::AuditRecordInput {
                event: AuditEvent::DdlChange,
                tenant_id: None,
                database_id: None,
                source: "metadata_group",
                detail: &detail_json,
                auth: &auth,
            },
        );
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn_blocking(emit);
        }
        // No reactor (unit tests, non-Tokio callers): emit inline. There is no
        // apply loop to protect in that context.
        Err(_) => emit(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::super::test_fixture::applier_with_shared_at;
    use super::*;
    use crate::control::security::audit::AuditEvent;
    use crate::control::security::audit::restore::replay_after_crash;
    use crate::control::security::catalog::StoredCollection;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ddl_audit_entry_survives_a_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_applier, state) = applier_with_shared_at(dir.path(), "ddl-audit.wal");
        let stamped = catalog_entry::CatalogEntry::PutCollection(Box::new(
            StoredCollection::stamped_for_test(7, "crash_coll", "tester"),
        ));
        emit_ddl_audit(&state, 11, &stamped, None);

        // The emit runs on a blocking task, so wait until it lands in the buffer.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let landed = state
                .audit
                .lock()
                .expect("audit lock")
                .all()
                .iter()
                .any(|e| e.source == "metadata_group");
            if landed {
                break;
            }
            assert!(Instant::now() < deadline, "the DDL audit emit never ran");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let replayed = replay_after_crash(&state.wal);
        assert!(
            replayed
                .iter()
                .any(|e| e.source == "metadata_group" && e.event == AuditEvent::DdlChange),
            "the metadata-applier entry must replay after a crash: {replayed:?}"
        );
    }
}
