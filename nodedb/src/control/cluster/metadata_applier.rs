//! Production metadata-group commit applier.
//!
//! Single branch for DDL: decode the opaque `CatalogDdl { payload }`
//! as a host-side [`CatalogEntry`], write through to `SystemCatalog`
//! redb via [`catalog_entry::apply_to`], and spawn the post-apply
//! side effects (Data Plane register, sequence registry sync, etc.).
//! All 16 per-DDL-object types are handled by adding a variant to
//! `CatalogEntry` — nothing in this file changes per type.
//!
//! The applier also advances the `AppliedIndexWatcher` (so sync
//! pgwire handlers unblock on commit) and broadcasts
//! `CatalogChangeEvent` (for future prepared-statement / catalog
//! cache invalidation).

use std::sync::{Arc, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;
use tracing::{debug, warn};

use nodedb_cluster::{MetadataApplier, MetadataCache, MetadataEntry, decode_entry};

use crate::control::catalog_entry;
use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Broadcast channel capacity — small, because consumers are
/// internal subsystems that keep up or are lagged intentionally.
pub const CATALOG_CHANNEL_CAPACITY: usize = 64;

/// Event published on every committed metadata entry.
#[derive(Debug, Clone)]
pub struct CatalogChangeEvent {
    pub applied_index: u64,
}

/// Production `MetadataApplier` installed on the `RaftLoop`.
pub struct MetadataCommitApplier {
    cache: Arc<RwLock<MetadataCache>>,
    watcher: Arc<AppliedIndexWatcher>,
    catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    credentials: Arc<CredentialStore>,
    /// Weak handle to `SharedState`. Installed by `start_raft` after
    /// construction so the applier can spawn async post-apply side
    /// effects (Data Plane register on `PutCollection`,
    /// `sequence_registry.create` on `PutSequence`, etc.). Weak to
    /// break the Arc cycle (SharedState → raft loop → applier →
    /// SharedState). `None` in unit tests.
    shared: OnceLock<Weak<SharedState>>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<RwLock<MetadataCache>>,
        watcher: Arc<AppliedIndexWatcher>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
        credentials: Arc<CredentialStore>,
    ) -> Self {
        Self {
            cache,
            watcher,
            catalog_change_tx,
            credentials,
            shared: OnceLock::new(),
        }
    }

    /// Install a weak handle to `SharedState` so the applier can
    /// spawn post-apply side effects. Must be called **before** the
    /// raft loop starts ticking; `start_raft` does this as part of
    /// its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }

    /// Apply a single decoded `MetadataEntry`'s host-side effects.
    ///
    /// - `CatalogDdl` → decode payload as `CatalogEntry`, write
    ///   through to redb via `catalog_entry::apply_to`, spawn async
    ///   post-apply side effects if `SharedState` is reachable.
    /// - Non-DDL variants (topology, routing, lease, version) have
    ///   no host-side redb effects in this crate — the cluster crate
    ///   already tracks them in the `MetadataCache`.
    fn apply_host_side_effects(&self, entry: &MetadataEntry, raft_index: u64) {
        // Atomic batches unpack one level: the sub-entries are
        // applied individually so each gets its own audit record
        // stamped with the same raft_index (they committed at the
        // same log position).
        if let MetadataEntry::Batch { entries } = entry {
            for sub in entries {
                self.apply_host_side_effects(sub, raft_index);
            }
            return;
        }

        // Handle non-CatalogDdl variants that still have host-side
        // effects. Drain start/end land on `shared.lease_drain` on
        // every node so the next `force_refresh_lease` check sees
        // the replicated drain state.
        match entry {
            MetadataEntry::DescriptorDrainStart {
                descriptor_id,
                up_to_version,
                expires_at,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_start(
                        descriptor_id.clone(),
                        *up_to_version,
                        *expires_at,
                    );
                    debug!(
                        descriptor = ?descriptor_id,
                        up_to_version,
                        "drain_start applied to host tracker"
                    );
                }
                return;
            }
            MetadataEntry::DescriptorDrainEnd { descriptor_id } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    shared.lease_drain.install_end(descriptor_id);
                    debug!(
                        descriptor = ?descriptor_id,
                        "drain_end applied to host tracker"
                    );
                }
                return;
            }
            MetadataEntry::CaTrustChange {
                add_ca_cert,
                remove_ca_fingerprint,
            } => {
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    apply_ca_trust_change(
                        &shared,
                        add_ca_cert.as_deref(),
                        remove_ca_fingerprint.as_ref(),
                        raft_index,
                    );
                }
                return;
            }
            MetadataEntry::SurrogateAlloc { hwm } => {
                // Advance the in-memory surrogate high-watermark on every
                // node. `restore_hwm` is idempotent and monotonic: calling
                // it with a value at or below the current HWM is a no-op,
                // so duplicate or reordered delivery cannot push the
                // counter backwards. Also persist the hwm to the catalog so
                // the local node survives a restart without re-reading the
                // full log.
                if let Some(weak) = self.shared.get()
                    && let Some(shared) = weak.upgrade()
                {
                    let reg = shared
                        .surrogate_assigner
                        .registry_handle()
                        .read()
                        .unwrap_or_else(|p| p.into_inner());
                    if let Err(e) = reg.restore_hwm(*hwm) {
                        warn!(hwm, error = %e, "surrogate_alloc apply: restore_hwm failed");
                    }
                    drop(reg);
                    // Best-effort catalog persist: a failure means the
                    // next restart will re-derive the HWM from the log,
                    // which is correct — just slightly slower. We must
                    // not block the apply loop on catalog I/O.
                    if let Some(catalog) = self.credentials.catalog()
                        && let Err(e) = catalog.put_surrogate_hwm(*hwm)
                    {
                        warn!(
                            hwm,
                            error = %e,
                            "surrogate_alloc apply: failed to persist hwm to catalog"
                        );
                    }
                    debug!(hwm, raft_index, "surrogate hwm advanced via raft");
                }
                return;
            }
            _ => {}
        }

        let Some(catalog) = self.credentials.catalog() else {
            return;
        };
        let (payload, audit) = match entry {
            MetadataEntry::CatalogDdl { payload } => (payload, None),
            MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id,
                auth_user_name,
                sql_text,
            } => (
                payload,
                Some((
                    auth_user_id.clone(),
                    auth_user_name.clone(),
                    sql_text.clone(),
                )),
            ),
            _ => return,
        };
        let catalog_entry = match catalog_entry::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "metadata applier: failed to decode CatalogEntry payload");
                return;
            }
        };

        // Stamp `descriptor_version` + `modification_hlc` for every
        // `Put*` variant that carries them. Gated on the rolling
        // upgrade flag — in mixed-version clusters the older nodes
        // do not have the stamp logic, so we leave the entry's
        // sentinel `0` / `Hlc::ZERO` in place and let resolvers
        // treat it as "unknown, always re-fetch". Only the proposing
        // node's apply path observes the gate; followers run the
        // same applier and will reach the same conclusion because
        // every node observes the same live topology (same
        // `wire_version` on every NodeInfo, replicated via the
        // gossip path).
        let stamped = if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            let compat = !shared.cluster_version_view().can_activate_feature(
                crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION,
            );
            if compat {
                catalog_entry
            } else {
                catalog_entry::descriptor_stamp::stamp(catalog_entry, &shared.hlc_clock, catalog)
            }
        } else {
            // Unit tests construct the applier without a SharedState.
            // They don't care about descriptor versioning — leave
            // the entry unstamped so existing assertions on
            // `descriptor_version == 0` still hold.
            catalog_entry
        };

        debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
        catalog_entry::apply::apply_to(&stamped, catalog);
        // Implicit drain clear: if the entry is a `Put*` for one
        // of the six stamped descriptor types, the DDL that was
        // waiting on drain has now committed — remove the drain
        // entry from every node's host tracker. Happens before
        // post_apply so a subsequent `acquire_lease` fired from
        // post_apply doesn't see a stale drain.
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            if let Some(drained_id) =
                crate::control::lease::drain_propose::descriptor_id_for_implicit_clear(&stamped)
            {
                shared.lease_drain.install_end(&drained_id);
            }
            // Run synchronous post-apply side effects INLINE so every
            // in-memory cache update (install_replicated_user,
            // install_replicated_owner, etc.) is visible before the
            // watcher bump. Any reader that observes `applied_index`
            // moving past `last` is guaranteed to see the sync side
            // effects of every entry up to `last`.
            //
            // The async tail (today: Data Plane Register dispatches
            // for PutCollection) is spawned separately and is NOT
            // part of the applied-index contract — it's a
            // performance optimisation, not a correctness gate.
            catalog_entry::post_apply::apply_post_apply_side_effects_sync(&stamped, &shared);

            // J.4: emit a DdlChange audit record on every replica.
            // Executed BEFORE spawning async post-apply side effects
            // so the audit entry lands synchronously with the rest of
            // the commit. When no audit context was attached
            // (internal lease / drain proposer), `audit` is `None`
            // and we still record the DDL — the user fields are empty.
            emit_ddl_audit(&shared, raft_index, &stamped, audit.as_ref());

            catalog_entry::post_apply::spawn_post_apply_async_side_effects(
                stamped, shared, raft_index,
            );
        }
    }
}

/// Apply a `MetadataEntry::CaTrustChange` on the host side: write or
/// delete `tls/ca.d/<fp>.crt`, emit an [`AuditEvent::CertRotation`]
/// record, and log for the operator. Hot-reload of the rustls config
/// picks up the new trust set on the next connection; the overlap
/// window guarantees existing connections keep working through the
/// rotation.
fn apply_ca_trust_change(
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
                warn!(error = %e, raft_index, "ca trust add failed");
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
                warn!(error = %e, raft_index, "ca trust remove failed");
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
            "metadata_group",
            &detail,
            &AuditAuth::default(),
        );
    }
}

/// Emit a [`AuditEvent::DdlChange`] record describing one applied
/// `CatalogEntry`. Kept as a free function so the applier stays the
/// orchestrator and the formatting lives next to the audit log.
fn emit_ddl_audit(
    shared: &SharedState,
    raft_index: u64,
    stamped: &catalog_entry::CatalogEntry,
    audit: Option<&(String, String, String)>,
) {
    use crate::control::security::audit::{AuditAuth, AuditEvent, DdlAuditDetail};
    use crate::control::security::catalog::StoredCollection;

    // Descriptor name + post-commit version.
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
    // `descriptor_kind` + `descriptor_name`. Keeping the helper
    // centralized means if we later propagate tenant on the wire
    // there's exactly one call site to update.
    let _ = std::any::type_name::<StoredCollection>();

    let mut log = match shared.audit.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    log.record_with_auth(
        AuditEvent::DdlChange,
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
fn describe_entry(e: &catalog_entry::CatalogEntry) -> (String, u64, String) {
    use catalog_entry::CatalogEntry as E;
    match e {
        E::PutCollection(c) => (
            c.name.clone(),
            c.descriptor_version,
            format!("{:?}", c.modification_hlc),
        ),
        E::DeactivateCollection { name, .. } => (name.clone(), 0, String::new()),
        E::PurgeCollection { name, .. } => (name.clone(), 0, String::new()),
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
        E::DeactivateUser { username, .. } => (username.clone(), 0, String::new()),
        E::PutRole(r) => (r.name.clone(), 0, String::new()),
        E::DeleteRole { name, .. } => (name.clone(), 0, String::new()),
        E::PutApiKey(k) => (k.key_id.clone(), 0, String::new()),
        E::RevokeApiKey { key_id, .. } => (key_id.clone(), 0, String::new()),
        E::PutMaterializedView(m) => (m.name.clone(), 0, String::new()),
        E::DeleteMaterializedView { name, .. } => (name.clone(), 0, String::new()),
        E::PutTenant(t) => (t.name.clone(), 0, String::new()),
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
    }
}

impl MetadataApplier for MetadataCommitApplier {
    fn apply(&self, entries: &[(u64, Vec<u8>)]) -> u64 {
        let mut last = 0u64;
        for (index, data) in entries {
            last = *index;
            if data.is_empty() {
                continue;
            }
            let entry = match decode_entry(data) {
                Ok(e) => e,
                Err(e) => {
                    warn!(index = *index, error = %e, "metadata decode failed");
                    continue;
                }
            };
            // 1. Cluster-owned cache state (topology, routing,
            //    leases, catalog_entries_applied counter).
            {
                let mut guard = self.cache.write().unwrap_or_else(|p| p.into_inner());
                guard.apply(*index, &entry);
            }
            // 2. Host side effects (redb writeback + async post-apply).
            self.apply_host_side_effects(&entry, *index);
        }
        if last > 0 {
            self.watcher.bump(last);
            let _ = self.catalog_change_tx.send(CatalogChangeEvent {
                applied_index: last,
            });
            debug!(applied_index = last, "metadata applier bumped watermark");
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::security::catalog::StoredCollection;
    use nodedb_cluster::encode_entry;

    fn make_applier() -> (
        MetadataCommitApplier,
        Arc<RwLock<MetadataCache>>,
        Arc<AppliedIndexWatcher>,
        Arc<CredentialStore>,
        tempfile::TempDir,
    ) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let credentials =
            Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        let cache = Arc::new(RwLock::new(MetadataCache::new()));
        let watcher = Arc::new(AppliedIndexWatcher::new());
        let (tx, _rx) = broadcast::channel(16);
        let applier =
            MetadataCommitApplier::new(cache.clone(), watcher.clone(), tx, credentials.clone());
        (applier, cache, watcher, credentials, tmp)
    }

    fn put_collection_entry(name: &str) -> MetadataEntry {
        let stored = StoredCollection::new(7, name, "tester");
        let catalog_entry = CatalogEntry::PutCollection(Box::new(stored));
        MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&catalog_entry).unwrap(),
        }
    }

    #[test]
    fn apply_put_collection_writes_through_to_redb() {
        let (applier, cache, watcher, credentials, _tmp) = make_applier();
        let bytes = encode_entry(&put_collection_entry("orders")).unwrap();
        assert_eq!(applier.apply(&[(11, bytes)]), 11);
        assert_eq!(watcher.current(), 11);

        let cache_guard = cache.read().unwrap();
        assert_eq!(cache_guard.applied_index, 11);
        assert_eq!(cache_guard.catalog_entries_applied, 1);
        drop(cache_guard);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(7, "orders")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders");
        assert_eq!(loaded.owner, "tester");
    }

    #[test]
    fn apply_deactivate_preserves_record() {
        let (applier, _cache, _watcher, credentials, _tmp) = make_applier();

        // Seed.
        applier.apply(&[(1, encode_entry(&put_collection_entry("archived")).unwrap())]);

        let drop_entry = MetadataEntry::CatalogDdl {
            payload: catalog_entry::encode(&CatalogEntry::DeactivateCollection {
                tenant_id: 7,
                name: "archived".into(),
            })
            .unwrap(),
        };
        applier.apply(&[(2, encode_entry(&drop_entry).unwrap())]);

        let loaded = credentials
            .catalog()
            .as_ref()
            .unwrap()
            .get_collection(7, "archived")
            .unwrap()
            .expect("preserved");
        assert!(!loaded.is_active);
    }

    #[test]
    fn apply_empty_batch_is_noop() {
        let (applier, _cache, watcher, _credentials, _tmp) = make_applier();
        assert_eq!(applier.apply(&[]), 0);
        assert_eq!(watcher.current(), 0);
    }
}
