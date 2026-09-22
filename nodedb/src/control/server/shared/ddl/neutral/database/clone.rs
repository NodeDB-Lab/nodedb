// SPDX-License-Identifier: BUSL-1.1

//! Handler for `CLONE DATABASE <new> FROM <source> [AS OF SYSTEM TIME <ms> | LATEST]`.
//!
//! Ported from the pgwire `ddl::database::clone` handler. Source resolution,
//! the superuser gate (after source resolution so the audit carries the source
//! db), mirror rejection, `MAX_CLONE_DEPTH` enforcement, duplicate-name check,
//! as-of LSN resolution, descriptor build, Raft propose / single-node
//! lineage-then-descriptor write with compensating rollback, shadow-collection
//! stamping, allocator-hwm flush, and `DatabaseCloned` audit are preserved
//! verbatim; only the result construction changed from pgwire `Response` to the
//! protocol-neutral [`DdlResult`].

use nodedb_sql::ddl_ast::CloneAsOf;
use nodedb_types::{DatabaseId, MAX_CLONE_DEPTH};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::custom_type::register_written;
use crate::control::clone::catalog_copy::copy_database_metadata;
use crate::control::clone::lsn_resolve::wall_ms_to_lsn;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::database_types::{
    DatabaseDescriptor, DatabaseStatus, ParentCloneRef,
};
use crate::control::security::catalog::{StoredOwner, UNASSIGNED_OID};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply;
use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_superuser;
use super::support::{ddl_err, status};

/// Parameters for `clone_database`, extracted from the parsed AST.
pub struct CloneDatabaseParams<'a> {
    pub new_name: &'a str,
    pub source_name: &'a str,
    pub as_of: &'a CloneAsOf,
}

/// Handle `CLONE DATABASE <new_name> FROM <source_name> [AS OF …]`.
///
/// Required role: `Superuser`.
pub async fn clone_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    params: CloneDatabaseParams<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let catalog = state.credentials.catalog();

    // ── Resolve source database ───────────────────────────────────────────────
    let source_db_id = catalog
        .get_database_id_by_name(params.source_name)
        .map_err(|e| ddl_err("XX000", format!("catalog lookup failed: {e}")))?
        .ok_or_else(|| {
            ddl_err(
                "42P01",
                format!("source database '{}' not found", params.source_name),
            )
        })?;

    // Gate after source_db_id resolution so the audit record carries the source db.
    require_superuser(state, identity, Some(source_db_id), "CLONE DATABASE")?;

    let source_descriptor = catalog
        .get_database(source_db_id)
        .map_err(|e| ddl_err("XX000", format!("catalog read failed: {e}")))?
        .ok_or_else(|| {
            ddl_err(
                "42P01",
                format!(
                    "source database '{}' descriptor missing",
                    params.source_name
                ),
            )
        })?;

    // ── Reject cloning a mirror ───────────────────────────────────────────────
    //
    // Mirror catalog entries don't exist yet in the current implementation.
    // The check below calls a helper that returns `Ok(false)` until the mirror
    // subsystem is wired; when mirrors land, this helper will inspect the
    // descriptor's status.
    if is_mirror_database(&source_descriptor) {
        return Err(DdlError::cannot_clone_mirror(format!(
            "database '{}' is a mirror and cannot be cloned; \
             promote it with ALTER DATABASE {} PROMOTE first",
            params.source_name, params.source_name,
        )));
    }

    // ── Enforce MAX_CLONE_DEPTH ────────────────────────────────────────────────
    let depth = clone_chain_depth(state, source_db_id)
        .map_err(|e| ddl_err("XX000", format!("clone depth check failed: {e}")))?;

    if depth >= MAX_CLONE_DEPTH {
        return Err(ddl_err(
            nodedb_types::error::sqlstate::CLONE_DEPTH_EXCEEDED,
            format!(
                "clone chain depth {} equals the maximum of {}; \
                 materialize a clone to flatten the chain before cloning again",
                depth, MAX_CLONE_DEPTH,
            ),
        ));
    }

    // ── Reject duplicate name ─────────────────────────────────────────────────
    match catalog.get_database_id_by_name(params.new_name) {
        Ok(Some(_)) => {
            return Err(ddl_err(
                "42P04",
                format!("database '{}' already exists", params.new_name),
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(ddl_err("XX000", format!("catalog lookup failed: {e}")));
        }
    }

    // ── Resolve as_of LSN ─────────────────────────────────────────────────────
    //
    // For `Latest` we use the current WAL frontier as the clone point.
    //
    // For `SystemTimeMs(t)` we resolve ms → LSN via the `LsnMsAnchor` map
    // held on `SharedState`.  When the map is populated (WAL anchors have been
    // replayed or emitted) this is a precise interpolation.  When the map is
    // empty the WAL frontier is used as the best available approximation,
    // which is correct for recent timestamps (within the same server session).
    let now_ms =
        current_wall_ms().map_err(|e| ddl_err("XX000", format!("clock read failed: {e}")))?;
    let (as_of_lsn, as_of_ms) = match params.as_of {
        CloneAsOf::Latest => (state.wal.next_lsn(), now_ms),
        CloneAsOf::SystemTimeMs(ms) => {
            // wall_ms_to_lsn resolves via the LsnMsAnchor map; falls back to
            // wal.next_lsn() when the map is empty (correct for recent clones).
            let lsn = wall_ms_to_lsn(state, *ms);
            (lsn, *ms)
        }
    };

    let clone_created_at = state.wal.next_lsn();

    // ── Allocate target database id ───────────────────────────────────────────
    let target_db_id = state.database_registry.alloc_one();

    // ── Build descriptor ──────────────────────────────────────────────────────
    let target_descriptor = DatabaseDescriptor {
        id: target_db_id,
        name: params.new_name.to_string(),
        status: DatabaseStatus::Cloning,
        created_at_lsn: clone_created_at.as_u64(),
        quota_ref: source_descriptor.quota_ref,
        parent_clone: Some(ParentCloneRef {
            source_db_id,
            as_of_lsn: as_of_lsn.as_u64(),
            as_of_ms: as_of_ms as u64,
            // Capture the surrogate high-water at clone-create time.
            // Source bindings allocated AFTER this point belong to writes
            // that happened after the clone's AS-OF and must not be
            // visible from the clone — the lazy KV read path uses this
            // ceiling to filter source-delegated rows.
            kv_surrogate_ceiling: Some(state.surrogate_assigner.current_hwm()),
        }),
        mirror_origin: None,
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };

    // ── Propose via Raft ──────────────────────────────────────────────────────
    let entry = CatalogEntry::CloneDatabase {
        target_descriptor: Box::new(target_descriptor.clone()),
        source_db_id: source_db_id.as_u64(),
    };

    let outcome = propose_catalog_entry(state, &entry)
        .map_err(|e| ddl_err("XX000", format!("catalog propose failed: {e}")))?;

    // Single-node fast path (`LocalOnly` means "no Raft, apply directly").
    //
    // Order matters for partial-failure safety: write the lineage edge first,
    // then the descriptor. If lineage succeeds and descriptor fails we roll the
    // lineage entry back — leaving no partial state. If we reversed the order,
    // a descriptor-then-lineage failure would create a clone that DROP DATABASE
    // on the source would not see as a dependent, allowing unsafe drops.
    if outcome.needs_local_apply() {
        catalog
            .add_clone_child(source_db_id, target_db_id)
            .map_err(|e| ddl_err("XX000", format!("lineage write failed: {e}")))?;

        if let Err(put_err) = catalog.put_database(&target_descriptor) {
            // Compensate: remove the lineage edge we just wrote. A failure here
            // is fatal — surface both errors so on-call can repair the catalog.
            if let Err(rb_err) = catalog.remove_clone_child(source_db_id, target_db_id) {
                return Err(ddl_err(
                    "XX000",
                    format!(
                        "catalog write failed: {put_err}; \
                         lineage rollback ALSO failed: {rb_err} — \
                         catalog left with orphan lineage edge \
                         (source={source_db_id}, target={target_db_id})",
                    ),
                ));
            }
            return Err(ddl_err("XX000", format!("catalog write failed: {put_err}")));
        }

        // Stamp every active source collection into the target database with
        // `cloned_from` set.  This lets the SQL planner resolve collection
        // names against the clone without knowing about clone indirection;
        // CoW delegation happens at dispatch time.
        let source_colls = catalog
            .load_all_collections(source_db_id)
            .map_err(|e| ddl_err("XX000", format!("clone: enumerate source collections: {e}")))?;
        let kv_surrogate_ceiling = Some(state.surrogate_assigner.current_hwm());
        for mut coll in source_colls.into_iter().filter(|c| c.is_active) {
            coll.database_id = target_db_id;
            coll.cloned_from = Some(nodedb_types::CloneOrigin {
                source_database: source_db_id,
                source_collection: coll.name.clone(),
                as_of_lsn,
                clone_created_at,
                kv_surrogate_ceiling,
            });
            coll.clone_status = nodedb_types::CloneStatus::Shadowed;
            coll.descriptor_version = 0;
            // Fatal, not a warning: an unstamped descriptor means the clone is
            // reported as created while one of the source's collections simply
            // does not resolve in it, and nothing later re-stamps it. Surfacing
            // the failure is the only way the caller learns the clone is
            // incomplete.
            catalog.put_collection(target_db_id, &coll).map_err(|e| {
                ddl_err(
                    "XX000",
                    format!(
                        "clone: stamping shadow descriptor for collection '{}' failed: {e}",
                        coll.name
                    ),
                )
            })?;

            // The owner row is keyed by database, so the clone needs its own.
            // Without it the collection resolves but every ownership check
            // against it reports no owner.
            let owner = StoredOwner {
                database_id: target_db_id.as_u64(),
                object_type: object_type::COLLECTION.to_string(),
                object_name: coll.name.clone(),
                tenant_id: coll.tenant_id,
                owner_username: coll.owner.clone(),
            };
            catalog.put_owner(&owner).map_err(|e| {
                ddl_err(
                    "XX000",
                    format!(
                        "clone: stamping owner for collection '{}' failed: {e}",
                        coll.name
                    ),
                )
            })?;
        }

        // Copy the source's database-scoped catalog rows: vector index
        // params, vector models, column statistics, index records, RLS and
        // redaction policies, triggers, retention policies, alert rules,
        // continuous aggregates, and streaming materialized views. Schedules
        // are not copied — see `catalog_copy::copy_scoped_objects`.
        //
        // A missing row is fatal, for the same reason an unstamped
        // descriptor is fatal. The clone reports itself created while it
        // answers queries the source answers differently, and nothing later
        // re-copies the row.
        copy_database_metadata(catalog, source_db_id, target_db_id)
            .map_err(|e| ddl_err("XX000", format!("clone: copying catalog metadata: {e}")))?;
    }

    // Synonym groups and custom types travel as proposed entries, not as a
    // catalog copy. Each needs two more effects than a redb write: the
    // in-memory registry SHOW reads, and for a group the FTS backend on every
    // node. A propose runs the applier and both post-apply lanes everywhere,
    // which is the only path that delivers all three.
    //
    // Outside the `needs_local_apply` block above on purpose: a propose is the
    // thing that reaches every node, and on a cluster proposer that branch is
    // false.
    copy_synonym_groups(state, source_db_id, target_db_id).await?;
    copy_custom_types(state, source_db_id, target_db_id)?;

    // Flush the allocator hwm so restarts pick up the correct next-id boundary.
    if state.database_registry.should_flush() {
        let hwm = state.database_registry.current_hwm();
        if let Err(e) = catalog.put_database_hwm(hwm) {
            tracing::warn!("database hwm flush failed after clone: {e}");
        }
    }

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseCloned,
        None,
        Some(target_db_id),
        &identity.username,
        &format!(
            "CLONE DATABASE {} FROM {} AS OF SYSTEM TIME {}",
            params.new_name, params.source_name, as_of_ms
        ),
    );

    Ok(status("CLONE DATABASE"))
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Propose one `PutSynonymGroup` per source group, rewritten to the target.
///
/// A group also lives in each node's FTS backend, which only the post-apply
/// lane reaches. On the `LocalOnly` path no applier runs, so this installs the
/// group and registers it here instead.
///
/// A failed propose is fatal, for the same reason an unstamped descriptor is:
/// the clone reports itself created while a text query against it expands
/// fewer terms than the source, and nothing later re-proposes the row.
async fn copy_synonym_groups(
    state: &SharedState,
    source: DatabaseId,
    target: DatabaseId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let groups = catalog
        .load_synonym_groups_in_database(source.as_u64())
        .map_err(|e| {
            ddl_err(
                "XX000",
                format!("clone: enumerate source synonym groups: {e}"),
            )
        })?;

    for mut group in groups {
        group.database_id = target.as_u64();
        let entry = CatalogEntry::PutSynonymGroup(Box::new(group.clone()));
        let outcome = propose_and_apply(state, &entry).map_err(|e| {
            ddl_err(
                "XX000",
                format!(
                    "clone: copying synonym group '{}': {}",
                    group.name, e.message
                ),
            )
        })?;
        if outcome.needs_local_apply() {
            state.synonym_registry.register(group.clone());
            crate::control::catalog_entry::post_apply::install_synonym_group(group, state).await;
        }
    }
    Ok(())
}

/// Propose one `PutCustomType` per source type, rewritten to the target.
///
/// The copy drops the source OID. A shared OID holds only while both
/// definitions match, and `ALTER TYPE ADD VALUE` on either side then leaves
/// two definitions under one identity. The catalog assigns each copy a fresh
/// OID when the entry applies, identically on every node.
///
/// A failed propose is fatal: the clone would resolve neither a copied
/// descriptor's typed column nor the OID a pgwire client reads back.
fn copy_custom_types(
    state: &SharedState,
    source: DatabaseId,
    target: DatabaseId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let types = catalog
        .load_custom_types_in_database(source.as_u64())
        .map_err(|e| {
            ddl_err(
                "XX000",
                format!("clone: enumerate source custom types: {e}"),
            )
        })?;

    for mut custom_type in types {
        custom_type.database_id = target.as_u64();
        custom_type.oid = UNASSIGNED_OID;
        let entry = CatalogEntry::PutCustomType(Box::new(custom_type.clone()));
        let outcome = propose_and_apply(state, &entry).map_err(|e| {
            ddl_err(
                "XX000",
                format!(
                    "clone: copying custom type '{}': {}",
                    custom_type.name, e.message
                ),
            )
        })?;
        if outcome.needs_local_apply() {
            register_written(
                custom_type.database_id,
                custom_type.tenant_id,
                &custom_type.name,
                state,
            );
        }
    }
    Ok(())
}

/// Returns `true` if `descriptor` represents a mirror database.
///
/// Mirror catalog entries do not exist in the current implementation;
/// this helper will be updated to inspect `DatabaseStatus::Mirroring`
/// when the mirror subsystem is wired.  Until then it returns `false`
/// so all non-mirror paths proceed normally.
fn is_mirror_database(descriptor: &DatabaseDescriptor) -> bool {
    matches!(descriptor.status, DatabaseStatus::Mirroring)
}

/// Walk the `parent_clone` chain upward from `start_db_id`, counting hops.
/// Returns the depth (0 = no clone ancestry, 1 = direct clone, …).
///
/// The chain is bounded by `MAX_CLONE_DEPTH` — if we count more hops than
/// that we short-circuit and return `MAX_CLONE_DEPTH + 1` so the caller's
/// `>= MAX_CLONE_DEPTH` guard fires.
fn clone_chain_depth(state: &SharedState, start_db_id: DatabaseId) -> crate::Result<u32> {
    let catalog = state.credentials.catalog();

    let mut current = start_db_id;
    let mut depth: u32 = 0;

    loop {
        if depth > MAX_CLONE_DEPTH {
            return Ok(depth);
        }
        let desc = catalog
            .get_database(current)
            .map_err(|e| crate::Error::Storage {
                engine: "catalog".into(),
                detail: format!("depth walk get_database failed: {e}"),
            })?;
        match desc.and_then(|d| d.parent_clone) {
            None => return Ok(depth),
            Some(parent) => {
                current = parent.source_db_id;
                depth += 1;
            }
        }
    }
}

/// Current wall-clock milliseconds since Unix epoch.
///
/// Returns `Err` if the system clock is set before the Unix epoch — caller
/// must surface the failure rather than silently substituting a sentinel.
fn current_wall_ms() -> crate::Result<i64> {
    nodedb_types::clock::since_epoch()
        .map(|d| d.as_millis() as i64)
        .ok_or_else(|| crate::Error::Internal {
            detail: "clone_database: system clock predates Unix epoch".to_owned(),
        })
}
