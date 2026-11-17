// SPDX-License-Identifier: BUSL-1.1

//! Handler for `CLONE DATABASE <new> FROM <source> [AS OF SYSTEM TIME <ms> | LATEST]`.
//!
//! Source resolution, the superuser gate (after source resolution so the
//! audit carries the source db), mirror rejection, `MAX_CLONE_DEPTH`
//! enforcement, duplicate-name check, as-of LSN resolution, descriptor build,
//! catalog propose (whose apply stamps the shadow collections), and
//! `DatabaseCloned` audit run here. The result is the protocol-neutral
//! [`DdlResult`].

use nodedb_sql::ddl_ast::CloneAsOf;
use nodedb_types::{DatabaseId, MAX_CLONE_DEPTH};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::clone::lsn_resolve::{anchored_clone_point, wall_ms_to_lsn};
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::UNASSIGNED_OID;
use crate::control::security::catalog::database_types::{
    DatabaseDescriptor, DatabaseStatus, ParentCloneRef,
};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply_async;
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
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?
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
        .map_err(|e| DdlError::from_error_in_context("catalog read failed", &e))?
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
        .map_err(|e| DdlError::from_error_in_context("clone depth check failed", &e))?;

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
            return Err(DdlError::from_error_in_context("catalog lookup failed", &e));
        }
    }

    // ── Resolve as_of LSN ─────────────────────────────────────────────────────
    //
    // For `Latest` we use the current WAL frontier as the clone point.
    //
    // For `SystemTimeMs(t)` the clone point is the highest LSN committed by
    // the end of millisecond `t`, from the WAL's time anchors. A `t` before
    // the oldest retained anchor is refused. So is a `t` before the source
    // database existed: it predates the commit of the WAL state the database
    // was created on.
    //
    // The clone's creation LSN is the WAL frontier, durable and anchored
    // before the clone exists. `Latest` clones at that frontier. A raw
    // frontier counts records no read cut reaches yet. A transaction that
    // reads the clone after its creation then resolves below it and reads
    // the clone as empty.
    let now_ms =
        current_wall_ms().map_err(|e| DdlError::from_error_in_context("clock read failed", &e))?;
    let clone_created_at = anchored_clone_point(state)
        .await
        .map_err(|e| DdlError::from_error_in_context("clone point fsync failed", &e))?;
    let (as_of_lsn, as_of_ms) = match params.as_of {
        CloneAsOf::Latest => (clone_created_at, now_ms),
        CloneAsOf::SystemTimeMs(ms) => {
            let lsn = wall_ms_to_lsn(state, *ms).map_err(|e| {
                ddl_err(
                    "22000",
                    format!("CLONE DATABASE AS OF SYSTEM TIME {ms}: {e}"),
                )
            })?;
            let created = nodedb_types::Lsn::new(source_descriptor.created_at_lsn);
            if let Some(created_ms) = state.ms_to_lsn_inverse(created)
                && *ms < created_ms
            {
                return Err(ddl_err(
                    "22000",
                    format!(
                        "CLONE DATABASE AS OF SYSTEM TIME {ms}: the time predates database \
                         '{}', created on the WAL state committed at {created_ms} ms; \
                         clone it AS OF a later time",
                        params.source_name
                    ),
                ));
            }
            (lsn, *ms)
        }
    };

    // ── Allocate target database id ───────────────────────────────────────────
    let target_db_id = crate::control::database::allocate_database_id(state)
        .await
        .map_err(|e| DdlError::from_error_in_context("database id allocation failed", &e))?;

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
    // The proposer stamps the incarnation the shadow collections take.
    let entry = CatalogEntry::CloneDatabase {
        target_descriptor: Box::new(target_descriptor),
        source_db_id: source_db_id.as_u64(),
        incarnation: nodedb_types::Hlc::ZERO,
    };

    // The apply writes the descriptor, stamps a shadow descriptor and an
    // owner row for every active source collection, copies the source's
    // database-scoped catalog rows, and writes the lineage edge last.
    propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;

    // Synonym groups and custom types travel as proposed entries, not as a
    // catalog copy. Each needs two more effects than a redb write: the
    // in-memory registry SHOW reads, and for a group the FTS backend on every
    // node. A propose runs the applier and both post-apply lanes everywhere,
    // which is the only path that delivers all three.
    copy_synonym_groups(state, source_db_id, target_db_id).await?;
    copy_custom_types(state, source_db_id, target_db_id).await?;

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
/// lane reaches.
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
            DdlError::from_error_in_context("clone: enumerate source synonym groups", &e)
        })?;

    for mut group in groups {
        group.database_id = target.as_u64();
        let entry = CatalogEntry::PutSynonymGroup(Box::new(group.clone()));
        propose_and_apply_async(state, &entry)
            .await
            .map_err(|e| e.in_context(&format!("clone: copying synonym group '{}'", group.name)))?;
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
/// A failed propose is fatal: the clone will resolve neither a copied
/// descriptor's typed column nor the OID a pgwire client reads back.
async fn copy_custom_types(
    state: &SharedState,
    source: DatabaseId,
    target: DatabaseId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let types = catalog
        .load_custom_types_in_database(source.as_u64())
        .map_err(|e| DdlError::from_error_in_context("clone: enumerate source custom types", &e))?;

    for mut custom_type in types {
        custom_type.database_id = target.as_u64();
        custom_type.oid = UNASSIGNED_OID;
        let entry = CatalogEntry::PutCustomType(Box::new(custom_type.clone()));
        propose_and_apply_async(state, &entry).await.map_err(|e| {
            e.in_context(&format!(
                "clone: copying custom type '{}'",
                custom_type.name
            ))
        })?;
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
        let desc = catalog.get_database(current)?;
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| crate::Error::Internal {
            detail: format!("clone_database: system clock predates Unix epoch: {e}"),
        })
}
