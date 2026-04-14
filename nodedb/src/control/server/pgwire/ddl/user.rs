use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::types::{parse_role, require_admin, sqlstate_error};

/// Parse a single-quoted string from split parts starting at `start`.
/// Handles passwords like 'hello world' spanning multiple whitespace-split parts.
pub(crate) fn extract_quoted_string(parts: &[&str], start: usize) -> Option<String> {
    if start >= parts.len() {
        return None;
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return None;
    }

    if first.ends_with('\'') && first.len() > 1 {
        return Some(first[1..first.len() - 1].to_string());
    }

    let mut result = first[1..].to_string();
    for &part in &parts[start + 1..] {
        result.push(' ');
        if let Some(stripped) = part.strip_suffix('\'') {
            result.push_str(stripped);
            return Some(result);
        }
        result.push_str(part);
    }

    None
}

/// Find the index of the first part after a quoted string starting at `start`.
pub(crate) fn next_after_quoted(parts: &[&str], start: usize) -> usize {
    if start >= parts.len() {
        return parts.len();
    }

    let first = parts[start];
    if !first.starts_with('\'') {
        return start + 1;
    }

    if first.ends_with('\'') && first.len() > 1 {
        return start + 1;
    }

    for (i, part) in parts[start + 1..].iter().enumerate() {
        if part.ends_with('\'') {
            return start + 1 + i + 1;
        }
    }
    parts.len()
}

/// CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]
pub fn create_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "create users")?;

    if parts.len() < 6 {
        return Err(sqlstate_error(
            "42601",
            "syntax: CREATE USER <name> WITH PASSWORD '<password>' [ROLE <role>] [TENANT <id>]",
        ));
    }

    let username = parts[2];
    if !parts[3].eq_ignore_ascii_case("WITH") || !parts[4].eq_ignore_ascii_case("PASSWORD") {
        return Err(sqlstate_error(
            "42601",
            "expected WITH PASSWORD after username",
        ));
    }

    let password = extract_quoted_string(parts, 5)
        .ok_or_else(|| sqlstate_error("42601", "password must be a single-quoted string"))?;

    let mut role = Role::ReadWrite;
    let mut tenant_id = identity.tenant_id;
    let mut i = next_after_quoted(parts, 5);
    while i < parts.len() {
        match parts[i].to_uppercase().as_str() {
            "ROLE" if i + 1 < parts.len() => {
                role = parse_role(parts[i + 1]);
                i += 2;
            }
            "TENANT" if i + 1 < parts.len() => {
                if !identity.is_superuser {
                    return Err(sqlstate_error("42501", "only superuser can assign tenants"));
                }
                let tid: u32 = parts[i + 1]
                    .parse()
                    .map_err(|_| sqlstate_error("42601", "TENANT must be a numeric ID"))?;
                tenant_id = TenantId::new(tid);
                i += 2;
            }
            _ => i += 1,
        }
    }

    // Build the full `StoredUser` locally (hash + salt + user_id).
    // Followers cannot reproduce the random salt, so this step
    // MUST happen on the proposer node. The computed record is
    // then replicated verbatim.
    let stored = state
        .credentials
        .prepare_user(username, &password, tenant_id, vec![role])
        .map_err(|e| sqlstate_error("42710", &e.to_string()))?;

    let entry = crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0 {
        // Single-node / no-cluster fallback: install into the
        // in-memory cache so subsequent reads see the user.
        // Persist to redb when a catalog is wired up — the
        // catalog write is best-effort durability, not a gate
        // on the cache update. Test fixtures (and any future
        // fully-in-memory deployment) can run without a redb
        // catalog and still get correct read-after-write.
        if let Some(catalog) = state.credentials.catalog() {
            catalog
                .put_user(&stored)
                .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
        }
        state.credentials.install_replicated_user(&stored);
    } else {
        // Cluster mode: `propose_catalog_entry` waits for the
        // entry to be applied on THIS node, which runs the
        // synchronous post_apply (`install_replicated_user`)
        // inline BEFORE the applied-index watermark bumps. So if
        // our entry really committed, `get_user` must see it now.
        //
        // If `get_user` returns None, the Raft log entry at the
        // index our leader assigned has been truncated and
        // overwritten with a noop from a new leader term (a known
        // Raft subtlety: `propose` returns the assigned log index
        // without waiting for commit; if leadership changes
        // before the quorum ack, the entry is dropped). Return a
        // retryable error so `exec_ddl_on_any_leader` re-proposes
        // on the next attempt against whoever is now leader.
        if state.credentials.get_user(username).is_none() {
            return Err(sqlstate_error(
                "40001",
                "transient: metadata entry truncated by leader change, retry",
            ));
        }
    }

    state.audit_record(
        AuditEvent::PrivilegeChange,
        Some(tenant_id),
        &identity.username,
        &format!("created user '{username}' in tenant {tenant_id}"),
    );

    Ok(vec![Response::Execution(Tag::new("CREATE USER"))])
}

/// ALTER USER <name> SET PASSWORD '<password>'
/// ALTER USER <name> SET ROLE <role>
pub fn alter_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    if parts.len() < 5 {
        return Err(sqlstate_error(
            "42601",
            "syntax: ALTER USER <name> SET PASSWORD '<password>' | ALTER USER <name> SET ROLE <role>",
        ));
    }

    let username = parts[2];

    // Users can change their own password; admin required for anything else.
    let is_self = username == identity.username;
    if !is_self && !identity.is_superuser && !identity.has_role(&Role::TenantAdmin) {
        return Err(sqlstate_error(
            "42501",
            "permission denied: can only alter your own user, or be superuser/tenant_admin",
        ));
    }

    if !parts[3].eq_ignore_ascii_case("SET") {
        return Err(sqlstate_error("42601", "expected SET after username"));
    }

    match parts[4].to_uppercase().as_str() {
        "PASSWORD" => {
            let password = extract_quoted_string(parts, 5).ok_or_else(|| {
                sqlstate_error("42601", "password must be a single-quoted string")
            })?;

            // Build the updated `StoredUser` locally (re-hashes
            // with a fresh salt on the leader — followers can't
            // reproduce the random salt) and replicate through
            // raft. Applier on every node writes redb + upserts
            // the in-memory cache.
            let stored = state
                .credentials
                .prepare_user_update(username, Some(&password), None)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            let entry =
                crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
            let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
            if log_index == 0 {
                if let Some(catalog) = state.credentials.catalog() {
                    catalog
                        .put_user(&stored)
                        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
                }
                state.credentials.install_replicated_user(&stored);
            }

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("changed password for user '{username}'"),
            );

            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }
        "ROLE" => {
            if is_self && !identity.is_superuser {
                return Err(sqlstate_error("42501", "cannot change your own role"));
            }
            require_admin(identity, "change roles")?;

            if parts.len() < 6 {
                return Err(sqlstate_error("42601", "expected role name after SET ROLE"));
            }

            let role: Role = parse_role(parts[5]);

            let stored = state
                .credentials
                .prepare_user_update(username, None, Some(vec![role.clone()]))
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
            let entry =
                crate::control::catalog_entry::CatalogEntry::PutUser(Box::new(stored.clone()));
            let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
            if log_index == 0 {
                if let Some(catalog) = state.credentials.catalog() {
                    catalog
                        .put_user(&stored)
                        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;
                }
                state.credentials.install_replicated_user(&stored);
            }

            state.audit_record(
                AuditEvent::PrivilegeChange,
                Some(identity.tenant_id),
                &identity.username,
                &format!("set role '{role}' for user '{username}'"),
            );

            Ok(vec![Response::Execution(Tag::new("ALTER USER"))])
        }
        other => Err(sqlstate_error(
            "42601",
            &format!("unknown ALTER USER property: {other}"),
        )),
    }
}

/// DROP USER <name>
pub fn drop_user(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "drop users")?;

    if parts.len() < 3 {
        return Err(sqlstate_error("42601", "syntax: DROP USER <name>"));
    }

    let username = parts[2];

    if username == identity.username {
        return Err(sqlstate_error("42501", "cannot drop your own user"));
    }

    // Look up user's tenant before dropping (for ownership reassignment).
    let user_tenant = state
        .credentials
        .get_user(username)
        .map(|u| u.tenant_id)
        .unwrap_or(identity.tenant_id);

    // Pre-check existence so a DROP USER on a missing user is a
    // clean error that doesn't touch raft.
    let exists_before = state.credentials.get_user(username).is_some();
    if !exists_before {
        return Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not exist"),
        ));
    }

    let entry = crate::control::catalog_entry::CatalogEntry::DeactivateUser {
        username: username.to_string(),
    };
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    let dropped = if log_index == 0 {
        // Single-node fallback.
        state
            .credentials
            .deactivate_user(username)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
    } else {
        // Cluster mode: the raft entry committed, so the
        // deactivation WILL be applied on every node. The
        // `post_apply` hook that updates the local in-memory
        // cache runs in a spawned tokio task and may not be
        // visible by the time this function returns — trust the
        // log index rather than re-reading the cache.
        true
    };

    if dropped {
        // Reassign owned collections to the tenant_admin of the
        // user's tenant. Mutating the parent `StoredCollection`
        // and re-proposing `PutCollection` is the durable path —
        // a bare `PutOwner` would be silently overwritten the
        // next time anyone re-proposed the parent (see
        // `pgwire/ddl/ownership.rs` for the same pattern).
        let admin_name = format!("{}_admin", user_tenant.as_u32());
        let grants = state.permissions.grants_for(&format!("user:{username}"));
        if let Some(catalog) = state.credentials.catalog() {
            for grant in &grants {
                let Some(owner_obj) = extract_collection_from_target(&grant.target) else {
                    continue;
                };
                if state
                    .permissions
                    .get_owner("collection", user_tenant, owner_obj)
                    .as_deref()
                    != Some(username)
                {
                    continue;
                }
                let mut stored = match catalog.get_collection(user_tenant.as_u32(), owner_obj) {
                    Ok(Some(c)) => c,
                    _ => continue,
                };
                stored.owner = admin_name.clone();
                let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(
                    stored.clone(),
                ));
                if let Ok(idx) =
                    crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                    && idx == 0
                {
                    let _ = catalog.put_collection(&stored);
                    state.permissions.install_replicated_owner(
                        &crate::control::security::catalog::StoredOwner {
                            object_type: "collection".into(),
                            object_name: stored.name.clone(),
                            tenant_id: stored.tenant_id,
                            owner_username: stored.owner.clone(),
                        },
                    );
                }
            }
        }

        state.audit_record(
            AuditEvent::PrivilegeChange,
            Some(identity.tenant_id),
            &identity.username,
            &format!("dropped user '{username}' (ownership reassigned to '{admin_name}')"),
        );
        Ok(vec![Response::Execution(Tag::new("DROP USER"))])
    } else {
        Err(sqlstate_error(
            "42704",
            &format!("user '{username}' does not exist"),
        ))
    }
}

/// Extract collection name from a permission target like "collection:1:users".
fn extract_collection_from_target(target: &str) -> Option<&str> {
    let parts: Vec<&str> = target.splitn(3, ':').collect();
    if parts.len() == 3 && parts[0] == "collection" {
        Some(parts[2])
    } else {
        None
    }
}
