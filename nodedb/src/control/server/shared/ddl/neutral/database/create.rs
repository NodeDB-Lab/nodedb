// SPDX-License-Identifier: BUSL-1.1

//! Handler for `CREATE [IF NOT EXISTS] DATABASE <name> [WITH (...)]`.
//!
//! The database id comes from `allocate_database_id`, which replicates it
//! through the metadata log. The descriptor is proposed through metadata
//! Raft.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::gate::require_cluster_admin;
use super::support::{ddl_err, status};

/// Options accepted in `CREATE DATABASE ... WITH (...)`. Resolving them here
/// up front makes the unknown-key error path explicit and keeps the descriptor
/// builder a pure function of the parsed options.
#[derive(Debug, Default)]
struct CreateDatabaseOptions {
    /// Quota reference id; `0` means "inherit global default".
    quota_id: u64,
}

fn parse_create_options(options: &[(String, String)]) -> Result<CreateDatabaseOptions, DdlError> {
    let mut out = CreateDatabaseOptions::default();
    for (k, v) in options {
        match k.to_ascii_lowercase().as_str() {
            "quota_id" | "quota" => {
                out.quota_id = v.parse::<u64>().map_err(|_| {
                    ddl_err(
                        "22023",
                        format!("CREATE DATABASE: invalid {k}='{v}' (expected unsigned integer)"),
                    )
                })?;
            }
            other => {
                return Err(ddl_err(
                    "0A000",
                    format!("CREATE DATABASE: unsupported WITH option '{other}'"),
                ));
            }
        }
    }
    Ok(out)
}

/// Handle `CREATE [IF NOT EXISTS] DATABASE <name> [WITH (...)]`.
///
/// Required role: `ClusterAdmin` or `Superuser`.
pub async fn create_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_not_exists: bool,
    options: &[(String, String)],
) -> Result<Vec<DdlResult>, DdlError> {
    require_cluster_admin(state, identity, None, &format!("CREATE DATABASE {name}"))?;

    let opts = parse_create_options(options)?;

    let catalog = state.credentials.catalog();

    // Check for duplicate name.
    match catalog.get_database_id_by_name(name) {
        Ok(Some(_)) => {
            if if_not_exists {
                return Ok(status("CREATE DATABASE"));
            }
            return Err(ddl_err(
                "42P04",
                format!("database '{name}' already exists"),
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(DdlError::from_error_in_context("catalog lookup failed", &e));
        }
    }

    let db_id = crate::control::database::allocate_database_id(state)
        .await
        .map_err(|e| DdlError::from_error_in_context("database id allocation failed", &e))?;

    // Stamp the descriptor with the next WAL LSN. This is the LSN the very
    // next WAL append on this server will receive; it is monotonically
    // greater than any record observed before this DDL ran and gives the
    // descriptor a well-ordered creation point relative to the WAL.
    let created_at_lsn = state.wal.next_lsn().as_u64();

    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: name.to_string(),
        status: DatabaseStatus::Active,
        created_at_lsn,
        quota_ref: opts.quota_id,
        parent_clone: None,
        mirror_origin: None,
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };

    // Propose through the metadata proposer so every replica applies the
    // descriptor atomically.
    propose_catalog_entry_async(
        state,
        &CatalogEntry::PutDatabase(Box::new(descriptor.clone())),
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseCreated,
        None,
        Some(db_id),
        &identity.username,
        &format!("CREATE DATABASE {name}"),
    );

    Ok(status("CREATE DATABASE"))
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::ErrorCode;

    use super::*;
    use crate::control::cluster::test_one_node;
    use crate::control::security::identity::{DatabaseSet, Role};
    use crate::types::TenantId;

    fn admin() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            0,
            "create_database_test",
            TenantId::new(1),
            vec![Role::Superuser],
            true,
            None,
            DatabaseSet::All,
        )
    }

    /// CREATE DATABASE of a name already taken is `duplicate_database`
    /// (`42P04`) with the already-exists code, never an internal error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn creating_an_existing_database_is_a_duplicate_database() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let identity = admin();
        create_database(state, &identity, "orders", false, &[])
            .await
            .expect("first create succeeds");

        let err = create_database(state, &identity, "orders", false, &[])
            .await
            .expect_err("a second create of the same name is refused");
        assert_eq!(err.sqlstate, "42P04", "{err:?}");
        assert_eq!(err.code, ErrorCode::ALREADY_EXISTS);

        let existing = create_database(state, &identity, "orders", true, &[])
            .await
            .expect("IF NOT EXISTS on an existing name succeeds");
        assert_eq!(existing.len(), 1);
        cluster.shutdown().await;
    }

    /// Every CREATE persists the hwm before it returns, so the id survives a
    /// restart that happens right after it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_persists_the_hwm_of_every_issued_id() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let identity = admin();
        let catalog = state.credentials.catalog();
        for name in ["a", "b"] {
            create_database(state, &identity, name, false, &[])
                .await
                .expect("create");
            let id = catalog
                .get_database_id_by_name(name)
                .expect("lookup")
                .expect("created");
            assert_eq!(catalog.get_database_hwm().expect("hwm"), id.as_u64());
        }
        cluster.shutdown().await;
    }
}
