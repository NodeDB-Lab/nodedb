// SPDX-License-Identifier: BUSL-1.1

//! Handler for `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = sync | async]`.
//!
//! The source must answer before any descriptor exists: a mirror keeps its
//! source's database id, and the hello is what names that id. The refusals:
//!
//! - A source the `[mirror.sources]` config does not name.
//! - This cluster as its own source, by name here and by id at the source.
//! - The source's default database. Its id is 0, and Calvin's mirror table keys a
//!   local transaction under 0, so a mirror of it could not tell the two apart.
//! - An unmaterialized clone, whose lineage means nothing on another cluster.
//! - A second mirror of one source database on this cluster.
//! - An id this cluster already uses.
//!
//! Each node's applier supervisor then finds the descriptor and starts following.
//! The result is the protocol-neutral [`DdlResult`].

use nodedb_cluster::mirror::{MirrorError, MirrorHelloAck, MirrorRefusal, MirrorSourceClient};
use nodedb_types::{DatabaseId, MirrorMode, MirrorOrigin, MirrorStatus};

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::mirror::applier::open_source_link;
use crate::control::security::catalog::database_types::{DatabaseDescriptor, DatabaseStatus};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_superuser;
use super::super::support::{ddl_err, status};

/// Refuse a second mirror of one source database on this cluster. A promoted
/// mirror follows nothing, so it does not count.
fn refuse_second_mirror(
    databases: &[DatabaseDescriptor],
    source_cluster: &str,
    source_database: &str,
) -> Result<(), DdlError> {
    let held = databases.iter().find(|database| {
        database.mirror_origin.as_ref().is_some_and(|origin| {
            origin.source_cluster == source_cluster
                && origin.source_database_name == source_database
                && !matches!(origin.status, MirrorStatus::Promoted)
        })
    });
    match held {
        Some(database) => Err(ddl_err(
            "42710",
            format!(
                "database '{}' already mirrors {source_cluster}.{source_database}; a cluster \
                 holds one mirror per source database",
                database.name
            ),
        )),
        None => Ok(()),
    }
}

/// Check what the source's hello names, and answer the id the mirror keeps.
fn check_hello_ack(
    ack: &MirrorHelloAck,
    cluster_id: u64,
    databases: &[DatabaseDescriptor],
    source_cluster: &str,
    source_database: &str,
) -> Result<DatabaseId, DdlError> {
    if ack.source_cluster_id == cluster_id {
        return Err(ddl_err(
            "0A000",
            format!(
                "MIRROR DATABASE: source cluster '{source_cluster}' is this cluster; \
                 self-mirroring is not supported"
            ),
        ));
    }
    let id = DatabaseId::new(ack.source_database_id);
    if id == DatabaseId::DEFAULT {
        return Err(ddl_err(
            "0A000",
            "MIRROR DATABASE: the source's default database cannot be mirrored",
        ));
    }
    if let Some(held) = databases.iter().find(|database| database.id == id) {
        return Err(ddl_err(
            "42710",
            format!(
                "MIRROR DATABASE: {source_cluster}.{source_database} has id {}, which database \
                 '{}' uses on this cluster",
                id.as_u64(),
                held.name
            ),
        ));
    }
    Ok(id)
}

/// The SQL error of a refused or unanswered hello.
fn hello_error(error: &MirrorError) -> DdlError {
    let MirrorError::Refused(refusal) = error else {
        return ddl_err(
            "08001",
            format!("MIRROR DATABASE: the source did not answer: {error}"),
        );
    };
    let sqlstate = match refusal {
        MirrorRefusal::SameCluster
        | MirrorRefusal::DefaultDatabase
        | MirrorRefusal::UnmaterializedClone { .. }
        | MirrorRefusal::SourceIsMirror { .. } => "0A000",
        MirrorRefusal::DatabaseNotAllowed { .. } | MirrorRefusal::LinkNotOwned { .. } => "42501",
        MirrorRefusal::DatabaseUnknown { .. } => "3D000",
        MirrorRefusal::BadVersion { .. } | MirrorRefusal::WrongCluster { .. } => "08004",
        MirrorRefusal::LinkUnknown { .. }
        | MirrorRefusal::PinExpired { .. }
        | MirrorRefusal::SourceDropped { .. }
        | MirrorRefusal::GroupNotTailed { .. }
        | MirrorRefusal::NotHosted { .. }
        | MirrorRefusal::LogTruncated { .. }
        | MirrorRefusal::FrameTooLarge { .. }
        | MirrorRefusal::Malformed { .. }
        | MirrorRefusal::Unavailable { .. }
        | MirrorRefusal::BootstrapFailed { .. }
        | MirrorRefusal::BootstrapRestart { .. } => "08006",
    };
    ddl_err(sqlstate, format!("MIRROR DATABASE: {refusal}"))
}

/// Refuse a source this node's config cannot reach, or that is this cluster.
fn check_source_config(state: &SharedState, source_cluster: &str) -> Result<(), DdlError> {
    let settings = state.mirror.settings.get();
    if settings.and_then(|settings| settings.cluster_name.as_deref()) == Some(source_cluster) {
        return Err(ddl_err(
            "0A000",
            format!(
                "MIRROR DATABASE: source cluster '{source_cluster}' is this cluster; \
                 self-mirroring is not supported"
            ),
        ));
    }
    if settings.is_none_or(|settings| !settings.sources.contains_key(source_cluster)) {
        return Err(ddl_err(
            "42704",
            format!(
                "MIRROR DATABASE: [mirror.sources] names no source '{source_cluster}'; add its \
                 seeds, server_name and tls"
            ),
        ));
    }
    Ok(())
}

/// Whether the catalog read back `claim` itself. The first statement in the log
/// wrote its own row, with its own name and WAL position.
fn claim_won(row: Option<&DatabaseDescriptor>, claim: &DatabaseDescriptor) -> bool {
    row.is_some_and(|row| {
        row.name == claim.name
            && row.created_at_lsn == claim.created_at_lsn
            && row
                .mirror_origin
                .as_ref()
                .map(|origin| &origin.source_database_name)
                == claim
                    .mirror_origin
                    .as_ref()
                    .map(|origin| &origin.source_database_name)
    })
}

/// Whether any live mirror on this cluster follows the source database.
fn mirrors_source(
    databases: &[DatabaseDescriptor],
    source_cluster: &str,
    source_database: &str,
) -> bool {
    refuse_second_mirror(databases, source_cluster, source_database).is_err()
}

/// Give back the pin of a hello whose statement was refused. The source's pin
/// expiry covers a release that does not arrive.
async fn release_stray_pin(
    client: &MirrorSourceClient,
    source_cluster: &str,
    source_database: &str,
) {
    if let Err(error) = client.release(client.link_id()).await {
        tracing::warn!(
            source = %source_cluster,
            database = %source_database,
            %error,
            "MIRROR DATABASE: the refused hello's pin stays until it expires"
        );
    }
}

/// Say hello through a new link, until the statement deadline.
async fn hello_source(
    state: &SharedState,
    source_cluster: &str,
    source_database: &str,
    mode: MirrorMode,
) -> Result<(MirrorHelloAck, MirrorSourceClient), DdlError> {
    let link = open_source_link(state, source_cluster, source_database, mode)
        .map_err(|e| DdlError::from_error_in_context("MIRROR DATABASE", &e))?;
    let client = MirrorSourceClient::new(link);
    let deadline = StatementDeadline::of_statement(state).tokio_instant();
    match tokio::time::timeout_at(deadline, client.hello()).await {
        Ok(Ok(ack)) => Ok((ack, client)),
        Ok(Err(error)) => Err(hello_error(&error)),
        Err(_) => Err(ddl_err(
            "08001",
            format!("MIRROR DATABASE: {source_cluster} did not answer before the deadline"),
        )),
    }
}

/// Handle `MIRROR DATABASE <local_name> FROM <source_cluster>.<source_database> [MODE = ...]`.
///
/// Required role: `Superuser`.
pub async fn mirror_database(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    local_name: &str,
    source_cluster: &str,
    source_database: &str,
    mode: MirrorMode,
) -> Result<Vec<DdlResult>, DdlError> {
    // db_id=None — local mirror does not exist yet at gate time.
    require_superuser(state, identity, None, "MIRROR DATABASE")?;

    let catalog = state.credentials.catalog();

    // Reject if the local name already exists.
    match catalog.get_database_id_by_name(local_name) {
        Ok(Some(_)) => {
            return Err(ddl_err(
                "42P04",
                format!("database '{local_name}' already exists"),
            ));
        }
        Ok(None) => {}
        Err(e) => {
            return Err(DdlError::from_error_in_context("catalog lookup failed", &e));
        }
    }

    check_source_config(state, source_cluster)?;
    let cluster_id = state.mirror.cluster_id.get().copied().ok_or_else(|| {
        ddl_err(
            "55000",
            "MIRROR DATABASE: this node has not joined its cluster yet",
        )
    })?;
    let databases = catalog
        .list_databases()
        .map_err(|e| DdlError::from_error_in_context("catalog lookup failed", &e))?;
    refuse_second_mirror(&databases, source_cluster, source_database)?;

    let (ack, client) = hello_source(state, source_cluster, source_database, mode).await?;
    let db_id = match check_hello_ack(
        &ack,
        cluster_id,
        &databases,
        source_cluster,
        source_database,
    ) {
        Ok(db_id) => db_id,
        Err(refusal) => {
            // The hello took a pin this mirror never uses. A mirror of the same
            // source shares the link, so its pin stays.
            if !mirrors_source(&databases, source_cluster, source_database) {
                release_stray_pin(&client, source_cluster, source_database).await;
            }
            return Err(refusal);
        }
    };
    let created_at_lsn = state.wal.next_lsn().as_u64();

    let descriptor = DatabaseDescriptor {
        id: db_id,
        name: local_name.to_string(),
        status: DatabaseStatus::Mirroring,
        created_at_lsn,
        quota_ref: 0,
        parent_clone: None,
        mirror_origin: Some(MirrorOrigin {
            source_cluster: source_cluster.to_string(),
            source_database: db_id,
            source_database_name: source_database.to_string(),
            mode,
            status: MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
        }),
        audit_dml: nodedb_types::AuditDmlMode::None,
        idle_session_timeout_secs: 0,
    };

    // The log decides between two statements that both passed their checks.
    propose_catalog_entry_async(
        state,
        &CatalogEntry::ClaimMirrorDatabase(Box::new(descriptor.clone())),
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))?;
    let row = catalog
        .get_database(db_id)
        .map_err(|e| DdlError::from_error_in_context("catalog read failed", &e))?;
    if !claim_won(row.as_ref(), &descriptor) {
        return Err(ddl_err(
            "42710",
            format!(
                "MIRROR DATABASE: another statement took '{local_name}' or \
                 {source_cluster}.{source_database} first; a cluster holds one mirror per source \
                 database"
            ),
        ));
    }

    state.audit_record_with_db(
        crate::control::security::audit::AuditEvent::DatabaseMirrored,
        None,
        Some(db_id),
        &identity.username,
        &format!(
            "MIRROR DATABASE {local_name} FROM {source_cluster}.{source_database} MODE={mode:?}"
        ),
    );

    Ok(status("MIRROR DATABASE"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::mirror::test_fixture::mirror_descriptor;

    const OWN_CLUSTER: u64 = 2;

    fn ack(cluster_id: u64, database: u64) -> MirrorHelloAck {
        MirrorHelloAck {
            source_cluster: "source".to_string(),
            source_cluster_id: cluster_id,
            source_database_id: database,
            timeline: 0,
            groups: Vec::new(),
        }
    }

    fn sqlstate<T: std::fmt::Debug>(result: Result<T, DdlError>) -> String {
        result.expect_err("a refusal").sqlstate
    }

    #[test]
    fn a_source_database_keeps_its_id_on_the_mirror() {
        let id = check_hello_ack(&ack(1, 1600), OWN_CLUSTER, &[], "source", "orders")
            .expect("a mirror of another cluster");
        assert_eq!(id, DatabaseId::new(1600));
    }

    /// The comparison is between cluster ids. A node id can equal a cluster id
    /// by chance, and a source's name says nothing of either.
    #[test]
    fn this_cluster_as_its_own_source_is_refused() {
        assert_eq!(
            sqlstate(check_hello_ack(
                &ack(OWN_CLUSTER, 1600),
                OWN_CLUSTER,
                &[],
                "source",
                "orders"
            )),
            "0A000"
        );
    }

    /// Calvin's mirror table keys a local transaction under database 0.
    #[test]
    fn the_default_database_is_refused() {
        assert_eq!(
            sqlstate(check_hello_ack(
                &ack(1, DatabaseId::DEFAULT.as_u64()),
                OWN_CLUSTER,
                &[],
                "source",
                "orders"
            )),
            "0A000"
        );
    }

    #[test]
    fn an_id_this_cluster_uses_is_refused() {
        let held = [mirror_descriptor(1600, "other", MirrorStatus::Promoted)];
        assert_eq!(
            sqlstate(check_hello_ack(
                &ack(1, 1600),
                OWN_CLUSTER,
                &held,
                "source",
                "orders"
            )),
            "42710"
        );
    }

    #[test]
    fn a_second_mirror_of_one_source_database_is_refused() {
        let held = [mirror_descriptor(1600, "first", MirrorStatus::Following)];
        assert_eq!(
            sqlstate(refuse_second_mirror(&held, "source", "source_db")),
            "42710"
        );
        assert!(refuse_second_mirror(&held, "source", "other_db").is_ok());
        assert!(refuse_second_mirror(&held, "elsewhere", "source_db").is_ok());
    }

    /// A statement that lost the log's race reads another statement's row.
    #[test]
    fn only_the_statement_that_wrote_the_row_won() {
        let mut claim = mirror_descriptor(1600, "first", MirrorStatus::Following);
        claim.created_at_lsn = 7;
        assert!(claim_won(Some(&claim), &claim));
        let mut other = claim.clone();
        other.name = "second".to_string();
        assert!(!claim_won(Some(&other), &claim));
        let mut later = claim.clone();
        later.created_at_lsn = 8;
        assert!(!claim_won(Some(&later), &claim));
        assert!(!claim_won(None, &claim));
    }

    #[test]
    fn a_promoted_mirror_leaves_its_source_free() {
        let held = [mirror_descriptor(1600, "first", MirrorStatus::Promoted)];
        assert!(refuse_second_mirror(&held, "source", "source_db").is_ok());
    }

    #[test]
    fn each_refusal_of_a_hello_gets_its_sqlstate() {
        for (refusal, expected) in [
            (MirrorRefusal::SameCluster, "0A000"),
            (MirrorRefusal::DefaultDatabase, "0A000"),
            (
                MirrorRefusal::UnmaterializedClone {
                    database: "orders".to_string(),
                },
                "0A000",
            ),
            (
                MirrorRefusal::DatabaseNotAllowed {
                    database: "orders".to_string(),
                },
                "42501",
            ),
            (
                MirrorRefusal::DatabaseUnknown {
                    database: "orders".to_string(),
                },
                "3D000",
            ),
        ] {
            assert_eq!(
                hello_error(&MirrorError::Refused(refusal)).sqlstate,
                expected
            );
        }
    }
}
