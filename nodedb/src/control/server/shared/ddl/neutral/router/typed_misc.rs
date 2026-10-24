// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `MiscStmt` (COPY FROM file or STDIN, COPY TO file, CREATE and DROP
//! SPATIAL_REF_SYS, CREATE, DROP and SHOW CREDENTIAL) and the `ClusterStmt` `AlterRaftGroup`
//! variant.

use nodedb_sql::ddl_ast::statement::{ClusterStmt, MiscStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::bulk;
use super::super::cluster;
use super::super::collection;
use super::super::credential;
use super::super::spatial_ref_sys;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        // `COPY <collection> FROM '<path>' [WITH (...)]` bulk import.
        NodedbStatement::Misc(MiscStmt::CopyFromFile {
            collection,
            sources,
            format,
            delimiter,
            header,
            raster,
        }) => Some(
            collection::copy_from_file(
                state,
                identity,
                collection,
                sources,
                collection::CopyFromOptions {
                    format: format.as_ref(),
                    delimiter: *delimiter,
                    header: *header,
                    raster,
                },
                database_id,
                txn_ctx,
            )
            .await,
        ),

        // `COPY <name> FROM STDIN` needs the pgwire COPY sub-protocol, which
        // opens an array load before the router runs. Every statement that
        // reaches here has no stream to read.
        NodedbStatement::Misc(MiscStmt::CopyFromStdin { .. }) => {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(bulk::copy_from(state, identity, &parts).await)
        }

        // `COPY <source> TO '<path>' [WITH (...)]` bulk export.
        NodedbStatement::Misc(MiscStmt::CopyToFile {
            source,
            path,
            format,
            delimiter,
            header,
        }) => Some(
            collection::copy_to_file(
                state,
                identity,
                source,
                path,
                collection::CopyToOptions {
                    format: format.as_ref(),
                    delimiter: *delimiter,
                    header: *header,
                },
                database_id,
            )
            .await,
        ),

        NodedbStatement::Misc(MiscStmt::CreateSpatialRefSys {
            srid,
            name,
            format,
            definition,
        }) => Some(
            spatial_ref_sys::create_spatial_ref_sys(
                state, identity, *srid, name, *format, definition,
            )
            .await,
        ),

        NodedbStatement::Misc(MiscStmt::DropSpatialRefSys { srid }) => {
            Some(spatial_ref_sys::drop_spatial_ref_sys(state, identity, *srid).await)
        }

        NodedbStatement::Misc(MiscStmt::CreateCredential { name, spec }) => {
            Some(credential::create_credential(state, identity, name, spec).await)
        }

        NodedbStatement::Misc(MiscStmt::DropCredential { name, if_exists }) => {
            Some(credential::drop_credential(state, identity, name, *if_exists).await)
        }

        NodedbStatement::Misc(MiscStmt::ShowCredentials) => {
            Some(credential::show_credentials(state, identity))
        }

        NodedbStatement::Cluster(ClusterStmt::AlterRaftGroup {
            group_id,
            action,
            node_id,
        }) => Some(cluster::alter_raft_group(
            state, identity, group_id, action, node_id,
        )),

        _ => None,
    }
}
