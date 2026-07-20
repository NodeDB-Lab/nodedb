// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `AuthStmt`: users, roles, grants, OIDC providers, and
//! permission introspection.

use nodedb_sql::ddl_ast::statement::{AuthStmt, NodedbStatement};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::grant;
use super::super::inspect;
use super::super::oidc;
use super::super::role;
use super::super::user;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            password,
            role,
            tenant,
            if_not_exists,
        }) => Some(user::create_user(
            state,
            identity,
            username,
            password,
            role.as_deref(),
            tenant.as_ref(),
            *if_not_exists,
        )),

        NodedbStatement::Auth(AuthStmt::AlterUser { username, op }) => {
            Some(user::alter_user(state, identity, username, op))
        }

        NodedbStatement::Auth(AuthStmt::AlterRole { name, sub_op }) => {
            Some(role::alter_role_typed(state, identity, name, sub_op))
        }

        NodedbStatement::Auth(AuthStmt::GrantRole { roles, grantee }) => {
            Some(grant::role::grant_role(state, identity, roles, grantee))
        }

        NodedbStatement::Auth(AuthStmt::RevokeRole { roles, grantee }) => {
            Some(grant::role::revoke_role(state, identity, roles, grantee))
        }

        NodedbStatement::Auth(AuthStmt::GrantPermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(grant::permission::grant_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokePermission {
            permissions,
            target_type,
            target_name,
            grantee,
        }) => Some(grant::permission::revoke_permission(
            state,
            identity,
            permissions,
            target_type,
            target_name,
            grantee,
        )),

        NodedbStatement::Auth(AuthStmt::GrantDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(grant::database_permission::grant_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Auth(AuthStmt::RevokeDatabasePermission {
            permission,
            db_name,
            grantee,
        }) => Some(grant::database_permission::revoke_database(
            state, identity, permission, db_name, grantee,
        )),

        NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
            name,
            issuer,
            jwks_uri,
            tenant_id,
            audience,
            claim_mappings,
        }) => Some(oidc::create_oidc_provider(
            state,
            identity,
            oidc::CreateOidcProviderParams {
                name,
                issuer,
                jwks_uri,
                tenant_id: *tenant_id,
                audience: audience.as_deref(),
                claim_mappings,
            },
        )),

        NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            name,
            claim_mappings,
        }) => Some(oidc::alter_oidc_provider_claim_mapping(
            state,
            identity,
            name,
            claim_mappings,
        )),

        NodedbStatement::Auth(AuthStmt::DropOidcProvider { name, if_exists }) => {
            Some(oidc::drop_oidc_provider(state, identity, name, *if_exists))
        }

        NodedbStatement::Auth(AuthStmt::ShowOidcProviders) => {
            Some(oidc::show_oidc_providers(state, identity))
        }

        // SHOW PERMISSIONS [ON <collection>] [FOR <grantee>]. Parses into a
        // typed `AuthStmt::ShowPermissions` and was dispatched from the pgwire
        // typed-AST sync router (`sync_ops`). The permission-store reads are
        // preserved verbatim in `inspect`.
        NodedbStatement::Auth(AuthStmt::ShowPermissions {
            on_collection,
            for_grantee,
        }) => Some(inspect::show_permissions(
            state,
            identity,
            database_id,
            on_collection.as_deref(),
            for_grantee.as_deref(),
        )),

        _ => None,
    }
}
