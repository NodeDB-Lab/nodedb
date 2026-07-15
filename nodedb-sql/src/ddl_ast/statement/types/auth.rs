// SPDX-License-Identifier: Apache-2.0

//! Auth DDL/DML statements.

use crate::ddl_ast::alter_ops::{AlterRoleOp, AlterUserOp};
use crate::ddl_ast::statement::auth::OidcClaimMappingClause;

/// A tenant referenced in DDL — either by numeric id or by display name.
/// Name references are resolved against the catalog by the handler, so an
/// admin is not forced to look up numeric ids before creating users.
#[derive(Debug, Clone, PartialEq)]
pub enum TenantSelector {
    Id(u64),
    Name(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AuthStmt {
    // ── User / auth / grant ──────────────────────────────────────
    CreateUser {
        username: String,
        password: String,
        role: Option<String>,
        tenant: Option<TenantSelector>,
        if_not_exists: bool,
    },
    DropUser {
        username: String,
    },
    AlterUser {
        username: String,
        op: AlterUserOp,
    },
    ShowUsers,
    /// `ALTER ROLE <name> GRANT/REVOKE/SET`.
    AlterRole {
        name: String,
        sub_op: AlterRoleOp,
    },
    /// `GRANT <role>[, ...] TO <grantee>` — role-membership grant.
    ///
    /// The grantee is a user (the roles are added to the user) or a custom
    /// role (the single granted role becomes the grantee's inheritance parent).
    GrantRole {
        roles: Vec<String>,
        grantee: String,
    },
    /// `REVOKE <role>[, ...] FROM <grantee>` — role-membership revocation.
    RevokeRole {
        roles: Vec<String>,
        grantee: String,
    },
    /// `GRANT <perm>[, ...] ON <object> TO <grantee>` — object-permission grant.
    ///
    /// `target_type` is `COLLECTION`, `FUNCTION`, `PROCEDURE`, or `TENANT`.
    GrantPermission {
        permissions: Vec<String>,
        target_type: String,
        target_name: String,
        grantee: String,
    },
    /// `GRANT <privilege> ON DATABASE <name> TO <user>`
    GrantDatabasePermission {
        permission: String,
        db_name: String,
        grantee: String,
    },
    /// `REVOKE <perm>[, ...] ON <object> FROM <grantee>` — object-permission revocation.
    RevokePermission {
        permissions: Vec<String>,
        target_type: String,
        target_name: String,
        grantee: String,
    },
    /// `REVOKE <privilege> ON DATABASE <name> FROM <user>`
    RevokeDatabasePermission {
        permission: String,
        db_name: String,
        grantee: String,
    },
    ShowPermissions {
        on_collection: Option<String>,
        for_grantee: Option<String>,
    },
    ShowGrants {
        username: Option<String>,
    },

    // ── OIDC providers ───────────────────────────────────────────
    /// `CREATE OIDC PROVIDER <name> ISSUER '<iss>' JWKS_URI '<uri>' TENANT <u64>
    ///  [AUDIENCE '<aud>'] [CLAIM MAPPING WHEN <claim_name> = '<value>'
    ///  SET DEFAULT_DATABASE = <id>, ADD DATABASES [<ids>], ADD ROLES ['<role>', ...]]`
    CreateOidcProvider {
        name: String,
        issuer: String,
        jwks_uri: String,
        tenant_id: u64,
        audience: Option<String>,
        /// `(claim_name, claim_value, default_database, add_databases, add_roles)` tuples.
        claim_mappings: Vec<OidcClaimMappingClause>,
    },
    /// `ALTER OIDC PROVIDER <name> SET CLAIM MAPPING WHEN <claim_name> = '<value>'
    ///  SET DEFAULT_DATABASE = <id>, ADD DATABASES [<ids>], ADD ROLES ['<role>', ...]`
    ///
    /// Replaces the entire claim-mapping list for the named provider.
    AlterOidcProviderClaimMapping {
        name: String,
        claim_mappings: Vec<OidcClaimMappingClause>,
    },
    /// `DROP OIDC PROVIDER [IF EXISTS] <name>`
    DropOidcProvider {
        name: String,
        if_exists: bool,
    },
    /// `SHOW OIDC PROVIDERS`
    ShowOidcProviders,
}
