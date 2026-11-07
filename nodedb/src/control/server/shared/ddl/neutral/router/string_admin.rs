// SPDX-License-Identifier: BUSL-1.1

//! String-recognized admin DDL arms: user/role, service accounts, auth-admin
//! (API keys, auth keys, auth users, blacklist), tenants, emergency DDL, and
//! system settings.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::apikey;
use super::super::auth_key;
use super::super::auth_user;
use super::super::blacklist;
use super::super::emergency_ddl;
use super::super::role;
use super::super::service_account;
use super::super::system_ddl;
use super::super::tenant;
use super::super::user;

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // String-recognized user/role families. `DROP USER` parses into a typed
    // `AuthStmt::DropUser` that carries no `if_exists` flag (so it mishandles
    // `DROP USER IF EXISTS`), and `CREATE ROLE` / `DROP ROLE` do not parse into
    // any typed variant at all — the router dispatches all three from the
    // raw token slice, before the parse gate, with token-based
    // `strip_if_exists` / `strip_if_not_exists` handling.
    if upper.starts_with("DROP USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(user::drop_user(state, identity, &parts).await);
    }
    if upper.starts_with("CREATE ROLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(role::create_role(state, identity, &parts).await);
    }
    if upper.starts_with("DROP ROLE ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(role::drop_role(state, identity, &parts).await);
    }

    // Service accounts. These statements do not parse into any typed AST
    // variant — the router dispatches all three from the raw token slice by
    // string prefix, before the parse gate, with token-based
    // `IF [NOT] EXISTS` stripping.
    if upper.starts_with("CREATE SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(service_account::create_service_account(state, identity, &parts).await);
    }
    if upper.starts_with("DROP SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(service_account::drop_service_account(state, identity, &parts).await);
    }
    if upper.starts_with("ALTER SERVICE ACCOUNT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(
            service_account::alter_service_account_set_databases(state, identity, &parts).await,
        );
    }

    // Auth-admin DDL families (API keys, auth-scoped API keys, auth user
    // management, blacklist). None of these parse into any typed AST variant —
    // the router dispatches all of them by string prefix from the raw token
    // slice, before the parse gate. The `BLACKLIST ` prefix intentionally
    // precedes the emergency `BLACKLIST AUTH USERS WHERE` handler, which it
    // shadows.
    if upper.starts_with("CREATE API KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::create_api_key(state, identity, &parts).await);
    }
    if upper.starts_with("REVOKE API KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::revoke_api_key(state, identity, &parts).await);
    }
    if upper.starts_with("LIST API KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::list_api_keys(state, identity, &parts));
    }
    if upper.starts_with("SHOW API KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(apikey::list_api_keys(state, identity, &parts));
    }
    if upper.starts_with("CREATE AUTH KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::create_auth_key(state, identity, &parts));
    }
    if upper.starts_with("ROTATE AUTH KEY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::rotate_auth_key(state, identity, &parts));
    }
    if upper.starts_with("LIST AUTH KEYS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_key::list_auth_keys(state, identity, &parts));
    }
    if upper.starts_with("DEACTIVATE AUTH USER ") || upper.starts_with("ALTER AUTH USER ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::handle_auth_user(state, identity, &parts));
    }
    if upper.starts_with("PURGE AUTH USERS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::purge_auth_users(state, identity, &parts));
    }
    if upper.starts_with("SHOW AUTH USERS") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(auth_user::show_auth_users(state, identity, &parts));
    }
    if upper.starts_with("BLACKLIST ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(blacklist::handle_blacklist(state, identity, &parts));
    }
    if upper.starts_with("UNBLACKLIST ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(blacklist::handle_unblacklist(state, identity, &parts));
    }
    if upper.starts_with("SHOW BLACKLIST") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(blacklist::show_blacklist(state, identity, &parts));
    }

    // Tenant management. `CREATE TENANT`, `DROP TENANT`, and `PURGE TENANT`
    // parse into no typed AST variant — the router dispatches all three by
    // string prefix from the raw token slice, before the parse gate, with
    // `IF [NOT] EXISTS` stripping. `PURGE TENANT` proposes one entry to each
    // data group's log.
    //
    // `ALTER TENANT ` is ambiguous: `ALTER TENANT <id|name> SET QUOTA ...`
    // (this string form) and `ALTER TENANT <name> IN DATABASE <db> SET QUOTA
    // (...)` (a typed `DatabaseStmt::AlterTenant`, handled in the typed match
    // below) share the same prefix. The typed `ddl_ast` tenant parser only
    // recognizes the `IN DATABASE` form when `parts.len() >= 8` and tokens 3/4
    // are `IN`/`DATABASE`; replicate that exact partition here so the
    // `IN DATABASE` form always falls through to the typed arm instead of
    // being shadowed by this string handler. The string form targets the
    // session's `database_id`, so both forms write the same quota row.
    //
    // `SHOW TENANT USAGE` / `SHOW TENANT QUOTA` (bare, no `IN DATABASE`) are
    // NOT recognized here: the typed `ddl_ast` tenant parser never returns
    // `None` for `SHOW TENANT USAGE|QUOTA...` — every such input resolves to
    // either the typed `IN DATABASE` variant or a `42601` parse error. A
    // neutral string prefix for them will be unreachable and break parity.
    if upper.starts_with("CREATE TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::create_tenant(state, identity, &parts).await);
    }
    if upper.starts_with("ALTER TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let is_in_database_form = parts.len() >= 8
            && parts[3].eq_ignore_ascii_case("IN")
            && parts[4].eq_ignore_ascii_case("DATABASE");
        if !is_in_database_form {
            return Some(tenant::alter_tenant(state, identity, database_id, &parts).await);
        }
    }
    if upper.starts_with("DROP TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::drop_tenant(state, identity, &parts).await);
    }
    if upper.starts_with("PURGE TENANT ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(tenant::purge_tenant(state, identity, database_id, &parts).await);
    }

    // Emergency & incident response DDL. `EMERGENCY LOCKDOWN` / `EMERGENCY
    // UNLOCK` parse into no typed AST variant — the router dispatches both by
    // string prefix from the raw token slice, before the parse gate.
    // `BLACKLIST AUTH USERS WHERE …` is likewise string-recognized, but the
    // `BLACKLIST ` prefix above already claims it. This guard is therefore
    // intentionally kept after the `BLACKLIST ` guard, so `bulk_blacklist`
    // stays unreachable.
    if upper.starts_with("EMERGENCY LOCKDOWN") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::emergency_lockdown(state, identity, &parts));
    }
    if upper.starts_with("EMERGENCY UNLOCK") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::emergency_unlock(state, identity, &parts));
    }
    if upper.starts_with("BLACKLIST AUTH USERS WHERE") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(emergency_ddl::bulk_blacklist(state, identity, &parts));
    }

    // System-level settings: `ALTER SYSTEM SET <field> = <value>`. Parses into
    // no typed AST variant — the router dispatches it by string prefix from
    // the raw token slice, before the parse gate, with `parts`-based field /
    // value extraction.
    if upper.starts_with("ALTER SYSTEM ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(system_ddl::alter_system(state, identity, &parts));
    }

    None
}
