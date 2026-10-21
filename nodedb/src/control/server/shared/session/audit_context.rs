// SPDX-License-Identifier: BUSL-1.1

//! Per-connection DDL audit context.
//!
//! The pgwire statement loop installs a snapshot of the authenticated
//! identity + the text of each statement on the connection slot before
//! it dispatches that statement. A statement that carries a secret
//! installs its redacted text, so no secret reaches the metadata log.
//! Every `propose_catalog_entry` call that fires inside that scope picks
//! up the context and attaches it to the replicated
//! [`MetadataEntry::CatalogDdlAudited`]
//! so the applier can emit the J.4 audit record on every replica —
//! including followers that never saw the raw SQL.
//!
//! The slot is connection-scoped, not thread-scoped: the statement
//! handler awaits between install and consumption, and tokio may move
//! the task to another worker at any of those awaits.

use std::cell::RefCell;

use crate::control::security::identity::AuthenticatedIdentity;

/// Minimal identity/SQL snapshot captured at pgwire statement entry.
///
/// Cloned into every `CatalogDdlAudited` entry proposed while it's
/// active. [`AuditScope::drop`] restores the outer context on scope exit.
#[derive(Debug, Clone)]
pub struct AuditCtx {
    pub auth_user_id: String,
    pub auth_user_name: String,
    pub sql_text: String,
}

/// Run `f` against this connection's audit slot, or return `default` when the
/// caller runs outside any connection scope.
fn with_slot<T>(default: T, f: impl FnOnce(&RefCell<Option<AuditCtx>>) -> T) -> T {
    super::conn_scope::with_scope(default, |scope| f(&scope.audit))
}

impl AuditCtx {
    /// The context of one statement `identity` runs. A statement that
    /// carries a secret (a credential, a user password) records its redacted
    /// text.
    pub fn for_statement(identity: &AuthenticatedIdentity, sql: &str) -> Self {
        Self {
            auth_user_id: identity.user_id.to_string(),
            auth_user_name: identity.username.clone(),
            sql_text: nodedb_sql::ddl_ast::parse::audit_text_for(sql)
                .unwrap_or_else(|| sql.to_string()),
        }
    }
}

/// RAII guard that installs `ctx` in the connection's audit slot on
/// construction and restores the outer context on drop. Install one per
/// statement so nested DDL proposers inherit that statement's context.
pub struct AuditScope {
    outer: Option<AuditCtx>,
}

impl AuditScope {
    pub fn new(ctx: AuditCtx) -> Self {
        let outer = with_slot(None, |c| c.borrow_mut().replace(ctx));
        Self { outer }
    }

    /// Install the context of one statement `identity` runs.
    pub fn statement(identity: &AuthenticatedIdentity, sql: &str) -> Self {
        Self::new(AuditCtx::for_statement(identity, sql))
    }
}

impl Drop for AuditScope {
    fn drop(&mut self) {
        let outer = self.outer.take();
        with_slot((), |c| *c.borrow_mut() = outer);
    }
}

/// Return a clone of the currently-installed audit context, if any.
pub fn current() -> Option<AuditCtx> {
    with_slot(None, |c| c.borrow().clone())
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    fn ctx(user: &str, sql: &str) -> AuditCtx {
        AuditCtx {
            auth_user_id: user.into(),
            auth_user_name: user.into(),
            sql_text: sql.into(),
        }
    }

    #[tokio::test]
    async fn no_context_by_default() {
        conn_scope::scoped(async {
            assert!(current().is_none());
        })
        .await;
    }

    #[test]
    fn outside_any_scope_is_inert() {
        assert!(current().is_none());
        let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn scope_installs_and_clears() {
        conn_scope::scoped(async {
            {
                let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
                let seen = current().expect("scope sets context");
                assert_eq!(seen.auth_user_name, "alice");
                assert_eq!(seen.sql_text, "CREATE COLLECTION x");
            }
            assert!(current().is_none());
        })
        .await;
    }

    #[tokio::test]
    async fn inner_scope_shadows_and_restores_outer() {
        conn_scope::scoped(async {
            {
                let _outer = AuditScope::new(ctx("root", "outer"));
                {
                    let _inner = AuditScope::new(ctx("bob", "inner"));
                    assert_eq!(
                        current().expect("inner scope sets context").auth_user_name,
                        "bob"
                    );
                }
                // `EXECUTE` runs its prepared body inside the outer
                // statement's scope, which resumes once the body ends.
                assert_eq!(current().expect("outer scope restored").sql_text, "outer");
            }
            assert!(current().is_none());
        })
        .await;
    }

    #[test]
    fn a_secret_bearing_statement_records_redacted_text() {
        let identity = AuthenticatedIdentity::new_internal_service(
            1,
            "root",
            crate::types::TenantId::new(1),
            Vec::new(),
            true,
            None,
            AuthenticatedIdentity::default_database_set(true),
        );
        let ctx = AuditCtx::for_statement(
            &identity,
            "CREATE CREDENTIAL c (TYPE 'bearer', TOKEN 'tok-secret')",
        );
        assert!(!ctx.sql_text.contains("tok-secret"), "{}", ctx.sql_text);
        let ctx = AuditCtx::for_statement(&identity, "ALTER USER bob SET PASSWORD 'pw-secret'");
        assert!(!ctx.sql_text.contains("pw-secret"), "{}", ctx.sql_text);
        let ctx = AuditCtx::for_statement(&identity, "CREATE COLLECTION a");
        assert_eq!(ctx.sql_text, "CREATE COLLECTION a");
    }

    /// The context must survive a worker-thread hop: the statement handler
    /// awaits `execute_sql` between installing the scope and proposing DDL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn context_survives_worker_thread_migration() {
        conn_scope::scoped(async {
            let _g = AuditScope::new(ctx("alice", "CREATE COLLECTION x"));
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                current().expect("context survives the hop").auth_user_name,
                "alice"
            );
        })
        .await;
    }
}
