// SPDX-License-Identifier: Apache-2.0

//! The audit text of a statement: the text the metadata log and every audit
//! record carry. A statement that carries a secret records a redacted form.
//!
//! Secret-bearing statements:
//! - `CREATE CREDENTIAL`: every option but `TYPE`.
//! - `CREATE USER ... PASSWORD '<password>'`.
//! - `ALTER USER ... SET PASSWORD '<password>'`.
//!
//! A secret-bearing statement that does not parse keeps only its leading
//! keywords, so a malformed one never carries its secret either.

use super::{credential, user_auth};
use crate::ddl_ast::alter_ops::AlterUserOp;
use crate::ddl_ast::statement::{AuthStmt, NodedbStatement, TenantSelector};

const REDACTED: &str = "'<redacted>'";

/// The redacted audit text of `sql`, or `None` when `sql` carries no secret
/// and its own text is the audit text.
pub fn audit_text_for(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if let Some(text) = credential::audit_text(trimmed, &upper) {
        return Some(text);
    }
    let create = upper.starts_with("CREATE USER ");
    if !create && !upper.starts_with("ALTER USER ") {
        return None;
    }
    let parsed = user_auth::try_parse(&upper, &[], trimmed)?;
    match parsed {
        Ok(NodedbStatement::Auth(AuthStmt::CreateUser {
            username,
            password,
            role,
            tenant,
            if_not_exists,
        })) => {
            let mut text = String::from("CREATE USER ");
            if if_not_exists {
                text.push_str("IF NOT EXISTS ");
            }
            text.push_str(&username);
            if !password.is_empty() {
                text.push_str(" WITH PASSWORD ");
                text.push_str(REDACTED);
            }
            if let Some(role) = role {
                text.push_str(" ROLE ");
                text.push_str(&role);
            }
            match tenant {
                Some(TenantSelector::Id(id)) => text.push_str(&format!(" TENANT {id}")),
                Some(TenantSelector::Name(name)) => text.push_str(&format!(" TENANT '{name}'")),
                None => {}
            }
            Some(text)
        }
        Ok(NodedbStatement::Auth(AuthStmt::AlterUser {
            username,
            op: AlterUserOp::SetPassword { .. },
        })) => Some(format!("ALTER USER {username} SET PASSWORD {REDACTED}")),
        // Every other form carries no secret.
        Ok(_) => None,
        Err(_) if create => Some("CREATE USER <redacted>".to_string()),
        Err(_) => Some("ALTER USER <redacted>".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::audit_text_for;

    #[test]
    fn a_credential_keeps_its_name_and_type_only() {
        assert_eq!(
            audit_text_for(" create credential earthdata (TYPE 'bearer', TOKEN 'tok-secret') ")
                .as_deref(),
            Some("CREATE CREDENTIAL earthdata (TYPE 'bearer', <redacted>)")
        );
        let s3 = audit_text_for(
            "CREATE CREDENTIAL l (TYPE 's3', ACCESS_KEY_ID 'tok-a', SECRET_ACCESS_KEY 'tok-b', \
             REGION 'r', SESSION_TOKEN 'tok-c')",
        );
        assert_eq!(
            s3.as_deref(),
            Some("CREATE CREDENTIAL l (TYPE 's3', <redacted>)")
        );
        assert_eq!(
            audit_text_for("CREATE CREDENTIAL x (TYPE 'bearer', TOKEN 'tok-secret', 'tok-2')")
                .as_deref(),
            Some("CREATE CREDENTIAL <redacted>")
        );
    }

    #[test]
    fn a_user_password_is_removed() {
        assert_eq!(
            audit_text_for("CREATE USER alice WITH PASSWORD 'pw-secret' ROLE readwrite TENANT 7")
                .as_deref(),
            Some("CREATE USER alice WITH PASSWORD '<redacted>' ROLE readwrite TENANT 7")
        );
        assert_eq!(
            audit_text_for("create user if not exists bob password 'pw-secret' tenant 'acme'")
                .as_deref(),
            Some("CREATE USER IF NOT EXISTS bob WITH PASSWORD '<redacted>' TENANT 'acme'")
        );
        assert_eq!(
            audit_text_for("ALTER USER alice SET PASSWORD 'pw-secret'").as_deref(),
            Some("ALTER USER alice SET PASSWORD '<redacted>'")
        );
        // A malformed statement keeps only its leading keywords.
        assert_eq!(
            audit_text_for("CREATE USER bob WITH PASSWORD 'pw-secret' BOGUS").as_deref(),
            Some("CREATE USER <redacted>")
        );
        assert_eq!(
            audit_text_for("ALTER USER bob SET PASSWORD 'pw-secret' extra").as_deref(),
            Some("ALTER USER <redacted>")
        );
    }

    #[test]
    fn a_statement_without_a_secret_keeps_its_text() {
        for sql in [
            "ALTER USER alice SET ROLE readonly",
            "ALTER USER alice PASSWORD NEVER EXPIRES",
            "DROP CREDENTIAL earthdata",
            "DROP USER alice",
            "CREATE COLLECTION tok",
        ] {
            assert_eq!(audit_text_for(sql), None, "{sql}");
        }
    }
}
