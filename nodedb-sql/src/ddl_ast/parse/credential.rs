// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE CREDENTIAL`, `DROP CREDENTIAL` and `SHOW CREDENTIALS`.
//!
//! Syntax:
//! - `CREATE CREDENTIAL <name> (TYPE 's3', ACCESS_KEY_ID '..', SECRET_ACCESS_KEY '..',
//!   REGION '..' [, SESSION_TOKEN '..'] [, ENDPOINT '..'])`
//! - `CREATE CREDENTIAL <name> (TYPE 'bearer', TOKEN '..')`
//! - `CREATE CREDENTIAL <name> (TYPE 'sas', TOKEN '..')`
//! - `DROP CREDENTIAL [IF EXISTS] <name>`
//! - `SHOW CREDENTIALS`
//!
//! Options come in any order, each once. No error message quotes an option
//! value, so a secret never reaches an error or a log line.

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{CredentialSpec, MiscStmt, NodedbStatement, SecretText};
use crate::error::SqlError;

/// The option keys `CREATE CREDENTIAL` takes.
const KEYS: &[&str] = &[
    "TYPE",
    "ACCESS_KEY_ID",
    "SECRET_ACCESS_KEY",
    "REGION",
    "SESSION_TOKEN",
    "ENDPOINT",
    "TOKEN",
];

/// Try to parse a credential statement.
pub(super) fn try_parse(upper: &str, trimmed: &str) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE CREDENTIAL ") {
        return Some(parse_create(trimmed));
    }
    if upper.starts_with("DROP CREDENTIAL ") {
        return Some(parse_drop(trimmed));
    }
    let bare = upper.trim_end_matches(';').trim_end();
    if bare == "SHOW CREDENTIALS" {
        return Some(Ok(NodedbStatement::Misc(MiscStmt::ShowCredentials)));
    }
    None
}

/// The audit text of a `CREATE CREDENTIAL` statement: its name and type,
/// with every secret removed. `None` for any other statement. A statement
/// that does not parse keeps only its leading keywords.
pub(super) fn audit_text(trimmed: &str, upper: &str) -> Option<String> {
    if !upper.starts_with("CREATE CREDENTIAL ") {
        return None;
    }
    Some(match parse_create(trimmed) {
        Ok(NodedbStatement::Misc(MiscStmt::CreateCredential { name, spec })) => format!(
            "CREATE CREDENTIAL {name} (TYPE '{}', <redacted>)",
            spec.type_name()
        ),
        _ => "CREATE CREDENTIAL <redacted>".to_string(),
    })
}

fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "CREATE CREDENTIAL");
    c.expect_keywords(&["CREATE", "CREDENTIAL"])?;
    let name = c.name("credential name")?;
    let shape = || {
        format!(
            "expected ( KEY 'value', ... ) with keys {}; values are quoted strings",
            KEYS.join(", ")
        )
    };
    let mut entries: Vec<(&'static str, String)> = Vec::new();
    c.expect_punct('(').map_err(|_| c.error(shape()))?;
    loop {
        let key = c.one_of("option", KEYS).map_err(|_| c.error(shape()))?;
        let value = c.string(key).map_err(|_| c.error(shape()))?;
        if entries.iter().any(|(seen, _)| *seen == key) {
            return Err(c.error(format!("{key} appears twice")));
        }
        entries.push((key, value));
        if !c.eat_punct(',') {
            break;
        }
    }
    c.expect_punct(')').map_err(|_| c.error(shape()))?;
    if !c.at_end() {
        return Err(c.error("unexpected content after the option list"));
    }
    let spec = build_spec(&c, entries)?;
    Ok(NodedbStatement::Misc(MiscStmt::CreateCredential {
        name,
        spec,
    }))
}

/// The credential the options describe, checked against its `TYPE`.
fn build_spec(
    c: &ClauseCursor<'_, '_>,
    entries: Vec<(&'static str, String)>,
) -> Result<CredentialSpec, SqlError> {
    let mut fields = Fields(entries);
    let kind = fields
        .take("TYPE")
        .ok_or_else(|| c.error("TYPE is required: 's3', 'bearer' or 'sas'"))?;
    let spec = match kind.to_ascii_lowercase().as_str() {
        "s3" => CredentialSpec::S3 {
            access_key_id: fields.secret(c, "ACCESS_KEY_ID", "s3")?,
            secret_access_key: fields.secret(c, "SECRET_ACCESS_KEY", "s3")?,
            region: fields.required(c, "REGION", "s3")?,
            session_token: fields.take("SESSION_TOKEN").map(SecretText::new),
            endpoint: fields.take("ENDPOINT"),
        },
        "bearer" => CredentialSpec::Bearer {
            token: fields.secret(c, "TOKEN", "bearer")?,
        },
        "sas" => CredentialSpec::Sas {
            token: fields.secret(c, "TOKEN", "sas")?,
        },
        other => {
            return Err(c.error(format!(
                "TYPE '{other}' is not a credential type; use 's3', 'bearer' or 'sas'"
            )));
        }
    };
    if let Some((key, _)) = fields.0.first() {
        return Err(c.error(format!(
            "{key} does not apply to TYPE '{}'",
            spec.type_name()
        )));
    }
    Ok(spec)
}

/// The options not yet read.
struct Fields(Vec<(&'static str, String)>);

impl Fields {
    fn take(&mut self, key: &str) -> Option<String> {
        let at = self.0.iter().position(|(k, _)| *k == key)?;
        Some(self.0.remove(at).1)
    }

    fn required(
        &mut self,
        c: &ClauseCursor<'_, '_>,
        key: &str,
        kind: &str,
    ) -> Result<String, SqlError> {
        let value = self
            .take(key)
            .ok_or_else(|| c.error(format!("TYPE '{kind}' needs {key}")))?;
        if value.is_empty() {
            return Err(c.error(format!("{key} must not be empty")));
        }
        Ok(value)
    }

    fn secret(
        &mut self,
        c: &ClauseCursor<'_, '_>,
        key: &str,
        kind: &str,
    ) -> Result<SecretText, SqlError> {
        self.required(c, key, kind).map(SecretText::new)
    }
}

fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "DROP CREDENTIAL");
    c.expect_keywords(&["DROP", "CREDENTIAL"])?;
    let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
    let name = c.name("credential name")?;
    c.finish()?;
    Ok(NodedbStatement::Misc(MiscStmt::DropCredential {
        name,
        if_exists,
    }))
}

#[cfg(test)]
mod tests {
    use crate::ddl_ast::parse::dispatch::parse;
    use crate::ddl_ast::statement::{CredentialSpec, MiscStmt, NodedbStatement};

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn refusal(sql: &str) -> String {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
            .to_string()
    }

    #[test]
    fn each_type_parses() {
        let stmt = ok(
            "CREATE CREDENTIAL landsat (TYPE 's3', ACCESS_KEY_ID 'AKIA1', \
             SECRET_ACCESS_KEY 'sek', REGION 'us-west-2', SESSION_TOKEN 'st')",
        );
        match stmt {
            NodedbStatement::Misc(MiscStmt::CreateCredential { name, spec }) => {
                assert_eq!(name, "landsat");
                match spec {
                    CredentialSpec::S3 {
                        access_key_id,
                        secret_access_key,
                        region,
                        session_token,
                        endpoint,
                    } => {
                        assert_eq!(access_key_id.expose(), "AKIA1");
                        assert_eq!(secret_access_key.expose(), "sek");
                        assert_eq!(region, "us-west-2");
                        assert_eq!(
                            session_token.map(|t| t.expose().to_string()),
                            Some("st".into())
                        );
                        assert_eq!(endpoint, None);
                    }
                    other => panic!("unexpected: {other:?}"),
                }
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(matches!(
            ok("CREATE CREDENTIAL earthdata (TOKEN 'eyJ', TYPE 'bearer')"),
            NodedbStatement::Misc(MiscStmt::CreateCredential {
                spec: CredentialSpec::Bearer { .. },
                ..
            })
        ));
        assert!(matches!(
            ok("CREATE CREDENTIAL blob (TYPE 'SAS', TOKEN 'sv=1')"),
            NodedbStatement::Misc(MiscStmt::CreateCredential {
                spec: CredentialSpec::Sas { .. },
                ..
            })
        ));
    }

    #[test]
    fn drop_and_show_parse() {
        assert_eq!(
            ok("DROP CREDENTIAL IF EXISTS earthdata"),
            NodedbStatement::Misc(MiscStmt::DropCredential {
                name: "earthdata".into(),
                if_exists: true
            })
        );
        assert_eq!(
            ok("SHOW CREDENTIALS"),
            NodedbStatement::Misc(MiscStmt::ShowCredentials)
        );
    }

    #[test]
    fn refusals_never_quote_a_value() {
        let message = refusal("CREATE CREDENTIAL x (TYPE 'bearer', TOKEN 'tok-secret', 'tok-2')");
        assert!(!message.contains("tok-"), "{message}");
        let message = refusal("CREATE CREDENTIAL x (TYPE 'bearer')");
        assert!(message.contains("TOKEN"), "{message}");
        let message = refusal("CREATE CREDENTIAL x (TYPE 'bearer', TOKEN 'a', REGION 'r')");
        assert!(message.contains("REGION"), "{message}");
        let message = refusal("CREATE CREDENTIAL x (TYPE 'ftp', TOKEN 'tok-secret')");
        assert!(!message.contains("tok-"), "{message}");
        let message = refusal("CREATE CREDENTIAL x (TYPE 's3', TOKEN 'a', TOKEN 'b')");
        assert!(message.contains("twice"), "{message}");
    }
}
