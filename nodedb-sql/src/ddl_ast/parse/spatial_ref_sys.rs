// SPDX-License-Identifier: Apache-2.0

//! Parse `CREATE SPATIAL_REF_SYS` and `DROP SPATIAL_REF_SYS`.
//!
//! Syntax:
//! - `CREATE SPATIAL_REF_SYS <code> (NAME '<name>', PROJ '<proj string>')`
//! - `CREATE SPATIAL_REF_SYS <code> (NAME '<name>', PROJJSON '<json>')`
//! - `CREATE SPATIAL_REF_SYS <code> (NAME '<name>', WKT2 '<wkt>')`
//! - `DROP SPATIAL_REF_SYS <code>`
//!
//! The two options come in either order, each once. Whether the code and
//! the definition are valid is the handler's question.

use super::clause::{ClauseCursor, tokenize};
use crate::ddl_ast::statement::{CrsTextFormat, MiscStmt, NodedbStatement};
use crate::error::SqlError;

/// Try to parse a user-defined CRS statement.
pub(super) fn try_parse(upper: &str, trimmed: &str) -> Option<Result<NodedbStatement, SqlError>> {
    if upper.starts_with("CREATE SPATIAL_REF_SYS") {
        return Some(parse_create(trimmed));
    }
    if upper.starts_with("DROP SPATIAL_REF_SYS") {
        return Some(parse_drop(trimmed));
    }
    None
}

fn parse_create(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "CREATE SPATIAL_REF_SYS");
    c.expect_keywords(&["CREATE", "SPATIAL_REF_SYS"])?;
    let srid = c.u64("SRID code")?;
    c.expect_punct('(')?;
    let mut name: Option<String> = None;
    let mut definition: Option<(CrsTextFormat, String)> = None;
    loop {
        let option = c.one_of("option", &["NAME", "PROJJSON", "PROJ", "WKT2"])?;
        let value = c.string(option)?;
        match option {
            "NAME" if name.is_none() => name = Some(value),
            "NAME" => return Err(c.error("NAME appears twice")),
            other if definition.is_some() => {
                return Err(c.error(format!(
                    "{other} conflicts with an earlier definition; give one of PROJ, PROJJSON, \
                     WKT2"
                )));
            }
            "PROJ" => definition = Some((CrsTextFormat::Proj, value)),
            "PROJJSON" => definition = Some((CrsTextFormat::ProjJson, value)),
            _ => definition = Some((CrsTextFormat::Wkt2, value)),
        }
        if !c.eat_punct(',') {
            break;
        }
    }
    c.expect_punct(')')?;
    c.finish()?;
    let name = name.ok_or_else(|| c.error("NAME is required"))?;
    if name.trim().is_empty() {
        return Err(c.error("NAME must not be empty"));
    }
    let (format, definition) =
        definition.ok_or_else(|| c.error("a definition is required: PROJ, PROJJSON or WKT2"))?;
    Ok(NodedbStatement::Misc(MiscStmt::CreateSpatialRefSys {
        srid,
        name,
        format,
        definition,
    }))
}

fn parse_drop(sql: &str) -> Result<NodedbStatement, SqlError> {
    let toks = tokenize(sql);
    let mut c = ClauseCursor::new(sql, &toks, "DROP SPATIAL_REF_SYS");
    c.expect_keywords(&["DROP", "SPATIAL_REF_SYS"])?;
    let srid = c.u64("SRID code")?;
    c.finish()?;
    Ok(NodedbStatement::Misc(MiscStmt::DropSpatialRefSys { srid }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::dispatch::parse;

    fn ok(sql: &str) -> NodedbStatement {
        parse(sql).expect("expected Some").expect("expected Ok")
    }

    fn err(sql: &str) -> SqlError {
        parse(sql)
            .expect("expected Some")
            .expect_err("expected Err")
    }

    #[test]
    fn create_each_format() {
        let stmt =
            ok("CREATE SPATIAL_REF_SYS 910001 (NAME 'Johor Grid', PROJ '+proj=tmerc +lon_0=103')");
        assert_eq!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CreateSpatialRefSys {
                srid: 910_001,
                name: "Johor Grid".into(),
                format: CrsTextFormat::Proj,
                definition: "+proj=tmerc +lon_0=103".into(),
            })
        );
        let stmt = ok(r#"create spatial_ref_sys 910002 (WKT2 'GEOGCRS["x"]', name 'n')"#);
        let NodedbStatement::Misc(MiscStmt::CreateSpatialRefSys {
            format, definition, ..
        }) = stmt
        else {
            panic!("expected CreateSpatialRefSys");
        };
        assert_eq!(format, CrsTextFormat::Wkt2);
        assert_eq!(definition, r#"GEOGCRS["x"]"#);
        let stmt =
            ok(r#"CREATE SPATIAL_REF_SYS 910003 (NAME 'j', PROJJSON '{"type": "GeographicCRS"}')"#);
        assert!(matches!(
            stmt,
            NodedbStatement::Misc(MiscStmt::CreateSpatialRefSys {
                format: CrsTextFormat::ProjJson,
                ..
            })
        ));
    }

    #[test]
    fn drop_reads_the_code() {
        assert_eq!(
            ok("DROP SPATIAL_REF_SYS 910001"),
            NodedbStatement::Misc(MiscStmt::DropSpatialRefSys { srid: 910_001 })
        );
    }

    #[test]
    fn malformed_statements_are_refused() {
        for sql in [
            "CREATE SPATIAL_REF_SYS 910001 (PROJ '+proj=longlat')",
            "CREATE SPATIAL_REF_SYS 910001 (NAME 'a')",
            "CREATE SPATIAL_REF_SYS 910001 (NAME 'a', NAME 'b', PROJ 'x')",
            "CREATE SPATIAL_REF_SYS 910001 (NAME 'a', PROJ 'x', WKT2 'y')",
            "CREATE SPATIAL_REF_SYS 910001 (NAME '', PROJ 'x')",
            "CREATE SPATIAL_REF_SYS abc (NAME 'a', PROJ 'x')",
            "CREATE SPATIAL_REF_SYS 910001 (NAME 'a', PROJ 'x') extra",
            "DROP SPATIAL_REF_SYS",
            "DROP SPATIAL_REF_SYS 910001 CASCADE",
        ] {
            let _ = err(sql);
        }
    }
}
