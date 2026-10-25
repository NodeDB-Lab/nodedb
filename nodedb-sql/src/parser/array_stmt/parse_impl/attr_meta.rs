// SPDX-License-Identifier: Apache-2.0

//! Attr metadata clauses: `NODATA n`, `SCALE n`, `OFFSET n`, `UNITS '..'`.
//!
//! `CREATE ARRAY` writes them after the attr type, space-separated:
//! `red UINT16 NODATA 0 SCALE 0.0000275 OFFSET -0.2 UNITS 'reflectance'`.
//! `ALTER ARRAY a ALTER ATTR red SET (NODATA 0, SCALE 0.5)` sets them, and
//! `ALTER ARRAY a ALTER ATTR red DROP NODATA, UNITS` removes them.
//!
//! A number is an integer or float literal, `NAN` or `INF`, or one of
//! `'nan'`, `'inf'`, `'-inf'` quoted. The lexer reads a leading `-` only
//! before a digit, so negative infinity takes the quoted form. The engine
//! checks that each value fits the attr type.

use crate::error::Result;
use crate::parser::array_stmt::lexer::Tok;
use crate::types_array::{ArrayAttrMetaAst, ArrayAttrMetaKey};

use super::core::Parser;

impl<'a> Parser<'a> {
    /// The metadata key the next token names, without consuming it.
    pub(super) fn peek_meta_key(&self) -> Option<ArrayAttrMetaKey> {
        match self.peek() {
            Some(Tok::Ident(s)) => meta_key(s),
            _ => None,
        }
    }

    /// Parse one `KEY value` pair into `meta`. A key given twice is refused.
    pub(super) fn parse_meta_pair(&mut self, meta: &mut ArrayAttrMetaAst) -> Result<()> {
        let key = self.expect_meta_key()?;
        if key.is_set(meta) {
            return Err(self.err(format!("{} is given twice", key.sql_name())));
        }
        match key {
            ArrayAttrMetaKey::Nodata => meta.nodata = Some(self.parse_meta_number(key)?),
            ArrayAttrMetaKey::Scale => meta.scale = Some(self.parse_meta_number(key)?),
            ArrayAttrMetaKey::Offset => meta.offset = Some(self.parse_meta_number(key)?),
            ArrayAttrMetaKey::Units => match self.bump().map(|t| &t.tok) {
                Some(Tok::Str(s)) => meta.units = Some(s.clone()),
                other => {
                    return Err(self.err(format!("UNITS expects a quoted string, got {other:?}")));
                }
            },
        }
        Ok(())
    }

    /// Parse `( KEY value [, KEY value]* )` after `SET`.
    pub(super) fn parse_meta_set(&mut self) -> Result<ArrayAttrMetaAst> {
        self.expect(&Tok::LParen)?;
        let mut meta = ArrayAttrMetaAst::default();
        loop {
            self.parse_meta_pair(&mut meta)?;
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        Ok(meta)
    }

    /// Parse `KEY [, KEY]*` after `DROP`. A key named twice is refused.
    pub(super) fn parse_meta_drop(&mut self) -> Result<Vec<ArrayAttrMetaKey>> {
        let mut keys: Vec<ArrayAttrMetaKey> = Vec::new();
        loop {
            let key = self.expect_meta_key()?;
            if keys.contains(&key) {
                return Err(self.err(format!("DROP {} is given twice", key.sql_name())));
            }
            keys.push(key);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        Ok(keys)
    }

    fn expect_meta_key(&mut self) -> Result<ArrayAttrMetaKey> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Ident(s)) => meta_key(s).ok_or_else(|| {
                self.err(format!(
                    "unknown attr metadata `{s}`; expected NODATA, SCALE, OFFSET or UNITS"
                ))
            }),
            other => Err(self.err(format!(
                "expected NODATA, SCALE, OFFSET or UNITS, got {other:?}"
            ))),
        }
    }

    fn parse_meta_number(&mut self, key: ArrayAttrMetaKey) -> Result<f64> {
        let named = |s: &str| match s.to_ascii_lowercase().as_str() {
            "nan" => Some(f64::NAN),
            "inf" | "infinity" | "+inf" | "+infinity" => Some(f64::INFINITY),
            "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
            _ => None,
        };
        let value = match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Some(*n as f64),
            Some(Tok::Float(f)) => Some(*f),
            Some(Tok::Ident(s)) | Some(Tok::Str(s)) => named(s),
            _ => None,
        };
        value.ok_or_else(|| {
            self.err(format!(
                "{} expects a number, NAN, INF or '-inf'",
                key.sql_name()
            ))
        })
    }
}

fn meta_key(s: &str) -> Option<ArrayAttrMetaKey> {
    match s.to_ascii_uppercase().as_str() {
        "NODATA" => Some(ArrayAttrMetaKey::Nodata),
        "SCALE" => Some(ArrayAttrMetaKey::Scale),
        "OFFSET" => Some(ArrayAttrMetaKey::Offset),
        "UNITS" => Some(ArrayAttrMetaKey::Units),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::array_stmt::ast::{AlterArrayAction, ArrayStatement};
    use crate::parser::array_stmt::try_parse_array_statement;
    use crate::types_array::{
        ArrayAttrMetaAlter, ArrayAttrMetaAst, ArrayAttrMetaChange, ArrayAttrMetaKey, ArrayAttrType,
    };

    fn create(attrs: &str) -> crate::parser::array_stmt::ast::CreateArrayAst {
        let sql = format!(
            "CREATE ARRAY scene DIMS (row INT64 [0..9], col INT64 [0..9]) ATTRS ({attrs}) \
             TILE_EXTENTS (10, 10)"
        );
        match try_parse_array_statement(&sql).expect("parses") {
            Some(ArrayStatement::Create(c)) => c,
            other => panic!("expected CREATE, got {other:?}"),
        }
    }

    #[test]
    fn create_reads_every_metadata_clause() {
        let c = create(
            "red UINT16 NODATA 0 SCALE 0.0000275 OFFSET -0.2 UNITS 'reflectance', \
             qa INT8 NOT NULL, sst FLOAT32 NODATA NAN",
        );
        assert_eq!(
            c.attrs[0].meta,
            ArrayAttrMetaAst {
                nodata: Some(0.0),
                scale: Some(0.0000275),
                offset: Some(-0.2),
                units: Some("reflectance".into()),
            }
        );
        assert_eq!(c.attrs[1].dtype, ArrayAttrType::Int8);
        assert!(!c.attrs[1].nullable);
        assert_eq!(c.attrs[1].meta, ArrayAttrMetaAst::default());
        assert!(c.attrs[2].meta.nodata.is_some_and(f64::is_nan));
    }

    #[test]
    fn not_null_and_metadata_mix_in_any_order() {
        let c = create("b1 UINT16 NODATA 0 NOT NULL UNITS 'dn'");
        assert!(!c.attrs[0].nullable);
        assert_eq!(c.attrs[0].meta.nodata, Some(0.0));
        assert_eq!(c.attrs[0].meta.units.as_deref(), Some("dn"));
    }

    #[test]
    fn a_repeated_or_malformed_clause_is_refused() {
        for attrs in [
            "b1 UINT16 NODATA 0 NODATA 1",
            "b1 UINT16 SCALE 'x'",
            "b1 UINT16 UNITS 3",
        ] {
            let sql =
                format!("CREATE ARRAY s DIMS (r INT64 [0..9]) ATTRS ({attrs}) TILE_EXTENTS (10)");
            assert!(try_parse_array_statement(&sql).is_err(), "{attrs}");
        }
    }

    fn alter(sql: &str) -> AlterArrayAction {
        match try_parse_array_statement(sql).expect("parses") {
            Some(ArrayStatement::Alter(a)) => a.action,
            other => panic!("expected ALTER, got {other:?}"),
        }
    }

    #[test]
    fn alter_attr_sets_and_drops() {
        assert_eq!(
            alter("ALTER ARRAY scene ALTER ATTR red SET (NODATA 0, SCALE 0.5)"),
            AlterArrayAction::AttrMeta(ArrayAttrMetaAlter {
                attr: "red".into(),
                change: ArrayAttrMetaChange::Set(ArrayAttrMetaAst {
                    nodata: Some(0.0),
                    scale: Some(0.5),
                    ..ArrayAttrMetaAst::default()
                }),
            })
        );
        assert_eq!(
            alter("ALTER ARRAY scene ALTER ATTR red DROP NODATA, UNITS"),
            AlterArrayAction::AttrMeta(ArrayAttrMetaAlter {
                attr: "red".into(),
                change: ArrayAttrMetaChange::Drop(vec![
                    ArrayAttrMetaKey::Nodata,
                    ArrayAttrMetaKey::Units
                ]),
            })
        );
        assert!(
            try_parse_array_statement("ALTER ARRAY s ALTER ATTR red DROP NODATA, NODATA").is_err()
        );
        assert!(try_parse_array_statement("ALTER ARRAY s ALTER ATTR red SET (COLOR 1)").is_err());
    }
}
