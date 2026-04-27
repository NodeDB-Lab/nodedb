//! Recursive-descent parser for the four `ARRAY` statements.
//!
//! `try_parse_array_statement` returns `Ok(None)` for any SQL that does
//! not begin with one of the array prefixes; this lets the caller fall
//! through to the standard sqlparser path. When the prefix matches, the
//! parser commits — any further error is surfaced as `SqlError::Parse`.

use super::ast::{
    AlterArrayAst, ArrayStatement, CreateArrayAst, DeleteArrayAst, DropArrayAst, InsertArrayAst,
};
use super::lexer::{Tok, Token, tokenize};
use crate::error::{Result, SqlError};
use crate::types_array::{
    ArrayAttrAst, ArrayAttrLiteral, ArrayAttrType, ArrayCellOrderAst, ArrayCoordLiteral,
    ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayInsertRow, ArrayTileOrderAst,
};

/// Top-level entry. Returns `Ok(None)` if the SQL doesn't start with an
/// array statement keyword sequence.
pub fn try_parse_array_statement(sql: &str) -> Result<Option<ArrayStatement>> {
    let trimmed = sql.trim_start();
    let upper: String = trimmed
        .chars()
        .take(40)
        .collect::<String>()
        .to_ascii_uppercase();

    if upper.starts_with("CREATE ARRAY ") || upper == "CREATE ARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("CREATE")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Create(p.parse_create()?)));
    }
    if upper.starts_with("DROP ARRAY ") || upper == "DROP ARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("DROP")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Drop(p.parse_drop()?)));
    }
    if upper.starts_with("INSERT INTO ARRAY ") {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("INSERT")?;
        p.expect_kw("INTO")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Insert(p.parse_insert()?)));
    }
    if upper.starts_with("DELETE FROM ARRAY ") {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("DELETE")?;
        p.expect_kw("FROM")?;
        p.expect_kw("ARRAY")?;
        return Ok(Some(ArrayStatement::Delete(p.parse_delete()?)));
    }
    if upper.starts_with("ALTER NDARRAY ") || upper == "ALTER NDARRAY" {
        let toks = tokenize(trimmed)?;
        let mut p = Parser::new(&toks);
        p.expect_kw("ALTER")?;
        p.expect_kw("NDARRAY")?;
        return Ok(Some(ArrayStatement::Alter(p.parse_alter()?)));
    }
    Ok(None)
}

struct Parser<'a> {
    toks: &'a [Token],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(toks: &'a [Token]) -> Self {
        Self { toks, i: 0 }
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i).map(|t| &t.tok)
    }

    fn bump(&mut self) -> Option<&'a Token> {
        let t = self.toks.get(self.i)?;
        self.i += 1;
        Some(t)
    }

    fn at_end(&self) -> bool {
        self.i >= self.toks.len()
    }

    fn err(&self, msg: impl Into<String>) -> SqlError {
        SqlError::Parse { detail: msg.into() }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        match self.peek() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => {
                self.i += 1;
                Ok(())
            }
            other => Err(self.err(format!("expected keyword `{kw}`, got {other:?}"))),
        }
    }

    fn match_kw(&mut self, kw: &str) -> bool {
        match self.peek() {
            Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw) => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Ident(s)) => Ok(s.clone()),
            other => Err(self.err(format!("expected identifier, got {other:?}"))),
        }
    }

    fn expect(&mut self, want: &Tok) -> Result<()> {
        if self.peek() == Some(want) {
            self.i += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected {want:?}, got {:?}", self.peek())))
        }
    }

    // ── CREATE ARRAY ─────────────────────────────────────────────

    fn parse_create(&mut self) -> Result<CreateArrayAst> {
        let name = self.expect_ident()?;
        self.expect_kw("DIMS")?;
        self.expect(&Tok::LParen)?;
        let mut dims = Vec::new();
        loop {
            dims.push(self.parse_dim()?);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;

        self.expect_kw("ATTRS")?;
        self.expect(&Tok::LParen)?;
        let mut attrs = Vec::new();
        loop {
            attrs.push(self.parse_attr()?);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;

        self.expect_kw("TILE_EXTENTS")?;
        self.expect(&Tok::LParen)?;
        let mut tile_extents = Vec::new();
        loop {
            tile_extents.push(self.expect_int()?);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;

        let mut cell_order = ArrayCellOrderAst::default();
        let mut tile_order = ArrayTileOrderAst::default();
        if self.match_kw("CELL_ORDER") {
            cell_order = self.parse_cell_order()?;
        }
        if self.match_kw("TILE_ORDER") {
            tile_order = self.parse_tile_order()?;
        }

        // Optional `WITH (key = value, ...)` clause.
        let mut prefix_bits: u8 = 8;
        let mut audit_retain_ms: Option<u64> = None;
        let mut minimum_audit_retain_ms: Option<u64> = None;
        if self.match_kw("WITH") {
            self.expect(&Tok::LParen)?;
            loop {
                let key = self.expect_ident()?;
                self.expect(&Tok::Eq)?;
                match key.to_ascii_lowercase().as_str() {
                    "prefix_bits" => {
                        let n = self.expect_int()?;
                        if !(1..=16).contains(&n) {
                            return Err(self.err(format!("WITH (prefix_bits = {n}): must be 1–16")));
                        }
                        prefix_bits = n as u8;
                    }
                    "audit_retain_ms" => {
                        let n = self.expect_int()?;
                        if n < 0 {
                            return Err(
                                self.err(format!("WITH (audit_retain_ms = {n}): must be >= 0"))
                            );
                        }
                        audit_retain_ms = Some(n as u64);
                    }
                    "minimum_audit_retain_ms" => {
                        let n = self.expect_int()?;
                        if n < 0 {
                            return Err(self.err(format!(
                                "WITH (minimum_audit_retain_ms = {n}): must be >= 0"
                            )));
                        }
                        minimum_audit_retain_ms = Some(n as u64);
                    }
                    other => {
                        return Err(self.err(format!(
                            "WITH: unknown option `{other}`; expected one of \
                             `prefix_bits`, `audit_retain_ms`, `minimum_audit_retain_ms`"
                        )));
                    }
                }
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;
        }

        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after CREATE ARRAY: {:?}",
                self.peek()
            )));
        }

        Ok(CreateArrayAst {
            name,
            dims,
            attrs,
            tile_extents,
            cell_order,
            tile_order,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        })
    }

    fn match_token(&mut self, want: &Tok) -> bool {
        if self.peek() == Some(want) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect_int(&mut self) -> Result<i64> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(*n),
            other => Err(self.err(format!("expected integer, got {other:?}"))),
        }
    }

    fn expect_float_or_int_as_f64(&mut self) -> Result<f64> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(*n as f64),
            Some(Tok::Float(f)) => Ok(*f),
            other => Err(self.err(format!("expected number, got {other:?}"))),
        }
    }

    fn parse_dim(&mut self) -> Result<ArrayDimAst> {
        let name = self.expect_ident()?;
        let type_name = self.expect_ident()?;
        let dtype = parse_dim_type(&type_name)
            .ok_or_else(|| self.err(format!("unknown dim type `{type_name}`")))?;
        self.expect(&Tok::LBracket)?;
        let lo = self.parse_domain_bound(dtype)?;
        self.expect(&Tok::DotDot)?;
        let hi = self.parse_domain_bound(dtype)?;
        self.expect(&Tok::RBracket)?;
        Ok(ArrayDimAst {
            name,
            dtype,
            lo,
            hi,
        })
    }

    fn parse_domain_bound(&mut self, dtype: ArrayDimType) -> Result<ArrayDomainBound> {
        match dtype {
            ArrayDimType::Int64 => Ok(ArrayDomainBound::Int64(self.expect_int()?)),
            ArrayDimType::TimestampMs => Ok(ArrayDomainBound::TimestampMs(self.expect_int()?)),
            ArrayDimType::Float64 => Ok(ArrayDomainBound::Float64(
                self.expect_float_or_int_as_f64()?,
            )),
            ArrayDimType::String => match self.bump().map(|t| &t.tok) {
                Some(Tok::Str(s)) => Ok(ArrayDomainBound::String(s.clone())),
                other => Err(self.err(format!("expected string literal, got {other:?}"))),
            },
        }
    }

    fn parse_attr(&mut self) -> Result<ArrayAttrAst> {
        let name = self.expect_ident()?;
        let type_name = self.expect_ident()?;
        let dtype = parse_attr_type(&type_name)
            .ok_or_else(|| self.err(format!("unknown attr type `{type_name}`")))?;
        let nullable = if self.match_kw("NOT") {
            self.expect_kw("NULL")?;
            false
        } else {
            // Default: nullable.
            true
        };
        Ok(ArrayAttrAst {
            name,
            dtype,
            nullable,
        })
    }

    fn parse_cell_order(&mut self) -> Result<ArrayCellOrderAst> {
        let id = self.expect_ident()?;
        match id.to_ascii_uppercase().as_str() {
            "ROW_MAJOR" => Ok(ArrayCellOrderAst::RowMajor),
            "COL_MAJOR" => Ok(ArrayCellOrderAst::ColMajor),
            "HILBERT" => Ok(ArrayCellOrderAst::Hilbert),
            "ZORDER" | "Z_ORDER" => Ok(ArrayCellOrderAst::ZOrder),
            other => Err(self.err(format!("unknown CELL_ORDER `{other}`"))),
        }
    }

    fn parse_tile_order(&mut self) -> Result<ArrayTileOrderAst> {
        let id = self.expect_ident()?;
        match id.to_ascii_uppercase().as_str() {
            "ROW_MAJOR" => Ok(ArrayTileOrderAst::RowMajor),
            "COL_MAJOR" => Ok(ArrayTileOrderAst::ColMajor),
            "HILBERT" => Ok(ArrayTileOrderAst::Hilbert),
            "ZORDER" | "Z_ORDER" => Ok(ArrayTileOrderAst::ZOrder),
            other => Err(self.err(format!("unknown TILE_ORDER `{other}`"))),
        }
    }

    // ── DROP ARRAY ───────────────────────────────────────────────

    fn parse_drop(&mut self) -> Result<DropArrayAst> {
        let if_exists = if self.match_kw("IF") {
            self.expect_kw("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after DROP ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(DropArrayAst { name, if_exists })
    }

    // ── INSERT INTO ARRAY ────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertArrayAst> {
        let name = self.expect_ident()?;
        let mut rows = Vec::new();
        loop {
            self.expect_kw("COORDS")?;
            self.expect(&Tok::LParen)?;
            let mut coords = Vec::new();
            loop {
                coords.push(self.parse_coord_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;

            self.expect_kw("VALUES")?;
            self.expect(&Tok::LParen)?;
            let mut attrs = Vec::new();
            loop {
                attrs.push(self.parse_attr_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;

            rows.push(ArrayInsertRow { coords, attrs });
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after INSERT INTO ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(InsertArrayAst { name, rows })
    }

    fn parse_coord_literal(&mut self) -> Result<ArrayCoordLiteral> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Int(n)) => Ok(ArrayCoordLiteral::Int64(*n)),
            Some(Tok::Float(f)) => Ok(ArrayCoordLiteral::Float64(*f)),
            Some(Tok::Str(s)) => Ok(ArrayCoordLiteral::String(s.clone())),
            other => Err(self.err(format!("expected coord literal, got {other:?}"))),
        }
    }

    fn parse_attr_literal(&mut self) -> Result<ArrayAttrLiteral> {
        match self.bump().map(|t| &t.tok) {
            Some(Tok::Null) => Ok(ArrayAttrLiteral::Null),
            Some(Tok::Int(n)) => Ok(ArrayAttrLiteral::Int64(*n)),
            Some(Tok::Float(f)) => Ok(ArrayAttrLiteral::Float64(*f)),
            Some(Tok::Str(s)) => Ok(ArrayAttrLiteral::String(s.clone())),
            other => Err(self.err(format!("expected attr literal, got {other:?}"))),
        }
    }

    // ── ALTER NDARRAY ────────────────────────────────────────────

    fn parse_alter(&mut self) -> Result<AlterArrayAst> {
        let name = self.expect_ident()?;
        self.expect_kw("SET")?;
        self.expect(&Tok::LParen)?;
        let mut set = Vec::new();
        loop {
            let key = self.expect_ident()?;
            self.expect(&Tok::Eq)?;
            let value = match self.peek() {
                Some(Tok::Null) => {
                    self.i += 1;
                    None
                }
                Some(Tok::Int(_)) => {
                    let n = self.expect_int()?;
                    Some(n)
                }
                other => {
                    return Err(self.err(format!(
                        "SET {key}: expected integer or NULL, got {other:?}"
                    )));
                }
            };
            let key_lower = key.to_ascii_lowercase();
            match key_lower.as_str() {
                "audit_retain_ms" => {
                    if let Some(n) = value
                        && n < 0
                    {
                        return Err(
                            self.err(format!("SET audit_retain_ms = {n}: must be >= 0 or NULL"))
                        );
                    }
                }
                "minimum_audit_retain_ms" => match value {
                    Some(n) if n < 0 => {
                        return Err(
                            self.err(format!("SET minimum_audit_retain_ms = {n}: must be >= 0"))
                        );
                    }
                    None => {
                        return Err(self.err(
                            "SET minimum_audit_retain_ms = NULL: floor cannot be NULL".to_string(),
                        ));
                    }
                    _ => {}
                },
                other => {
                    return Err(self.err(format!(
                        "SET: unknown key `{other}`; expected `audit_retain_ms` \
                         or `minimum_audit_retain_ms`"
                    )));
                }
            }
            set.push((key_lower, value));
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after ALTER NDARRAY: {:?}",
                self.peek()
            )));
        }
        if set.is_empty() {
            return Err(self.err("ALTER NDARRAY SET (): at least one key is required"));
        }
        Ok(AlterArrayAst { name, set })
    }

    // ── DELETE FROM ARRAY ────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteArrayAst> {
        let name = self.expect_ident()?;
        self.expect_kw("WHERE")?;
        self.expect_kw("COORDS")?;
        self.expect_kw("IN")?;
        self.expect(&Tok::LParen)?;
        let mut coords = Vec::new();
        loop {
            self.expect(&Tok::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_coord_literal()?);
                if !self.match_token(&Tok::Comma) {
                    break;
                }
            }
            self.expect(&Tok::RParen)?;
            coords.push(row);
            if !self.match_token(&Tok::Comma) {
                break;
            }
        }
        self.expect(&Tok::RParen)?;
        if !self.at_end() {
            return Err(self.err(format!(
                "trailing tokens after DELETE FROM ARRAY: {:?}",
                self.peek()
            )));
        }
        Ok(DeleteArrayAst { name, coords })
    }
}

fn parse_dim_type(s: &str) -> Option<ArrayDimType> {
    match s.to_ascii_uppercase().as_str() {
        "INT64" | "INT" | "BIGINT" => Some(ArrayDimType::Int64),
        "FLOAT64" | "DOUBLE" | "FLOAT" => Some(ArrayDimType::Float64),
        "TIMESTAMP_MS" | "TIMESTAMPMS" => Some(ArrayDimType::TimestampMs),
        "STRING" | "TEXT" | "VARCHAR" => Some(ArrayDimType::String),
        _ => None,
    }
}

fn parse_attr_type(s: &str) -> Option<ArrayAttrType> {
    match s.to_ascii_uppercase().as_str() {
        "INT64" | "INT" | "BIGINT" => Some(ArrayAttrType::Int64),
        "FLOAT64" | "DOUBLE" | "FLOAT" => Some(ArrayAttrType::Float64),
        "STRING" | "TEXT" | "VARCHAR" => Some(ArrayAttrType::String),
        "BYTES" | "BLOB" | "BYTEA" => Some(ArrayAttrType::Bytes),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_non_array_sql() {
        assert!(
            try_parse_array_statement("SELECT * FROM t")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_array_statement("CREATE TABLE t (x INT)")
                .unwrap()
                .is_none()
        );
        assert!(
            try_parse_array_statement("INSERT INTO foo VALUES (1)")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn parse_create_array_full() {
        let sql = "CREATE ARRAY genome \
                   DIMS (chrom INT64 [1..23], pos INT64 [0..300000000]) \
                   ATTRS (variant STRING, qual FLOAT64) \
                   TILE_EXTENTS (1, 1000000) \
                   CELL_ORDER HILBERT";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.name, "genome");
                assert_eq!(c.dims.len(), 2);
                assert_eq!(c.attrs.len(), 2);
                assert_eq!(c.tile_extents, vec![1, 1_000_000]);
                assert_eq!(c.cell_order, ArrayCellOrderAst::Hilbert);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_drop_array_if_exists() {
        let stmt = try_parse_array_statement("DROP ARRAY IF EXISTS g")
            .unwrap()
            .unwrap();
        match stmt {
            ArrayStatement::Drop(d) => {
                assert!(d.if_exists);
                assert_eq!(d.name, "g");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_insert_multi_row() {
        let sql = "INSERT INTO ARRAY g \
                   COORDS (1, 100) VALUES ('SNP', 99.5), \
                   COORDS (1, 200) VALUES ('INS', 88.0)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Insert(i) => {
                assert_eq!(i.name, "g");
                assert_eq!(i.rows.len(), 2);
                assert_eq!(i.rows[0].coords.len(), 2);
                assert_eq!(i.rows[0].attrs.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_delete_coords_in() {
        let sql = "DELETE FROM ARRAY g WHERE COORDS IN ((1, 100), (1, 200))";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Delete(d) => {
                assert_eq!(d.name, "g");
                assert_eq!(d.coords.len(), 2);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn create_rejects_unknown_dim_type() {
        let sql = "CREATE ARRAY g DIMS (x BOGUS [0..10]) ATTRS (v INT64) TILE_EXTENTS (1)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_create_array_with_audit_retain() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..100]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (10) \
                   WITH (audit_retain_ms = 86400000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.audit_retain_ms, Some(86_400_000));
                assert_eq!(c.minimum_audit_retain_ms, None);
                assert_eq!(c.prefix_bits, 8);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_create_array_with_all_retention_keys() {
        let sql = "CREATE ARRAY genomes \
                   DIMS (variant_id INT64 [0..1000000000], sample_id INT64 [0..100000]) \
                   ATTRS (gt INT64, dp INT64) \
                   TILE_EXTENTS (1024, 256) \
                   WITH (prefix_bits = 8, audit_retain_ms = 86400000, minimum_audit_retain_ms = 3600000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Create(c) => {
                assert_eq!(c.prefix_bits, 8);
                assert_eq!(c.audit_retain_ms, Some(86_400_000));
                assert_eq!(c.minimum_audit_retain_ms, Some(3_600_000));
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_create_array_unknown_with_key_rejected() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..10]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (1) \
                   WITH (bogus_key = 42)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_create_array_negative_retain_rejected() {
        let sql = "CREATE ARRAY g \
                   DIMS (x INT64 [0..10]) \
                   ATTRS (v INT64) \
                   TILE_EXTENTS (1) \
                   WITH (audit_retain_ms = -1)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_ndarray_single_key() {
        let sql = "ALTER NDARRAY my_array SET (audit_retain_ms = 86400000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.name, "my_array");
                assert_eq!(a.set.len(), 1);
                assert_eq!(a.set[0].0, "audit_retain_ms");
                assert_eq!(a.set[0].1, Some(86_400_000));
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_ndarray_null_value() {
        let sql = "ALTER NDARRAY my_array SET (audit_retain_ms = NULL)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.set[0].0, "audit_retain_ms");
                assert_eq!(a.set[0].1, None);
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_ndarray_multi_key() {
        let sql = "ALTER NDARRAY arr SET (audit_retain_ms = 5000, minimum_audit_retain_ms = 1000)";
        let stmt = try_parse_array_statement(sql).unwrap().unwrap();
        match stmt {
            ArrayStatement::Alter(a) => {
                assert_eq!(a.set.len(), 2);
                assert!(
                    a.set
                        .iter()
                        .any(|(k, v)| k == "audit_retain_ms" && *v == Some(5000))
                );
                assert!(
                    a.set
                        .iter()
                        .any(|(k, v)| k == "minimum_audit_retain_ms" && *v == Some(1000))
                );
            }
            _ => panic!("expected Alter variant"),
        }
    }

    #[test]
    fn parse_alter_ndarray_unknown_key_rejected() {
        let sql = "ALTER NDARRAY arr SET (bogus_key = 42)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_ndarray_minimum_null_rejected() {
        let sql = "ALTER NDARRAY arr SET (minimum_audit_retain_ms = NULL)";
        assert!(try_parse_array_statement(sql).is_err());
    }

    #[test]
    fn parse_alter_not_matched_for_other_sql() {
        assert!(
            try_parse_array_statement("ALTER TABLE foo ADD COLUMN x INT")
                .unwrap()
                .is_none()
        );
    }
}
