// SPDX-License-Identifier: Apache-2.0

//! `CREATE ARRAY` statement parsing.

use crate::error::Result;
use crate::parser::array_stmt::ast::CreateArrayAst;
use crate::parser::array_stmt::lexer::Tok;
use crate::types_array::{
    ArrayAttrAst, ArrayAttrMetaAst, ArrayAttrType, ArrayCellOrderAst, ArrayDimAst, ArrayDimType,
    ArrayDomainBound, ArrayTileOrderAst,
};

use super::core::Parser;

impl<'a> Parser<'a> {
    pub(in crate::parser::array_stmt::parse) fn parse_create(&mut self) -> Result<CreateArrayAst> {
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
        let georef = if self.match_kw("GEOREF") {
            Some(self.parse_georef()?)
        } else {
            None
        };

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
            georef,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        })
    }

    fn parse_dim(&mut self) -> Result<ArrayDimAst> {
        let name = self.expect_ident()?;
        let type_name = self.expect_ident()?;
        let dtype = parse_dim_type(&type_name)
            .ok_or_else(|| self.err(format!("unknown dim type `{type_name}`")))?;
        // Domain bounds [lo..hi] are optional — omitting them defaults to the
        // full representable range for the dim type.
        let (lo, hi) = if self.match_token(&Tok::LBracket) {
            let lo = self.parse_domain_bound(dtype)?;
            self.expect(&Tok::DotDot)?;
            let hi = self.parse_domain_bound(dtype)?;
            self.expect(&Tok::RBracket)?;
            (lo, hi)
        } else {
            let (lo, hi) = default_domain_bounds(dtype);
            (lo, hi)
        };
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
        // Default: nullable. `NOT NULL` and the metadata clauses follow the
        // type in any order.
        let mut nullable = true;
        let mut meta = ArrayAttrMetaAst::default();
        loop {
            if self.match_kw("NOT") {
                // The lexer reads `NULL` as its own token, never as an identifier.
                self.expect(&Tok::Null)?;
                nullable = false;
            } else if self.peek_meta_key().is_some() {
                self.parse_meta_pair(&mut meta)?;
            } else {
                break;
            }
        }
        Ok(ArrayAttrAst {
            name,
            dtype,
            nullable,
            meta,
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
}

fn default_domain_bounds(dtype: ArrayDimType) -> (ArrayDomainBound, ArrayDomainBound) {
    match dtype {
        ArrayDimType::Int64 => (
            ArrayDomainBound::Int64(i64::MIN),
            ArrayDomainBound::Int64(i64::MAX),
        ),
        ArrayDimType::Float64 => (
            ArrayDomainBound::Float64(f64::MIN),
            ArrayDomainBound::Float64(f64::MAX),
        ),
        ArrayDimType::TimestampMs => (
            ArrayDomainBound::TimestampMs(0),
            ArrayDomainBound::TimestampMs(i64::MAX),
        ),
        ArrayDimType::String => (
            ArrayDomainBound::String(String::new()),
            ArrayDomainBound::String("\u{FFFF}".repeat(8)),
        ),
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

/// An attr type name. The canonical names give the width in bits, so
/// `INT8` is the 1-byte signed type, like `UINT8`. The PostgreSQL aliases
/// give it in bytes: `INT2` is `INT16`, `INT4` is `INT32`, and an 8-byte
/// integer is `INT64` or `BIGINT`. `INT` is the 4-byte `INT32`, as in
/// PostgreSQL. A bare `FLOAT` is the 8-byte `FLOAT64`.
fn parse_attr_type(s: &str) -> Option<ArrayAttrType> {
    match s.to_ascii_uppercase().as_str() {
        "INT8" | "TINYINT" => Some(ArrayAttrType::Int8),
        "INT64" | "BIGINT" => Some(ArrayAttrType::Int64),
        "FLOAT64" | "DOUBLE" | "FLOAT" => Some(ArrayAttrType::Float64),
        "STRING" | "TEXT" | "VARCHAR" => Some(ArrayAttrType::String),
        "BYTES" | "BLOB" | "BYTEA" => Some(ArrayAttrType::Bytes),
        "UINT8" => Some(ArrayAttrType::UInt8),
        "INT16" | "SMALLINT" | "INT2" => Some(ArrayAttrType::Int16),
        "UINT16" => Some(ArrayAttrType::UInt16),
        "INT32" | "INTEGER" | "INT" | "INT4" => Some(ArrayAttrType::Int32),
        "UINT32" => Some(ArrayAttrType::UInt32),
        "FLOAT32" | "REAL" | "FLOAT4" => Some(ArrayAttrType::Float32),
        _ => None,
    }
}
