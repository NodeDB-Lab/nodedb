// SPDX-License-Identifier: BUSL-1.1

//! Typeguards with every expression parsed once, at registration.
//!
//! The write path enforces guards on every row, so the type, CHECK, DEFAULT
//! and VALUE texts are parsed when the collection registers and never again
//! per row. A guard whose text does not parse keeps the parse error, and every
//! write the guard judges reports it.

use nodedb_query::expr::SqlExpr;
use nodedb_sql::parser::type_expr::{TypeExpr, parse_type_expr};
use nodedb_types::TypeGuardFieldDef;

use crate::bridge::envelope::ErrorCode;

/// The parsed clauses of one typeguard.
#[derive(Debug, Clone, PartialEq)]
pub struct GuardClauses {
    pub type_expr: TypeExpr,
    /// The CHECK text (for messages) and its parsed form.
    pub check: Option<(String, SqlExpr)>,
    /// Filled into the field when it is absent or null.
    pub default: Option<SqlExpr>,
    /// Written into the field on every write.
    pub value: Option<SqlExpr>,
}

/// One typeguard, ready to enforce on every write without re-parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledTypeGuard {
    /// The collection name the user wrote, which every violation names.
    pub collection: String,
    /// The guarded field, a dot path for a nested field.
    pub field: String,
    /// The declared type spelling, for violation messages.
    pub type_text: String,
    pub required: bool,
    /// The parsed clauses, or the parse error every write this guard judges
    /// reports. Declaration refuses every text that fails to parse, so an
    /// error names a catalog row the declaration gate never saw.
    clauses: Result<GuardClauses, ErrorCode>,
}

impl CompiledTypeGuard {
    /// Parse `def` once. `collection` is the name the user wrote, never a
    /// database-qualified key.
    pub fn compile(collection: &str, def: &TypeGuardFieldDef) -> Self {
        Self {
            collection: collection.to_string(),
            field: def.field.clone(),
            type_text: def.type_expr.clone(),
            required: def.required,
            clauses: parse_clauses(collection, def),
        }
    }

    /// Parse every guard of one collection.
    pub fn compile_all(collection: &str, defs: &[TypeGuardFieldDef]) -> Vec<Self> {
        defs.iter()
            .map(|def| Self::compile(collection, def))
            .collect()
    }

    /// The parsed clauses, or the error the guard's declaration carries.
    pub fn clauses(&self) -> Result<&GuardClauses, ErrorCode> {
        self.clauses.as_ref().map_err(Clone::clone)
    }

    /// Whether the guard declares a CHECK. A guard whose text did not parse
    /// counts as one, so the write it judges reports the parse error.
    pub fn has_check(&self) -> bool {
        match &self.clauses {
            Ok(clauses) => clauses.check.is_some(),
            Err(_) => true,
        }
    }
}

/// Parse the type, CHECK, DEFAULT and VALUE texts of `def`.
fn parse_clauses(collection: &str, def: &TypeGuardFieldDef) -> Result<GuardClauses, ErrorCode> {
    let invalid = |what: &str, text: &str, detail: String| ErrorCode::TypeGuardViolation {
        collection: collection.to_string(),
        detail: format!("field '{}': invalid {what} '{text}': {detail}", def.field),
    };
    let parse = |what: &str, text: &str| {
        nodedb_query::expr_parse::parse_generated_expr(text)
            .map(|(expr, _deps)| expr)
            .map_err(|e| invalid(what, text, e.to_string()))
    };
    let type_expr = parse_type_expr(&def.type_expr)
        .map_err(|e| invalid("type expression", &def.type_expr, e.to_string()))?;
    let check = match def.check_expr.as_deref() {
        Some(text) => Some((text.to_string(), parse("CHECK expression", text)?)),
        None => None,
    };
    let default = match def.default_expr.as_deref() {
        Some(text) => Some(parse("DEFAULT expression", text)?),
        None => None,
    };
    let value = match def.value_expr.as_deref() {
        Some(text) => Some(parse("VALUE expression", text)?),
        None => None,
    };
    Ok(GuardClauses {
        type_expr,
        check,
        default,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every clause a guard declares is parsed once, at compile.
    #[test]
    fn compile_parses_every_clause_once() {
        let def = TypeGuardFieldDef {
            field: "slug".to_string(),
            type_expr: "STRING".to_string(),
            required: true,
            check_expr: Some("slug <> ''".to_string()),
            default_expr: None,
            value_expr: Some("LOWER(title)".to_string()),
        };
        let compiled = CompiledTypeGuard::compile("c", &def);
        assert_eq!(compiled.field, "slug");
        assert!(compiled.required);
        let clauses = compiled.clauses().expect("the guard parses");
        assert!(clauses.check.is_some());
        assert!(clauses.value.is_some());
        assert!(clauses.default.is_none());
    }

    /// A type the engine cannot resolve is kept as a violation naming the
    /// field, reported by every write the guard judges.
    #[test]
    fn an_invalid_type_expression_is_a_violation() {
        let def = TypeGuardFieldDef {
            field: "x".to_string(),
            type_expr: "NOTATYPE".to_string(),
            required: false,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        };
        let compiled = CompiledTypeGuard::compile("c", &def);
        let error = compiled.clauses().expect_err("NOTATYPE is no type");
        assert!(matches!(
            error,
            ErrorCode::TypeGuardViolation { ref detail, .. } if detail.contains("'x'")
        ));
        assert!(compiled.has_check());
    }
}
