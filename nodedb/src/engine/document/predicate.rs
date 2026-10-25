// SPDX-License-Identifier: BUSL-1.1

//! Partial-index predicate: parse once, evaluate per row.
//!
//! A partial index declared as `CREATE INDEX ... WHERE <expr>` is
//! populated only with rows where `<expr>` evaluates to true. The
//! predicate text travels over the wire (see
//! [`nodedb_physical::physical_plan::RegisteredIndex::predicate`]) and
//! gets parsed once when the Data Plane installs the index via
//! `DocumentOp::Register` or runs the initial backfill.
//!
//! Parsing reuses `nodedb_query::expr_parse::parse_generated_expr`,
//! the same entry point CHECK constraints use. Evaluation reuses
//! `SqlExpr::eval` against a `Value::Object` built from the document's
//! fields. SQL three-valued logic: only `Bool(true)` counts — `NULL`,
//! `Bool(false)`, and anything else treat the row as excluded. This
//! matches Postgres partial-index semantics (`WHERE` passes only when
//! the predicate is explicitly true, unlike CHECK which also accepts
//! NULL).

use nodedb_query::SqlExpr;
use nodedb_query::expr_parse;
use nodedb_types::Value;

/// A parsed partial-index predicate, ready for per-row evaluation.
#[derive(Debug, Clone)]
pub struct IndexPredicate {
    expr: SqlExpr,
}

impl IndexPredicate {
    /// Parse the raw SQL text of a partial-index predicate. Returns
    /// `None` if the text cannot be parsed — callers should treat an
    /// unparsable predicate as a bug in the stored catalog entry,
    /// not as a pass-through (defaulting to "index everything" would
    /// silently over-populate a partial index). The caller is
    /// expected to surface a clear error; the DDL path already
    /// validates the text at CREATE INDEX time.
    pub fn parse(text: &str) -> Option<Self> {
        let (expr, _deps) = expr_parse::parse_generated_expr(text).ok()?;
        Some(Self { expr })
    }

    /// Evaluate the predicate against a document value. Returns `true`
    /// only when the expression evaluates to `Bool(true)` — `NULL`,
    /// `Bool(false)`, any non-boolean result, and a division/modulo-by-zero
    /// evaluation error all exclude the row from
    /// the index. This mirrors the existing "anything but exactly `true`
    /// excludes" partial-index philosophy already documented above (which
    /// already fails closed rather than defaulting to "index everything");
    /// an eval error gets the same fail-closed treatment as an ordinary
    /// `false`/`NULL` result, not a distinct outcome.
    pub fn evaluate(&self, doc: &Value, crs: &nodedb_crs::CrsContext<'_>) -> bool {
        matches!(self.expr.eval(doc, crs), Ok(Value::Bool(true)))
    }

    /// Convenience overload for the document write path, which holds
    /// the decoded document as a `serde_json::Value`. Conversion to
    /// `nodedb_types::Value` uses the shared `json_to_value` helper.
    pub fn evaluate_json(&self, doc: &serde_json::Value, crs: &nodedb_crs::CrsContext<'_>) -> bool {
        let v = nodedb_types::conversion::json_to_value(doc.clone());
        self.evaluate(&v, crs)
    }
}
