// SPDX-License-Identifier: Apache-2.0

//! Per-position inference state: the slot table, its conflict rule, and the
//! parsing of a `$N` placeholder body into a position.

use nodedb_types::columnar::{FloatWidth, IntWidth};
use sqlparser::ast::{Expr, Value, ValueWithSpan};

use crate::types::ColumnInfo;
use crate::types_expr::SqlDataType;

/// A parameter type the statement pins down.
///
/// Carries the declared numeric width alongside the logical type because the
/// two are not interchangeable on the wire: a column declared `INT` must be
/// advertised as `int4` (OID 23), not `int8` (OID 20), and one declared `REAL`
/// as `float4` (OID 700), not `float8` (OID 701). The client encodes its bind
/// value at exactly the width the `ParameterDescription` names, so collapsing
/// every integer to `int8` or every float to `float8` here would put a 4-byte
/// column behind an 8-byte promise — the precise failure the catalog's
/// resolved [`IntWidth`] / [`FloatWidth`] exist to prevent.
#[derive(Debug, Clone, PartialEq)]
pub struct InferredParamType {
    /// The logical type this position resolves to.
    pub data_type: SqlDataType,
    /// Declared width for an integer position, or `None` when the form that
    /// resolved it had no catalog column behind it.
    pub int_width: Option<IntWidth>,
    /// Declared width for a floating-point position, or `None` when the form
    /// that resolved it had no catalog column behind it.
    pub float_width: Option<FloatWidth>,
}

impl InferredParamType {
    /// A type the SQL text names outright with no width: a row count.
    /// `LIMIT` / `OFFSET` are genuinely `bigint`.
    pub(super) fn from_sql_type(data_type: SqlDataType) -> Self {
        Self {
            data_type,
            int_width: None,
            float_width: None,
        }
    }

    /// The type a cast to `type_name` names, its width included:
    /// `$1::SMALLINT` is `int2`, `$1::INTEGER` and `$1::INT` are `int4`,
    /// `$1::REAL` is `float4`, as the cast evaluates and as PostgreSQL
    /// describes them.
    pub(super) fn from_cast(data_type: SqlDataType, type_name: &str) -> Self {
        Self {
            data_type,
            int_width: IntWidth::from_declared_type(type_name),
            float_width: FloatWidth::from_declared_type(type_name),
        }
    }

    /// The type of the catalog column a placeholder is compared or assigned
    /// against, declared width included.
    pub(super) fn from_column(column: &ColumnInfo) -> Self {
        Self {
            data_type: column.data_type.clone(),
            int_width: column.int_width,
            float_width: column.float_width,
        }
    }
}

/// One placeholder position's inference state.
#[derive(Clone, PartialEq)]
enum Slot {
    /// Seen, but in no position this pass can type.
    Unresolved,
    /// Typed by exactly one form (or by several that agree).
    Resolved(InferredParamType),
    /// Typed by two forms that disagree — e.g. `LIMIT $1` and `$1::TEXT`
    /// in the same statement. Reported as unknown, never as a guess.
    Conflicted,
}

/// Accumulated inference state for one SQL string.
///
/// Both passes — the catalog-free visitor and the catalog-backed walk — route
/// every conclusion through [`Self::record`], so the conflict rule applies
/// uniformly no matter which form typed a position.
#[derive(Default)]
pub(super) struct InferenceContext {
    slots: Vec<Slot>,
}

/// The highest `$N` index this pass will size the slot table for.
///
/// Matches the pgwire wire format's own ceiling: `Parse` and `Bind` both
/// carry the parameter count as an `Int16`, so no real client statement
/// names a placeholder above this. A hostile SQL string can still spell
/// `$99999999999999` — parsed as a `usize` with nothing to stop it — and
/// without this cap `Vec::resize` would attempt an allocation far beyond
/// any real statement's needs. Rejecting it here degrades to the same
/// "unresolved" outcome as any other position this pass doesn't type.
const MAX_PLACEHOLDER_INDEX: usize = u16::MAX as usize;

impl InferenceContext {
    /// Grow the slot table so a 1-based placeholder index is addressable.
    ///
    /// `None` for an index of `0` or one beyond [`MAX_PLACEHOLDER_INDEX`] —
    /// neither is a position this pass will size the slot table for.
    fn slot_mut(&mut self, index_1based: usize) -> Option<&mut Slot> {
        let zero_based = index_1based.checked_sub(1)?;
        if zero_based >= MAX_PLACEHOLDER_INDEX {
            return None;
        }
        if self.slots.len() <= zero_based {
            self.slots.resize(zero_based + 1, Slot::Unresolved);
        }
        self.slots.get_mut(zero_based)
    }

    /// Note that `$N` exists without typing it.
    pub(super) fn observe(&mut self, index_1based: usize) {
        let _ = self.slot_mut(index_1based);
    }

    /// Record a resolved type for `$N`, demoting to `Conflicted` when a
    /// different type was already recorded for the same position.
    pub(super) fn record(&mut self, index_1based: usize, ty: InferredParamType) {
        let Some(slot) = self.slot_mut(index_1based) else {
            return;
        };
        let next = match slot {
            Slot::Unresolved => Slot::Resolved(ty),
            Slot::Resolved(existing) if *existing == ty => Slot::Resolved(ty),
            Slot::Resolved(_) | Slot::Conflicted => Slot::Conflicted,
        };
        *slot = next;
    }

    /// Record `ty` for `expr` when `expr` is a bare placeholder, and do
    /// nothing otherwise.
    pub(super) fn record_if_placeholder(&mut self, expr: &Expr, ty: InferredParamType) {
        if let Some(index) = placeholder_index(expr) {
            self.record(index, ty);
        }
    }

    pub(super) fn finish(self) -> Vec<Option<InferredParamType>> {
        self.slots
            .into_iter()
            .map(|slot| match slot {
                Slot::Resolved(ty) => Some(ty),
                Slot::Unresolved | Slot::Conflicted => None,
            })
            .collect()
    }
}

/// The 1-based index of `expr` when it is (a parenthesised) `$N`.
pub(super) fn placeholder_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Placeholder(body),
            ..
        }) => parse_placeholder_body(body),
        Expr::Nested(inner) => placeholder_index(inner),
        // Not a bare placeholder — nothing to attribute a type to.
        _ => None,
    }
}

/// Parse the `1` out of a `$1` placeholder body.
///
/// Returns `None` for any other placeholder spelling (`?`, `:name`, `$`,
/// `$0`, `$abc`) rather than assuming a position.
pub(super) fn parse_placeholder_body(body: &str) -> Option<usize> {
    let digits = body.strip_prefix('$')?;
    let index: usize = digits.parse().ok()?;
    (index > 0).then_some(index)
}
