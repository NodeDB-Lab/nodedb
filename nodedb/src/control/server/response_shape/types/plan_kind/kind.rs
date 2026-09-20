// SPDX-License-Identifier: BUSL-1.1

//! The response shape a physical plan produces.
//!
//! Carries no pgwire wire types, so every protocol-specific shaper shares it.

#[derive(Debug, Clone, Copy)]
pub enum PlanKind {
    SingleDocument,
    MultiRow,
    /// Array slice result — decoded via `ArraySliceResponse` to surface the
    /// `truncated_before_horizon` flag as a pgwire NOTICE when set.
    ArraySlice,
    /// Opaque execution result: DDL, maintenance, an internal stage, or a
    /// function-call payload its dispatcher reads directly. pgwire renders a
    /// bare `OK` tag. Never a client-facing row-count DML.
    Execution,
    /// DML operation that returns affected row count.
    /// The tag name is used in the pgwire `CommandComplete` message (e.g., "UPDATE", "DELETE").
    DmlResult(&'static str),
    /// DML whose verb is decided by the handler at apply time: the payload
    /// carries `affected` plus `op` (`"insert"` or `"update"`), read via
    /// `extract_kv_conflict_op`. Renders `INSERT 0 n` or `UPDATE n`.
    DmlResultByOp,
    /// DML with RETURNING clause — payload is a `RowsPayload` (msgpack).
    /// Decoded into one pgwire field per column.
    ReturningRows,
}
