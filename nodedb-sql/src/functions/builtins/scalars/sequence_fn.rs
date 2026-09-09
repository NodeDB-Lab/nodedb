// SPDX-License-Identifier: Apache-2.0

//! Sequence accessor registrations: `nextval`, `currval`, `setval`.
//!
//! These are `Scalar` and `Volatile` at once. `Volatile` keeps the constant
//! folder from freezing a call into a literal and keeps a plan holding one
//! out of the plan cache, so each execution allocates a fresh value.
//! `nodedb_sql::planner::catalog_expr_fold` routes each call to the
//! `SqlCatalog` sequence methods at plan time.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn sequence_fn_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "nextval",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::SEQUENCE_NAME_ARGS,
        )
        .volatile(),
        m(
            "currval",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::SEQUENCE_NAME_ARGS,
        )
        .volatile(),
        m(
            "setval",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::SETVAL_ARGS,
        )
        .volatile(),
    ]
}
