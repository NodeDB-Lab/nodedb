// SPDX-License-Identifier: Apache-2.0

//! ID generation / detection scalar function registrations.
//!
//! Evaluated by `nodedb_query::functions::id::try_eval`, but historically
//! absent from this registry entirely — the plan-time
//! existence gate (`resolver::expr::functions::convert_function_depth`)
//! checks every scalar call against this registry, so a `SELECT
//! gen_random_uuid()` (or any of its siblings below) would have been
//! rejected at plan time despite the runtime evaluator fully supporting it.
//! This file closes that gap. The list is audited to match every match arm
//! in `nodedb_query::functions::id::try_eval` exactly: `uuid` / `uuid_v4` /
//! `gen_random_uuid`, `uuid_v7`, `ulid`, `cuid2`, `nanoid`, `is_uuid`,
//! `is_ulid`, `is_cuid2`, `is_nanoid`, `id_type`, `uuid_version`,
//! `ulid_timestamp`. Keep both lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn id_fn_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "uuid",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Uuid),
            arg_types::NO_ARGS,
        )
        .volatile(),
        // `uuid_v4` and `gen_random_uuid` are aliases for `uuid` — same
        // `nodedb_query::functions::id::try_eval` match arm
        // (`"uuid" | "uuid_v4" | "gen_random_uuid"`).
        m(
            "uuid_v4",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Uuid),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "gen_random_uuid",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Uuid),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "uuid_v7",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Uuid),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "ulid",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Ulid),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "cuid2",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::NO_ARGS,
        )
        .volatile(),
        // `nanoid(length?)` — the optional length argument means max_args is
        // 1 even though the common call form `nanoid()` takes none.
        m(
            "nanoid",
            Scalar,
            0,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::NANOID_ARGS,
        )
        .volatile(),
        m(
            "is_uuid",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "is_ulid",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "is_cuid2",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "is_nanoid",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Bool),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "id_type",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "uuid_version",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::ID_CHECK_ARGS,
        ),
        m(
            "ulid_timestamp",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::ID_CHECK_ARGS,
        ),
    ]
}
