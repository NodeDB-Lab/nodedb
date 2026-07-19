// SPDX-License-Identifier: Apache-2.0

//! Miscellaneous scalar function registrations (coalesce, nullif, make_array, utility).

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta};

use super::super::helpers::{m, no_trigger};

pub(super) fn misc_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "coalesce",
            Scalar,
            1,
            255,
            no_trigger(),
            None,
            arg_types::COALESCE_ARGS,
        ),
        m(
            "nullif",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::NULLIF_ARGS,
        ),
        m(
            "make_array",
            Scalar,
            0,
            255,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::MAKE_ARRAY_ARGS,
        ),
        m(
            "ndb_chunk_text",
            Scalar,
            2,
            3,
            no_trigger(),
            Some(ColumnType::Array),
            arg_types::NDB_CHUNK_TEXT_ARGS,
        ),
        m(
            "version",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::NO_ARGS,
        ),
        m(
            "format_type",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
        m(
            "pg_get_expr",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
        m(
            "col_description",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::PG_CATALOG_2_ARGS,
        ),
    ]
}
