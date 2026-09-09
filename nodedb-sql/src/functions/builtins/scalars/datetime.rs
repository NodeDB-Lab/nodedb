// SPDX-License-Identifier: Apache-2.0

//! Date/time scalar function registrations.
//!
//! The plan-time existence gate rejects any name missing
//! here, so this list is audited to match every match arm in
//! `nodedb_query::functions::datetime::try_eval` exactly: `now` /
//! `current_timestamp`, `datetime` / `to_datetime`, `unix_secs` /
//! `epoch_secs`, `unix_millis` / `epoch_millis`, `extract` / `date_part`,
//! `date_trunc` / `datetrunc`, `date_add` / `datetime_add`, `date_sub` /
//! `datetime_sub`, `date_diff` / `datediff`, `duration` / `to_duration`,
//! `decimal` / `to_decimal`, `time_bucket`. Before this audit only
//! `time_bucket`, `now`, and `current_timestamp` were registered — the rest
//! resolved at runtime but were unreachable from SQL because the gate
//! rejected them first. Note: the individual date PARTS (`year`, `month`,
//! `day`, `hour`, `minute`, `second`, `dow`/`dayofweek`, `epoch`) are string
//! literals consumed as the first argument of `extract`/`date_part`/
//! `date_trunc`, not standalone function names, so they are deliberately
//! NOT registered here. Keep both lists in sync.

use nodedb_types::columnar::ColumnType;

use crate::functions::arg_types;
use crate::functions::registry::{FunctionCategory::Scalar, FunctionMeta, SearchTrigger};

use super::super::helpers::{m, no_trigger};

pub(super) fn datetime_functions() -> Vec<FunctionMeta> {
    vec![
        m(
            "time_bucket",
            Scalar,
            2,
            2,
            SearchTrigger::TimeBucket,
            Some(ColumnType::Timestamp),
            arg_types::TIME_BUCKET_ARGS,
        ),
        m(
            "now",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Timestamptz),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "current_timestamp",
            Scalar,
            0,
            0,
            no_trigger(),
            Some(ColumnType::Timestamptz),
            arg_types::NO_ARGS,
        )
        .volatile(),
        m(
            "datetime",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
        // `to_datetime` is an alias for `datetime` — same
        // `nodedb_query::functions::datetime::try_eval` match arm
        // (`"datetime" | "to_datetime"`).
        m(
            "to_datetime",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
        m(
            "unix_secs",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATETIME_1_ARGS,
        ),
        // `epoch_secs` is an alias for `unix_secs` — same match arm
        // (`"unix_secs" | "epoch_secs"`).
        m(
            "epoch_secs",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATETIME_1_ARGS,
        ),
        m(
            "unix_millis",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATETIME_1_ARGS,
        ),
        // `epoch_millis` is an alias for `unix_millis` — same match arm
        // (`"unix_millis" | "epoch_millis"`).
        m(
            "epoch_millis",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATETIME_1_ARGS,
        ),
        m(
            "extract",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATE_PART_ARGS,
        ),
        // `date_part` is an alias for `extract` — same match arm
        // (`"extract" | "date_part"`).
        m(
            "date_part",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::Int64),
            arg_types::DATE_PART_ARGS,
        ),
        m(
            "date_trunc",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_PART_ARGS,
        ),
        // `datetrunc` is an alias for `date_trunc` — same match arm
        // (`"date_trunc" | "datetrunc"`).
        m(
            "datetrunc",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_PART_ARGS,
        ),
        m(
            "date_add",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_ARITH_ARGS,
        ),
        // `datetime_add` is an alias for `date_add` — same match arm
        // (`"date_add" | "datetime_add"`).
        m(
            "datetime_add",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_ARITH_ARGS,
        ),
        m(
            "date_sub",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_ARITH_ARGS,
        ),
        // `datetime_sub` is an alias for `date_sub` — same match arm
        // (`"date_sub" | "datetime_sub"`).
        m(
            "datetime_sub",
            Scalar,
            2,
            2,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATE_ARITH_ARGS,
        ),
        m(
            "date_diff",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::DATE_DIFF_ARGS,
        ),
        // `datediff` is an alias for `date_diff` — same match arm
        // (`"date_diff" | "datediff"`).
        m(
            "datediff",
            Scalar,
            2,
            2,
            no_trigger(),
            None,
            arg_types::DATE_DIFF_ARGS,
        ),
        m(
            "duration",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
        // `to_duration` is an alias for `duration` — same match arm
        // (`"duration" | "to_duration"`).
        m(
            "to_duration",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
        m(
            "decimal",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
        // `to_decimal` is an alias for `decimal` — same match arm
        // (`"decimal" | "to_decimal"`).
        m(
            "to_decimal",
            Scalar,
            1,
            1,
            no_trigger(),
            Some(ColumnType::String),
            arg_types::DATETIME_1_ARGS,
        ),
    ]
}
