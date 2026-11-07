// SPDX-License-Identifier: BUSL-1.1

//! Why a group of timeseries partitions does not combine.

use crate::engine::timeseries::columnar_memtable::ColumnType;

/// Why partitions cannot combine without changing a row.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MergeIncompatibility {
    #[error("no source partitions")]
    NoSources,
    #[error("partition intervals differ: {first_ms} and {other_ms}")]
    IntervalsDiffer { first_ms: u64, other_ms: u64 },
    #[error("a partition schema names no time column")]
    NoTimeColumn,
    #[error("time columns differ: '{first}' and '{other}'")]
    TimeColumnsDiffer { first: String, other: String },
    #[error("column '{column}' is {first:?} in one partition and {other:?} in another")]
    ColumnTypesDiffer {
        column: String,
        first: ColumnType,
        other: ColumnType,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_to_a_storage_error_naming_the_reason() {
        let error = crate::Error::from(MergeIncompatibility::IntervalsDiffer {
            first_ms: 1,
            other_ms: 2,
        });
        assert!(
            matches!(&error, crate::Error::Storage { detail, .. } if detail.contains("intervals differ")),
            "{error}"
        );
    }
}
