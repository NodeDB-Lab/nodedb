// SPDX-License-Identifier: Apache-2.0

//! The payload of a sorted-index read inside an explicit transaction.
//!
//! A transaction must see its own DDL and its own writes. A sorted index that
//! the transaction created has no tree before COMMIT, and a committed index's
//! tree holds none of the transaction's staged writes. The Data Plane
//! therefore answers such a read from a transaction-local tree. It builds that
//! tree from the collection's base rows with the transaction's staged writes
//! folded in.

/// The definition `CREATE SORTED INDEX` registers, carried for an index the
/// transaction created and has not committed.
///
/// The fields match `KvOp::RegisterSortedIndex`, so the Data Plane builds the
/// transaction-local definition with the same code that builds a registered one.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SortedIndexSpec {
    /// Sort columns: (column_name, direction "ASC"/"DESC").
    pub sort_columns: Vec<(String, String)>,
    /// Primary key column name.
    pub key_column: String,
    /// Window type: "none", "daily", "weekly", "monthly", or "custom".
    pub window_type: String,
    /// Window timestamp column (empty if window_type == "none").
    pub window_timestamp_column: String,
    /// Custom window start (ms since epoch, 0 if N/A).
    pub window_start_ms: u64,
    /// Custom window end (ms since epoch, 0 if N/A).
    pub window_end_ms: u64,
}

/// Which sorted-index read a transaction runs.
///
/// Each arm answers exactly like its autocommit counterpart:
/// `SortedIndexRank`, `SortedIndexTopK`, `SortedIndexRange`,
/// `SortedIndexCount` and `SortedIndexScore`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SortedIndexRead {
    /// The 1-based rank of one key.
    Rank { primary_key: Vec<u8> },
    /// The top `k` entries.
    TopK { k: u32 },
    /// The entries whose leading sort column lies in the score range.
    Range {
        score_min: Option<Vec<u8>>,
        score_max: Option<Vec<u8>>,
    },
    /// The number of entries.
    Count,
    /// The sort key of one key (ZSCORE equivalent).
    Score { primary_key: Vec<u8> },
}
