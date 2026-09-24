// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL and SQL function handlers for sorted indexes
//! (leaderboards).
//!
//! DDL:
//!   CREATE SORTED INDEX name ON collection (score DESC [, tiebreak ASC]) KEY key_col [WINDOW DAILY ON ts_col]
//!   DROP SORTED INDEX name
//!
//! SQL functions (intercepted in DDL router):
//!   SELECT RANK(index_name, 'key_value')
//!   SELECT * FROM TOPK(index_name, k)
//!   SELECT * FROM RANGE(index_name, score_min, score_max)
//!   SELECT SORTED_COUNT(index_name)

pub mod ddl;
pub mod dispatch;
pub mod gate;
pub mod parse;
pub mod query;
mod txn_read;

pub use ddl::{create_sorted_index, drop_sorted_index};
pub use dispatch::{SortedIndexTarget, drop_in_engine};
pub(crate) use query::run_read;
pub use query::{select_range, select_rank, select_sorted_count, select_topk};
