// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral maintenance DDL family — ANALYZE / COMPACT / REINDEX,
//! SHOW STORAGE / SHOW COMPACTION STATUS, and vector-index lifecycle
//! (SHOW / ALTER VECTOR INDEX).

pub mod analyze;
pub mod auto_analyze;
mod collection_column;
pub mod compact;
pub mod distributed;
pub mod reindex;
pub mod stats_collector;
pub mod storage_info;
pub mod support;
pub mod vector_index;
pub mod vector_index_set;

pub use analyze::handle_analyze;
pub use compact::handle_compact;
pub use reindex::handle_reindex;
pub use storage_info::{handle_show_compaction_status, handle_show_storage};
pub use vector_index::{
    handle_alter_vector_index_compact, handle_alter_vector_index_seal, handle_show_vector_index,
};
pub use vector_index_set::handle_alter_vector_index_set;
