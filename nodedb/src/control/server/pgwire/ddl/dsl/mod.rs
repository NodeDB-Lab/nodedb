//! NodeDB DSL extensions — custom SQL-like commands beyond standard SQL.
//!
//! - SEARCH <collection> USING VECTOR(<field>, ARRAY[...], <k>)
//! - SEARCH <collection> USING FUSION(vector=..., graph=..., top_k=...)
//! - CREATE VECTOR INDEX <name> ON <collection> [METRIC ...] [M ...] [EF_CONSTRUCTION ...] [DIM ...]
//!   [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M ...] [IVF_CELLS ...] [IVF_NPROBE ...]
//! - CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
//! - CREATE SEARCH INDEX ON <collection> FIELDS ...
//! - CREATE SPARSE INDEX [name] ON <collection> (<field>)
//! - CRDT MERGE INTO <collection> FROM <source_id> TO <target_id>

mod crdt_merge;
mod fulltext_index;
mod helpers;
mod search_fusion;
mod search_index;
mod search_vector;
mod sparse_index;
mod vector_index;

pub use crdt_merge::crdt_merge;
pub use fulltext_index::create_fulltext_index;
pub use search_fusion::search_fusion;
pub use search_index::create_search_index;
pub use search_vector::search_vector;
pub use sparse_index::create_sparse_index;
pub use vector_index::create_vector_index;
