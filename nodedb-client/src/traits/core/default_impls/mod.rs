// SPDX-License-Identifier: Apache-2.0

//! Default-provided method bodies for [`crate::traits::core::NodeDb`].
//!
//! Each function implements one default-provided `NodeDb` method.

mod batch;
mod collections;
mod dispatch;
mod graph;

pub(super) use batch::{batch_graph_insert_edges_default, batch_vector_insert_default};
pub(super) use collections::{
    drop_collection_purge_default, list_dropped_collections_default, on_collection_purged_default,
    undrop_collection_default,
};
pub(super) use dispatch::{
    document_get_as_of_default, document_put_with_valid_time_default,
    document_put_with_vector_default, text_search_default, vector_insert_field_default,
    vector_search_field_default,
};
pub(super) use graph::{graph_pagerank_default, graph_shortest_path_default};
