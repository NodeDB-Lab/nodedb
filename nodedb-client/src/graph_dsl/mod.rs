// SPDX-License-Identifier: Apache-2.0

//! Shared graph-DSL builders and result decoders used by both the native and
//! the remote clients: one implementation per concern, no per-transport
//! duplicates.

pub(crate) mod decode;
pub(crate) mod edge_predicate;
pub(crate) mod pagerank;
pub(crate) mod traverse;

pub(crate) use decode::{decode_path_result, decode_traverse_result};
pub(crate) use pagerank::{build_pagerank_sql, parse_pagerank_rows};
pub(crate) use traverse::{build_graph_path_sql, build_graph_traverse_sql};
