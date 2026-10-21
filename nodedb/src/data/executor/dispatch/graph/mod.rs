// SPDX-License-Identifier: BUSL-1.1

//! Graph operation dispatch.
//!
//! - `route` — the `GraphOp` match that routes each op to its handler.
//! - `node_labels` — node-label set and removal on a graph partition.
//! - `rag` — RAG fusion routing, whole or by stage.

pub mod node_labels;
mod rag;
pub mod route;
