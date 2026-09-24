// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral graph-overlay DDL family: GRAPH INSERT/DELETE EDGE,
//! GRAPH LABEL/UNLABEL, GRAPH TRAVERSE/NEIGHBORS/PATH, GRAPH ALGO,
//! GRAPH RAG FUSION, and SHOW GRAPH STATS.
//!
//! Handlers build [`DdlResult`](super::super::result::DdlResult) directly and
//! carry no pgwire types. Each consumes already-parsed typed fields from
//! `nodedb_sql::ddl_ast::statement::GraphStmt`; raw-SQL tokenising lives in
//! `nodedb-sql::ddl_ast::graph_parse`.

pub mod algo;
pub mod dispatch;
pub mod edge;
mod edge_batch;
mod edge_parse;
mod edge_rls;
mod edge_stage;
pub mod rag_fusion;
pub mod response;
pub mod stats;
pub mod support;
pub mod traverse;

pub use dispatch::dispatch_graph;
