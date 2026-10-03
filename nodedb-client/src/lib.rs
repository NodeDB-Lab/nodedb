// SPDX-License-Identifier: Apache-2.0

//! NodeDB client SDK: the [`NodeDb`](traits::NodeDb) trait, the
//! `NodeDbRemote` client (native MessagePack via TLS, opt-in via the
//! `native` feature; pgwire compatibility via the `remote` feature), and
//! capability negotiation.
//!
//! For embedded use, depend on `nodedb-lite` directly. For server use,
//! enable the `native` feature and connect to a NodeDB Origin node via
//! its native protocol — pgwire compatibility (`remote`) is provided so
//! existing PostgreSQL drivers can connect for read-mostly workloads, but
//! it is not the long-term ORM target.

pub mod capabilities;
pub mod traits;

/// Shared row decoders used by both the trait default impls and the
/// feature-gated clients. Feature-agnostic on purpose — one parser per
/// row shape regardless of which transport delivered the row.
mod row_decode;

/// Shared SQL escaping helpers (string-literal and identifier quoting).
/// Used by both the native and the remote clients — one implementation
/// per escape rule, no per-feature duplicates.
#[cfg(any(feature = "native", feature = "remote"))]
mod sql_escape;

/// Shared graph-DSL builders and result decoders. Used by both clients so the
/// `GRAPH ALGO`, `GRAPH TRAVERSE` and `GRAPH PATH` SQL construction and result
/// decoding exist once, not per transport.
#[cfg(any(feature = "native", feature = "remote"))]
mod graph_dsl;

/// The identity column of a stored schemaless document, shared by both
/// clients so a document reads back with the same fields whichever wrote it.
#[cfg(any(feature = "native", feature = "remote"))]
mod document_identity;

#[cfg(feature = "remote")]
pub mod remote;
#[cfg(feature = "remote")]
mod remote_parse;

#[cfg(feature = "native")]
pub mod native;

pub use capabilities::Capabilities;
pub use traits::NodeDb;

#[cfg(feature = "remote")]
pub use remote::NodeDbRemote;

#[cfg(feature = "native")]
pub use native::builder::ConnectionBuilder;
#[cfg(feature = "native")]
pub use native::client::NativeClient;

// Re-export core types so users only need `nodedb-client` in their Cargo.toml.
pub use nodedb_types::error::{NodeDbError, NodeDbResult};
pub use nodedb_types::{
    Document, EdgeFilter, EdgeId, MetadataFilter, NodeId, QueryResult, SearchResult, SubGraph,
    Value,
};
