pub mod traits;

#[cfg(feature = "remote")]
pub mod remote;
#[cfg(feature = "remote")]
mod remote_parse;

#[cfg(feature = "native")]
pub mod native;

pub use traits::NodeDb;

#[cfg(feature = "remote")]
pub use remote::NodeDbRemote;

#[cfg(feature = "native")]
pub use native::client::NativeClient;

// Re-export core types so users only need `nodedb-client` in their Cargo.toml.
pub use nodedb_types::error::{NodeDbError, NodeDbResult};
pub use nodedb_types::{
    Document, EdgeFilter, EdgeId, MetadataFilter, NodeId, QueryResult, SearchResult, SubGraph,
    Value,
};
