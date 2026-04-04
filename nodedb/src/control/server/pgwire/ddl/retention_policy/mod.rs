pub mod alter;
pub mod create;
pub mod drop;
mod parse;
pub mod show;

pub use alter::alter_retention_policy;
pub use create::create_retention_policy;
pub use drop::drop_retention_policy;
pub use show::show_retention_policy;

/// CRDT collection name for retention policy sync between Origin and Lite.
const RETENTION_POLICIES_CRDT_COLLECTION: &str = "_retention_policies";
