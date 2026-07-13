//! Protocol-neutral authorization for SQL physical tasks.

pub mod error;
pub mod requirements;
pub mod service;

pub use error::AuthorizationError;
pub use requirements::{AuthorizationRequirement, plan_requirements};
pub use service::{authorize_collection, authorize_database, authorize_task_set};
