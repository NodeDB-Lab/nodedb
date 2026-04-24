//! Trigger dispatcher: bridges Event Plane events to Control Plane trigger fire.

pub mod batch;
pub mod identity;
pub mod single;

pub use batch::dispatch_trigger_batch;
pub use single::{dispatch_triggers, retry_single};
