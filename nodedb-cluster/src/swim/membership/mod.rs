pub mod list;
pub mod merge;

pub use list::{MembershipList, MembershipSnapshot};
pub use merge::{MergeOutcome, merge_update};
