// SPDX-License-Identifier: BUSL-1.1

//! Superseded KV row versions and the system-time stamp rule.

pub mod stamp;
pub mod store;

pub use stamp::VersionStamp;
pub use store::{HistoryVersion, KvHistory};
