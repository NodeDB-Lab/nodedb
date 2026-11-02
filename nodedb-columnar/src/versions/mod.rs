// SPDX-License-Identifier: Apache-2.0

//! Row versions of a columnar collection.
//!
//! Every row carries `sys_from`, the system-time ordinal of the write that
//! created it. A write that overwrites or deletes a row ends it: the row
//! records `sys_to`, the ending write's ordinal, and joins its key's version
//! chain. [`version_visible`] states which rows a read at an ordinal sees.

pub mod chain;
pub mod cut;
pub mod segment;

pub use chain::VersionChains;
pub use cut::{VersionCut, version_visible};
pub use segment::{SegmentVersions, SegmentVersionsSnapshot};
