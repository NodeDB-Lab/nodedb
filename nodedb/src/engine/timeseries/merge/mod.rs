// SPDX-License-Identifier: BUSL-1.1

pub mod incompatibility;
pub mod partitions;

pub use incompatibility::MergeIncompatibility;
pub use partitions::{
    MergeOutcome, MergeSource, MergedPartition, merge_sources, schema_incompatibility,
};
