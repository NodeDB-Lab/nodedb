// SPDX-License-Identifier: BUSL-1.1

//! Raft group disks: per-group log storage whose writes are made durable by
//! a writer thread of their own, off the `MultiRaft` lock and off the async
//! threads.

pub mod signals;
pub mod staged;
pub mod ticket;
pub mod writer;

pub use signals::{DiskSignals, GroupDiskHalt, GroupDiskHalts};
pub use staged::StagedLogStorage;
pub use ticket::DurabilityTicket;
pub use writer::{DiskProgress, GroupDisk};
