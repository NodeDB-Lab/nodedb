// SPDX-License-Identifier: BUSL-1.1

//! Request/response envelopes exchanged over the SPSC bridge.

pub mod crdt_error_code;
pub mod error_code;
pub mod error_code_from;
pub mod payload;
pub mod request;
pub mod response;
pub mod stage_vote;
pub mod status;
pub mod sync_hold;

pub use error_code::ErrorCode;
pub use nodedb_physical::kv_atomic::CounterFault;
pub use nodedb_physical::physical_plan::PhysicalPlan;
pub use payload::Payload;
pub use request::{Admission, ExemptReason, Request};
pub use response::{EdgeImage, Response, RowEffect, RowVersion, WriteSetEntry};
pub use stage_vote::StageVote;
pub use status::{Priority, Status};
pub use sync_hold::SyncHold;
