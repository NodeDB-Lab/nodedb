// SPDX-License-Identifier: BUSL-1.1

//! Dispatch to this node's own Data Plane: collect a request's bounded
//! response, turn an error response into a typed `Err`, and issue
//! read-only internal scans. `security` and `server` both build on it.

mod collect;
mod error_status;
mod local_read;

pub(crate) use collect::{
    DeadlineCollect, DispatchCollectError, collect_bounded_response, collect_under_deadline,
};
pub(crate) use error_status::{is_not_found, reject_data_plane_error};
pub(crate) use local_read::{LocalRead, dispatch_local_read};
