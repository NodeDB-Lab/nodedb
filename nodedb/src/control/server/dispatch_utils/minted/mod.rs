// SPDX-License-Identifier: BUSL-1.1

//! The records a write appends for one Data-Plane dispatch, and how their
//! outcome-floor window closes.

mod owned;
mod records;
mod resolve;

pub(crate) use owned::{Collect, OwnedResponse, OwnedWait, await_response_owned};
pub(crate) use records::{MintedRecords, RecordOwner};
pub(crate) use resolve::resolve_on_response;
