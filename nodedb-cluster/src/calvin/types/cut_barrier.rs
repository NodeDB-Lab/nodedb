// SPDX-License-Identifier: BUSL-1.1

//! Plain-data wire twins of the data-group barrier a backup's cut orders.
//!
//! A cut marker that carries a [`CutBarrierWire`] asks every data group for
//! one barrier at the marker's watermark. The leader of each group proposes
//! the barrier once every scheduler of the group's vShards on that node
//! passed the marker. Until it applied the barrier, the leader holds the
//! redo of every transaction sequenced after the marker. So every slice of a
//! transaction before the marker sits before the barrier in its group, and
//! every slice of a later one sits after it.
//!
//! The sequencer crate cannot depend on the host's capture request, so
//! [`CutCaptureWire`] mirrors its fields.

use serde::{Deserialize, Serialize};

/// Wire twin of a database backup's capture request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CutCaptureWire {
    /// Unique per backup. It keys every parked capture.
    pub request_id: u64,
    pub database_id: u64,
    pub tenants: Vec<u64>,
}

/// The barrier a cut marker orders into every data group.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CutBarrierWire {
    /// The capture each group's leader takes when it applies the barrier,
    /// `None` for a cut that captures nothing.
    pub capture: Option<CutCaptureWire>,
}
