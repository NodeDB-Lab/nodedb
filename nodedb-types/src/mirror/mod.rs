// SPDX-License-Identifier: Apache-2.0

//! Mirror catalog types shared between the catalog layer and the control plane.
//!
//! A mirror database is a continuously-updated read-only replica of a source
//! database in another cluster. Promotion to writable is one-way and permanent.
//! Exhaustive matches are required everywhere these enums are matched — no
//! `_ =>` arms.

pub mod cut;
pub mod failure;
pub mod origin;
pub mod pin;
pub mod status;
#[cfg(test)]
mod test_support;

pub use cut::{
    MirrorCutAdvance, MirrorCutMark, MirrorCutState, MirrorEntryStamp, MirrorEpochRange,
    MirrorGroupPosition,
};
pub use failure::MirrorFailure;
pub use origin::{MirrorMode, MirrorOrigin};
pub use pin::{MirrorPin, MirrorPinState};
pub use status::{MirrorStatus, MirrorStatusStep};
