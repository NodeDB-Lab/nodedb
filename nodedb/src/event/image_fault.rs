// SPDX-License-Identifier: BUSL-1.1

//! A row image the Data Plane could not render for the Event Plane.
//!
//! The Event Plane reads row images as MessagePack. A strict collection
//! stores a Binary Tuple, which the Data Plane decodes against the
//! collection's schema before emitting. A stored row that does not decode is
//! corrupt on disk. Its image slot stays empty and the event names the
//! fault, so no consumer reads an absent image as a row without one. The
//! delivery pipeline dead-letters such an event instead of running its side
//! effects.

/// Which row image of a write event did not render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageFault {
    /// The post-image (`new_value`).
    New,
    /// The pre-image (`old_value`).
    Old,
    /// Both images.
    Both,
}

impl ImageFault {
    /// The fault of an event whose post-image failed when `new` is true and
    /// whose pre-image failed when `old` is true. `None` when neither failed.
    pub fn of(new: bool, old: bool) -> Option<Self> {
        match (new, old) {
            (true, true) => Some(Self::Both),
            (true, false) => Some(Self::New),
            (false, true) => Some(Self::Old),
            (false, false) => None,
        }
    }

    /// Stable label for logs and dead-letter records.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Old => "old",
            Self::Both => "new_and_old",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_names_the_failed_images() {
        assert_eq!(ImageFault::of(false, false), None);
        assert_eq!(ImageFault::of(true, false), Some(ImageFault::New));
        assert_eq!(ImageFault::of(false, true), Some(ImageFault::Old));
        assert_eq!(ImageFault::of(true, true), Some(ImageFault::Both));
    }
}
