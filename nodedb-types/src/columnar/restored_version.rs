// SPDX-License-Identifier: Apache-2.0

//! The system-time interval of one row version a backup restore reinstalls.

use serde::{Deserialize, Serialize};

/// The stamps a restored row version keeps: the ordinal its write created it
/// at, and the ordinal a later write ended it at.
///
/// A RESTORE re-issue carries one per row, index-aligned with the rows of
/// its insert. The engine installs each row at exactly these stamps, so a
/// read at any cut after the restore sees what it saw before the backup.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RestoredVersion {
    /// The system-time ordinal the version starts at.
    pub sys_from: i64,
    /// The system-time ordinal a later write ended the version at. `None`
    /// for a version no write ended.
    pub sys_to: Option<i64>,
    /// A clone copy: the row installs only where its key holds no version,
    /// live, ended or closed. A write into the clone then wins over a copy.
    pub clone_copy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let versions = vec![
            RestoredVersion {
                sys_from: 10,
                sys_to: Some(20),
                clone_copy: false,
            },
            RestoredVersion {
                sys_from: 20,
                sys_to: None,
                clone_copy: true,
            },
        ];
        let bytes = zerompk::to_msgpack_vec(&versions).expect("encode");
        let back: Vec<RestoredVersion> = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, versions);
    }
}
