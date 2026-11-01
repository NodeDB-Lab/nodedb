// SPDX-License-Identifier: Apache-2.0

//! Secondary-index probe carried by an indexed fetch.

/// One further indexed equality of an `IndexedFetch`. The handler reads the
/// ids this index holds for `value` and keeps only the rows the main lookup
/// shares with it, before it decodes any row body.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct IndexProbe {
    /// Indexed field path (e.g. `$.session_id`).
    pub path: String,
    /// Equality value, lowercased for a COLLATE NOCASE index.
    pub value: String,
}
