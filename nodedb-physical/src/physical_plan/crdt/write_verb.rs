// SPDX-License-Identifier: Apache-2.0

//! The SQL verb behind a `CrdtOp::DocUpsert`.
//!
//! `INSERT`, `UPSERT` and `UPDATE` against a CRDT collection all lower to the
//! same Loro map write. The verb rides on the op so the response layer can
//! render the command tag the client's statement expects.

/// The SQL statement that produced a `CrdtOp::DocUpsert`.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CrdtWriteVerb {
    /// Plain `INSERT`: full-row replace, tagged `INSERT 0 n`.
    Insert,
    /// `UPSERT` / `INSERT ... ON CONFLICT DO UPDATE`: full-row replace,
    /// tagged `UPSERT n`.
    Upsert,
    /// `UPDATE ... SET`: partial field write, tagged `UPDATE n`.
    Update,
}

impl CrdtWriteVerb {
    /// The pgwire command-tag word for this verb.
    pub fn command_tag(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Upsert => "UPSERT",
            Self::Update => "UPDATE",
        }
    }
}
