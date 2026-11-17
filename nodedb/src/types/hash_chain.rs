// SPDX-License-Identifier: BUSL-1.1

//! Hash-chain types shared by the Data Plane, storage, and the Control Plane.
//!
//! A chained row stores two system fields: [`CHAIN_HASH_FIELD`], its link, and
//! [`CHAIN_SEQ_FIELD`], its 1-based position in install order. The link is
//! `SHA-256(previous link || row id || canonical contents)`, where the row id
//! is the row's storage key and the canonical contents are the stored row,
//! decoded and without the two chain fields, as MessagePack.

/// The field a chained row stores its link in.
pub const CHAIN_HASH_FIELD: &str = "_chain_hash";

/// The field a chained row stores its 1-based install-order position in.
pub const CHAIN_SEQ_FIELD: &str = "_chain_seq";

/// The link the first row of a chain links from.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Whether `name` is a chain system field.
pub fn is_chain_field(name: &str) -> bool {
    name == CHAIN_HASH_FIELD || name == CHAIN_SEQ_FIELD
}

/// One link of a chain: its position and hash.
///
/// As a collection's head it is the last link. `seq == 0` is genesis.
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
pub struct ChainHead {
    pub seq: u64,
    pub hash: String,
}

impl ChainHead {
    /// The head of a chain with no rows.
    pub fn genesis() -> Self {
        Self {
            seq: 0,
            hash: GENESIS_HASH.to_string(),
        }
    }

    /// The link after this one, carrying `hash`.
    pub fn next(&self, hash: String) -> Self {
        Self {
            seq: self.seq + 1,
            hash,
        }
    }
}

/// The first position at which a chain walk fails.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ChainBreak {
    /// 0-based position in install order.
    pub index: u64,
    /// Storage key of the row at `index`. `None` when no row holds it.
    pub document_id: Option<String>,
    /// The link the chain requires at `index`. `None` when it cannot be
    /// derived, because the row or its predecessor is missing.
    pub expected: Option<String>,
    /// The link stored at `index`. `None` when no row or no link is stored.
    pub found: Option<String>,
}

/// The result of walking a collection's chain from genesis.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct ChainVerdict {
    /// Links verified before the first break, or every link.
    pub entries: u64,
    /// The last verified link, or [`GENESIS_HASH`].
    pub last_hash: String,
    /// The first break. `None` means the chain is intact.
    pub broken: Option<ChainBreak>,
}
