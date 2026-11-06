// SPDX-License-Identifier: BUSL-1.1

//! The first exchange on a mirror link.
//!
//! The client's certificate already named it (see [`super::allowlist`]). The
//! hello names the source and the database it wants, and the link that follows
//! it. The source answers with the ids the mirror keeps, and the groups it
//! tails. Both travel as [`super::tail_wire`] frames.

use serde::{Deserialize, Serialize};

/// Current cross-cluster wire protocol version.
pub const MIRROR_PROTOCOL_VERSION: u16 = 1;

/// Opening request of a mirror link.
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
#[msgpack(map)]
pub struct MirrorHello {
    /// The name the mirror knows the source cluster by. The source refuses a
    /// name that is not its own, so a mirror never follows the wrong cluster.
    pub source_cluster: String,
    /// The source database's name.
    pub source_database: String,
    /// The mirror cluster's id. A source refuses its own id.
    pub mirror_cluster_id: u64,
    /// The link's id. A hello with a new id takes a retention pin.
    pub link_id: u64,
    /// Wire protocol version for this link.
    pub protocol_version: u16,
}

/// What the source tells a mirror it accepted.
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
#[msgpack(map)]
pub struct MirrorHelloAck {
    /// The source cluster's name, as the hello named it.
    pub source_cluster: String,
    /// The source cluster's id.
    pub source_cluster_id: u64,
    /// The source database's id, which the mirror keeps.
    pub source_database_id: u64,
    /// The source's metadata timeline. A change means the source restored.
    pub timeline: u64,
    /// Every group the mirror tails: the data groups and the metadata group.
    pub groups: Vec<u64>,
}
