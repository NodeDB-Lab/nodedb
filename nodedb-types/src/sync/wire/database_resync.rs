// SPDX-License-Identifier: Apache-2.0

//! Database resync directive (server → client, 0x26).

use serde::{Deserialize, Serialize};

use crate::DatabaseId;

/// Database resync directive (server → client, 0x26).
///
/// Origin sends it ahead of a `HandshakeAck` when it cannot name every
/// purge the client missed. The client drops its local state of every
/// collection it holds in the database, with their pending outbound writes,
/// and subscribes its shapes again. It keeps its shape definitions.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct DatabaseResyncMsg {
    /// Numeric tenant ID of the session.
    pub tenant_id: u64,
    /// Database whose local state the client drops.
    pub database_id: DatabaseId,
    /// Why Origin cannot replay the missed purges.
    pub reason: DatabaseResyncReason,
}

/// Why Origin sends a [`DatabaseResyncMsg`].
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
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DatabaseResyncReason {
    /// Origin pruned purge records past the client's watermark: the client
    /// was offline longer than the purge-record retention.
    #[serde(rename = "below_retention_floor")]
    BelowRetentionFloor {
        /// The watermark the client sent.
        client_watermark: u64,
        /// The highest purge position Origin pruned.
        retention_floor: u64,
    },
    /// The client's watermark is past every position this server applied. It
    /// came from another cluster, or from a cluster this one was restored
    /// over.
    #[serde(rename = "ahead_of_server")]
    AheadOfServer {
        /// The watermark the client sent.
        client_watermark: u64,
        /// The server's applied position.
        server_watermark: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::wire::{SyncFrame, SyncMessageType};

    #[test]
    fn database_resync_roundtrip() {
        let msg = DatabaseResyncMsg {
            tenant_id: 3,
            database_id: DatabaseId::new(9),
            reason: DatabaseResyncReason::BelowRetentionFloor {
                client_watermark: 10,
                retention_floor: 40,
            },
        };
        let frame = SyncFrame::new_msgpack(SyncMessageType::DatabaseResync, &msg).unwrap();
        let bytes = frame.to_bytes();
        assert_eq!(bytes[1], 0x26);
        let decoded: DatabaseResyncMsg = SyncFrame::from_bytes(&bytes)
            .unwrap()
            .decode_body()
            .unwrap();
        assert_eq!(decoded.tenant_id, 3);
        assert_eq!(decoded.database_id, DatabaseId::new(9));
        assert_eq!(decoded.reason, msg.reason);
    }
}
