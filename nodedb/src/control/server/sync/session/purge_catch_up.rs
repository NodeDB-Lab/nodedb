// SPDX-License-Identifier: BUSL-1.1

//! The purge replay a handshake owes a reconnecting client.
//!
//! The client's handshake names the metadata log position through which it
//! applied every purge (`last_server_watermark`), and the collections it
//! holds (its vector clock keys). The purge log of the session's tenant and
//! database names every purge past that position. The handshake queues a
//! `CollectionPurged` for each held collection purged past it, or one
//! `DatabaseResync` when the log cannot name every purge past it, and the
//! session sends them ahead of the `HandshakeAck`. The ack carries the new
//! watermark, so a client that crashes before the ack replays the same
//! frames at its next handshake.
//!
//! Holding a collection is enough to receive its purge, with no read grant:
//! the grants of a dropped collection are gone, and the notice reveals only
//! the name and the log position.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nodedb_types::sync::wire::{
    CollectionPurgedMsg, DatabaseResyncMsg, DatabaseResyncReason, SyncFrame, SyncMessageType,
};

use crate::control::security::catalog::collection_purge_log::{CollectionPurge, PurgeLogTail};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::state::SyncSession;

/// How long a handshake waits for this node to apply the position a client
/// names. A client that last synced with a node ahead of this one names a
/// position this node reaches within the replication lag.
const WATERMARK_WAIT: Duration = Duration::from_secs(2);

/// What a handshake owes the client about purges past its watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CatchUp {
    /// No collection the client holds was purged past its watermark.
    Current,
    /// The newest purge of each held collection past the watermark, in log
    /// order.
    Replay(Vec<CollectionPurge>),
    /// The log cannot name every purge past the watermark.
    Resync(DatabaseResyncReason),
}

/// Decide the catch-up of a client at `client_watermark` holding `held`,
/// against this node at `server_watermark` and the purge log `tail` past
/// the client's watermark.
///
/// A client that holds nothing owes nothing. Rows at or below the
/// retention floor are gone, so a client below the floor can have missed
/// one. A watermark at 0 with a floor at 0 has the whole log to replay.
pub(super) fn select_catch_up(
    client_watermark: u64,
    server_watermark: u64,
    held: &BTreeSet<String>,
    tail: &PurgeLogTail,
) -> CatchUp {
    if held.is_empty() {
        return CatchUp::Current;
    }
    if client_watermark > server_watermark {
        return CatchUp::Resync(DatabaseResyncReason::AheadOfServer {
            client_watermark,
            server_watermark,
        });
    }
    if client_watermark < tail.retention_floor {
        return CatchUp::Resync(DatabaseResyncReason::BelowRetentionFloor {
            client_watermark,
            retention_floor: tail.retention_floor,
        });
    }
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut newest: Vec<CollectionPurge> = tail
        .purges
        .iter()
        .rev()
        .filter(|purge| purge.purge_index > client_watermark && held.contains(&purge.name))
        .filter(|purge| seen.insert(purge.name.as_str()))
        .cloned()
        .collect();
    newest.reverse();
    if newest.is_empty() {
        CatchUp::Current
    } else {
        CatchUp::Replay(newest)
    }
}

/// The collections the client holds: its vector clock keys that name a
/// collection of `database_id`. Any other key names nothing the client can
/// hold here.
fn held_collections(session: &SyncSession, database_id: DatabaseId) -> BTreeSet<String> {
    session
        .client_clock
        .keys()
        .filter_map(|name| {
            crate::control::server::shared::request_collection::request_collection(
                database_id,
                name,
            )
            .ok()
        })
        .collect()
}

fn encode<T: zerompk::ToMessagePack>(
    msg_type: SyncMessageType,
    body: &T,
) -> crate::Result<SyncFrame> {
    SyncFrame::try_encode(msg_type, body).ok_or_else(|| crate::Error::Internal {
        detail: format!("sync purge catch-up: {msg_type:?} frame does not encode"),
    })
}

impl SyncSession {
    /// Queue the purge catch-up of the authenticated handshake in
    /// [`SyncSession::purge_catch_up`] and return the watermark its ack
    /// carries.
    ///
    /// The watermark is this node's metadata applied index, read before the
    /// purge log: every purge at or below it is on the log the read sees.
    /// An `Err` is a catalog read failure, and the handshake is refused as
    /// retryable.
    pub(super) async fn prepare_purge_catch_up(
        &mut self,
        shared: Option<&Arc<SharedState>>,
    ) -> crate::Result<u64> {
        self.purge_catch_up.clear();
        let (Some(state), Some(tenant_id)) = (shared, self.tenant_id) else {
            return Ok(0);
        };
        let watcher = state.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
        if self.client_watermark > watcher.current() {
            // A lagging node catches up; a position no node reaches stays
            // ahead and resyncs.
            let _ = watcher
                .wait_for_async(self.client_watermark, Instant::now() + WATERMARK_WAIT)
                .await;
        }
        let server_watermark = watcher.current();
        let database_id = self.database_id();
        let tail = state.credentials.catalog().collection_purges_after(
            database_id.as_u64(),
            tenant_id.as_u64(),
            self.client_watermark,
        )?;
        let held = held_collections(self, database_id);
        match select_catch_up(self.client_watermark, server_watermark, &held, &tail) {
            CatchUp::Current => {}
            CatchUp::Replay(purges) => {
                for purge in purges {
                    let msg = CollectionPurgedMsg {
                        tenant_id: purge.tenant_id,
                        database_id: DatabaseId::new(purge.database_id),
                        name: purge.name,
                        purge_index: purge.purge_index,
                        incarnation: purge.record.incarnation,
                    };
                    self.purge_catch_up
                        .push(encode(SyncMessageType::CollectionPurged, &msg)?);
                }
            }
            CatchUp::Resync(reason) => {
                let msg = DatabaseResyncMsg {
                    tenant_id: tenant_id.as_u64(),
                    database_id,
                    reason,
                };
                self.purge_catch_up
                    .push(encode(SyncMessageType::DatabaseResync, &msg)?);
                return Ok(server_watermark);
            }
        }
        Ok(server_watermark.max(self.client_watermark))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Hlc;

    use super::*;
    use crate::control::security::catalog::collection_purge_log::StoredPurgeRecord;

    fn purge(index: u64, name: &str) -> CollectionPurge {
        CollectionPurge {
            database_id: 0,
            tenant_id: 1,
            purge_index: index,
            name: name.to_string(),
            record: StoredPurgeRecord {
                incarnation: Hlc::new(index, 0),
                purged_at_ms: 0,
            },
        }
    }

    fn held(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn tail(floor: u64, purges: Vec<CollectionPurge>) -> PurgeLogTail {
        PurgeLogTail {
            retention_floor: floor,
            purges,
        }
    }

    #[test]
    fn only_held_collections_are_replayed() {
        let log = tail(0, vec![purge(11, "orders"), purge(12, "users")]);
        assert_eq!(
            select_catch_up(10, 20, &held(&["orders"]), &log),
            CatchUp::Replay(vec![purge(11, "orders")])
        );
        assert_eq!(
            select_catch_up(10, 20, &held(&["other"]), &log),
            CatchUp::Current
        );
    }

    #[test]
    fn a_purge_at_or_below_the_watermark_is_not_replayed() {
        let log = tail(0, vec![purge(10, "orders")]);
        assert_eq!(
            select_catch_up(10, 20, &held(&["orders"]), &log),
            CatchUp::Current
        );
    }

    #[test]
    fn the_newest_purge_of_a_name_is_replayed_once() {
        let log = tail(0, vec![purge(11, "orders"), purge(15, "orders")]);
        assert_eq!(
            select_catch_up(10, 20, &held(&["orders"]), &log),
            CatchUp::Replay(vec![purge(15, "orders")])
        );
    }

    #[test]
    fn a_watermark_below_the_floor_resyncs() {
        let log = tail(30, vec![purge(31, "orders")]);
        assert_eq!(
            select_catch_up(10, 40, &held(&["orders"]), &log),
            CatchUp::Resync(DatabaseResyncReason::BelowRetentionFloor {
                client_watermark: 10,
                retention_floor: 30,
            })
        );
        assert_eq!(
            select_catch_up(0, 40, &held(&["orders"]), &log),
            CatchUp::Resync(DatabaseResyncReason::BelowRetentionFloor {
                client_watermark: 0,
                retention_floor: 30,
            })
        );
    }

    #[test]
    fn a_zero_watermark_with_the_whole_log_replays_it() {
        let log = tail(0, vec![purge(5, "orders")]);
        assert_eq!(
            select_catch_up(0, 40, &held(&["orders"]), &log),
            CatchUp::Replay(vec![purge(5, "orders")])
        );
    }

    #[test]
    fn a_watermark_ahead_of_the_server_resyncs() {
        assert_eq!(
            select_catch_up(50, 40, &held(&["orders"]), &tail(0, Vec::new())),
            CatchUp::Resync(DatabaseResyncReason::AheadOfServer {
                client_watermark: 50,
                server_watermark: 40,
            })
        );
    }

    #[test]
    fn a_client_holding_nothing_owes_nothing() {
        let log = tail(30, vec![purge(31, "orders")]);
        assert_eq!(select_catch_up(0, 40, &held(&[]), &log), CatchUp::Current);
    }
}
