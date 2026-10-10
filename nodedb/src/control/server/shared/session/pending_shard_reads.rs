// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard reads recorded into the transaction read-set.
//!
//! Some reads span many vShards through a coordinator that holds no session:
//!
//! - A cluster graph read (a MATCH, a walk, a gathered algorithm) reads edges
//!   on many key vShards.
//! - A cluster array read (a slice or an aggregate) reads cells on many tile
//!   vShards.
//!
//! The coordinator notes every vShard it read, each at the version the serving
//! core reported, in the connection scope ([`note`]). Each protocol records
//! the noted reads when the request ends ([`record_pending`]).
//!
//! Each vShard becomes one [`ReadSetEntry`] homed on that vShard. Commit
//! validation then checks the entry on the vShard that holds the data:
//!
//! - A Calvin participant checks the collection's version on that vShard.
//! - Any other commit fetches that version from the vShard's leader
//!   (`commit::read_validation`).
//!
//! A version is a data-group log position, the same on every replica. So a
//! version above the recorded one means the collection changed on that vShard
//! after the read, whichever replica served it.

use nodedb_types::WriteVersion;

use crate::types::{DatabaseId, TenantId, VShardId};

use super::conn_scope::with_scope;
use super::connection::SessionId;
use super::read_set::{EngineTag, ReadKey, ReadOrigin, ReadSetEntry};
use super::store::SessionStore;

/// One vShard a cross-shard read observed, at the version its serving core
/// reported (`WriteVersion::ZERO` when the core reported none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardObservation {
    pub vshard: VShardId,
    pub version: WriteVersion,
}

/// The vShards one cross-shard read observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardReads {
    /// The engine that served the read: `Graph` or `ClusterArray`.
    pub engine: EngineTag,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The database-qualified collection the read scoped, or `None` when it
    /// read every collection.
    pub collection: Option<String>,
    pub shards: Vec<ShardObservation>,
}

/// Note a cross-shard read for the request now running. Outside a
/// connection scope there is no session to record it for, so it is dropped.
pub fn note(reads: ShardReads) {
    if reads.shards.is_empty() {
        return;
    }
    let mut pending = Some(reads);
    with_scope((), |scope| {
        if let Some(reads) = pending.take() {
            scope.pending_shard_reads.borrow_mut().push(reads);
        }
    });
}

/// Take every cross-shard read the request noted, oldest first.
pub fn take() -> Vec<ShardReads> {
    with_scope(Vec::new(), |scope| {
        std::mem::take(&mut *scope.pending_shard_reads.borrow_mut())
    })
}

/// Record the cross-shard reads the request noted into `session_id`'s
/// transaction read-set. Outside a transaction block the reads are dropped.
pub fn record_pending(sessions: &SessionStore, session_id: SessionId) {
    let pending = take();
    if pending.is_empty() || !sessions.is_in_transaction_block(session_id) {
        return;
    }
    for reads in pending {
        let entries = homed_entries(sessions, session_id, reads);
        sessions.record_read_entries(session_id, entries);
    }
}

/// One predicate entry per observed vShard, homed there.
fn homed_entries(
    sessions: &SessionStore,
    session_id: SessionId,
    reads: ShardReads,
) -> Vec<ReadSetEntry> {
    let ShardReads {
        engine,
        tenant_id,
        database_id,
        collection,
        shards,
    } = reads;
    let collection = collection.unwrap_or_default();
    shards
        .into_iter()
        .map(|shard| {
            // The session's own committed writes to the collection raise the
            // read version, as `record_read_set` does, so a read never aborts
            // on the session's own earlier write.
            let own = if collection.is_empty() {
                WriteVersion::ZERO
            } else {
                sessions.own_write_version(
                    session_id,
                    database_id,
                    tenant_id,
                    &collection,
                    shard.vshard,
                )
            };
            ReadSetEntry {
                engine,
                database_id,
                tenant_id,
                collection: collection.clone(),
                key: ReadKey::Predicate,
                read_version: shard.version.max(own),
                origin: ReadOrigin::Session,
                home: Some(shard.vshard),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    fn session_id() -> SessionId {
        SessionId::from(
            "127.0.0.1:5611"
                .parse::<std::net::SocketAddr>()
                .expect("test address"),
        )
    }

    fn reads(engine: EngineTag, collection: Option<&str>) -> ShardReads {
        ShardReads {
            engine,
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            collection: collection.map(str::to_owned),
            shards: vec![
                ShardObservation {
                    vshard: VShardId::new(3),
                    version: WriteVersion::logged(1, 10),
                },
                ShardObservation {
                    vshard: VShardId::new(9),
                    version: WriteVersion::logged(2, 20),
                },
            ],
        }
    }

    fn store_in_block() -> (SessionStore, SessionId) {
        let sessions = SessionStore::new();
        let session_id = session_id();
        sessions.ensure_session(match session_id {
            SessionId::LegacySocket(addr) => addr,
            SessionId::Connection(_) => unreachable!("legacy test identity"),
        });
        sessions.begin(session_id, 0).expect("begin");
        (sessions, session_id)
    }

    #[tokio::test]
    async fn each_observed_vshard_becomes_one_homed_entry() {
        let (sessions, session_id) = store_in_block();
        conn_scope::scoped(async {
            note(reads(EngineTag::Graph, Some("db1.edges")));
            record_pending(&sessions, session_id);
        })
        .await;
        let entries = sessions.take_read_set(session_id);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].home, Some(VShardId::new(3)));
        assert_eq!(entries[0].read_version, WriteVersion::logged(1, 10));
        assert_eq!(entries[1].home, Some(VShardId::new(9)));
        assert_eq!(entries[1].read_version, WriteVersion::logged(2, 20));
        assert!(entries.iter().all(|e| e.key == ReadKey::Predicate));
        assert!(entries.iter().all(|e| e.collection == "db1.edges"));
        assert!(entries.iter().all(|e| e.engine == EngineTag::Graph));
    }

    #[tokio::test]
    async fn entries_carry_the_engine_that_served_the_read() {
        let (sessions, session_id) = store_in_block();
        conn_scope::scoped(async {
            note(reads(EngineTag::Graph, Some("db1.edges")));
            note(reads(EngineTag::ClusterArray, Some("db1.grid")));
            record_pending(&sessions, session_id);
        })
        .await;
        let entries = sessions.take_read_set(session_id);
        assert_eq!(entries.len(), 4);
        assert!(
            entries[..2]
                .iter()
                .all(|e| e.engine == EngineTag::Graph && e.collection == "db1.edges")
        );
        assert!(
            entries[2..]
                .iter()
                .all(|e| e.engine == EngineTag::ClusterArray && e.collection == "db1.grid")
        );
    }

    #[tokio::test]
    async fn a_read_outside_a_transaction_block_records_nothing() {
        let sessions = SessionStore::new();
        let session_id = session_id();
        conn_scope::scoped(async {
            note(reads(EngineTag::ClusterArray, None));
            record_pending(&sessions, session_id);
            assert!(take().is_empty(), "the pending reads are drained");
        })
        .await;
        assert!(sessions.take_read_set(session_id).is_empty());
    }
}
