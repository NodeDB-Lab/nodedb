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
//! The coordinator notes every vShard it read in the connection scope
//! ([`note`]). Each protocol records the noted reads when the request ends
//! ([`record_pending`]).
//!
//! Each vShard becomes one [`ReadSetEntry`] homed on that vShard. The commit
//! enlists that vShard, and its apply validates the entry there at the
//! transaction's snapshot.

use crate::types::{DatabaseId, TenantId, VShardId};

use super::conn_scope::with_scope;
use super::connection::SessionId;
use super::read_set::{EngineTag, ReadKey, ReadSetEntry};
use super::store::SessionStore;

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
    /// Every vShard the read observed.
    pub shards: Vec<VShardId>,
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
        sessions.record_read_entries(session_id, homed_entries(reads));
    }
}

/// One predicate entry per observed vShard, homed there.
fn homed_entries(reads: ShardReads) -> Vec<ReadSetEntry> {
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
        .map(|vshard| ReadSetEntry {
            engine,
            database_id,
            tenant_id,
            collection: collection.clone(),
            key: ReadKey::Predicate,
            // The read ran at the block's snapshot.
            observed_at: None,
            home: Some(vshard),
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
            shards: vec![VShardId::new(3), VShardId::new(9)],
        }
    }

    fn store_in_block() -> (SessionStore, SessionId) {
        let sessions = SessionStore::new();
        let session_id = session_id();
        sessions.ensure_session(match session_id {
            SessionId::LegacySocket(addr) => addr,
            SessionId::Connection(_) => unreachable!("legacy test identity"),
        });
        sessions.begin(session_id).expect("begin");
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
        assert_eq!(entries[1].home, Some(VShardId::new(9)));
        assert!(entries.iter().all(|e| e.observed_at.is_none()));
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
