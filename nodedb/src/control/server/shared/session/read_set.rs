// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral, LSN-versioned transaction read-set capture.
//!
//! Every read a transaction performs is recorded here as one or more
//! [`ReadSetEntry`]s, keyed by the same `(database_id, tenant_id, collection,
//! key)` namespace writes use so that read keys and write keys compare
//! directly. Capture is transport-agnostic: native (the canonical client),
//! pgwire, native direct-ops, and single-node multi-core fan reads all funnel
//! through [`record_read_set`], so no transport silently drops the read-set.
//!
//! A point read records [`ReadKey::Point`] carrying the row's [`KeyRepr`]; a
//! scan / search / aggregate records [`ReadKey::Predicate`] (collection scope
//! — the day-one phantom-safe floor). A multi-shard read records one entry per
//! participating shard, each stamped with that shard's own watermark LSN.
//! Absent-key / empty-result reads are recorded too: a "not found" is a
//! validatable phantom observation, not a no-op.
//!
//! No validation happens here — the entries are captured for the commit-time
//! optimistic-concurrency check to consume.

use std::net::SocketAddr;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::plan_util::{extract_collection, plan_engine, read_key_of};
use crate::types::{DatabaseId, KeyRepr, Lsn, TenantId, VShardId};

use super::store::SessionStore;

/// Which peer engine served a read. Mirrors the top-level [`PhysicalPlan`]
/// variants one-to-one so the classifier is total and a new engine forces a
/// decision at compile time.
///
/// Defined in `nodedb-types` because it also travels on the replicated Calvin
/// `TxClass` versioned read-set; re-exported here so read-capture call sites
/// keep referring to it by this path.
pub use nodedb_types::calvin::EngineTag;

/// The identity a read observed within a collection.
///
/// `Point` carries the exact row identity for a keyed lookup (per-key OCC
/// validation later). `Predicate` is the coarse, collection-scoped observation
/// for scans / searches / aggregates and for keyed ops whose observation spans
/// more than one row (batch gets, secondary-index equality) — safe against
/// phantoms, never under-approximating. A future refinement may narrow
/// `Predicate` to an index-range signature without a type change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadKey {
    /// A single-row keyed observation.
    Point { repr: KeyRepr },
    /// A collection-scoped predicate observation.
    Predicate,
}

/// One LSN-versioned, predicate-aware read-set entry. Scoped by
/// `(database_id, tenant_id)` exactly like the write path so two tenants (or
/// databases) never alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub engine: EngineTag,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    pub key: ReadKey,
    pub read_lsn: Lsn,
}

/// Record a completed read into the session's transaction read-set.
///
/// Transport-agnostic: every read post-dispatch seam calls this with the plan
/// that ran and the per-shard watermark(s) it observed. Records one
/// [`ReadSetEntry`] per `(vshard, watermark)` pair — a predicate read fanned
/// over N shards yields N entries, each carrying that shard's own watermark
/// LSN. A point read observes a single shard and yields one entry.
///
/// Guarded on the connection being inside a transaction block (the session
/// write path drops the entries otherwise), so autocommit reads never touch
/// the read-set. Absent-key / empty-result reads MUST reach this with a
/// non-empty `watermarks` slice — a "not found" is a validatable observation.
pub fn record_read_set(
    sessions: &SessionStore,
    addr: &SocketAddr,
    tenant_id: TenantId,
    plan: &PhysicalPlan,
    watermarks: &[(VShardId, Lsn)],
) {
    if watermarks.is_empty() {
        return;
    }

    let engine = plan_engine(plan);
    let key = read_key_of(plan);
    let collection = extract_collection(plan)
        .map(String::from)
        .unwrap_or_default();
    // Scope exactly like writes: the caller passes the authenticated
    // `tenant_id` (from the dispatched task / identity), and the database is the
    // session's current database.
    let database_id = sessions
        .get_current_database(addr)
        .unwrap_or(DatabaseId::DEFAULT);

    let entries: Vec<ReadSetEntry> = watermarks
        .iter()
        .map(|(_vshard, read_lsn)| ReadSetEntry {
            engine,
            database_id,
            tenant_id,
            collection: collection.clone(),
            key: key.clone(),
            read_lsn: *read_lsn,
        })
        .collect();

    sessions.record_read_entries(addr, entries);
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};

    fn addr() -> SocketAddr {
        "127.0.0.1:5599".parse().expect("test addr")
    }

    fn kv_get(collection: &str, key: &[u8]) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: collection.to_string(),
            key: key.to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    fn kv_batch_get(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::BatchGet {
            collection: collection.to_string(),
            keys: vec![b"a".to_vec(), b"b".to_vec()],
        })
    }

    fn begun_session() -> (SessionStore, SocketAddr) {
        let sessions = SessionStore::new();
        let a = addr();
        sessions.ensure_session(a);
        sessions.begin(&a, Lsn::new(5), 0, 0).expect("begin");
        (sessions, a)
    }

    #[test]
    fn point_read_records_point_key() {
        let (sessions, a) = begun_session();
        record_read_set(
            &sessions,
            &a,
            TenantId::new(1),
            &kv_get("c", b"k1"),
            &[(VShardId::new(0), Lsn::new(7))],
        );
        let rs = sessions.take_read_set(&a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].engine, EngineTag::Kv);
        assert_eq!(rs[0].collection, "c");
        assert_eq!(rs[0].read_lsn, Lsn::new(7));
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: KeyRepr::KvKey(Box::from(b"k1".as_slice())),
            }
        );
    }

    #[test]
    fn predicate_read_records_predicate_key() {
        let (sessions, a) = begun_session();
        // A batch get spans multiple keys — recorded as a collection-scoped
        // predicate (never under-approximated to a single key).
        record_read_set(
            &sessions,
            &a,
            TenantId::new(1),
            &kv_batch_get("c"),
            &[(VShardId::new(0), Lsn::new(9))],
        );
        let rs = sessions.take_read_set(&a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].key, ReadKey::Predicate);
    }

    #[test]
    fn multi_shard_read_records_one_entry_per_watermark() {
        let (sessions, a) = begun_session();
        // A predicate fanned over three cores records one entry per shard, each
        // stamped with that shard's own watermark — NOT a single collapsed max.
        record_read_set(
            &sessions,
            &a,
            TenantId::new(1),
            &kv_batch_get("c"),
            &[
                (VShardId::new(0), Lsn::new(3)),
                (VShardId::new(1), Lsn::new(11)),
                (VShardId::new(2), Lsn::new(7)),
            ],
        );
        let rs = sessions.take_read_set(&a);
        assert_eq!(rs.len(), 3);
        let mut lsns: Vec<u64> = rs.iter().map(|e| e.read_lsn.as_u64()).collect();
        lsns.sort_unstable();
        assert_eq!(lsns, vec![3, 7, 11]);
    }

    #[test]
    fn absent_key_point_read_is_recorded() {
        let (sessions, a) = begun_session();
        // A "not found" is a validatable phantom observation: the point entry is
        // recorded at the current watermark just like a hit.
        record_read_set(
            &sessions,
            &a,
            TenantId::new(1),
            &kv_get("c", b"missing"),
            &[(VShardId::new(0), Lsn::new(5))],
        );
        let rs = sessions.take_read_set(&a);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: KeyRepr::KvKey(Box::from(b"missing".as_slice())),
            }
        );
    }

    #[test]
    fn autocommit_reads_are_not_recorded() {
        let sessions = SessionStore::new();
        let a = addr();
        sessions.ensure_session(a);
        // No BEGIN: outside a transaction block the read-set stays empty.
        record_read_set(
            &sessions,
            &a,
            TenantId::new(1),
            &kv_get("c", b"k1"),
            &[(VShardId::new(0), Lsn::new(7))],
        );
        assert!(sessions.take_read_set(&a).is_empty());
    }

    #[test]
    fn empty_watermarks_records_nothing() {
        let (sessions, a) = begun_session();
        record_read_set(&sessions, &a, TenantId::new(1), &kv_get("c", b"k1"), &[]);
        assert!(sessions.take_read_set(&a).is_empty());
    }

    #[test]
    fn point_get_document_uses_surrogate_identity() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".to_string(),
            document_id: "d1".to_string(),
            surrogate: nodedb_types::Surrogate::new(42),
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        });
        assert_eq!(
            read_key_of(&plan),
            ReadKey::Point {
                repr: KeyRepr::Surrogate(42),
            }
        );
        assert_eq!(plan_engine(&plan), EngineTag::Document);
    }
}
