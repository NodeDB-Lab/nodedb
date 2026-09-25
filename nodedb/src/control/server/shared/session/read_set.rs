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
//! A point read that HIT records [`ReadKey::Point`] carrying the row's
//! [`KeyRepr`]; an absent DOCUMENT point read records [`ReadKey::Predicate`]
//! (its placeholder surrogate would never collide with the phantom insert's
//! fresh surrogate), while an absent KV point read keeps its precise `Point`
//! key (the byte key any future write reuses). A
//! scan / search / aggregate records [`ReadKey::Predicate`] (collection scope
//! — the day-one phantom-safe floor). A multi-shard read records one entry per
//! participating shard, each stamped with that shard's own watermark LSN.
//! Absent-key / empty-result reads are recorded too: a "not found" is a
//! validatable phantom observation, not a no-op.
//!
//! No validation happens here — the entries are captured for the commit-time
//! optimistic-concurrency check to consume.

use std::sync::Arc;

use nodedb_cluster::calvin::types::LockKeyWire;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::lock::LockKey;
use crate::control::planner::calvin::reservation::submit_reserve_read;
use crate::control::server::shared::plan_util::{extract_collection, plan_engine, read_key_of};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, KeyRepr, Lsn, TenantId, VShardId};

use super::connection::SessionId;
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
    /// A secondary-index equality observation on one indexed field, carrying
    /// the canonical stringified index value.
    IndexEq { field: String, value: String },
    /// A secondary-index range observation on one indexed field. `lo`/`hi` are
    /// optional so a one-sided native range is representable; both `None` is
    /// never emitted.
    IndexRange {
        field: String,
        lo: Option<String>,
        hi: Option<String>,
    },
}

/// Why a read is on the transaction's read-set — and therefore whether the
/// read-your-own-write exclusion is allowed to drop it.
///
/// The exclusion (in the Calvin `TxClass` builders) removes reads whose
/// collection the transaction also WRITES, because validating such a read
/// against the transaction's own staged write would abort the transaction on
/// itself. That reasoning holds only for reads the SESSION issued inside the
/// transaction. It does NOT hold for a read the Control Plane performed at plan
/// time to DERIVE a value the transaction now ships, so the two kinds are
/// distinguished here rather than guessed at the filter site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOrigin {
    /// A read this transaction itself issued, through any transport, after it
    /// began. Its observation may legitimately be superseded by the
    /// transaction's own writes, so the own-write exclusion applies.
    Session,
    /// A read the Control Plane performed BEFORE the transaction existed, whose
    /// observed value a value this transaction writes was computed from — a
    /// materialized-sum settlement's pre-image is the case that exists today.
    ///
    /// This is NOT a read-your-own-write. It observed COMMITTED base state at a
    /// point in time, and the derived value the transaction ships is only
    /// correct if that observation still holds at apply time. Dropping it
    /// because the transaction happens to write the same collection would
    /// discard the one check that catches a concurrent writer moving the base
    /// row out from under the derivation, so it survives the exclusion.
    PlanDerivation,
}

impl ReadOrigin {
    /// Whether an entry of this origin must stay in the OCC read-set even when
    /// the transaction also writes the entry's collection.
    pub fn survives_own_write_exclusion(self) -> bool {
        match self {
            ReadOrigin::Session => false,
            ReadOrigin::PlanDerivation => true,
        }
    }
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
    /// Per-collection read-version LSN (the read collection's `coll_write_lsn`
    /// at read time, a WAL LSN) — the SOUND comparand for cross-shard OCC
    /// validation, which compares it against the same collection's recorded
    /// write versions in that one domain. `read_lsn` above stays the core-global
    /// watermark used by single-shard SI (`si_conflict_abort`).
    pub read_version_lsn: Lsn,
    /// Whether this observation is the transaction's own session read or a
    /// plan-time derivation read. Required at construction — the own-write
    /// exclusion in the `TxClass` builders reads it, and an entry that guessed
    /// would be silently dropped from validation.
    pub origin: ReadOrigin,
}

/// The observed read passed to [`record_read_set`]: the executed plan, the
/// responding shards' write-LSN watermarks, the read-version floor, and whether
/// a point read hit.
pub struct ReadCapture<'a> {
    pub plan: &'a PhysicalPlan,
    pub watermarks: &'a [(VShardId, Lsn)],
    pub read_version_lsn: Lsn,
    pub found: bool,
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
///
/// `read_version_lsn` is the read collection's per-collection write floor at
/// read time (a WAL LSN) — one scalar, since a read op
/// resolves to a single collection (joins collapse to one via
/// `extract_collection`). It stamps every entry produced here and is the SOUND
/// comparand cross-shard OCC validation consumes; the per-shard `watermarks`
/// still source the core-global `read_lsn` used by single-shard SI.
///
/// `found` reports whether a point read observed a present row (`true` on a
/// hit, `false` on a miss). It only affects document point reads — an absent
/// document read degrades to a collection-scoped predicate; see [`read_key_of`].
pub async fn record_read_set(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    tenant_id: TenantId,
    capture: ReadCapture<'_>,
) {
    let ReadCapture {
        plan,
        watermarks,
        read_version_lsn,
        found,
    } = capture;
    if watermarks.is_empty() {
        return;
    }

    let engine = plan_engine(plan);
    let key = read_key_of(plan, found);
    let collection = extract_collection(plan)
        .map(String::from)
        .unwrap_or_default();
    // Scope exactly like writes: the caller passes the authenticated
    // `tenant_id` (from the dispatched task / identity), and the database is the
    // session's current database.
    let database_id = sessions
        .get_current_database(session_id)
        .unwrap_or(DatabaseId::DEFAULT);

    // Read-your-writes floor: raise the captured read-version to the session's
    // OWN highest committed write-version for this collection. Without it, a
    // read that observed a stale collection floor (0) before the session's own
    // prior committed write was reflected on the serving core would, at
    // cross-shard OCC validation, see that write's `coll_write_lsn` exceed the
    // read-version and false-abort with a serialization failure on the session's
    // OWN write. The floor is only ever raised by this session's own committed
    // writes to this exact `(database, tenant, collection)`, so a concurrent
    // OTHER-session write (higher `coll_write_lsn`) still exceeds the floor and
    // still aborts — this removes only the self-abort. `read_lsn` (the per-shard
    // watermark used by single-shard SI) is deliberately left untouched.
    let own_write_version =
        sessions.own_write_version(session_id, database_id, tenant_id, &collection);
    let effective_read_version = read_version_lsn.max(own_write_version);

    let entries: Vec<ReadSetEntry> = watermarks
        .iter()
        .map(|(_vshard, read_lsn)| ReadSetEntry {
            engine,
            database_id,
            tenant_id,
            collection: collection.clone(),
            key: key.clone(),
            read_lsn: *read_lsn,
            read_version_lsn: effective_read_version,
            // Every entry captured here is a read the session issued inside its
            // own transaction, so the own-write exclusion applies to it.
            origin: ReadOrigin::Session,
        })
        .collect();

    sessions.record_read_entries(session_id, entries);

    // RESERVE-AT-READ: when an interactive transaction reads a HOT point key,
    // take a sequenced SHARED reservation on it and remember the granted owner on
    // the session so the eventual commit can carry it as `lock_owner`. The
    // reservation is a hint — `is_hot` varies per node, and a failed/absent
    // reservation simply means the read proceeds under plain OCC. It never
    // changes the read result and never fails the read.

    // Autocommit reads never reserve: there is no transaction to carry the owner.
    if !sessions.is_in_transaction_block(session_id) {
        return;
    }

    // Only single-row point reads are lockable; scans / index / absent-document
    // observations have no single lock key to reserve.
    let Some(lock_key) = lock_key_of_read(&key, &collection) else {
        return;
    };

    // Hotness check — scope the table guard so it drops BEFORE any await.
    let now = std::time::Instant::now();
    let hot = {
        let table = state
            .calvin
            .hot_key_table
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        table.is_hot(&lock_key, now)
    };
    if !hot {
        return;
    }

    // Route the shared reservation to the SAME vshard the commit batch will use
    // for this key (write/commit routing derives the shard identically), so the
    // self-upgrade at commit finds the shared lock on the right scheduler.
    let vshard = VShardId::from_collection_in_database(database_id, &collection).as_u32();
    // Reuse the transaction's single reservation owner (None on the first hot-key
    // read; the assignment mints it, and `record_reservation` adopts it). Guard
    // dropped inside the accessor — nothing is held across the await below.
    let owner = sessions.current_reservation_owner(session_id);
    let wire_key = lock_key_to_wire(&lock_key);
    match submit_reserve_read(state, wire_key, vshard, owner).await {
        Ok(r) => sessions.record_reservation(session_id, vshard, r),
        Err(e) => {
            tracing::debug!(error = %e, "hot-key read reservation failed; proceeding under OCC");
        }
    }
}

/// Map a completed point read (`ReadKey` + collection) to the deterministic CP
/// [`LockKey`] it observed, when the read was a single-row point read
/// (`Surrogate` or `KvKey`). Every other shape (predicate / index-eq /
/// index-range scans, absent document) has no single lock key to reserve.
///
/// `pub(super)` so [`super::hot_key::record_read_set_aborts`] can reuse the
/// same construction instead of duplicating the `KeyRepr` match against a
/// [`ReadSetEntry`]'s `(key, collection)` pair.
pub(super) fn lock_key_of_read(key: &ReadKey, collection: &str) -> Option<LockKey> {
    match key {
        ReadKey::Point {
            repr: KeyRepr::Surrogate(s),
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection),
            surrogate: *s,
        }),
        ReadKey::Point {
            repr: KeyRepr::KvKey(k),
        } => Some(LockKey::Kv {
            collection: Arc::from(collection),
            key: Arc::from(&**k),
        }),
        _ => None,
    }
}

/// Convert a CP [`LockKey`] into its [`LockKeyWire`] transport twin — the
/// inverse of the scheduler driver's `decode_lock_key`.
fn lock_key_to_wire(key: &LockKey) -> LockKeyWire {
    match key {
        LockKey::Surrogate {
            collection,
            surrogate,
        } => LockKeyWire::Surrogate {
            collection: collection.to_string(),
            surrogate: *surrogate,
        },
        LockKey::Kv { collection, key } => LockKeyWire::Kv {
            collection: collection.to_string(),
            key: key.to_vec(),
        },
        LockKey::Edge {
            collection,
            src,
            dst,
        } => LockKeyWire::Edge {
            collection: collection.to_string(),
            src: *src,
            dst: *dst,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, KvOp};
    use nodedb_types::QualifiedCollection;

    fn session_id() -> SessionId {
        SessionId::from(
            "127.0.0.1:5599"
                .parse::<std::net::SocketAddr>()
                .expect("test address"),
        )
    }

    fn kv_get(collection: &str, key: &[u8]) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            key: key.to_vec(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
        })
    }

    fn kv_batch_get(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::BatchGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            keys: vec![b"a".to_vec(), b"b".to_vec()],
            rls_filters: Vec::new(),
        })
    }

    fn begun_session() -> (SessionStore, SessionId) {
        let sessions = SessionStore::new();
        let session_id = session_id();
        sessions.ensure_session(match session_id {
            SessionId::LegacySocket(addr) => addr,
            SessionId::Connection(_) => unreachable!("legacy test identity"),
        });
        sessions.begin(session_id, Lsn::new(5), 0).expect("begin");
        (sessions, session_id)
    }

    /// Build a minimal `SharedState` for the read-capture seam. The hot-key
    /// table starts empty, so `is_hot` is always false here and the
    /// reserve-at-read path is a no-op — these tests exercise read-set capture,
    /// not reservation. The returned `TempDir` must outlive the state (it backs
    /// the test WAL).
    fn test_state() -> (std::sync::Arc<SharedState>, tempfile::TempDir) {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        (state, dir)
    }

    #[tokio::test]
    async fn point_read_records_point_key() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                watermarks: &[(VShardId::new(0), Lsn::new(7))],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
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

    #[tokio::test]
    async fn predicate_read_records_predicate_key() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A batch get spans multiple keys — recorded as a collection-scoped
        // predicate (never under-approximated to a single key).
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_batch_get("c"),
                watermarks: &[(VShardId::new(0), Lsn::new(9))],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].key, ReadKey::Predicate);
    }

    #[tokio::test]
    async fn multi_shard_read_records_one_entry_per_watermark() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A predicate fanned over three cores records one entry per shard, each
        // stamped with that shard's own watermark — NOT a single collapsed max.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_batch_get("c"),
                watermarks: &[
                    (VShardId::new(0), Lsn::new(3)),
                    (VShardId::new(1), Lsn::new(11)),
                    (VShardId::new(2), Lsn::new(7)),
                ],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 3);
        let mut lsns: Vec<u64> = rs.iter().map(|e| e.read_lsn.as_u64()).collect();
        lsns.sort_unstable();
        assert_eq!(lsns, vec![3, 7, 11]);
    }

    #[tokio::test]
    async fn absent_key_point_read_is_recorded() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A "not found" is a validatable phantom observation: the KV point entry
        // is recorded (as `found = false`) at the current watermark. KV keeps the
        // precise byte key — the identity any future insert of that key reuses.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"missing"),
                watermarks: &[(VShardId::new(0), Lsn::new(5))],
                read_version_lsn: Lsn::ZERO,
                found: false,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: KeyRepr::KvKey(Box::from(b"missing".as_slice())),
            }
        );
    }

    fn doc_point_get(collection: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "d".to_string(),
            surrogate: nodedb_types::Surrogate::new(surrogate),
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        })
    }

    #[tokio::test]
    async fn document_point_read_hit_records_precise_surrogate() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A hit keeps the precise cross-engine surrogate so the common case is
        // validated per-key (no over-abort).
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &doc_point_get("docs", 42),
                watermarks: &[(VShardId::new(0), Lsn::new(7))],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: KeyRepr::Surrogate(42),
            }
        );
    }

    #[tokio::test]
    async fn absent_document_point_read_records_predicate() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A miss degrades to the collection-scoped predicate: the placeholder
        // surrogate would never collide with a phantom insert's fresh surrogate,
        // so the collection floor is the only safe read identity.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &doc_point_get("docs", 999),
                watermarks: &[(VShardId::new(0), Lsn::new(5))],
                read_version_lsn: Lsn::ZERO,
                found: false,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].key, ReadKey::Predicate);
    }

    #[tokio::test]
    async fn autocommit_reads_are_not_recorded() {
        let (state, _dir) = test_state();
        let sessions = SessionStore::new();
        let a = session_id();
        sessions.ensure_session(std::net::SocketAddr::from(([127, 0, 0, 1], 5599)));
        // No BEGIN: outside a transaction block the read-set stays empty.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                watermarks: &[(VShardId::new(0), Lsn::new(7))],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        assert!(sessions.take_read_set(a).is_empty());
    }

    #[tokio::test]
    async fn empty_watermarks_records_nothing() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                watermarks: &[],
                read_version_lsn: Lsn::ZERO,
                found: true,
            },
        )
        .await;
        assert!(sessions.take_read_set(a).is_empty());
    }

    #[test]
    fn point_get_document_uses_surrogate_identity() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: nodedb_types::Surrogate::new(42),
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        });
        assert_eq!(
            read_key_of(&plan, true),
            ReadKey::Point {
                repr: KeyRepr::Surrogate(42),
            }
        );
        assert_eq!(plan_engine(&plan), EngineTag::Document);
    }

    fn indexed_fetch(collection: &str, path: &str, value: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            path: path.to_string(),
            value: value.to_string(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
        })
    }

    fn range_scan(
        collection: &str,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::RangeScan {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            field: field.to_string(),
            lower: lower.map(|b| b.to_vec()),
            upper: upper.map(|b| b.to_vec()),
            limit: 0,
            rls_filters: Vec::new(),
        })
    }

    #[test]
    fn indexed_fetch_always_records_index_eq() {
        // A secondary-index equality read always captures the indexed field +
        // canonical value.
        let plan = indexed_fetch("users", "$.email", "a@b.c");
        assert_eq!(
            read_key_of(&plan, true),
            ReadKey::IndexEq {
                field: "$.email".to_string(),
                value: "a@b.c".to_string(),
            }
        );
    }

    #[test]
    fn range_scan_always_records_index_range() {
        let plan = range_scan("users", "$.age", Some(b"18"), Some(b"65"));
        assert_eq!(
            read_key_of(&plan, true),
            ReadKey::IndexRange {
                field: "$.age".to_string(),
                lo: Some("18".to_string()),
                hi: Some("65".to_string()),
            }
        );
    }
}
