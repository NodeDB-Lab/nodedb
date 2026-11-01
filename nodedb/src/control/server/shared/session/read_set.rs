// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral transaction read-set capture.
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
//! (an unbound key carries no surrogate, and a phantom insert takes a fresh
//! one no point key will name), while an absent KV point read keeps its precise `Point`
//! key (the byte key any future write reuses). A
//! scan / search / aggregate records [`ReadKey::Predicate`] (collection scope
//! — the day-one phantom-safe floor).
//! Absent-key / empty-result reads are recorded too: a "not found" is a
//! validatable phantom observation, not a no-op.
//!
//! An entry carries no version. A read inside a transaction block reads at
//! the block's snapshot ordinal, and COMMIT validates every entry at that
//! ordinal: the transaction commits only when no committed version of what it
//! read has an ordinal above it. A plan-derivation read that pins its own cut
//! records that cut's ordinal on the entry ([`ReadSetEntry::observed_at`]).
//!
//! No validation happens here. The apply of the transaction's writes
//! validates the entries on every replica.

use std::sync::Arc;

use nodedb_cluster::calvin::types::LockKeyWire;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::cluster::calvin::scheduler::lock::LockKey;
use crate::control::planner::calvin::reservation::submit_reserve_read;
use crate::control::server::shared::plan_util::{extract_collection, plan_engine, read_key_of};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, KeyRepr, ReadShards, TenantId, VShardId};

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
/// phantoms, never under-approximating. A future refinement can narrow
/// `Predicate` to an index-range signature without a type change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadKey {
    /// A single-row keyed observation.
    Point { repr: ReadPoint },
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

/// The row a point read names.
///
/// A graph read records a predicate on each vShard it observed. No point
/// read names an edge, so no read lock or validation home places one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPoint {
    /// Cross-engine `u32` surrogate of a document or vector row.
    Surrogate(u32),
    /// Raw Key-Value engine key bytes.
    KvKey(Box<[u8]>),
}

impl ReadPoint {
    /// The write-index identity this read validates against.
    pub fn to_key_repr(&self) -> KeyRepr {
        match self {
            ReadPoint::Surrogate(surrogate) => KeyRepr::Surrogate(*surrogate),
            ReadPoint::KvKey(key) => KeyRepr::KvKey(key.clone()),
        }
    }
}

/// One predicate-aware read-set entry. Scoped by `(database_id, tenant_id)`
/// exactly like the write path so two tenants (or databases) never alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub engine: EngineTag,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    pub key: ReadKey,
    /// The ordinal of the cut a read pinned for itself: a plan-derivation
    /// read takes a cut before it reads. `None` for a read at the
    /// transaction block's snapshot.
    pub observed_at: Option<i64>,
    /// The vShard whose apply validates this entry, or `None` for the
    /// collection's own vShard. A cross-shard graph or array read records one
    /// entry per vShard it read, each homed there. A homed entry with an empty
    /// `collection` observed every collection on its vShard.
    pub home: Option<VShardId>,
}

impl ReadSetEntry {
    /// The vShard whose apply validates this entry: its home, else its
    /// collection's vShard. `None` for an unhomed entry whose collection
    /// names no vShard.
    pub fn validation_vshard(&self) -> Option<VShardId> {
        match self.home {
            Some(home) => Some(home),
            None => {
                nodedb_types::CollectionKey::from_qualified_str(self.database_id, &self.collection)
                    .ok()
                    .map(|key| key.vshard())
            }
        }
    }
}

/// The snapshot ordinal a commit validates `reads` at: the lowest of the
/// block's snapshot and every cut a read pinned for itself. A lower ordinal
/// only widens what counts as a change, so no change a read could miss
/// passes.
///
/// `block_snapshot` is `None` outside a transaction block, and for a block
/// that took no snapshot. A read with no cut of its own then has no ordinal
/// to validate at, so the commit is refused.
pub fn read_snapshot_ordinal(
    block_snapshot: Option<i64>,
    reads: &[ReadSetEntry],
) -> crate::Result<Option<i64>> {
    let mut snapshot = block_snapshot;
    for read in reads {
        let observed = match read.observed_at.or(block_snapshot) {
            Some(observed) => observed,
            None => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "a read of '{}' joined a transaction that holds no snapshot, so the \
                         commit cannot validate it; retry the transaction",
                        read.collection
                    ),
                });
            }
        };
        snapshot = Some(snapshot.map_or(observed, |held| held.min(observed)));
    }
    Ok(snapshot)
}

/// The observed read passed to [`record_read_set`]: the executed plan, the
/// vShards its responding cores reported, and whether a point read hit.
pub struct ReadCapture<'a> {
    pub plan: &'a PhysicalPlan,
    pub read_vshards: &'a ReadShards,
    pub found: bool,
}

/// Record a completed read into the session's transaction read-set.
///
/// Transport-agnostic: every read post-dispatch seam calls this with the plan
/// that ran and the vShards its cores reported.
///
/// A read of one collection records one entry on the collection's vShard. A
/// read that reported other vShards too records one more entry per other
/// reported vShard, homed there.
///
/// A read that names no collection observed every collection on each vShard
/// it reported. It records one entry per reported vShard, homed there.
///
/// Guarded on the connection being inside a transaction block (the session
/// write path drops the entries otherwise), so autocommit reads never touch
/// the read-set. Absent-key / empty-result reads are recorded too — a "not
/// found" is a validatable observation.
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
        read_vshards,
        found,
    } = capture;

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

    let entry = |home: Option<VShardId>| ReadSetEntry {
        engine,
        database_id,
        tenant_id,
        collection: collection.clone(),
        key: key.clone(),
        // The read ran at the block's snapshot.
        observed_at: None,
        home,
    };
    let entries: Vec<ReadSetEntry> =
        match nodedb_types::CollectionKey::from_qualified_str(database_id, &collection) {
            Ok(collection_key) if !collection.is_empty() => {
                let validation = collection_key.vshard();
                let mut entries = vec![entry(None)];
                entries.extend(
                    read_vshards
                        .iter()
                        .filter(|vshard| *vshard != validation)
                        .map(|vshard| entry(Some(vshard))),
                );
                entries
            }
            _ => read_vshards
                .iter()
                .map(|vshard| entry(Some(vshard)))
                .collect(),
        };
    if entries.is_empty() {
        return;
    }

    sessions.record_read_entries(session_id, entries);

    // RESERVE-AT-READ: when an interactive transaction reads a HOT point key,
    // take a sequenced SHARED reservation on it and remember the granted owner on
    // the session so the eventual commit can carry it as `lock_owner`. The
    // reservation is a hint — `is_hot` varies per node, and a failed/absent
    // reservation means the read proceeds under plain OCC. It never
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
    // `collection` is the plan's database-qualified name. A name that does
    // not de-qualify takes no reservation, and the read proceeds under OCC.
    let Ok(collection_key) =
        nodedb_types::CollectionKey::from_qualified_str(database_id, &collection)
    else {
        return;
    };
    let vshard = collection_key.vshard().as_u32();
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
            repr: ReadPoint::Surrogate(s),
        } => Some(LockKey::Surrogate {
            collection: Arc::from(collection),
            surrogate: *s,
        }),
        ReadKey::Point {
            repr: ReadPoint::KvKey(k),
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
        LockKey::Collection { collection } => LockKeyWire::Collection {
            collection: collection.to_string(),
        },
        LockKey::Unique {
            collection,
            index,
            value,
        } => LockKeyWire::Unique {
            collection: collection.to_string(),
            index: index.to_string(),
            value: value.to_vec(),
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
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    fn kv_batch_get(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::BatchGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            keys: vec![b"a".to_vec(), b"b".to_vec()],
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
        })
    }

    fn begun_session() -> (SessionStore, SessionId) {
        let sessions = SessionStore::new();
        let session_id = session_id();
        sessions.ensure_session(match session_id {
            SessionId::LegacySocket(addr) => addr,
            SessionId::Connection(_) => unreachable!("legacy test identity"),
        });
        sessions.begin(session_id).expect("begin");
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

    /// The vShard a read of `collection` validates on.
    fn home_of(collection: &str) -> VShardId {
        nodedb_types::CollectionKey::from_qualified_str(DatabaseId::DEFAULT, collection)
            .expect("collection key")
            .vshard()
    }

    /// `collection`'s home vShard, as a read of it reports it.
    fn observed(collection: &str) -> ReadShards {
        ReadShards::single(home_of(collection))
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
                read_vshards: &observed("c"),
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].engine, EngineTag::Kv);
        assert_eq!(rs[0].collection, "c");
        assert_eq!(rs[0].observed_at, None);
        assert_eq!(rs[0].home, None);
        assert_eq!(rs[0].validation_vshard(), Some(home_of("c")));
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: ReadPoint::KvKey(Box::from(b"k1".as_slice())),
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
                read_vshards: &observed("c"),
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].key, ReadKey::Predicate);
    }

    #[tokio::test]
    async fn a_read_records_every_vshard_it_reported() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // The collection's own vShard validates the unhomed entry. Another
        // reported vShard validates an entry homed there.
        let other = VShardId::new((home_of("c").as_u32() + 1) % VShardId::COUNT);
        let mut versions = observed("c");
        versions.note(other);
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_batch_get("c"),
                read_vshards: &versions,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].home, None);
        assert_eq!(rs[1].home, Some(other));
        assert_eq!(rs[1].collection, "c");
    }

    #[tokio::test]
    async fn a_read_with_no_reported_vshard_records_its_collection() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                read_vshards: &ReadShards::new(),
                found: false,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].home, None);
        assert_eq!(rs[0].validation_vshard(), Some(home_of("c")));
    }

    fn snapshot_read(observed_at: Option<i64>) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "c".to_owned(),
            key: ReadKey::Predicate,
            observed_at,
            home: None,
        }
    }

    #[test]
    fn a_block_validates_at_its_snapshot() {
        assert_eq!(
            read_snapshot_ordinal(Some(40), &[snapshot_read(None)]).expect("snapshot"),
            Some(40)
        );
        // A write-only block still validates its write keys at its snapshot.
        assert_eq!(
            read_snapshot_ordinal(Some(40), &[]).expect("snapshot"),
            Some(40)
        );
        assert_eq!(read_snapshot_ordinal(None, &[]).expect("snapshot"), None);
    }

    #[test]
    fn a_pinned_read_lowers_the_snapshot_to_its_cut() {
        let reads = [snapshot_read(None), snapshot_read(Some(25))];
        assert_eq!(
            read_snapshot_ordinal(Some(40), &reads).expect("snapshot"),
            Some(25)
        );
        // Outside a block only pinned reads carry an ordinal.
        assert_eq!(
            read_snapshot_ordinal(None, &[snapshot_read(Some(25)), snapshot_read(Some(31))])
                .expect("snapshot"),
            Some(25)
        );
    }

    #[test]
    fn a_read_with_no_ordinal_outside_a_snapshot_is_refused() {
        assert!(read_snapshot_ordinal(None, &[snapshot_read(None)]).is_err());
    }

    #[tokio::test]
    async fn absent_key_point_read_is_recorded() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A "not found" is a validatable phantom observation: the KV point entry
        // is recorded (as `found = false`). KV keeps the
        // precise byte key — the identity any future insert of that key reuses.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"missing"),
                read_vshards: &observed("c"),
                found: false,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: ReadPoint::KvKey(Box::from(b"missing".as_slice())),
            }
        );
    }

    fn doc_point_get(collection: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "d".to_string(),
            surrogate: Some(nodedb_types::Surrogate::new(surrogate)),
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
                read_vshards: &observed("docs"),
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(
            rs[0].key,
            ReadKey::Point {
                repr: ReadPoint::Surrogate(42),
            }
        );
    }

    #[tokio::test]
    async fn absent_document_point_read_records_predicate() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A miss degrades to the collection-scoped predicate: the placeholder
        // surrogate will never collide with a phantom insert's fresh surrogate,
        // so the collection floor is the only safe read identity.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &doc_point_get("docs", 999),
                read_vshards: &observed("docs"),
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
                read_vshards: &observed("c"),
                found: true,
            },
        )
        .await;
        assert!(sessions.take_read_set(a).is_empty());
    }

    #[tokio::test]
    async fn a_read_of_no_collection_records_one_homed_entry_per_vshard() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A read that names no collection observed every collection on each
        // vShard it reported.
        let plan = PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::CreateSnapshot);
        let mut versions = ReadShards::single(VShardId::new(3));
        versions.note(VShardId::new(8));
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &plan,
                read_vshards: &versions,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].home, Some(VShardId::new(3)));
        assert_eq!(rs[1].home, Some(VShardId::new(8)));
        assert!(rs.iter().all(|entry| entry.collection.is_empty()));
    }

    #[test]
    fn point_get_document_uses_surrogate_identity() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "d1".to_string(),
            surrogate: Some(nodedb_types::Surrogate::new(42)),
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: Default::default(),
            valid_at_ms: None,
        });
        assert_eq!(
            read_key_of(&plan, true),
            ReadKey::Point {
                repr: ReadPoint::Surrogate(42),
            }
        );
        assert_eq!(plan_engine(&plan), EngineTag::Document);
    }

    fn indexed_fetch(collection: &str, path: &str, value: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            path: path.to_string(),
            value: value.to_string(),
            also: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            limit: 0,
            offset: 0,
            sort_keys: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
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
