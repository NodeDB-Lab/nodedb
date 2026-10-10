// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral, versioned transaction read-set capture.
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
//! Each entry carries the write version its read observed on its validation
//! vShard. A version is the data-group log position of the write that set it,
//! so every replica of the vShard reports the same version for the same state.
//! A read served by a follower validates against the leader's versions.
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
use crate::types::{DatabaseId, KeyRepr, ReadVersions, TenantId, VShardId};
use nodedb_types::WriteVersion;

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
/// against the transaction's own staged write will abort the transaction on
/// itself. That reasoning holds only for reads the SESSION issued inside the
/// transaction. It does NOT hold for a read the Control Plane performed at plan
/// time to DERIVE a value the transaction now ships, so the two kinds are
/// distinguished here rather than guessed at the filter site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOrigin {
    /// A read this transaction itself issued, through any transport, after it
    /// began. Its observation can legitimately be superseded by the
    /// transaction's own writes, so the own-write exclusion applies.
    Session,
    /// A read the Control Plane performed at plan time, whose observed value a
    /// value this transaction writes was computed from — a materialized-sum
    /// settlement's pre-image is the case that exists today.
    ///
    /// This is NOT a read-your-own-write. Its version is the COMMITTED base
    /// state at a point in time, and the derived value the transaction ships is
    /// only correct if that observation still holds at apply time. Inside a
    /// transaction the read also folds the transaction's staging overlay, but
    /// a staged write moves no committed version, so the check still compares
    /// committed base only. Dropping it because the transaction happens to
    /// write the same collection will discard the one check that catches a
    /// concurrent writer moving the base row out from under the derivation, so
    /// it survives the exclusion.
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

/// One versioned, predicate-aware read-set entry. Scoped by
/// `(database_id, tenant_id)` exactly like the write path so two tenants (or
/// databases) never alias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSetEntry {
    pub engine: EngineTag,
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    pub key: ReadKey,
    /// The version the read observed on its validation vShard. Commit
    /// validation compares it with that vShard's current version of the
    /// collection. Every replica numbers versions alike, so the read can have
    /// been served by any replica.
    pub read_version: WriteVersion,
    /// Whether this observation is the transaction's own session read or a
    /// plan-time derivation read. Required at construction — the own-write
    /// exclusion in the `TxClass` builders reads it, and an entry that guessed
    /// will be silently dropped from validation.
    pub origin: ReadOrigin,
    /// The vShard whose write versions validate this entry, or `None` for the
    /// collection's own vShard. A cross-shard graph or array read records one
    /// entry per vShard it read, each homed there. A homed entry with an empty
    /// `collection` observed every collection on its vShard.
    pub home: Option<VShardId>,
}

impl ReadSetEntry {
    /// The vShard whose versions validate this entry: its home, else its
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

/// The observed read passed to [`record_read_set`]: the executed plan, the
/// versions its responding cores reported, and whether a point read hit.
pub struct ReadCapture<'a> {
    pub plan: &'a PhysicalPlan,
    pub read_versions: &'a ReadVersions,
    pub found: bool,
}

/// Record a completed read into the session's transaction read-set.
///
/// Transport-agnostic: every read post-dispatch seam calls this with the plan
/// that ran and the versions it observed.
///
/// A read of one collection records one entry that validates on the
/// collection's vShard at the version the read observed there. A vShard the
/// read reported no version of had no write the serving core knew of, so the
/// read observed it at `WriteVersion::ZERO`. A read that reported other
/// vShards too records one more entry per other reported vShard, homed
/// there.
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
        read_versions,
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

    let entry = |home: Option<VShardId>, observed: WriteVersion, validation: VShardId| {
        // Read-your-writes floor: raise the observed version to the
        // session's own highest committed write of this collection on the
        // validation vShard. A read that ran before the serving replica
        // applied the session's own prior write will otherwise abort on that
        // write. Only this session's own writes raise the floor, so a
        // concurrent write by another session still exceeds it and aborts.
        let own = if collection.is_empty() {
            WriteVersion::ZERO
        } else {
            sessions.own_write_version(session_id, database_id, tenant_id, &collection, validation)
        };
        ReadSetEntry {
            engine,
            database_id,
            tenant_id,
            collection: collection.clone(),
            key: key.clone(),
            read_version: observed.max(own),
            // Every entry captured here is a read the session issued inside its
            // own transaction, so the own-write exclusion applies to it.
            origin: ReadOrigin::Session,
            home,
        }
    };
    let entries: Vec<ReadSetEntry> =
        match nodedb_types::CollectionKey::from_qualified_str(database_id, &collection) {
            Ok(collection_key) if !collection.is_empty() => {
                let validation = collection_key.vshard();
                let observed = read_versions.of(validation).unwrap_or_default();
                let mut entries = vec![entry(None, observed, validation)];
                entries.extend(
                    read_versions
                        .iter()
                        .map(|shard| VShardId::new(shard.vshard))
                        .filter(|vshard| *vshard != validation)
                        .map(|vshard| {
                            let observed = read_versions.of(vshard).unwrap_or_default();
                            entry(Some(vshard), observed, vshard)
                        }),
                );
                entries
            }
            _ => read_versions
                .iter()
                .map(|shard| {
                    let vshard = VShardId::new(shard.vshard);
                    entry(Some(vshard), shard.version, vshard)
                })
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
        sessions.begin(session_id, 0).expect("begin");
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

    /// `collection`'s vShard observed at log index `index`.
    fn observed(collection: &str, index: u64) -> ReadVersions {
        ReadVersions::single(home_of(collection), WriteVersion::logged(1, index))
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
                read_versions: &observed("c", 7),
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].engine, EngineTag::Kv);
        assert_eq!(rs[0].collection, "c");
        assert_eq!(rs[0].read_version, WriteVersion::logged(1, 7));
        assert_eq!(rs[0].home, None);
        assert_eq!(rs[0].validation_vshard(), Some(home_of("c")));
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
                read_versions: &observed("c", 9),
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
        let other = VShardId::new(home_of("c").as_u32() + 1);
        let mut versions = observed("c", 11);
        versions.note(other, WriteVersion::logged(1, 90));
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_batch_get("c"),
                read_versions: &versions,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].home, None);
        assert_eq!(rs[0].read_version, WriteVersion::logged(1, 11));
        assert_eq!(rs[1].home, Some(other));
        assert_eq!(rs[1].read_version, WriteVersion::logged(1, 90));
        assert_eq!(rs[1].collection, "c");
    }

    #[tokio::test]
    async fn a_read_with_no_reported_version_observed_zero() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // The serving core knew no write of the vShard, so any write since
        // the read moves it above the recorded version.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                read_versions: &ReadVersions::new(),
                found: false,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].read_version, WriteVersion::ZERO);
    }

    #[tokio::test]
    async fn the_sessions_own_write_floors_the_read_version() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        sessions.note_own_write(
            a,
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "c",
            &observed("c", 20),
        );
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"k1"),
                read_versions: &observed("c", 12),
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs[0].read_version, WriteVersion::logged(1, 20));
    }

    #[tokio::test]
    async fn absent_key_point_read_is_recorded() {
        let (state, _dir) = test_state();
        let (sessions, a) = begun_session();
        // A "not found" is a validatable phantom observation: the KV point entry
        // is recorded (as `found = false`) at the observed version. KV keeps the
        // precise byte key — the identity any future insert of that key reuses.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &kv_get("c", b"missing"),
                read_versions: &observed("c", 5),
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
                read_versions: &observed("docs", 7),
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
        // surrogate will never collide with a phantom insert's fresh surrogate,
        // so the collection floor is the only safe read identity.
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &doc_point_get("docs", 999),
                read_versions: &observed("docs", 5),
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
                read_versions: &observed("c", 7),
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
        let plan = PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::HomeVersions {
            probes: Vec::new(),
        });
        let mut versions = ReadVersions::single(VShardId::new(3), WriteVersion::logged(1, 4));
        versions.note(VShardId::new(8), WriteVersion::logged(2, 6));
        record_read_set(
            &state,
            &sessions,
            a,
            TenantId::new(1),
            ReadCapture {
                plan: &plan,
                read_versions: &versions,
                found: true,
            },
        )
        .await;
        let rs = sessions.take_read_set(a);
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].home, Some(VShardId::new(3)));
        assert_eq!(rs[0].read_version, WriteVersion::logged(1, 4));
        assert_eq!(rs[1].home, Some(VShardId::new(8)));
        assert_eq!(rs[1].read_version, WriteVersion::logged(2, 6));
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
