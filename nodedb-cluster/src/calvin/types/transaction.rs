// SPDX-License-Identifier: BUSL-1.1

//! Calvin transaction class types.
//!
//! Provides [`TxClass`] — the core transaction representation submitted to
//! the sequencer — over the [`ReadWriteSet`] key sets.

use nodedb_types::TenantId;
use nodedb_types::id::{DatabaseId, VShardId};
use serde::{Deserialize, Serialize};

use crate::error::CalvinError;

use super::lock_wire::TxnIdWire;
use super::primitives::{DependentReadSpec, VersionedReadSet};
use super::read_write_set::ReadWriteSet;

// ── TxClass ───────────────────────────────────────────────────────────────────

/// A fully-declared Calvin transaction class.
///
/// Constructed via [`TxClass::new`], which validates the write set and caches
/// the participating-vshard set. The `participating_vshards` field is skipped
/// during serialization and re-derived on decode to keep serialized bytes
/// byte-deterministic.
///
/// Map-encoded (`#[msgpack(map)]`) so fields can be added additively: an older
/// serialized `TxClass` that predates a field decodes it to its default (the
/// field carries `#[serde(default)]` + `#[msgpack(default)]`). This is what
/// lets `TxClass` bytes already on the sequencer Raft log survive a schema
/// addition and still replay on restart.
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
pub struct TxClass {
    /// Keys that must be read (may be empty for pure-write transactions).
    ///
    /// This is the key-IDENTITY set used for locking/routing. The
    /// LSN-versioned read observations used for optimistic-concurrency
    /// validation live in `versioned_reads`.
    pub read_set: ReadWriteSet,
    /// Keys that will be written. Must span at least two vShards.
    pub write_set: ReadWriteSet,
    /// Opaque msgpack-encoded physical plan bytes. Decoded by the executor
    /// in the `nodedb` crate; the sequencer treats this as an opaque blob.
    pub plans: Vec<u8>,
    /// Tenant scope. All keys in `read_set` and `write_set` must belong to
    /// this tenant; cross-tenant transactions are rejected at construction.
    pub tenant_id: TenantId,
    /// Database scope used for collection homing, execution, WAL, and CDC.
    #[serde(default)]
    #[msgpack(default)]
    pub database_id: DatabaseId,
    /// Optional dependent-read specification.
    ///
    /// When present, this transaction is a dependent-read Calvin txn: the
    /// passive vshards listed here must read their keys and broadcast the
    /// results (via `ReplicatedWrite::CalvinReadResult`) before the active
    /// participants may write.
    ///
    /// `None` for static-set transactions (the common case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub dependent_reads: Option<DependentReadSpec>,
    /// LSN-versioned, predicate-aware read-set captured during the session.
    ///
    /// Each entry carries the responding shard's write-LSN watermark at read
    /// time plus the point/predicate identity, so a participant can validate
    /// the read at the commit serialization point (the local commit vote on
    /// a stage response). Empty for pure-write and autocommit transactions.
    #[serde(default)]
    #[msgpack(default)]
    pub versioned_reads: VersionedReadSet,
    /// Optional lock-table owner id for this transaction, propagated to
    /// `SequencedTxn.lock_owner`. `Some(R)` when the committing session holds
    /// read reservations under `R` — the commit batch then acquires its keys as
    /// `R` and self-upgrades those shared reservations. `None` (default) for
    /// transactions with no reservation. Wire-additive: decodes to `None` on
    /// older log entries.
    #[serde(default)]
    #[msgpack(default)]
    pub lock_owner: Option<TxnIdWire>,
    /// The metadata-group index the coordinator had applied when it planned
    /// the transaction. Every replica's scheduler runs the transaction only
    /// once its own metadata apply reached this index, so it never writes a
    /// collection this node has not registered yet. `0` holds nothing.
    #[serde(default)]
    #[msgpack(default)]
    pub metadata_floor: u64,
    /// WAL event-source code every participant stamps on the transaction's
    /// writes, so a server-run body's writes fire no triggers. `0` is the
    /// client default.
    #[serde(default)]
    #[msgpack(default)]
    pub event_source: u8,
    /// Indexes into `plans` of the plans a trigger body buffered. Every
    /// participant commits their rows under the trigger source, so they fire
    /// no trigger. Empty for a transaction no body joined.
    #[serde(default)]
    #[msgpack(default)]
    pub body_plans: Vec<u32>,
    /// Opaque msgpack-encoded `PUBLISH TO` messages the transaction's trigger
    /// bodies sent, decoded by the `nodedb` crate. The participant named by
    /// [`TxClass::publish_vshard`] commits them in its redo record. Empty for
    /// a transaction that published nothing.
    #[serde(default)]
    #[msgpack(default)]
    pub publishes: Vec<u8>,
    /// Opaque msgpack-encoded dedup key of the cross-shard trigger request
    /// the transaction applies, decoded by the `nodedb` crate. The participant
    /// named by [`TxClass::applied_key_home`] writes it into its redo record,
    /// so the key is durable exactly when the request's writes are. Empty
    /// for every other transaction.
    #[serde(default)]
    #[msgpack(default)]
    pub applied_key: Vec<u8>,
    /// The vShard the cross-shard request addresses.
    #[serde(default)]
    #[msgpack(default)]
    pub applied_key_vshard: u32,
    /// Every user collection the plans name, with the incarnation the
    /// coordinator's catalog held when it planned the transaction. A
    /// participant whose catalog no longer holds one votes abort, so the
    /// transaction never lands in a same-name recreate.
    #[serde(default)]
    #[msgpack(default)]
    pub incarnations: Vec<CalvinIncarnation>,
    /// The plan manifest when the plans travel as parts (see
    /// [`super::multi_part`]). `plans` is empty then. `None` for a
    /// transaction whose plans ride its own sequencer entry.
    #[serde(default)]
    #[msgpack(default)]
    pub multi_part: Option<super::multi_part::MultiPartPlans>,
    /// The RESTORE that re-issues the transaction's writes. Every participant
    /// raises the tenant's restore mark under it instead of the user write
    /// mark, so a retry of the same RESTORE finds only its own marks. `0` for
    /// every other transaction.
    #[serde(default)]
    #[msgpack(default)]
    pub restore_id: u64,
    /// Cached participating-vshard set. Re-derived on decode; not serialized.
    #[serde(skip)]
    #[msgpack(ignore)]
    participating_vshards: Vec<VShardId>,
}

impl TxClass {
    /// Construct a validated **multi-vshard** transaction class.
    ///
    /// Rejects:
    /// - An empty write set (nothing to commit).
    /// - A write set that resolves to a single vshard — a `>=2`-intended
    ///   construction that collapses to one participant is a routing bug, so
    ///   it is rejected here. A transaction that is *legitimately* single-vshard
    ///   (a contended point write that must sequence to join the shared
    ///   per-vShard lock domain) must opt in explicitly via
    ///   [`TxClass::new_single_vshard`].
    ///
    /// Pass `dependent_reads: None` for static-set transactions (the common
    /// case).  Pass `Some(spec)` for dependent-read (OLLP) transactions.
    ///
    /// `versioned_reads` carries the LSN-versioned read observations; pass
    /// [`VersionedReadSet::default`] (empty) for pure-write / autocommit
    /// transactions that accumulated no session read-set.
    pub fn new(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new_in_database(
            read_set,
            write_set,
            plans,
            tenant_id,
            DatabaseId::DEFAULT,
            dependent_reads,
            versioned_reads,
        )
    }

    /// Construct a validated multi-vshard class in an explicit database.
    pub fn new_in_database(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::unchecked(
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
        )
        .checked(false)
    }

    /// Construct a validated transaction class that is permitted to resolve to a
    /// **single vshard**.
    ///
    /// This is the explicit opt-in for a contended single-vshard point write:
    /// the write-admission gate returned `RouteToCalvin` because a pending commit
    /// already holds the write's key, so the write must be sequenced through the
    /// deterministic scheduler to serialize on the SAME shared per-vShard
    /// `LockManager` the scheduler uses for multi-vshard transactions. Everything
    /// downstream of construction (sequencer inbox
    /// fan-out bound, per-vshard scheduler acquire/dispatch/commit, staged 2-phase
    /// commit) already tolerates a single participant — the `< 2` reject on
    /// [`TxClass::new`] was the only structural block.
    ///
    /// An empty write set is still rejected (nothing to commit), and a write set
    /// that resolves to *zero* participating vshards is rejected as unroutable.
    /// Signature mirrors [`TxClass::new`].
    pub fn new_single_vshard(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new_single_vshard_in_database(
            read_set,
            write_set,
            plans,
            tenant_id,
            DatabaseId::DEFAULT,
            dependent_reads,
            versioned_reads,
        )
    }

    /// Construct a single-vshard class in an explicit database.
    pub fn new_single_vshard_in_database(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::unchecked(
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
        )
        .checked(true)
    }

    /// The class the constructors validate, with no participants derived yet.
    fn unchecked(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Self {
        Self {
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
            lock_owner: None,
            metadata_floor: 0,
            event_source: 0,
            body_plans: Vec::new(),
            publishes: Vec::new(),
            applied_key: Vec::new(),
            applied_key_vshard: 0,
            incarnations: Vec::new(),
            multi_part: None,
            restore_id: 0,
            participating_vshards: Vec::new(),
        }
    }

    /// Shared construction body. `allow_single_vshard` relaxes the participant
    /// floor from 2 (multi-vshard) to 1 (single-vshard opt-in); an empty write
    /// set and a zero-participant write set are rejected on both paths.
    fn checked(mut self, allow_single_vshard: bool) -> Result<Self, CalvinError> {
        if self.write_set.is_empty() {
            return Err(CalvinError::EmptyWriteSet);
        }
        let write_vshards = self
            .write_set
            .participating_vshards_in_database(self.database_id)?;
        let min_participants = if allow_single_vshard { 1 } else { 2 };
        // The participant FLOOR is computed from the WRITE set ONLY, and BEFORE
        // the read-set union: a txn that writes a single shard but reads N
        // additional shards is a legitimate single-write-shard txn and must not
        // trip the `>= 2` floor.
        if write_vshards.len() < min_participants {
            let vshard = write_vshards.first().map(|v| v.as_u32()).unwrap_or(0);
            return Err(CalvinError::SingleVshardTxn { vshard });
        }
        self.participating_vshards = self.union_participants(write_vshards)?;
        Ok(self)
    }

    /// `write_vshards` joined with the read set's vShards and the passive
    /// dependent-read vShards, sorted by id and deduplicated.
    ///
    /// A shard that is only READ still participates, so it can validate the
    /// read at the commit serialization point. `participating_vshards` is
    /// `#[serde(skip)]` and re-derived on decode, so construction and
    /// [`Self::restore_derived`] both derive it here. No caller reads the
    /// unsorted order. The sorted order is stable across encode and decode.
    fn union_participants(
        &self,
        mut participants: Vec<VShardId>,
    ) -> Result<Vec<VShardId>, CalvinError> {
        participants.extend(
            self.read_set
                .participating_vshards_in_database(self.database_id)?,
        );
        if let Some(spec) = &self.dependent_reads {
            participants.extend(spec.passive_reads.keys().map(|&v| VShardId::new(v)));
        }
        participants.sort_unstable_by_key(|v| v.as_u32());
        participants.dedup_by_key(|v| v.as_u32());
        Ok(participants)
    }

    /// Ergonomic constructor for dependent-read Calvin transactions.
    ///
    /// Equivalent to `TxClass::new(read_set, write_set, plans, tenant_id,
    /// Some(dependent_reads), versioned_reads)`.
    pub fn new_dependent(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: DependentReadSpec,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new(
            read_set,
            write_set,
            plans,
            tenant_id,
            Some(dependent_reads),
            versioned_reads,
        )
    }

    /// The vShards that must receive this transaction's slice.
    ///
    /// Derived from the write set's collection names. Re-derived after
    /// deserialization via [`TxClass::restore_derived`].
    pub fn participating_vshards(&self) -> &[VShardId] {
        &self.participating_vshards
    }

    /// Re-derive fields skipped during serialization.
    ///
    /// Call this immediately after deserializing a `TxClass` that came off
    /// the wire or out of the Raft log. Fails when a key-set collection name
    /// lacks the qualifier of `database_id`. Construction rejects such a
    /// class, so a failure here means the decoded bytes are not a class any
    /// constructor built.
    pub fn restore_derived(&mut self) -> Result<(), CalvinError> {
        let write_vshards = self
            .write_set
            .participating_vshards_in_database(self.database_id)?;
        self.participating_vshards = self.union_participants(write_vshards)?;
        Ok(())
    }

    /// Set the lock-table owner id propagated to `SequencedTxn.lock_owner`.
    pub fn set_lock_owner(&mut self, owner: Option<TxnIdWire>) {
        self.lock_owner = owner;
    }

    /// Set the WAL event-source code the transaction's writes carry.
    pub fn set_event_source(&mut self, code: u8) {
        self.event_source = code;
    }

    /// Mark the transaction as the re-issue of RESTORE `restore_id`.
    pub fn set_restore_id(&mut self, restore_id: u64) {
        self.restore_id = restore_id;
    }

    /// Set the indexes into `plans` of the plans a trigger body buffered.
    pub fn set_body_plans(&mut self, body_plans: Vec<u32>) {
        self.body_plans = body_plans;
    }

    /// Set the encoded messages the transaction's trigger bodies published.
    pub fn set_publishes(&mut self, publishes: Vec<u8>) {
        self.publishes = publishes;
    }

    /// The participant whose redo record carries the transaction's messages:
    /// the lowest vShard the transaction writes. Every write participant
    /// resolves a redo record, so this one always commits them. `None` when
    /// the write set names no vShard.
    ///
    /// Fails when a write-set collection name lacks the qualifier of the
    /// class's database. Construction rejects such a class.
    pub fn publish_vshard(&self) -> Result<Option<u32>, CalvinError> {
        Ok(self.write_vshards()?.into_iter().min())
    }

    /// Set the encoded dedup key of the cross-shard request the transaction
    /// applies, and the vShard the request addresses.
    pub fn set_applied_key(&mut self, applied_key: Vec<u8>, vshard: u32) {
        self.applied_key = applied_key;
        self.applied_key_vshard = vshard;
    }

    /// The participant whose redo record carries the applied key: the vShard
    /// the request addresses when the transaction writes it, else the lowest
    /// vShard it writes. `None` for a transaction with no applied key.
    ///
    /// Fails as [`Self::publish_vshard`] does.
    pub fn applied_key_home(&self) -> Result<Option<u32>, CalvinError> {
        if self.applied_key.is_empty() {
            return Ok(None);
        }
        let writes = self.write_vshards()?;
        if writes.contains(&self.applied_key_vshard) {
            return Ok(Some(self.applied_key_vshard));
        }
        Ok(writes.into_iter().min())
    }

    /// Every vShard of the class's database the write set names.
    fn write_vshards(&self) -> Result<Vec<u32>, CalvinError> {
        Ok(self
            .write_set
            .participating_vshards_in_database(self.database_id)?
            .into_iter()
            .map(|vshard| vshard.as_u32())
            .collect())
    }

    /// Set the collections the plans name, with their planned incarnations.
    pub fn set_incarnations(&mut self, incarnations: Vec<CalvinIncarnation>) {
        self.incarnations = incarnations;
    }
}

/// A user collection a Calvin transaction names, and the incarnation its
/// coordinator planned against.
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
pub struct CalvinIncarnation {
    /// The collection as the plans name it: database-qualified outside the
    /// default database.
    pub collection: String,
    pub incarnation: nodedb_types::Hlc,
}

#[cfg(test)]
mod tests {
    use super::super::primitives::{
        EngineKeySet, EngineTag, PassiveReadKey, ReadKeyIdent, SortedVec, VersionedReadEntry,
        VersionedReadSet,
    };
    use super::*;
    use nodedb_types::id::CollectionKey;
    use nodedb_types::{KeyRepr, Lsn};

    fn doc_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Document {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn vec_set(collection: &str, surrogates: Vec<u32>) -> EngineKeySet {
        EngineKeySet::Vector {
            collection: collection.to_owned(),
            surrogates: SortedVec::new(surrogates),
        }
    }

    fn multi_vshard_write_set() -> ReadWriteSet {
        // Use two different collections that hash to different vShards.
        // We can't pick known-distinct names without running the hash, so we
        // scan at test time.
        let (a, b) = find_two_distinct_collections();
        ReadWriteSet::new(vec![doc_set(&a, vec![1, 2]), doc_set(&b, vec![3])])
    }

    /// Find two collection names whose vShards differ.
    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_tx_class(write_set: ReadWriteSet) -> TxClass {
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![0x01, 0x02],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    // ── ReadWriteSet ──────────────────────────────────────────────────────────

    #[test]
    fn read_write_set_participating_vshards_distinct() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards().expect("participants");
        assert!(vshards.len() >= 2, "expected at least 2 distinct vShards");
    }

    #[test]
    fn read_write_set_participating_vshards_sorted() {
        let ws = multi_vshard_write_set();
        let vshards = ws.participating_vshards().expect("participants");
        let ids: Vec<u32> = vshards.iter().map(|v| v.as_u32()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn read_write_set_same_collection_counted_once() {
        // Two EngineKeySets for the same collection: still one vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1]), vec_set("users", vec![1])]);
        let vshards = ws.participating_vshards().expect("participants");
        assert_eq!(vshards.len(), 1);
    }

    // ── TxClass construction ──────────────────────────────────────────────────

    #[test]
    fn tx_class_new_rejects_empty_write_set() {
        use crate::error::CalvinError;
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![]),
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::EmptyWriteSet));
    }

    #[test]
    fn tx_class_new_rejects_single_vshard() {
        use crate::error::CalvinError;
        // Single collection → single vshard.
        let ws = ReadWriteSet::new(vec![doc_set("users", vec![1, 2])]);
        let err = TxClass::new(
            ReadWriteSet::new(vec![]),
            ws,
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::SingleVshardTxn { .. }));
    }

    #[test]
    fn tx_class_new_accepts_multi_vshard() {
        let tc = make_tx_class(multi_vshard_write_set());
        assert!(tc.participating_vshards().len() >= 2);
    }

    #[test]
    fn tx_class_participating_vshards_cached() {
        let tc = make_tx_class(multi_vshard_write_set());
        // Two calls return the same slice.
        assert_eq!(tc.participating_vshards(), tc.participating_vshards());
    }

    // ── Byte-determinism ──────────────────────────────────────────────────────

    /// Byte-determinism: two TxClass values with logically identical sets
    /// (different insertion order) must produce byte-identical JSON.
    ///
    /// `participating_vshards` is `#[serde(skip)]` so it is excluded from
    /// serialization; only the stable sorted fields participate.
    #[test]
    fn tx_class_byte_deterministic_across_insertion_order() {
        let (col_a, col_b) = find_two_distinct_collections();

        let ws_forward = ReadWriteSet::new(vec![
            doc_set(&col_a, vec![3, 1, 2]),
            doc_set(&col_b, vec![10, 5]),
        ]);
        let ws_backward = ReadWriteSet::new(vec![
            doc_set(&col_b, vec![5, 10]),
            doc_set(&col_a, vec![2, 3, 1]),
        ]);

        // Both write sets have the same logical content but different
        // ordering.  We compare the serialized *inner sets* after sorting
        // the outer Vec by collection name so key-set order doesn't matter.
        let forward_bytes = sonic_rs::to_vec(&ws_forward).unwrap();
        let backward_bytes = sonic_rs::to_vec(&ws_backward).unwrap();

        // The outer Vec order may differ; compare after canonical-sort.
        let mut fw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&forward_bytes).unwrap();
        let mut bw_parsed: Vec<serde_json::Value> = sonic_rs::from_slice(&backward_bytes).unwrap();

        let sort_key = |v: &serde_json::Value| -> String {
            v.as_object()
                .and_then(|o| o.values().next())
                .and_then(|inner| inner.get("collection"))
                .and_then(|c| c.as_str())
                .unwrap_or("")
                .to_owned()
        };
        fw_parsed.sort_by_key(sort_key);
        bw_parsed.sort_by_key(sort_key);
        assert_eq!(fw_parsed, bw_parsed);
    }

    /// Byte-determinism for the full TxClass: serialize → deserialize →
    /// restore_derived → serialize again; both bytes must be identical.
    #[test]
    fn tx_class_roundtrip_bytes_stable() {
        let tc = make_tx_class(multi_vshard_write_set());
        let first = sonic_rs::to_vec(&tc).unwrap();

        let mut restored: TxClass = sonic_rs::from_slice(&first).unwrap();
        restored.restore_derived().expect("restore derived");

        let second = sonic_rs::to_vec(&restored).unwrap();
        assert_eq!(first, second);
    }

    // ── MessagePack roundtrips ────────────────────────────────────────────────

    #[test]
    fn tx_class_msgpack_roundtrip() {
        let tc = make_tx_class(multi_vshard_write_set());
        let bytes = zerompk::to_msgpack_vec(&tc).unwrap();
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).unwrap();
        decoded.restore_derived().expect("restore derived");
        assert_eq!(tc.tenant_id, decoded.tenant_id);
        assert_eq!(tc.plans, decoded.plans);
        assert_eq!(tc.write_set, decoded.write_set);
        assert_eq!(tc.read_set, decoded.read_set);
        assert_eq!(tc.participating_vshards(), decoded.participating_vshards());
    }

    #[test]
    fn tx_class_with_dependent_reads_participating_vshards_includes_passives() {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![doc_set(&col_a, vec![1]), doc_set(&col_b, vec![2])]);

        // Pick a vshard id that's different from col_a and col_b.
        let passive_vshard_id = {
            let a = CollectionKey::from_bare(DatabaseId::DEFAULT, &col_a)
                .vshard()
                .as_u32();
            let b = CollectionKey::from_bare(DatabaseId::DEFAULT, &col_b)
                .vshard()
                .as_u32();
            // Find one that differs from both.
            let mut candidate = 9999u32;
            for i in 0u32..64 {
                let name = format!("passive_col_{i}");
                let v = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                    .vshard()
                    .as_u32();
                if v != a && v != b {
                    candidate = v;
                    break;
                }
            }
            candidate
        };

        let spec = DependentReadSpec {
            passive_reads: {
                let mut m = std::collections::BTreeMap::new();
                m.insert(
                    passive_vshard_id,
                    vec![PassiveReadKey {
                        engine_key: doc_set("passive_col", vec![99]),
                    }],
                );
                m
            },
        };

        let tc = TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(1),
            Some(spec),
            VersionedReadSet::default(),
        )
        .expect("valid TxClass with dependent reads");

        // The participating vshards must include the passive vshard.
        let vshard_ids: Vec<u32> = tc
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        assert!(
            vshard_ids.contains(&passive_vshard_id),
            "participating_vshards must include passive vshard {passive_vshard_id}; got {vshard_ids:?}"
        );
    }

    fn sample_versioned_reads() -> VersionedReadSet {
        VersionedReadSet::new(vec![
            VersionedReadEntry {
                engine: EngineTag::Kv,
                collection: "kv_col".to_owned(),
                key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(&b"k1"[..]))),
                read_lsn: Lsn::new(7),
                home_vshard: None,
                served_by: 0,
            },
            VersionedReadEntry {
                engine: EngineTag::Document,
                collection: "doc_col".to_owned(),
                key: ReadKeyIdent::Predicate,
                read_lsn: Lsn::new(11),
                home_vshard: None,
                served_by: 0,
            },
        ])
    }

    fn two_home_write_set() -> ReadWriteSet {
        let (_src, _dst, sv, dv) = two_distinct_key_vshards();
        ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![sv, dv]),
        }])
    }

    #[test]
    fn versioned_reads_survive_msgpack_roundtrip() {
        let reads = sample_versioned_reads();
        let tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x09, 0x09],
            TenantId::new(1),
            None,
            reads.clone(),
        )
        .expect("valid TxClass");

        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode TxClass");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode TxClass");
        decoded.restore_derived().expect("restore derived");

        // Every read_lsn and the Point/Predicate distinction survive exactly.
        assert_eq!(decoded.versioned_reads, reads);
        assert_eq!(decoded.versioned_reads.len(), 2);
        let point = decoded
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Point(_)))
            .expect("point entry");
        assert_eq!(point.read_lsn, Lsn::new(7));
        assert_eq!(
            point.key,
            ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(&b"k1"[..])))
        );
        let predicate = decoded
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Predicate))
            .expect("predicate entry");
        assert_eq!(predicate.read_lsn, Lsn::new(11));
    }

    /// Mirror of `TxClass`'s wire shape from BEFORE `versioned_reads` existed:
    /// map-encoded with the original fields only. Proves an old serialized
    /// `TxClass` (no `versioned_reads` key) still decodes — the field defaults
    /// to empty — so Raft-logged transactions survive the schema addition.
    #[test]
    fn database_scope_survives_msgpack_roundtrip() {
        let tx = TxClass::new_in_database(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(1),
            DatabaseId::new(9),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived().expect("restore derived");
        assert_eq!(decoded.database_id, DatabaseId::new(9));
        assert_eq!(decoded.participating_vshards(), tx.participating_vshards());
    }

    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct LegacyTxClass {
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
    }

    #[test]
    fn decodes_legacy_bytes_without_versioned_reads_field() {
        let legacy = LegacyTxClass {
            read_set: ReadWriteSet::new(vec![]),
            write_set: two_home_write_set(),
            plans: vec![0x01, 0x02],
            tenant_id: TenantId::new(3),
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).expect("encode legacy");

        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode legacy as TxClass");
        decoded.restore_derived().expect("restore derived");

        assert!(decoded.versioned_reads.is_empty());
        assert!(decoded.dependent_reads.is_none());
        assert_eq!(decoded.tenant_id, TenantId::new(3));
        assert_eq!(decoded.database_id, DatabaseId::DEFAULT);
        assert_eq!(decoded.plans, vec![0x01, 0x02]);
        assert_eq!(decoded.participating_vshards().len(), 2);
        assert_eq!(decoded.metadata_floor, 0, "a legacy class holds nothing");
        assert_eq!(decoded.event_source, 0, "a legacy class is a client's");
        assert!(decoded.body_plans.is_empty());
        assert!(decoded.publishes.is_empty());
        assert_eq!(decoded.restore_id, 0, "a legacy class re-issues no RESTORE");
    }

    /// The event-source code survives the codec, so every participant stamps
    /// the same source on the transaction's writes.
    #[test]
    fn event_source_roundtrips_through_the_codec() {
        let mut tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(3),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        assert_eq!(tx.event_source, 0);
        tx.set_event_source(2);
        tx.set_body_plans(vec![1]);
        tx.set_restore_id(77);
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.event_source, 2);
        assert_eq!(decoded.body_plans, vec![1]);
        assert_eq!(
            decoded.restore_id, 77,
            "every participant raises the restore's mark"
        );
    }

    /// The published messages survive the codec, and one participant, the
    /// lowest write vShard, carries them.
    #[test]
    fn publishes_roundtrip_and_name_the_lowest_write_vshard() {
        let mut tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(3),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        assert!(tx.publishes.is_empty());
        tx.set_publishes(vec![0x91, 0xa1, 0x61]);
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived().expect("restore derived");
        assert_eq!(decoded.publishes, vec![0x91, 0xa1, 0x61]);
        let lowest = decoded
            .participating_vshards()
            .iter()
            .map(|vshard| vshard.as_u32())
            .min();
        assert!(lowest.is_some());
        assert_eq!(decoded.publish_vshard(), Ok(lowest));
    }

    /// A class whose write set lacks its database's qualifier cannot name a
    /// write vShard. Both homes report the error instead of `None`.
    #[test]
    fn redo_homes_report_an_underivable_write_set() {
        let mut tx = make_tx_class(multi_vshard_write_set());
        tx.set_applied_key(vec![0x80], 0);
        // The bare document collection names carry no `9/` qualifier.
        tx.database_id = DatabaseId::new(9);
        assert!(tx.publish_vshard().is_err());
        assert!(tx.applied_key_home().is_err());
    }

    /// The applied key rides the vShard its request addresses when the
    /// transaction writes it, else the lowest write vShard.
    #[test]
    fn applied_key_home_prefers_the_addressed_vshard() {
        let mut tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(3),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        assert_eq!(tx.applied_key_home(), Ok(None), "no key, no home");
        let mut homes: Vec<u32> = tx
            .participating_vshards()
            .iter()
            .map(|vshard| vshard.as_u32())
            .collect();
        homes.sort_unstable();
        let (lowest, highest) = (homes[0], homes[homes.len() - 1]);
        tx.set_applied_key(vec![0x80], highest);
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived().expect("restore derived");
        assert_eq!(decoded.applied_key, vec![0x80]);
        assert_eq!(decoded.applied_key_home(), Ok(Some(highest)));
        let unwritten = (0..u32::MAX)
            .find(|vshard| !homes.contains(vshard))
            .unwrap_or(u32::MAX);
        decoded.set_applied_key(vec![0x80], unwritten);
        assert_eq!(decoded.applied_key_home(), Ok(Some(lowest)));
    }

    /// The metadata floor survives the codec, so every replica waits for the
    /// catalog the coordinator planned against.
    #[test]
    fn metadata_floor_roundtrips_through_the_codec() {
        let mut tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(3),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        tx.metadata_floor = 42;
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded.metadata_floor, 42);
    }

    /// Find two distinct string keys whose `from_key` vShards differ.
    fn two_distinct_key_vshards() -> (String, String, u32, u32) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let key = format!("node_{i}");
            let v = VShardId::from_key(key.as_bytes()).as_u32();
            if let Some((ref fkey, fv)) = first {
                if fv != v {
                    return (fkey.clone(), key, fv, v);
                }
            } else {
                first = Some((key, v));
            }
        }
        panic!("could not find two distinct-vshard keys in 2048 tries");
    }

    #[test]
    fn edge_keyset_participating_vshards_are_key_homed() {
        // An edge whose endpoints hash to two DISTINCT from_key vShards must
        // contribute exactly those two homes — NOT the collection's vShard.
        let (src_key, dst_key, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        // Pick a collection name whose collection-homed vShard differs from
        // both endpoint homes, to prove routing ignores the collection.
        let coll_v = CollectionKey::from_bare(DatabaseId::DEFAULT, "follows")
            .vshard()
            .as_u32();

        let ws = ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![src_v, dst_v]),
        }]);

        let mut got: Vec<u32> = ws
            .participating_vshards()
            .expect("participants derive")
            .iter()
            .map(|v| v.as_u32())
            .collect();
        got.sort();
        let mut want = vec![src_v, dst_v];
        want.sort();
        assert_eq!(got, want, "edge routes to its from_key homes");
        assert!(
            !got.contains(&coll_v) || coll_v == src_v || coll_v == dst_v,
            "edge must NOT route by collection vShard {coll_v}"
        );

        // Sanity: the keys we hashed actually produce these homes.
        assert_eq!(VShardId::from_key(src_key.as_bytes()).as_u32(), src_v);
        assert_eq!(VShardId::from_key(dst_key.as_bytes()).as_u32(), dst_v);
    }

    #[test]
    fn new_single_vshard_accepts_one_participant_write_set() {
        // A single Document collection resolves to exactly one vshard. `new`
        // rejects it; `new_single_vshard` accepts it and caches the one home.
        let ws = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "users".to_owned(),
            surrogates: SortedVec::new(vec![7u32]),
        }]);
        let want_vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
            .vshard()
            .as_u32();

        // Strict path still rejects.
        let strict = TxClass::new(
            ReadWriteSet::new(vec![]),
            ws.clone(),
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        );
        assert!(matches!(strict, Err(CalvinError::SingleVshardTxn { .. })));

        // Opt-in path accepts and produces a single participating vshard.
        let tx = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ws,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("single-vshard TxClass accepted");
        assert_eq!(tx.participating_vshards().len(), 1);
        assert_eq!(tx.participating_vshards()[0].as_u32(), want_vshard);
    }

    #[test]
    fn new_single_vshard_still_rejects_empty_write_set() {
        let err = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![]),
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::EmptyWriteSet));
    }

    /// Find two collection names whose default-database vShards differ.
    fn two_distinct_vshard_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let name = format!("coll_{i}");
            let v = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != v {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, v));
            }
        }
        panic!("could not find two distinct-vshard collections in 2048 tries");
    }

    #[test]
    fn read_set_vshards_union_into_participants_and_survive_roundtrip() {
        // A single-write-shard txn that READS a second collection homed on a
        // different vShard: the read shard joins the participant set (the
        // write-only floor still passes via the single-vshard opt-in), and the
        // union is reproduced identically on decode.
        let (wcoll, rcoll) = two_distinct_vshard_collections();
        let wv = CollectionKey::from_bare(DatabaseId::DEFAULT, &wcoll)
            .vshard()
            .as_u32();
        let rv = CollectionKey::from_bare(DatabaseId::DEFAULT, &rcoll)
            .vshard()
            .as_u32();
        assert_ne!(wv, rv);

        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: wcoll,
            surrogates: SortedVec::new(vec![1]),
        }]);
        // Read-set keyset carries no identity (empty surrogates) — homed by
        // collection, exactly as the builders' `read_set_from` constructs it.
        let read_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: rcoll,
            surrogates: SortedVec::new(vec![]),
        }]);

        let tx = TxClass::new_single_vshard(
            read_set,
            write_set,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("single-write-shard txn with a cross-shard read is valid");

        let mut participants: Vec<u32> = tx
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        participants.sort_unstable();
        let mut want = vec![wv, rv];
        want.sort_unstable();
        assert_eq!(
            participants, want,
            "read shard must union into participants"
        );

        // Encode → decode → restore_derived reproduces the identical participant
        // set (participants are `#[serde(skip)]`, re-derived in lockstep).
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived().expect("restore derived");
        assert_eq!(
            tx.participating_vshards(),
            decoded.participating_vshards(),
            "restore_derived must reproduce the constructor's read∪write participants"
        );
    }

    #[test]
    fn read_only_extra_shard_does_not_trip_write_floor() {
        // `new` (>=2 floor) still rejects a single-WRITE-shard txn even when the
        // read-set adds shards: the floor is computed from the write set only.
        let (wcoll, rcoll) = two_distinct_vshard_collections();
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: wcoll,
            surrogates: SortedVec::new(vec![1]),
        }]);
        let read_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: rcoll,
            surrogates: SortedVec::new(vec![]),
        }]);
        let err = TxClass::new(
            read_set,
            write_set,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::SingleVshardTxn { .. }));
    }

    #[test]
    fn edge_keyset_single_home_when_endpoints_collide() {
        // When src and dst hash to the same vShard, the deduped home set is
        // a single vShard.
        let only = VShardId::from_key(b"same").as_u32();
        let ws = ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![only, only]),
        }]);
        let got: Vec<u32> = ws
            .participating_vshards()
            .expect("participants derive")
            .iter()
            .map(|v| v.as_u32())
            .collect();
        assert_eq!(got, vec![only]);
    }

    #[test]
    fn participating_vshards_dequalify_named_database_collections() {
        let db = DatabaseId::new(1024);
        let qualified = nodedb_types::QualifiedCollection::new(db, "users");
        let ws = ReadWriteSet::new(vec![doc_set(qualified.as_str(), vec![1])]);
        let vshards = ws
            .participating_vshards_in_database(db)
            .expect("participants");
        assert_eq!(
            vshards,
            vec![CollectionKey::from_bare(db, "users").vshard()]
        );

        let unqualified = ReadWriteSet::new(vec![doc_set("users", vec![1])]);
        assert!(unqualified.participating_vshards_in_database(db).is_err());
    }
}
