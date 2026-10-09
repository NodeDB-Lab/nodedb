// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for a static write task slice (every write key
//! known upfront).

use crate::Error;
use crate::control::server::shared::session::read_set::ReadSetEntry;
use nodedb_cluster::calvin::types::{ReadWriteSet, TxClass};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use super::shared::{read_set_from, versioned_reads_from};
use super::write_keys::task_write_keys;

/// Build a **multi-vshard** `TxClass` from a static write task slice.
///
/// Extracts each write task's deterministic identity into the matching
/// `EngineKeySet` (document / vector surrogates, KV raw keys, graph-edge
/// pairs), constructs the `ReadWriteSet`, msgpack-encodes plans into `Vec<u8>`,
/// and calls `TxClass::new`. A write set that collapses to a single vshard is
/// rejected (`SingleVshardTxn`) — that shape indicates a misrouted multi-shard
/// dispatch. For the legitimate contended-single-vshard point-write path, use
/// [`build_single_vshard_tx_class`].
///
/// `reads` is the neutral session read-set captured during the transaction; it
/// is projected onto the `TxClass`'s routing/identity `read_set` (collection-
/// homed) so a txn that writes shard A but reads shard B enumerates B as a
/// participant, and onto `versioned_reads` (LSN-versioned OCC validation set)
/// for commit-time optimistic-concurrency validation. Autocommit and
/// pure-write paths pass an empty slice, yielding an empty read_set and
/// versioned_reads.
///
/// Returns `Err(SequencerUnavailable)` if msgpack encoding of plans fails.
pub fn build_static_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<TxClass> {
    build_static_tx_class_impl(tasks, tenant_id, reads, false)
}

/// Build a `TxClass` from a static write task slice that is permitted to resolve
/// to a **single vshard**.
///
/// Used only by the contended point-write routing path
/// (`route_write_to_calvin`): the write-admission gate returned
/// `RouteToCalvin` because a pending commit holds the write's
/// key, so the write must sequence through the deterministic scheduler to
/// serialize on the SAME shared per-vShard `LockManager`. Identical extraction
/// to [`build_static_tx_class`]; only the participant floor differs.
pub fn build_single_vshard_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
) -> crate::Result<TxClass> {
    build_static_tx_class_impl(tasks, tenant_id, reads, true)
}

/// Shared body for the static builders. `allow_single_vshard` selects between
/// [`TxClass::new`] (multi-vshard, `>=2` floor) and
/// [`TxClass::new_single_vshard`] (single-vshard opt-in).
fn build_static_tx_class_impl(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: &[ReadSetEntry],
    allow_single_vshard: bool,
) -> crate::Result<TxClass> {
    let database_id = tasks
        .first()
        .map_or(DatabaseId::DEFAULT, |task| task.database_id);
    if tasks.iter().any(|task| task.database_id != database_id)
        || reads.iter().any(|read| read.database_id != database_id)
    {
        return Err(Error::BadRequest {
            detail: "Calvin transaction spans multiple databases".to_owned(),
        });
    }
    // Every replica resolves a sequenced transaction on its own, and each
    // will accept other lines than its peers. A timeseries ingest resolves
    // to its rows on the submitting node, before it is sequenced.
    if let Some(task) = tasks
        .iter()
        .find(|task| crate::control::write_resolve::is_unresolved_ingest(&task.plan))
    {
        return Err(Error::Internal {
            detail: format!(
                "internal invariant break: a timeseries ingest on '{}' reached the Calvin \
                 sequencer unresolved; the submitter resolves it with `resolve_tasks_for_log`",
                task.plan.collection().unwrap_or("<unknown>")
            ),
        });
    }

    // One key set per engine and collection, ordered by collection. Every
    // write op is matched by name: an edge locks its pair and both node
    // pairs on both homes, a KV write its raw key, a CRDT document its id,
    // and a row write its surrogate.
    let write_sets = task_write_keys(tasks)?.into_key_sets();

    // Read-your-own-write: a SESSION read of a collection this txn also WRITES
    // must NOT enter the OCC read set. The txn's own staged write advances that
    // collection's write floor, so validating the earlier read against it will
    // flag it stale and abort the commit — a false serialization conflict. This
    // mirrors the written-collection exclusion the single-shard
    // `si_conflict_abort` path already applies.
    //
    // A PLAN-DERIVATION read is exempt, and the exemption is what makes the
    // derivation guard exist at all. Such a read observed COMMITTED base state
    // BEFORE this transaction existed, and it is precisely the observation a
    // value the transaction ships was computed from — a cross-shard
    // materialized-sum settlement folds a delta from a pre-image of the source
    // row and sends it to another shard. The source collection is one this
    // statement always writes, so the exclusion will drop every such entry and
    // `read_set_still_current` will validate nothing: a concurrent write
    // between the fold and the apply will commit a total folded from an image
    // that has moved. The kind is carried on the entry rather than inferred
    // here, so no entry can be classified by accident of which collection it
    // names.
    let written_collections: std::collections::HashSet<String> = write_sets
        .iter()
        .map(|ks| ks.collection().to_string())
        .collect();
    let owned_reads: Vec<ReadSetEntry> = reads
        .iter()
        .filter(|e| {
            e.origin.survives_own_write_exclusion()
                || !written_collections.contains(e.collection.as_str())
        })
        .cloned()
        .collect();

    let write_set = ReadWriteSet::new(write_sets);
    // Populate the routing/identity read_set from the (own-write-filtered)
    // session read-set so a txn that writes shard A but reads shard B enumerates
    // B as a participant. An empty read set (autocommit / pure-write, or all
    // reads self-written) yields an empty read_set.
    let read_set = read_set_from(&owned_reads);

    // Encode all plans as msgpack bytes.
    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin TxClass: {e}"),
    })?;

    // versioned_reads carries the LSN-versioned OCC validation set, populated
    // from the same session read-set the routing `read_set` above was built
    // from.
    let versioned_reads = versioned_reads_from(&owned_reads);

    let result = if allow_single_vshard {
        TxClass::new_single_vshard_in_database(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            database_id,
            None,
            versioned_reads,
        )
    } else {
        TxClass::new_in_database(
            read_set,
            write_set,
            plans_bytes,
            tenant_id,
            database_id,
            None,
            versioned_reads,
        )
    };
    result.map_err(|e| Error::BadRequest {
        detail: format!("invalid TxClass: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::read_set::{EngineTag, ReadKey, ReadOrigin};
    use crate::types::{DatabaseId, KeyRepr, Lsn, VShardId};
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::Surrogate;

    /// Find two collection names whose default-database vShards differ, so the
    /// built `TxClass` spans ≥2 vShards (required by `TxClass::new`).
    pub(super) fn two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..1024 {
            let name = format!("coll_{i}");
            let v = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            match &first {
                Some((fname, fv)) if *fv != v => return (fname.clone(), name),
                Some(_) => {}
                None => first = Some((name, v)),
            }
        }
        panic!("could not find two distinct-vShard collections in 1024 tries");
    }

    pub(super) fn point_insert_task(collection: &str, surrogate: u32) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                document_id: "d1".to_owned(),
                surrogate: Surrogate::new(surrogate),
                value: vec![],
                if_absent: false,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn read_entry(collection: &str, key: ReadKey, read_lsn: u64) -> ReadSetEntry {
        origin_read_entry(collection, key, read_lsn, ReadOrigin::Session)
    }

    fn origin_read_entry(
        collection: &str,
        key: ReadKey,
        read_lsn: u64,
        origin: ReadOrigin,
    ) -> ReadSetEntry {
        ReadSetEntry {
            engine: EngineTag::Document,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: collection.to_owned(),
            key,
            read_lsn: Lsn::new(read_lsn),
            // The per-collection read-version is the OCC comparand
            // `versioned_reads_from` propagates; give it the same synthetic LSN.
            read_version_lsn: Lsn::new(read_lsn),
            origin,
            home: None,
            home_node: 0,
        }
    }

    #[test]
    fn single_vshard_builder_preserves_database_scope() {
        // A plan in a non-default database names its collection qualified.
        let mut task = point_insert_task("7/db_scoped", 1);
        task.database_id = DatabaseId::new(7);
        let tx = build_single_vshard_tx_class(&[task], TenantId::new(1), &[])
            .expect("valid single-vshard TxClass");
        assert_eq!(tx.database_id, DatabaseId::new(7));
    }

    #[test]
    fn builder_rejects_cross_database_batches() {
        let (col_a, col_b) = two_distinct_collections();
        let first = point_insert_task(&col_a, 1);
        let mut second = point_insert_task(&col_b, 2);
        second.database_id = DatabaseId::new(7);
        let error = build_static_tx_class(&[first, second], TenantId::new(1), &[])
            .expect_err("cross-database Calvin batch must fail");
        assert!(error.to_string().contains("multiple databases"));
    }

    #[test]
    fn build_static_populates_read_set_and_unions_read_participants() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];

        // Synthetic read-set: one point read (surrogate identity) at LSN 7 and
        // one collection-scoped predicate read at LSN 11, on two collections
        // distinct from the write collections.
        let reads = vec![
            read_entry(
                "read_col",
                ReadKey::Point {
                    repr: KeyRepr::Surrogate(42),
                },
                7,
            ),
            read_entry("scan_col", ReadKey::Predicate, 11),
        ];

        let tx = build_static_tx_class(&tasks, TenantId::new(1), &reads)
            .expect("valid multi-vShard TxClass");

        // versioned_reads (the LSN-versioned OCC validation set) carries the
        // same session reads, 1:1, for commit-time validation.
        assert_eq!(
            tx.versioned_reads.len(),
            reads.len(),
            "versioned_reads must carry one entry per session read"
        );
        for (entry, read) in tx.versioned_reads.iter().zip(reads.iter()) {
            assert_eq!(entry.collection, read.collection);
            assert_eq!(entry.read_lsn, read.read_version_lsn);
            let expected_key = match &read.key {
                ReadKey::Point { repr } => {
                    nodedb_cluster::calvin::types::ReadKeyIdent::Point(repr.clone())
                }
                ReadKey::Predicate => nodedb_cluster::calvin::types::ReadKeyIdent::Predicate,
                ReadKey::IndexEq { field, value } => {
                    nodedb_cluster::calvin::types::ReadKeyIdent::IndexEq {
                        field: field.clone(),
                        value: value.clone(),
                    }
                }
                ReadKey::IndexRange { field, lo, hi } => {
                    nodedb_cluster::calvin::types::ReadKeyIdent::IndexRange {
                        field: field.clone(),
                        lo: lo.clone(),
                        hi: hi.clone(),
                    }
                }
            };
            assert_eq!(entry.key, expected_key);
        }

        // read_set is collection-homed from the session reads: both read
        // collections appear (Document engine → Document keyset).
        let read_colls: std::collections::BTreeSet<&str> =
            tx.read_set.0.iter().map(|ks| ks.collection()).collect();
        assert!(
            read_colls.contains("read_col"),
            "read_col must be in read_set"
        );
        assert!(
            read_colls.contains("scan_col"),
            "scan_col must be in read_set"
        );

        // participating_vshards is write ∪ read: it contains the two write
        // collections' vShards AND both read collections' vShards.
        let participants: std::collections::BTreeSet<u32> = tx
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        for coll in [col_a.as_str(), col_b.as_str(), "read_col", "scan_col"] {
            let v = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, coll)
                .vshard()
                .as_u32();
            assert!(
                participants.contains(&v),
                "participant set must include the vShard of {coll}"
            );
        }
        // Every write shard is still present (the read union never drops one).
        for v in tx.write_set.participating_vshards().expect("participants") {
            assert!(
                participants.contains(&v.as_u32()),
                "read union must not drop a write shard"
            );
        }
    }

    /// The lock request the scheduler takes for `tx_class` at `position`.
    fn locks(
        tx_class: TxClass,
        position: u32,
    ) -> std::collections::BTreeMap<
        crate::control::cluster::calvin::scheduler::LockKey,
        crate::control::cluster::calvin::scheduler::LockMode,
    > {
        crate::control::cluster::calvin::scheduler::driver::helpers::expand_rw_set(
            &nodedb_cluster::calvin::types::SequencedTxn {
                epoch: 1,
                position,
                tx_class,
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 2,
                lock_owner: None,
            },
        )
    }

    /// A predicate read locks its whole collection `Shared`. A later row
    /// writer of that collection takes `Intent` on it, so it waits until the
    /// reader releases. A point read locks only its row.
    #[test]
    fn a_predicate_read_blocks_a_later_writer_until_released() {
        use crate::control::cluster::calvin::scheduler::LockMode;
        use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
        use crate::control::cluster::calvin::scheduler::{AcquireOutcome, LockKey, LockManager};

        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];
        let reads = vec![
            read_entry("scan_col", ReadKey::Predicate, 11),
            read_entry(
                "read_col",
                ReadKey::Point {
                    repr: KeyRepr::Surrogate(9),
                },
                10,
            ),
        ];
        let reader = build_static_tx_class(&tasks, TenantId::new(1), &reads)
            .expect("valid multi-vShard TxClass");
        let reader_locks = locks(reader, 0);
        assert_eq!(
            reader_locks.get(&LockKey::Collection {
                collection: std::sync::Arc::from("scan_col"),
            }),
            Some(&LockMode::Shared)
        );
        assert_eq!(
            reader_locks.get(&LockKey::Surrogate {
                collection: std::sync::Arc::from("read_col"),
                surrogate: 9,
            }),
            Some(&LockMode::Shared)
        );
        assert!(
            !reader_locks.contains_key(&LockKey::Collection {
                collection: std::sync::Arc::from("read_col"),
            }),
            "a point read locks nothing collection-wide"
        );

        let writer = build_single_vshard_tx_class(
            &[point_insert_task("scan_col", 5)],
            TenantId::new(1),
            &[],
        )
        .expect("writer TxClass");

        let (first, second) = (TxnId::new(1, 0), TxnId::new(1, 1));
        let mut table = LockManager::new();
        assert_eq!(table.acquire(first, reader_locks), AcquireOutcome::Ready);
        assert_eq!(
            table.acquire(second, locks(writer, 1)),
            AcquireOutcome::Blocked,
            "a row writer waits for the predicate reader of its collection"
        );
        assert_eq!(table.release(first), vec![second]);
    }

    #[test]
    fn empty_read_set_yields_empty_read_and_versioned_reads() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];
        let tx = build_static_tx_class(&tasks, TenantId::new(1), &[])
            .expect("valid multi-vShard TxClass");
        assert!(tx.versioned_reads.is_empty());
        assert!(tx.read_set.is_empty(), "no session reads → empty read_set");
        // Participants collapse to the write-derived set when there are no reads.
        assert_eq!(
            tx.participating_vshards(),
            tx.write_set
                .participating_vshards()
                .expect("participants")
                .as_slice()
        );
    }

    /// A PLAN-DERIVATION read of a collection the transaction WRITES survives
    /// the own-write exclusion, reaching BOTH the routing read_set and the
    /// LSN-versioned `versioned_reads` the Data Plane's
    /// `read_set_still_current` check consumes.
    ///
    /// This is the whole of the cross-shard materialized-sum guard: the delta
    /// shipped to the target was folded from a pre-image of a source row, and
    /// the source collection is one the statement always writes. Dropped here,
    /// nothing validates the fold and a concurrent write to the source row
    /// between the fold and the apply commits a wrong total.
    #[test]
    fn a_derivation_read_of_a_written_collection_survives_the_own_write_exclusion() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];

        let reads = vec![origin_read_entry(
            &col_a,
            ReadKey::Point {
                repr: KeyRepr::Surrogate(11),
            },
            42,
            ReadOrigin::PlanDerivation,
        )];

        let tx = build_static_tx_class(&tasks, TenantId::new(1), &reads)
            .expect("valid multi-vShard TxClass");

        assert_eq!(
            tx.versioned_reads.len(),
            1,
            "a derivation read must reach versioned_reads — that set IS the guard"
        );
        assert_eq!(tx.versioned_reads.0[0].collection, col_a);
        assert_eq!(tx.versioned_reads.0[0].read_lsn, Lsn::new(42));
        assert_eq!(
            tx.versioned_reads.0[0].key,
            nodedb_cluster::calvin::types::ReadKeyIdent::Point(KeyRepr::Surrogate(11))
        );

        let read_colls: std::collections::BTreeSet<&str> =
            tx.read_set.0.iter().map(|ks| ks.collection()).collect();
        assert!(
            read_colls.contains(col_a.as_str()),
            "a derivation read must also reach the routing read_set"
        );
    }

    /// The other half: an ordinary SESSION read of a collection the transaction
    /// writes is excluded. Without this, disabling the exclusion will pass the
    /// test above, and every transaction that reads a row it
    /// then writes will abort on its own staged write.
    #[test]
    fn a_session_read_of_a_written_collection_is_still_excluded() {
        let (col_a, col_b) = two_distinct_collections();
        let tasks = vec![point_insert_task(&col_a, 1), point_insert_task(&col_b, 2)];

        let reads = vec![read_entry(
            &col_a,
            ReadKey::Point {
                repr: KeyRepr::Surrogate(11),
            },
            42,
        )];

        let tx = build_static_tx_class(&tasks, TenantId::new(1), &reads)
            .expect("valid multi-vShard TxClass");

        assert!(
            tx.versioned_reads.is_empty(),
            "a session read of a self-written collection must not be validated \
             against the transaction's own staged write"
        );
        assert!(
            tx.read_set.is_empty(),
            "and it must not enter the routing read_set either"
        );
    }

    #[test]
    fn single_point_write_strict_rejects_but_single_vshard_builder_accepts() {
        // One point-write task → one collection → one vshard. This is exactly the
        // shape the contended point-write routing path builds.
        let tasks = vec![point_insert_task("users", 7)];
        let want_vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users")
            .vshard()
            .as_u32();

        // Strict builder rejects the single-vshard write set.
        let strict = build_static_tx_class(&tasks, TenantId::new(1), &[]);
        assert!(
            matches!(strict, Err(crate::Error::BadRequest { .. })),
            "strict builder must reject single-vshard write set"
        );

        // Single-vshard builder accepts it, with exactly one participating vshard.
        let tx = build_single_vshard_tx_class(&tasks, TenantId::new(1), &[])
            .expect("single-vshard TxClass accepted");
        assert_eq!(tx.participating_vshards().len(), 1);
        assert_eq!(tx.participating_vshards()[0].as_u32(), want_vshard);
    }
}
