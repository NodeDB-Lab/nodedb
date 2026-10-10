// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for a read-dependent transaction: a cross-shard
//! write whose values depend on rows of another vShard.
//!
//! The coordinator reads the rows first and plans the writes from what it
//! read. The class carries a [`DependentReadSpec`]: the rows each passive
//! vShard reads again under the transaction's locks, and the values the
//! coordinator read. Each active vShard waits at its dependent-read barrier
//! for every passive vShard's values, through its own data-group log, and
//! votes `PredictionDrift` when one differs. The coordinator then reads
//! again and resubmits.
//!
//! Every passive key set also enters the read set, so the passive vShard
//! locks the rows it reads shared until the transaction's verdict.

use std::collections::BTreeMap;

use nodedb_cluster::calvin::types::{
    DependentReadSpec, EngineKeySet, PassiveReadKey, PassiveReadKeyId, ReadWriteSet, TxClass,
    VersionedReadSet,
};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;
use nodedb_types::{DatabaseId, TenantId};

use super::write_keys::task_write_keys;
use crate::Error;

/// The rows a read-dependent transaction reads on other vShards, and the
/// values its coordinator read for them.
#[derive(Debug, Clone, Default)]
pub struct PassiveReads {
    /// Each passive vShard and the key sets it reads.
    pub keys: BTreeMap<u32, Vec<EngineKeySet>>,
    /// The stored bytes the coordinator read for each row, `None` for an
    /// absent row.
    pub expected: BTreeMap<PassiveReadKeyId, Option<Vec<u8>>>,
}

/// Build a multi-vShard `TxClass` for `tasks`, whose values depend on
/// `reads`.
///
/// The write set comes from the tasks' static write keys, as
/// [`super::build_static_tx_class`] derives it. The write set spans two or
/// more vShards. A `PassiveReads` with no key set is refused: the class
/// would carry a barrier that waits for nothing.
pub fn build_read_dependent_tx_class(
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
    reads: PassiveReads,
) -> crate::Result<TxClass> {
    let database_id = tasks
        .first()
        .map_or(DatabaseId::DEFAULT, |task| task.database_id);
    if tasks.iter().any(|task| task.database_id != database_id) {
        return Err(Error::BadRequest {
            detail: "Calvin transaction spans multiple databases".to_owned(),
        });
    }
    if reads.keys.values().all(Vec::is_empty) {
        return Err(Error::Internal {
            detail: "a read-dependent Calvin transaction names no row it reads".to_owned(),
        });
    }

    let write_set = ReadWriteSet::new(task_write_keys(tasks)?.into_key_sets());
    let read_set = ReadWriteSet::new(reads.keys.values().flatten().cloned().collect());
    let passive_reads = reads
        .keys
        .into_iter()
        .map(|(vshard, key_sets)| {
            let keys = key_sets
                .into_iter()
                .map(|engine_key| PassiveReadKey { engine_key })
                .collect();
            (vshard, keys)
        })
        .collect();
    let spec = DependentReadSpec {
        passive_reads,
        expected: reads.expected,
    };

    let plans: Vec<&PhysicalPlan> = tasks.iter().map(|t| &t.plan).collect();
    let plans_bytes = zerompk::to_msgpack_vec(&plans).map_err(|e| Error::Serialization {
        format: "msgpack".to_owned(),
        detail: format!("failed to encode PhysicalPlan vec for Calvin read-dependent TxClass: {e}"),
    })?;

    TxClass::new_dependent(
        read_set,
        write_set,
        plans_bytes,
        tenant_id,
        database_id,
        spec,
        VersionedReadSet::default(),
    )
    .map_err(|e| Error::BadRequest {
        detail: format!("invalid read-dependent TxClass: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::SortedVec;
    use nodedb_physical::physical_plan::KvOp;
    use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;
    use crate::types::VShardId;

    /// Two collection names whose default-database vShards differ.
    fn two_distinct_collections() -> (String, String) {
        let first = "read_dep_0".to_owned();
        let second = (1u32..4096)
            .map(|i| format!("read_dep_{i}"))
            .find(|name| vshard_of(name) != vshard_of(&first))
            .expect("a collection on another vShard");
        (first, second)
    }

    fn kv_task(plan: KvOp, collection: &str) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
                .vshard(),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Kv(plan),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn vshard_of(collection: &str) -> u32 {
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
            .vshard()
            .as_u32()
    }

    /// The class carries the passive reads and the coordinator's values,
    /// locks the passive rows shared, and spans the write vShards.
    #[test]
    fn a_read_dependent_class_carries_its_passive_reads() {
        let (source, dest) = two_distinct_collections();
        let tasks = vec![
            kv_task(
                KvOp::Delete {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, &source),
                    keys: vec![b"alice:sword".to_vec()],
                    rls_write_check: RlsWriteCheck::pending_injection(),
                    returning: None,
                    rls_filters: Vec::new(),
                    provenance: None,
                },
                &source,
            ),
            kv_task(
                KvOp::Put {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, &dest),
                    key: b"bob:sword".to_vec(),
                    value: b"item".to_vec(),
                    ttl_ms: 0,
                    surrogate: Surrogate::new(3),
                    returning: None,
                    rls_filters: Vec::new(),
                    provenance: None,
                },
                &dest,
            ),
        ];
        let row = PassiveReadKeyId::kv(
            QualifiedCollection::new(DatabaseId::DEFAULT, &source),
            b"alice:sword".to_vec(),
        );
        let reads = PassiveReads {
            keys: BTreeMap::from([(
                vshard_of(&source),
                vec![EngineKeySet::Kv {
                    collection: source.clone(),
                    keys: SortedVec::new(vec![b"alice:sword".to_vec()]),
                }],
            )]),
            expected: BTreeMap::from([(row.clone(), Some(b"item".to_vec()))]),
        };

        let class =
            build_read_dependent_tx_class(&tasks, TenantId::new(1), reads).expect("valid class");

        let spec = class.dependent_reads.as_ref().expect("dependent reads");
        assert_eq!(
            spec.passive_reads.keys().copied().collect::<Vec<_>>(),
            vec![vshard_of(&source)]
        );
        assert_eq!(spec.expected.get(&row), Some(&Some(b"item".to_vec())));
        assert_eq!(class.read_set.0.len(), 1);
        let mut active = class.active_vshards().expect("active vShards");
        active.sort_unstable();
        let mut expected_active = vec![vshard_of(&source), vshard_of(&dest)];
        expected_active.sort_unstable();
        assert_eq!(active, expected_active);
        assert!(
            class
                .participating_vshards()
                .contains(&VShardId::new(vshard_of(&source)))
        );
    }

    /// A class that names no passive row is refused.
    #[test]
    fn a_class_with_no_passive_row_is_refused() {
        let (source, _) = two_distinct_collections();
        let tasks = vec![kv_task(
            KvOp::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, &source),
                keys: vec![b"k".to_vec()],
                rls_write_check: RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                provenance: None,
            },
            &source,
        )];
        assert!(
            build_read_dependent_tx_class(&tasks, TenantId::new(1), PassiveReads::default())
                .is_err()
        );
    }
}
