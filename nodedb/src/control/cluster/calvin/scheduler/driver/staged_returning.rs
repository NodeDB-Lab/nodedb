// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` state a staged Calvin slice holds on its participant
//! leader: the stage's images and the frozen replies, keyed by plan index,
//! and the txn's sequence transcripts.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::SequencedTxn;

use super::returning_transcripts::TxnTranscripts;
use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::returning::precommit::{
    FrozenReturningReply, PreparedReturningRow, PreparedRowIdentity,
};
use crate::data::executor::response_codec::{StagedPlanImages, StagedReturningImages};

/// The `RETURNING` state of one staged slice.
#[derive(Debug, Default)]
pub(super) struct StagedReturning {
    /// The index in the class's plan batch of each plan in this vShard's
    /// local slice, in slice order. Empty when no plan carries `RETURNING`.
    pub plan_indexes: Vec<u32>,
    /// The prepared rows of every local `RETURNING` plan, keyed by its index
    /// in the class's plan batch. Each plan keeps its own rows.
    pub images: BTreeMap<u32, Vec<PreparedReturningRow>>,
    /// Why the slice's images cannot be read: a local plan with no index in
    /// the class's plans, or a stage answer that does not decode. The vote
    /// is an abort.
    pub failed: Option<String>,
    /// The sequence results of every evaluation of the txn's `RETURNING`
    /// lists, shared with every later stage of the txn on this leader.
    pub transcripts: Arc<TxnTranscripts>,
    /// The evaluation in flight, by its sequence number. The vote waits
    /// for it.
    pub evaluating: Option<u64>,
    /// The rows of the images an evaluation took. `images` is empty after
    /// that, so [`Self::row_count`] reads this too.
    pub taken_rows: usize,
    /// The frozen reply of each local `RETURNING` plan the contract names,
    /// keyed by its index in the class's plan batch.
    pub frozen: BTreeMap<u32, FrozenReturningReply>,
}

impl StagedReturning {
    /// The state of a slice whose local plans sit at `plan_indexes` in the
    /// class's plan batch.
    pub(super) fn of_slice(plan_indexes: Vec<u32>) -> Self {
        Self {
            plan_indexes,
            ..Self::default()
        }
    }

    /// The state of `txn`'s slice whose local plans sit at `local` in
    /// `txn.plans()`. An assembled multi-part txn runs only its local
    /// tasks, so each index maps to its task's index in the whole txn,
    /// which the contract names. An index with no task fails the slice.
    pub(super) fn of_txn_slice(
        txn: &SequencedTxn,
        local: Vec<u32>,
        transcripts: Arc<TxnTranscripts>,
    ) -> Self {
        let mapped: Option<Vec<u32>> = local.iter().map(|at| txn.task_index(*at)).collect();
        let mut returning = match mapped {
            Some(indexes) => Self::of_slice(indexes),
            None => Self {
                failed: Some(format!(
                    "local plans {local:?} name no task of the txn's {} assembled tasks",
                    txn.local_plans
                        .as_ref()
                        .map_or(0, |plans| plans.task_indexes.len())
                )),
                ..Self::default()
            },
        };
        returning.transcripts = transcripts;
        returning
    }

    /// The rows every plan holds.
    pub(super) fn row_count(&self) -> usize {
        self.taken_rows + self.images.values().map(Vec::len).sum::<usize>()
    }

    /// Take the images a stage answered with. A stage answers once, so any
    /// earlier images are replaced.
    pub(super) fn absorb(&mut self, staged: StagedReturningImages) -> Result<(), String> {
        let mut images = BTreeMap::new();
        for plan in staged.plans {
            let index = self.class_index(plan.plan)?;
            let rows = prepared_rows(index, plan)?;
            if images.insert(index, rows).is_some() {
                return Err(format!("the stage answered plan {index} twice"));
            }
        }
        self.images = images;
        self.taken_rows = 0;
        Ok(())
    }

    /// The class plan index of the plan at `slice` in the local slice.
    fn class_index(&self, slice: u32) -> Result<u32, String> {
        usize::try_from(slice)
            .ok()
            .and_then(|slice| self.plan_indexes.get(slice))
            .copied()
            .ok_or_else(|| {
                format!(
                    "the stage answered images for local plan {slice} of a slice of {} \
                     indexed plans",
                    self.plan_indexes.len()
                )
            })
    }
}

/// The prepared rows of the plan at class index `index`. Each identity's
/// operation index is `index << 32 | ordinal`.
fn prepared_rows(index: u32, plan: StagedPlanImages) -> Result<Vec<PreparedReturningRow>, String> {
    let StagedPlanImages {
        collection, rows, ..
    } = plan;
    rows.into_iter()
        .map(|row| {
            let fields: ShapedRow = match row.fields {
                nodedb_types::Value::Object(fields) => fields.into_iter().collect(),
                other => {
                    return Err(format!(
                        "row {} of plan {index} is not an object: {other:?}",
                        row.ordinal
                    ));
                }
            };
            Ok(PreparedReturningRow {
                identity: PreparedRowIdentity {
                    collection: collection.clone(),
                    stable_row_key: row.key,
                    operation_index: (u64::from(index) << 32) | u64::from(row.ordinal),
                },
                fields,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;
    use crate::data::executor::response_codec::StagedImageRow;

    fn image(key: &[u8], ordinal: u32, v: i64) -> StagedImageRow {
        StagedImageRow {
            key: key.to_vec(),
            ordinal,
            fields: Value::Object(std::collections::HashMap::from([(
                "v".to_string(),
                Value::Integer(v),
            )])),
        }
    }

    fn plan(slice: u32, rows: Vec<StagedImageRow>) -> StagedPlanImages {
        StagedPlanImages {
            plan: slice,
            collection: "cache".into(),
            rows,
        }
    }

    /// Local slice plans 0 and 1 sit at class indexes 2 and 5. Each keeps
    /// its own rows, under its class index.
    #[test]
    fn images_key_by_class_plan_index() {
        let mut staged = StagedReturning::of_slice(vec![2, 5]);
        staged
            .absorb(StagedReturningImages {
                plans: vec![
                    plan(0, vec![image(b"a", 0, 1), image(b"b", 1, 2)]),
                    plan(1, vec![image(b"a", 3, 9)]),
                ],
            })
            .expect("absorb");
        assert_eq!(
            staged.images.keys().copied().collect::<Vec<_>>(),
            vec![2, 5]
        );
        assert_eq!(staged.row_count(), 3);
        let first = &staged.images[&2];
        assert_eq!(
            first
                .iter()
                .map(|row| row.identity.operation_index)
                .collect::<Vec<_>>(),
            vec![2 << 32, (2 << 32) | 1]
        );
        let second = &staged.images[&5][0];
        assert_eq!(
            second.identity,
            PreparedRowIdentity {
                collection: "cache".into(),
                stable_row_key: b"a".to_vec(),
                operation_index: (5 << 32) | 3,
            }
        );
        assert_eq!(second.fields.get("v"), Some(&Value::Integer(9)));
    }

    #[test]
    fn an_image_for_a_plan_outside_the_slice_is_refused() {
        let mut staged = StagedReturning::of_slice(vec![0]);
        let result = staged.absorb(StagedReturningImages {
            plans: vec![plan(1, vec![image(b"a", 0, 1)])],
        });
        assert!(result.is_err());
        assert!(staged.images.is_empty());
    }

    #[test]
    fn a_row_that_is_not_an_object_is_refused() {
        let mut staged = StagedReturning::of_slice(vec![0]);
        let mut row = image(b"a", 0, 1);
        row.fields = Value::Integer(1);
        let result = staged.absorb(StagedReturningImages {
            plans: vec![plan(0, vec![row])],
        });
        assert!(result.is_err());
    }

    fn txn(local_plans: Option<Vec<u32>>) -> SequencedTxn {
        use nodedb_cluster::calvin::types::{
            EngineKeySet, LocalPlans, ReadWriteSet, SortedVec, TxClass, VersionedReadSet,
        };
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "cache".to_string(),
            surrogates: SortedVec::new(vec![1]),
        }]);
        let tx_class = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            nodedb_types::TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        SequencedTxn {
            epoch: 9,
            position: 0,
            tx_class: Arc::new(tx_class),
            epoch_system_ms: 1_700_000_000_000,
            epoch_vshard_txn_count: 1,
            lock_owner: None,
            local_plans: local_plans.map(|task_indexes| {
                Arc::new(LocalPlans {
                    plans: Vec::new(),
                    body_plans: Vec::new(),
                    task_indexes,
                })
            }),
        }
    }

    /// An assembled multi-part txn runs its local tasks only. Local plans 0
    /// and 1 are tasks 3 and 8 of the whole txn, the indexes the contract
    /// names. A txn with no assembled plans keeps its own indexes.
    #[test]
    fn a_multi_part_slice_keys_by_the_whole_txn_task_index() {
        let staged =
            StagedReturning::of_txn_slice(&txn(Some(vec![3, 8])), vec![0, 1], Arc::default());
        assert_eq!(staged.plan_indexes, vec![3, 8]);
        assert!(staged.failed.is_none());

        let staged = StagedReturning::of_txn_slice(&txn(None), vec![0, 2], Arc::default());
        assert_eq!(staged.plan_indexes, vec![0, 2]);

        let staged = StagedReturning::of_txn_slice(&txn(Some(vec![3])), vec![1], Arc::default());
        assert!(staged.plan_indexes.is_empty());
        assert!(staged.failed.is_some());
    }
}
