// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` images a staged Calvin slice holds on its participant
//! leader, keyed by plan index.

use std::collections::BTreeMap;

use crate::control::server::response_shape::types::ShapedRow;
use crate::control::server::shared::returning::precommit::{
    PreparedReturningRow, PreparedRowIdentity,
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
}

impl StagedReturning {
    /// The state of a slice whose local plans sit at `plan_indexes` in the
    /// class's plan batch.
    pub(super) fn of_slice(plan_indexes: Vec<u32>) -> Self {
        Self {
            plan_indexes,
            images: BTreeMap::new(),
        }
    }

    /// The rows every plan holds.
    pub(super) fn row_count(&self) -> usize {
        self.images.values().map(Vec::len).sum()
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
}
