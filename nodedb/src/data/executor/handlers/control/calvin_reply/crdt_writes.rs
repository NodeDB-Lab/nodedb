// SPDX-License-Identifier: BUSL-1.1

//! The CRDT row a Calvin install leaves, predicted when the slice stages.
//!
//! The install replays each CRDT row write of the slice in plan order
//! against the collection's Loro document, and each write lands the row its
//! prediction names. The stage predicts the same rows without running any
//! write: it starts from the row the Loro document holds and folds every
//! earlier write of the slice to that row, in plan order, before the write
//! it answers for.
//!
//! A sync delta that merges into the row between the stage and the install
//! is part of the installed row only.

use loro::LoroValue;
use nodedb_crdt::state::ScalarRowBase;
use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};

use crate::bridge::envelope::ErrorCode;
use crate::engine::crdt::tenant_state::TenantCrdtEngine;

/// One CRDT row write a plan of the slice stages.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct StagedCrdtWrite {
    collection: String,
    document_id: String,
    write: CrdtRowWrite,
}

#[derive(Debug, Clone, PartialEq)]
enum CrdtRowWrite {
    /// Replace (`partial == false`) or merge the row's scalar fields.
    Upsert { fields: Vec<u8>, partial: bool },
    /// Remove the row.
    Delete,
}

impl StagedCrdtWrite {
    /// The row write `plan` stages, or `None` for a plan that writes no CRDT
    /// row. A delete of an unbound key names no row.
    pub(super) fn of_plan(plan: &PhysicalPlan) -> Option<Self> {
        let (collection, document_id, write) = match plan {
            PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                collection,
                document_id,
                fields,
                partial,
                ..
            }) => (
                collection,
                document_id,
                CrdtRowWrite::Upsert {
                    fields: fields.clone(),
                    partial: *partial,
                },
            ),
            PhysicalPlan::Crdt(CrdtOp::DocDelete {
                collection,
                document_id,
                surrogate: Some(_),
                ..
            }) => (collection, document_id, CrdtRowWrite::Delete),
            _ => return None,
        };
        Some(Self {
            collection: collection.as_str().to_string(),
            document_id: document_id.clone(),
            write,
        })
    }

    fn same_row(&self, other: &Self) -> bool {
        self.collection == other.collection && self.document_id == other.document_id
    }
}

/// The deep row the upsert `this` leaves once the install replayed every
/// write of `earlier` to the same row, then `this`. `engine` is the
/// tenant's CRDT engine, `None` when it holds no document yet.
pub(super) fn predicted_upsert_row(
    engine: Option<&TenantCrdtEngine>,
    earlier: &[StagedCrdtWrite],
    this: &StagedCrdtWrite,
) -> Result<LoroValue, ErrorCode> {
    let mut base =
        engine.and_then(|engine| engine.scalar_row_base(&this.collection, &this.document_id));
    let mut predicted = None;
    for write in earlier
        .iter()
        .filter(|write| write.same_row(this))
        .chain(std::iter::once(this))
    {
        match &write.write {
            CrdtRowWrite::Delete => {
                base = None;
                predicted = None;
            }
            CrdtRowWrite::Upsert { fields, partial } => {
                let row = predict_upsert(base.as_ref(), write, fields, *partial)?;
                base = Some(ScalarRowBase::after_write(base.as_ref(), &row));
                predicted = Some(row);
            }
        }
    }
    predicted.ok_or_else(|| ErrorCode::Internal {
        detail: format!(
            "calvin RETURNING: the CRDT write of '{}'/'{}' is not an upsert",
            this.collection, this.document_id
        ),
    })
}

/// The row one upsert of `fields` leaves over `base`, with the error the
/// install raises for it.
fn predict_upsert(
    base: Option<&ScalarRowBase>,
    write: &StagedCrdtWrite,
    fields: &[u8],
    partial: bool,
) -> Result<LoroValue, ErrorCode> {
    let field_map =
        nodedb_physical::physical_plan::decode_fields_body(fields).map_err(|detail| {
            ErrorCode::DataException {
                detail: format!(
                    "crdt doc upsert into '{}'/'{}': {detail}",
                    write.collection, write.document_id
                ),
            }
        })?;
    let loro_fields = nodedb_crdt::loro_value::fields_to_loro_map(&field_map)
        .map_err(|error| ErrorCode::from(&error))?;
    let fields: Vec<(&str, LoroValue)> = loro_fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()))
        .collect();
    ScalarRowBase::predict(
        base,
        &write.collection,
        &write.document_id,
        &fields,
        !partial,
    )
    .map_err(|error| ErrorCode::from(crate::Error::Crdt(error)))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{CrdtWriteVerb, encode_fields_body};
    use nodedb_types::{Surrogate, Value};

    use super::super::fixtures::coll;
    use super::*;

    fn upsert(id: &str, fields: &[(&str, i64)], partial: bool) -> StagedCrdtWrite {
        let fields = fields
            .iter()
            .map(|(name, value)| ((*name).to_string(), Value::Integer(*value)))
            .collect();
        let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: coll("tasks"),
            document_id: id.to_string(),
            fields: encode_fields_body(&fields).expect("fields body encodes"),
            surrogate: Surrogate::new(9),
            partial,
            verb: CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        });
        StagedCrdtWrite::of_plan(&plan).expect("a CRDT row write")
    }

    fn delete(id: &str) -> StagedCrdtWrite {
        let plan = PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: coll("tasks"),
            document_id: id.to_string(),
            surrogate: Some(Surrogate::new(9)),
            returning: None,
            rls_filters: Vec::new(),
        });
        StagedCrdtWrite::of_plan(&plan).expect("a CRDT row write")
    }

    fn field(row: &LoroValue, name: &str) -> Option<LoroValue> {
        match row {
            LoroValue::Map(map) => map.get(name).cloned(),
            _ => None,
        }
    }

    /// A merge after an earlier upsert of the same row keeps the fields the
    /// upsert wrote. A write to another row changes nothing.
    #[test]
    fn a_merge_folds_the_earlier_writes_to_its_row() {
        let earlier = [
            upsert("t1", &[("a", 1), ("b", 2)], false),
            upsert("t2", &[("a", 7)], false),
        ];
        let row = predicted_upsert_row(None, &earlier, &upsert("t1", &[("b", 3)], true))
            .expect("predict");
        assert_eq!(field(&row, "a"), Some(LoroValue::I64(1)));
        assert_eq!(field(&row, "b"), Some(LoroValue::I64(3)));
    }

    /// A replace drops the scalar fields it does not name. A delete before
    /// it leaves no row to merge into.
    #[test]
    fn a_replace_and_a_delete_drop_earlier_fields() {
        let earlier = [upsert("t1", &[("a", 1)], false)];
        let replaced = predicted_upsert_row(None, &earlier, &upsert("t1", &[("b", 2)], false))
            .expect("predict");
        assert_eq!(field(&replaced, "a"), None);

        let earlier = [upsert("t1", &[("a", 1)], false), delete("t1")];
        let merged = predicted_upsert_row(None, &earlier, &upsert("t1", &[("b", 2)], true))
            .expect("predict");
        assert_eq!(field(&merged, "a"), None);
        assert_eq!(field(&merged, "b"), Some(LoroValue::I64(2)));
    }

    #[test]
    fn a_delete_of_an_unbound_key_writes_no_row() {
        let plan = PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: coll("tasks"),
            document_id: "t1".to_string(),
            surrogate: None,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(StagedCrdtWrite::of_plan(&plan).is_none());
    }
}
