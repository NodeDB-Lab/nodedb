// SPDX-License-Identifier: BUSL-1.1

//! Turn the rows a Calvin `RETURNING` plan staged into the images its stage
//! response carries.

use nodedb_types::Value;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::handlers::rls_eval::rls_check_value;
use crate::data::executor::response_codec::{StagedImageRow, StagedPlanImages};

/// The rows a `RETURNING` plan wrote, in write order: each row's stable
/// identity bytes beside its full source document.
#[derive(Debug, Default)]
pub(super) struct PlanRows {
    pub keys: Vec<Vec<u8>>,
    pub docs: Vec<Value>,
}

impl PlanRows {
    pub(super) fn push(&mut self, key: Vec<u8>, doc: Value) {
        self.keys.push(key);
        self.docs.push(doc);
    }

    /// Rows that carry no key, as timeseries points. Each row's key is its
    /// ordinal, big-endian.
    pub(super) fn keyed_by_ordinal(docs: Vec<Value>) -> Result<Self, ErrorCode> {
        let keys = (0..docs.len())
            .map(|ordinal| ordinal_of(ordinal).map(|ordinal| ordinal.to_be_bytes().to_vec()))
            .collect::<Result<Vec<_>, ErrorCode>>()?;
        Ok(Self { keys, docs })
    }
}

/// The images of the plan at slice index `plan`. A row the read policy
/// `rls_filters` hides carries no image, and the rest keep their ordinals.
pub(super) fn plan_images(
    plan: u32,
    collection: &str,
    rls_filters: &[u8],
    rows: PlanRows,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<StagedPlanImages, ErrorCode> {
    let PlanRows { keys, docs } = rows;
    if keys.len() != docs.len() {
        return Err(ErrorCode::Internal {
            detail: format!(
                "calvin RETURNING: plan {plan} on '{collection}' staged {} keys for {} rows",
                keys.len(),
                docs.len()
            ),
        });
    }
    let mut images = Vec::with_capacity(docs.len());
    for (ordinal, (key, doc)) in keys.into_iter().zip(docs).enumerate() {
        if !matches!(doc, Value::Object(_)) {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "calvin RETURNING: row {ordinal} of plan {plan} on '{collection}' is not \
                     an object"
                ),
            });
        }
        if !rls_check_value(rls_filters, &doc, crs) {
            continue;
        }
        images.push(StagedImageRow {
            key,
            ordinal: ordinal_of(ordinal)?,
            fields: doc,
        });
    }
    Ok(StagedPlanImages {
        plan,
        collection: collection.to_owned(),
        rows: images,
        rejected: 0,
    })
}

fn ordinal_of(ordinal: usize) -> Result<u32, ErrorCode> {
    u32::try_from(ordinal).map_err(|_| ErrorCode::Internal {
        detail: format!("calvin RETURNING: row ordinal {ordinal} exceeds u32"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};

    fn row(id: &str, owner: &str) -> Value {
        Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::String(id.into())),
            ("owner".to_string(), Value::String(owner.into())),
        ]))
    }

    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: FilterOp::Eq,
            value: Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    fn rows() -> PlanRows {
        let mut rows = PlanRows::default();
        rows.push(b"r1".to_vec(), row("r1", "alice"));
        rows.push(b"r2".to_vec(), row("r2", "bob"));
        rows
    }

    #[test]
    fn every_row_keeps_its_key_and_ordinal() {
        let crs = nodedb_crs::CrsContext::builtin_only();
        let images = plan_images(3, "orders", &[], rows(), &crs).expect("images");
        assert_eq!(images.plan, 3);
        assert_eq!(images.collection, "orders");
        let keys: Vec<(&[u8], u32)> = images
            .rows
            .iter()
            .map(|row| (row.key.as_slice(), row.ordinal))
            .collect();
        assert_eq!(keys, vec![(&b"r1"[..], 0), (&b"r2"[..], 1)]);
        assert_eq!(images.rows[1].fields, row("r2", "bob"));
    }

    /// A row the read policy hides carries no image. The shown row keeps the
    /// ordinal it was written at.
    #[test]
    fn a_hidden_row_drops_and_the_rest_keep_their_ordinals() {
        let crs = nodedb_crs::CrsContext::builtin_only();
        let images = plan_images(0, "orders", &owner_policy("bob"), rows(), &crs).expect("images");
        assert_eq!(images.rows.len(), 1);
        assert_eq!(images.rows[0].key, b"r2".to_vec());
        assert_eq!(images.rows[0].ordinal, 1);
    }

    #[test]
    fn a_keyless_row_is_keyed_by_its_ordinal() {
        let rows = PlanRows::keyed_by_ordinal(vec![row("a", "x"), row("b", "y")]).expect("rows");
        assert_eq!(
            rows.keys,
            vec![0u32.to_be_bytes().to_vec(), 1u32.to_be_bytes().to_vec()]
        );
    }

    #[test]
    fn a_row_that_is_not_an_object_is_refused() {
        let crs = nodedb_crs::CrsContext::builtin_only();
        let mut rows = PlanRows::default();
        rows.push(b"r1".to_vec(), Value::Integer(1));
        assert!(plan_images(0, "orders", &[], rows, &crs).is_err());
    }
}
