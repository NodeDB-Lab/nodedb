// SPDX-License-Identifier: BUSL-1.1

//! Re-typing a KV row body to its collection's declared numeric columns.
//!
//! A body keeps its shape: a map stays a map, and a raw `value` body stays
//! raw. The rule per value is `strict_format::coerce_declared`, the one the
//! schemaless document path runs.

use std::borrow::Cow;

use nodedb_physical::physical_plan::{DeclaredColumn, KvOp};
use nodedb_query::msgpack_scan::{KvBodyError, kv_body_to_row, row_to_kv_body};
use nodedb_types::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::strict_format::coerce_declared;
use crate::engine::kv::AtomicError;

/// `body` with every declared column re-typed. Borrowed when no value
/// changes.
pub(in crate::data::executor) fn coerce_kv_body<'a>(
    body: &'a [u8],
    declared: &[DeclaredColumn],
) -> crate::Result<Cow<'a, [u8]>> {
    if declared.is_empty() {
        return Ok(Cow::Borrowed(body));
    }
    let (row, shape) = kv_body_to_row(body).map_err(KvBodyError::from)?;
    let Value::Object(mut map) = row else {
        return Ok(Cow::Borrowed(body));
    };
    let mut changed = false;
    for column in declared {
        let Some(slot) = map.get_mut(&column.name) else {
            continue;
        };
        let coerced = coerce_declared(slot, column)?;
        if coerced != *slot {
            *slot = coerced;
            changed = true;
        }
    }
    if !changed {
        return Ok(Cow::Borrowed(body));
    }
    Ok(Cow::Owned(row_to_kv_body(&Value::Object(map), shape)?))
}

/// The image a KV write stores for the image it computed: `None` when every
/// declared column already holds the value it stores, else the fitted image.
///
/// INCR, INCRBYFLOAT, CAS, and GETSET compute the bytes they store, and
/// TRANSFER_ITEM moves a row into a collection with its own declared columns.
/// Each one runs this on those bytes before it stores them, on every path:
/// autocommit, transaction staging, resolve, and WAL replay.
pub(in crate::data::executor) fn fit_kv_image(
    image: &[u8],
    declared: &[DeclaredColumn],
) -> crate::Result<Option<Vec<u8>>> {
    Ok(match coerce_kv_body(image, declared)? {
        Cow::Owned(bytes) => Some(bytes),
        Cow::Borrowed(_) => None,
    })
}

/// The atomic gate of a live write: fit the computed `image` to `declared`,
/// then decide the image it stores against the write policy.
pub(in crate::data::executor) fn fit_and_admit_kv_image(
    image: &[u8],
    declared: &[DeclaredColumn],
    rls_write_check: &nodedb_types::RlsWriteCheck,
    key: &[u8],
    tid: u64,
    collection: &str,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<Option<Vec<u8>>, AtomicError> {
    let fitted =
        fit_kv_image(image, declared).map_err(|error| AtomicError::Declared(Box::new(error)))?;
    super::rls::admit_kv_row(
        rls_write_check,
        fitted.as_deref().unwrap_or(image),
        key,
        tid,
        collection,
        crs,
    )
    .map_err(|error| AtomicError::Rejected(Box::new(error)))?;
    Ok(fitted)
}

/// The atomic gate of a WAL redo: fit the computed `image` to `declared`.
///
/// A redo re-applies a write whose policy verdict was reached when it was
/// first accepted, so no policy is decided here. The declared rule is part of
/// the computation, so the redo stores the bytes the live write stored, and a
/// value the live write refused is refused again.
pub(in crate::data::executor) fn fit_replayed_kv_image(
    image: &[u8],
    declared: &[DeclaredColumn],
) -> Result<Option<Vec<u8>>, AtomicError> {
    fit_kv_image(image, declared).map_err(|error| AtomicError::Declared(Box::new(error)))
}

impl CoreLoop {
    /// `op` with every row body it supplies whole re-typed to its
    /// collection's declared numeric columns. `None` when no body changes.
    ///
    /// Covers `Put`, `Insert`, `InsertIfAbsent`, and `BatchPut`. A field
    /// merge re-types its merged row in `merge_field_updates`, a conflict
    /// upsert re-types both its branches in `merge_kv_conflict_body`, and an
    /// atomic or a transfer fits the image it computes.
    pub(in crate::data::executor) fn coerce_kv_op_bodies(
        &self,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> crate::Result<Option<KvOp>> {
        let collection = match op {
            KvOp::Put { collection, .. }
            | KvOp::Insert { collection, .. }
            | KvOp::InsertIfAbsent { collection, .. }
            | KvOp::BatchPut { collection, .. } => collection.as_str(),
            // These ops carry no row body supplied whole. A field merge and a
            // conflict upsert re-type the row they compute, in the merge. An
            // atomic and a transfer fit the image they compute through
            // `fit_kv_image` or `compute_transfer`.
            KvOp::Get { .. }
            | KvOp::InsertOnConflictUpdate { .. }
            | KvOp::Delete { .. }
            | KvOp::Scan { .. }
            | KvOp::Expire { .. }
            | KvOp::Persist { .. }
            | KvOp::GetTtl { .. }
            | KvOp::BatchGet { .. }
            | KvOp::RegisterIndex { .. }
            | KvOp::DropIndex { .. }
            | KvOp::FieldGet { .. }
            | KvOp::FieldSet { .. }
            | KvOp::Truncate { .. }
            | KvOp::Incr { .. }
            | KvOp::IncrFloat { .. }
            | KvOp::Cas { .. }
            | KvOp::GetSet { .. }
            | KvOp::Transfer { .. }
            | KvOp::TransferItem { .. }
            | KvOp::RegisterSortedIndex { .. }
            | KvOp::DropSortedIndex { .. }
            | KvOp::SortedIndexRank { .. }
            | KvOp::SortedIndexTopK { .. }
            | KvOp::SortedIndexRange { .. }
            | KvOp::SortedIndexCount { .. }
            | KvOp::SortedIndexScore { .. }
            | KvOp::SortedIndexTxnRead { .. }
            | KvOp::MaterializeScan { .. }
            | KvOp::ResolveWrite(_)
            | KvOp::ResolvedWrite { .. }
            | KvOp::PredicateUpdate { .. }
            | KvOp::PredicateDelete { .. } => return Ok(None),
        };
        let declared = self.declared_columns_of(did, tid, collection);
        if declared.is_empty() {
            return Ok(None);
        }

        if let KvOp::BatchPut { entries, .. } = op {
            let mut coerced: Vec<Option<Vec<u8>>> = Vec::with_capacity(entries.len());
            for (_, value) in entries {
                coerced.push(match coerce_kv_body(value, declared)? {
                    Cow::Owned(bytes) => Some(bytes),
                    Cow::Borrowed(_) => None,
                });
            }
            if coerced.iter().all(Option::is_none) {
                return Ok(None);
            }
            let mut rewritten = op.clone();
            if let KvOp::BatchPut { entries, .. } = &mut rewritten {
                for ((_, value), bytes) in entries.iter_mut().zip(coerced) {
                    if let Some(bytes) = bytes {
                        *value = bytes;
                    }
                }
            }
            return Ok(Some(rewritten));
        }

        let (KvOp::Put { value, .. }
        | KvOp::Insert { value, .. }
        | KvOp::InsertIfAbsent { value, .. }) = op
        else {
            return Ok(None);
        };
        let Cow::Owned(bytes) = coerce_kv_body(value, declared)? else {
            return Ok(None);
        };
        let mut rewritten = op.clone();
        if let KvOp::Put { value, .. }
        | KvOp::Insert { value, .. }
        | KvOp::InsertIfAbsent { value, .. } = &mut rewritten
        {
            *value = bytes;
        }
        Ok(Some(rewritten))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn declared(declared: &str) -> Vec<DeclaredColumn> {
        vec![DeclaredColumn::from_declared("v", declared).expect("numeric declaration")]
    }

    fn map_body(value: Value) -> Vec<u8> {
        let mut map = HashMap::new();
        map.insert("v".to_string(), value);
        map.insert("note".to_string(), Value::String("x".into()));
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode body")
    }

    #[test]
    fn map_body_is_retyped_and_stays_a_map() {
        let body = map_body(Value::String("1.005".into()));
        let coerced = coerce_kv_body(&body, &declared("DECIMAL(5,2)")).expect("fits");
        let row = nodedb_types::value_from_msgpack(&coerced).expect("map body");
        assert_eq!(row.get("v"), Some(&Value::String("1.01".into())));
        assert_eq!(row.get("note"), Some(&Value::String("x".into())));
    }

    #[test]
    fn unchanged_body_is_borrowed() {
        let body = map_body(Value::String("1.50".into()));
        assert!(matches!(
            coerce_kv_body(&body, &declared("DECIMAL(5,2)")).expect("fits"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn raw_value_body_stays_raw() {
        let declared =
            vec![DeclaredColumn::from_declared("value", "DECIMAL(5,2)").expect("decimal")];
        let coerced = coerce_kv_body(b"1.005", &declared).expect("fits");
        assert_eq!(coerced.as_ref(), b"1.01");
    }

    #[test]
    fn computed_image_is_fitted_or_refused() {
        let declared =
            vec![DeclaredColumn::from_declared("value", "DECIMAL(5,2)").expect("decimal")];
        assert_eq!(
            fit_kv_image(b"1.505", &declared).expect("fits"),
            Some(b"1.51".to_vec())
        );
        assert_eq!(fit_kv_image(b"1.50", &declared).expect("unchanged"), None);
        let err = fit_replayed_kv_image(b"1000.99", &declared).expect_err("past precision");
        assert!(
            matches!(
                err,
                AtomicError::Declared(ref e)
                    if matches!(**e, crate::Error::NumericValueOutOfRange { .. })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn out_of_range_body_is_refused() {
        let err = coerce_kv_body(&map_body(Value::Integer(40000)), &declared("SMALLINT"))
            .expect_err("past smallint");
        assert!(
            matches!(err, crate::Error::NumericValueOutOfRange { .. }),
            "{err:?}"
        );
    }
}
