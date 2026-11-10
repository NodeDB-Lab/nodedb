// SPDX-License-Identifier: BUSL-1.1

//! Storage-mode-aware `ScanFilter` predicate evaluation on a STORED row body.
//!
//! `ScanFilter::matches_binary` operates on MessagePack. Strict (Binary
//! Tuple) collections store rows in a different wire format, so a predicate
//! evaluated directly against the raw stored bytes of a strict row silently
//! never matches. Every caller that re-checks a scan predicate against a
//! stored body — the base document scan, the bulk DML base scan, and the
//! in-transaction predicate staging handlers (`stage_bulk_update`,
//! `stage_bulk_delete`) — must decode Binary Tuple bodies to MessagePack
//! first. This is the single, shared implementation of that decode-then-match
//! step so the staging handlers (and, ideally, the two base-scan call sites)
//! evaluate strict predicates identically.

use nodedb_crs::CrsContext;
use nodedb_query::EvalError;
use nodedb_types::StorageKey;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::strict_format;
use crate::types::TenantId;

use super::CoreLoop;

/// The first filter-evaluation error a scan's infallible predicate met.
///
/// Storage scans take a `bool` predicate. A fallible matcher runs through
/// [`Self::check`], which holds its first error and reads it as no match.
/// The caller returns the held error once the scan ends, so a division by
/// zero fails the statement instead of excluding the row.
#[derive(Default)]
pub(in crate::data::executor) struct HeldEvalError(std::cell::RefCell<Option<EvalError>>);

impl HeldEvalError {
    /// The verdict of `eval`. After an error is held, no row is evaluated
    /// and every row reads as no match.
    pub(in crate::data::executor) fn check(
        &self,
        eval: impl FnOnce() -> Result<bool, EvalError>,
    ) -> bool {
        if self.0.borrow().is_some() {
            return false;
        }
        match eval() {
            Ok(matched) => matched,
            Err(e) => {
                self.0.replace(Some(e));
                false
            }
        }
    }

    /// `value`, or the held error when a predicate met one.
    pub(in crate::data::executor) fn finish<T, E: From<EvalError>>(
        &self,
        value: T,
    ) -> Result<T, E> {
        match self.0.borrow_mut().take() {
            Some(e) => Err(E::from(e)),
            None => Ok(value),
        }
    }
}

/// Evaluate `filters` against a stored row `body`, given the row's ALREADY
/// RESOLVED strict schema (`Some` for a strict collection, `None` for
/// schemaless). Callers that check many rows against the same
/// collection/filters (a scan, a bulk staging match) must resolve the schema
/// ONCE outside the per-row loop via [`CoreLoop::resolve_strict_schema`] or
/// [`CoreLoop::strict_aware_matcher`] — this function does no lookup itself,
/// so it never re-pays the `doc_configs` hash lookup per row.
///
/// Returns `Ok(false)` for a strict body that fails to decode against its
/// own schema (a malformed or stale Binary Tuple never matches, mirroring
/// the base scan's behavior). Returns `Err(EvalError::DivisionByZero)` when
/// `filters` contains a `FilterOp::Expr` predicate that divides or takes a
/// modulus by zero — this is the base document scan's WHERE predicate, so
/// the behavior-flip rule applies: the query fails instead of the row being
/// silently excluded. Geo functions in an expression filter resolve SRIDs
/// through `crs`.
///
/// `row_key` is the row's storage key. A row that lacks its identity column
/// carries its identity only in that key, so the image is matched with the
/// identity injected under `identity_column` — the same injection
/// [`super::super::row_shape::sparse_row_to_doc`] applies to a materialized
/// row, so a predicate on the identity column sees the identity a reader of
/// the same row sees. A declared-key row holds its key and gains no `id`.
pub(in crate::data::executor) fn matches_with_resolved_schema(
    strict_schema: Option<&StrictSchema>,
    filters: &[ScanFilter],
    row_key: &StorageKey,
    body: &[u8],
    identity_column: &str,
    crs: &CrsContext<'_>,
) -> Result<bool, EvalError> {
    let mut doc = match strict_schema {
        Some(schema) => match strict_format::binary_tuple_to_row_value(body, schema) {
            Some(value) => value,
            None => return Ok(false),
        },
        None => crate::data::executor::doc_format::decode_document_value(body).map_err(|_| {
            EvalError::ArgumentType {
                function: "stored document predicate",
                position: 1,
                expected: "readable document body",
                got: "malformed document body",
            }
        })?,
    };
    if !matches!(doc, nodedb_types::Value::Object(_)) {
        return Err(EvalError::ArgumentType {
            function: "stored document predicate",
            position: 1,
            expected: "document object",
            got: doc.type_name(),
        });
    }
    super::super::row_shape::attach_sparse_row_identity(&mut doc, row_key, identity_column);
    ScanFilter::all_match_value(filters, &doc, crs)
}

impl CoreLoop {
    /// Resolve `collection`'s strict schema (if any) under tenant `tid`, once.
    /// `Some` for a strict (Binary Tuple) collection, `None` for schemaless
    /// (already MessagePack). Extracted so callers that build a per-row
    /// matcher over a scan resolve this a single time rather than per row.
    pub(in crate::data::executor) fn resolve_strict_schema(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Option<StrictSchema> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        })
    }

    /// Build a reusable `Fn(&StorageKey, &[u8]) -> Result<bool, EvalError>`
    /// closure evaluating `filters` against a stored row's `(row_key, body)`,
    /// resolving `collection`'s strict schema ONCE up front (not per row) and
    /// capturing it in the closure. A scan that takes an infallible predicate
    /// wraps it with [`HeldEvalError::check`]. Geo functions resolve SRIDs
    /// through `crs`.
    pub(in crate::data::executor) fn strict_aware_matcher<'a>(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        filters: &'a [ScanFilter],
        crs: &'a CrsContext<'a>,
    ) -> impl Fn(&StorageKey, &[u8]) -> Result<bool, EvalError> + 'a {
        let strict_schema = self.resolve_strict_schema(database_id, tid, collection);
        let identity_column = self.identity_column(database_id, tid, collection);
        move |row_key: &StorageKey, body: &[u8]| {
            matches_with_resolved_schema(
                strict_schema.as_ref(),
                filters,
                row_key,
                body,
                &identity_column,
                crs,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::FilterOp;
    use nodedb_types::{Surrogate, Value};

    #[test]
    fn native_predicate_reads_exact_fields_and_stored_identity() {
        let key = StorageKey::for_surrogate(Surrogate::new(7));
        let amount = Value::Decimal("9007199254740993.0001".parse().expect("decimal"));
        let doc = Value::Object(
            [
                ("amount".into(), amount.clone()),
                (nodedb_types::ROWID_COLUMN.into(), Value::Integer(42)),
            ]
            .into_iter()
            .collect(),
        );
        let bytes = zerompk::to_msgpack_vec(&doc).expect("native body");
        let filters = [
            ScanFilter {
                field: "amount".into(),
                op: FilterOp::Eq,
                value: amount,
                clauses: vec![],
                expr: None,
            },
            ScanFilter {
                field: "sku".into(),
                op: FilterOp::Eq,
                value: Value::String("42".into()),
                clauses: vec![],
                expr: None,
            },
        ];
        assert!(
            matches_with_resolved_schema(
                None,
                &filters,
                &key,
                &bytes,
                "sku",
                &CrsContext::builtin_only()
            )
            .expect("predicate")
        );
        assert!(
            matches_with_resolved_schema(
                None,
                &filters,
                &key,
                &[0x92, 0x01],
                "sku",
                &CrsContext::builtin_only()
            )
            .is_err()
        );
    }
}
