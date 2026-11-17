// SPDX-License-Identifier: BUSL-1.1

//! Pure value computation for the atomic `Transfer` op (fungible balance
//! move), shared by the autocommit handler (`transfer.rs`) and the
//! in-transaction staging handler (`stage_kv_transfer.rs`) so a staged
//! value and its COMMIT-time durable replay are always computed by the
//! exact same code — mirrors the `nodedb_physical::kv_atomic::compute` / `stage_kv_atomic`
//! split for `Incr`/`Cas`/etc.
//!
//! A `DECIMAL` field moves by exact decimal arithmetic. Every other field
//! moves by float arithmetic.

use std::collections::HashMap;
use std::fmt;

use nodedb_physical::physical_plan::{DeclaredColumn, TransferAmount};
use nodedb_query::msgpack_scan::{KvBodyShape, kv_body_shape, kv_body_to_row, row_to_kv_body};
use nodedb_types::Value;
use nodedb_types::columnar::ColumnType;
use rust_decimal::Decimal;

use crate::data::executor::strict_format::coerce_declared_row;

/// Failure modes of [`compute_transfer`], translated to `ErrorCode` at each
/// call site (the live handler and the staging handler render slightly
/// different `ErrorCode` variants around the same detail message).
#[derive(Debug)]
pub(in crate::data::executor) enum TransferError {
    TypeMismatch(String),
    InsufficientBalance {
        have: TransferNumber,
        need: TransferNumber,
    },
    /// A computed balance breaks a declared column rule of the collection,
    /// such as a `SMALLINT` range or a `DECIMAL(p,s)` precision. An exact
    /// sum past the `Decimal` range is refused the same way.
    Declared(crate::Error),
}

/// A balance or amount of a transfer, in the arithmetic its field uses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::data::executor) enum TransferNumber {
    Float(f64),
    Decimal(Decimal),
}

impl TransferNumber {
    /// The response form: a float is a JSON number, a decimal its exact
    /// text.
    pub(in crate::data::executor) fn to_json(self) -> serde_json::Value {
        match self {
            Self::Float(f) => serde_json::json!(f),
            Self::Decimal(d) => serde_json::Value::String(d.to_string()),
        }
    }
}

impl fmt::Display for TransferNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(v) => write!(f, "{v}"),
            Self::Decimal(d) => write!(f, "{d}"),
        }
    }
}

/// The two updated document bodies and post-transfer balances for a
/// `Transfer` op, computed from BASE ∪ OVERLAY current values.
#[derive(Debug)]
pub(in crate::data::executor) struct TransferComputation {
    pub new_source: Vec<u8>,
    pub new_dest: Vec<u8>,
    /// The amount moved, in the field's arithmetic.
    pub amount: TransferNumber,
    /// The stored balances. A decimal balance is the value its declared
    /// column fitted.
    pub source_balance_after: TransferNumber,
    pub dest_balance_after: TransferNumber,
}

/// Compute the read-validate-write outcome of an atomic fungible transfer.
///
/// `dest_bytes` is `None` when the destination key does not exist under
/// BASE ∪ OVERLAY -- a fresh document is created holding just `field`. A
/// destination that exists but lacks `field` starts from 0. Either side
/// holding a bare value (the single-`value` SQL form, RESP `SET`) or a
/// non-numeric `field` is a type mismatch, never silently treated as 0.
///
/// A `DECIMAL` field moves by exact decimal arithmetic: the planner sent a
/// decimal amount, or the collection declares the field `DECIMAL`. The
/// balance check is exact, and the results are stored as decimal text.
///
/// Both rows then meet the collection's `declared` numeric columns, so a
/// balance past a declared width refuses the whole transfer. A `DECIMAL(p,s)`
/// balance rounds to `s` digits, half away from zero.
pub(in crate::data::executor) fn compute_transfer(
    source_bytes: &[u8],
    dest_bytes: Option<&[u8]>,
    field: &str,
    amount: TransferAmount,
    declared: &[DeclaredColumn],
) -> Result<TransferComputation, TransferError> {
    let mut source = map_row(source_bytes, "source")?;
    // Each side re-encodes in the shape it was read in, so a native row keeps
    // its typed columns. A fresh destination takes the source's shape.
    let source_shape = kv_body_shape(source_bytes);
    let dest_shape = dest_bytes
        .filter(|bytes| !bytes.is_empty())
        .map_or(source_shape, kv_body_shape);
    let Moved {
        mut dest,
        amount,
        source_after,
        dest_after,
    } = if moves_decimal(declared, field, amount) {
        move_decimal(&mut source, dest_bytes, field, amount)?
    } else {
        move_float(&mut source, dest_bytes, field, amount.to_f64())?
    };
    coerce_declared_row(&mut source, declared).map_err(TransferError::Declared)?;
    coerce_declared_row(&mut dest, declared).map_err(TransferError::Declared)?;

    Ok(TransferComputation {
        source_balance_after: fitted(&source, field, source_after),
        dest_balance_after: fitted(&dest, field, dest_after),
        new_source: encode_map(source, source_shape, "source")?,
        new_dest: encode_map(dest, dest_shape, "destination")?,
        amount,
    })
}

/// The destination row and the balances one arithmetic computed.
struct Moved {
    dest: HashMap<String, Value>,
    amount: TransferNumber,
    source_after: TransferNumber,
    dest_after: TransferNumber,
}

/// Whether `field` moves by exact decimal arithmetic.
fn moves_decimal(declared: &[DeclaredColumn], field: &str, amount: TransferAmount) -> bool {
    matches!(amount, TransferAmount::Decimal(_))
        || declared
            .iter()
            .any(|c| c.name == field && matches!(c.column_type, ColumnType::Decimal(_)))
}

/// Move `amount` by float arithmetic. A whole result stays an integer.
fn move_float(
    source: &mut HashMap<String, Value>,
    dest_bytes: Option<&[u8]>,
    field: &str,
    amount: f64,
) -> Result<Moved, TransferError> {
    let source_balance = float_field(source, field)?.ok_or_else(|| missing(field))?;
    if source_balance < amount {
        return Err(TransferError::InsufficientBalance {
            have: TransferNumber::Float(source_balance),
            need: TransferNumber::Float(amount),
        });
    }
    let mut dest = dest_row(dest_bytes)?;
    let dest_balance = float_field(&dest, field)?.unwrap_or(0.0);

    let source_after = source_balance - amount;
    let dest_after = dest_balance + amount;
    source.insert(field.to_string(), numeric_value(source_after));
    dest.insert(field.to_string(), numeric_value(dest_after));
    Ok(Moved {
        dest,
        amount: TransferNumber::Float(amount),
        source_after: TransferNumber::Float(source_after),
        dest_after: TransferNumber::Float(dest_after),
    })
}

/// Move `amount` by exact decimal arithmetic. Both results are stored as
/// decimal text, the form a decimal column stores.
fn move_decimal(
    source: &mut HashMap<String, Value>,
    dest_bytes: Option<&[u8]>,
    field: &str,
    amount: TransferAmount,
) -> Result<Moved, TransferError> {
    let amount = amount.to_decimal().ok_or_else(|| {
        out_of_range(format!(
            "TRANSFER amount {amount} is out of range for DECIMAL field '{field}'"
        ))
    })?;
    let source_balance = decimal_field(source, field)?.ok_or_else(|| missing(field))?;
    if source_balance < amount {
        return Err(TransferError::InsufficientBalance {
            have: TransferNumber::Decimal(source_balance),
            need: TransferNumber::Decimal(amount),
        });
    }
    let mut dest = dest_row(dest_bytes)?;
    let dest_balance = decimal_field(&dest, field)?.unwrap_or(Decimal::ZERO);

    let source_after = source_balance.checked_sub(amount).ok_or_else(|| {
        out_of_range(format!(
            "field '{field}': {source_balance} - {amount} is out of the DECIMAL range"
        ))
    })?;
    let dest_after = dest_balance.checked_add(amount).ok_or_else(|| {
        out_of_range(format!(
            "field '{field}': {dest_balance} + {amount} is out of the DECIMAL range"
        ))
    })?;
    source.insert(field.to_string(), Value::String(source_after.to_string()));
    dest.insert(field.to_string(), Value::String(dest_after.to_string()));
    Ok(Moved {
        dest,
        amount: TransferNumber::Decimal(amount),
        source_after: TransferNumber::Decimal(source_after),
        dest_after: TransferNumber::Decimal(dest_after),
    })
}

/// The stored balance of `field` once the declared rule fitted it. A float
/// balance is the computed value. A decimal balance is read back, so the
/// answer shows the rounding its typmod applied.
fn fitted(row: &HashMap<String, Value>, field: &str, computed: TransferNumber) -> TransferNumber {
    let TransferNumber::Decimal(_) = computed else {
        return computed;
    };
    match row.get(field) {
        Some(Value::String(text)) => text
            .trim()
            .parse()
            .map_or(computed, TransferNumber::Decimal),
        Some(Value::Decimal(d)) => TransferNumber::Decimal(*d),
        _ => computed,
    }
}

/// The SQLSTATE `22003` refusal of an exact balance.
fn out_of_range(detail: String) -> TransferError {
    TransferError::Declared(crate::Error::NumericValueOutOfRange { detail })
}

/// The refusal of a source row that holds no number in `field`.
fn missing(field: &str) -> TransferError {
    TransferError::TypeMismatch(format!("field '{field}' is not numeric or missing"))
}

/// The destination row: empty when the key is absent.
fn dest_row(dest_bytes: Option<&[u8]>) -> Result<HashMap<String, Value>, TransferError> {
    match dest_bytes.filter(|b| !b.is_empty()) {
        None => Ok(HashMap::with_capacity(1)),
        Some(bytes) => map_row(bytes, "destination"),
    }
}

/// Decode a KV body as the typed-column map a transfer operates on.
fn map_row(bytes: &[u8], side: &str) -> Result<HashMap<String, Value>, TransferError> {
    let (row, shape) = kv_body_to_row(bytes)
        .map_err(|e| TransferError::TypeMismatch(format!("{side} body: {e}")))?;
    if shape == KvBodyShape::Raw {
        return Err(TransferError::TypeMismatch(format!(
            "{side} holds a bare value, not a hash"
        )));
    }
    match row {
        Value::Object(map) => Ok(map),
        other => Err(TransferError::TypeMismatch(format!(
            "{side} body is {}, not an object",
            other.type_name()
        ))),
    }
}

/// `field` as f64: `Ok(None)` when absent, a type mismatch when present but
/// not numeric.
fn float_field(map: &HashMap<String, Value>, field: &str) -> Result<Option<f64>, TransferError> {
    match map.get(field) {
        None => Ok(None),
        Some(Value::Float(f)) => Ok(Some(*f)),
        Some(Value::Integer(i)) => Ok(Some(*i as f64)),
        Some(other) => Err(TransferError::TypeMismatch(format!(
            "field '{field}' is {}, not numeric",
            other.type_name()
        ))),
    }
}

/// `field` as an exact decimal: `Ok(None)` when absent, a type mismatch
/// when present but not a number. A decimal column stores its value as
/// text. A float converts through its shortest round-trip text.
fn decimal_field(
    map: &HashMap<String, Value>,
    field: &str,
) -> Result<Option<Decimal>, TransferError> {
    let parsed = match map.get(field) {
        None => return Ok(None),
        Some(Value::Decimal(d)) => Some(*d),
        Some(Value::Integer(i)) => Some(Decimal::from(*i)),
        Some(Value::Float(f)) => f.to_string().parse().ok(),
        Some(Value::String(text)) => text.trim().parse().ok(),
        Some(other) => {
            return Err(TransferError::TypeMismatch(format!(
                "field '{field}' is {}, not numeric",
                other.type_name()
            )));
        }
    };
    parsed.map(Some).ok_or_else(|| {
        TransferError::TypeMismatch(format!("field '{field}' does not hold a DECIMAL value"))
    })
}

/// A whole-number balance stays an integer on disk; anything else is a float.
fn numeric_value(v: f64) -> Value {
    if v.fract() == 0.0 && v >= i64::MIN as f64 && v <= i64::MAX as f64 {
        Value::Integer(v as i64)
    } else {
        Value::Float(v)
    }
}

fn encode_map(
    map: HashMap<String, Value>,
    shape: KvBodyShape,
    side: &str,
) -> Result<Vec<u8>, TransferError> {
    row_to_kv_body(&Value::Object(map), shape)
        .map_err(|e| TransferError::TypeMismatch(format!("serialize {side}: {e}")))
}

/// Extract a numeric field from a MessagePack-encoded KV value.
#[cfg(test)]
pub(in crate::data::executor) fn extract_numeric_field(value: &[u8], field: &str) -> Option<f64> {
    let (row, _) = kv_body_to_row(value).ok()?;
    match row.get(field)? {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(field: &str, value: f64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ field: value })).unwrap()
    }

    fn text_doc(field: &str, value: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ field: value })).unwrap()
    }

    fn float(v: f64) -> TransferAmount {
        TransferAmount::Float(v)
    }

    fn dec(text: &str) -> Decimal {
        text.parse().expect("test decimal parses")
    }

    fn exact(text: &str) -> TransferAmount {
        TransferAmount::Decimal(dec(text))
    }

    fn decimal_10_2() -> [DeclaredColumn; 1] {
        [DeclaredColumn::from_declared("balance", "DECIMAL(10,2)").expect("decimal")]
    }

    /// The stored cell of `field`.
    fn stored(body: &[u8], field: &str) -> Value {
        let (row, _) = kv_body_to_row(body).expect("decode body");
        row.get(field).cloned().expect("field present")
    }

    #[test]
    fn transfer_moves_balance_between_existing_docs() {
        let source = doc("balance", 100.0);
        let dest = doc("balance", 10.0);
        let result = compute_transfer(&source, Some(&dest), "balance", float(30.0), &[]).unwrap();
        assert_eq!(result.source_balance_after, TransferNumber::Float(70.0));
        assert_eq!(result.dest_balance_after, TransferNumber::Float(40.0));
    }

    /// A credit that takes a declared `SMALLINT` balance past its width
    /// refuses the whole transfer.
    #[test]
    fn transfer_past_a_declared_width_is_refused() {
        let declared = [DeclaredColumn::from_declared("balance", "SMALLINT").expect("smallint")];
        let source = doc("balance", 100.0);
        let dest = doc("balance", 32700.0);
        let err = compute_transfer(&source, Some(&dest), "balance", float(100.0), &declared)
            .expect_err("credit past smallint");
        assert!(
            matches!(
                err,
                TransferError::Declared(crate::Error::NumericValueOutOfRange { .. })
            ),
            "{err:?}"
        );

        let fits = compute_transfer(&source, Some(&dest), "balance", float(50.0), &declared)
            .expect("credit fits smallint");
        assert_eq!(
            extract_numeric_field(&fits.new_dest, "balance"),
            Some(32750.0)
        );
    }

    #[test]
    fn transfer_creates_dest_when_absent() {
        let source = doc("balance", 100.0);
        let result = compute_transfer(&source, None, "balance", float(30.0), &[]).unwrap();
        assert_eq!(result.dest_balance_after, TransferNumber::Float(30.0));
        assert_eq!(
            extract_numeric_field(&result.new_dest, "balance"),
            Some(30.0)
        );
    }

    #[test]
    fn transfer_rejects_insufficient_balance() {
        let source = doc("balance", 10.0);
        let err = compute_transfer(&source, None, "balance", float(30.0), &[]);
        assert!(matches!(
            err,
            Err(TransferError::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn transfer_rejects_a_bare_value_destination() {
        // A raw body (the single-`value` form, RESP `SET`) is not a hash:
        // never treated as a zero balance and re-encoded as a map.
        let source = doc("balance", 100.0);
        let err = compute_transfer(&source, Some(b"5"), "balance", float(30.0), &[]);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_a_bare_value_source() {
        let err = compute_transfer(b"100", None, "balance", float(30.0), &[]);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_non_numeric_destination_field() {
        let source = doc("balance", 100.0);
        let dest = text_doc("balance", "abc");
        let err = compute_transfer(&source, Some(&dest), "balance", float(30.0), &[]);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }

    #[test]
    fn transfer_rejects_non_numeric_field() {
        let source = text_doc("balance", "abc");
        let err = compute_transfer(&source, None, "balance", float(30.0), &[]);
        assert!(matches!(err, Err(TransferError::TypeMismatch(_))));
    }

    /// A `DECIMAL(10,2)` balance moves by the exact amount and is stored as
    /// decimal text.
    #[test]
    fn decimal_transfer_is_exact() {
        let source = text_doc("balance", "100.00");
        let dest = text_doc("balance", "5.25");
        let result = compute_transfer(
            &source,
            Some(&dest),
            "balance",
            exact("30.10"),
            &decimal_10_2(),
        )
        .expect("decimal transfer");
        assert_eq!(
            stored(&result.new_source, "balance"),
            Value::String("69.90".into())
        );
        assert_eq!(
            stored(&result.new_dest, "balance"),
            Value::String("35.35".into())
        );
        assert_eq!(result.amount, TransferNumber::Decimal(dec("30.10")));
        assert_eq!(
            result.source_balance_after,
            TransferNumber::Decimal(dec("69.90"))
        );
        assert_eq!(
            result.dest_balance_after,
            TransferNumber::Decimal(dec("35.35"))
        );
    }

    /// Each balance rounds to the declared scale, half away from zero.
    #[test]
    fn decimal_transfer_rounds_each_balance_to_its_scale() {
        let source = text_doc("balance", "100.00");
        let dest = text_doc("balance", "5.25");
        let result = compute_transfer(
            &source,
            Some(&dest),
            "balance",
            exact("0.005"),
            &decimal_10_2(),
        )
        .expect("decimal transfer");
        assert_eq!(
            stored(&result.new_source, "balance"),
            Value::String("100.00".into())
        );
        assert_eq!(
            stored(&result.new_dest, "balance"),
            Value::String("5.26".into())
        );
        assert_eq!(
            result.dest_balance_after,
            TransferNumber::Decimal(dec("5.26"))
        );
    }

    /// A credit past `DECIMAL(10,2)` is refused with `22003`.
    #[test]
    fn decimal_credit_past_the_precision_is_refused() {
        let source = text_doc("balance", "100.00");
        let dest = text_doc("balance", "99999999.99");
        let err = compute_transfer(&source, Some(&dest), "balance", exact("1"), &decimal_10_2())
            .expect_err("credit past precision");
        assert!(
            matches!(
                err,
                TransferError::Declared(crate::Error::NumericValueOutOfRange { .. })
            ),
            "{err:?}"
        );
    }

    /// The balance check compares exact decimals.
    #[test]
    fn decimal_balance_check_is_exact() {
        let source = text_doc("balance", "0.10");
        let err = compute_transfer(&source, None, "balance", exact("0.11"), &decimal_10_2())
            .expect_err("0.10 does not cover 0.11");
        assert!(
            matches!(
                err,
                TransferError::InsufficientBalance {
                    have: TransferNumber::Decimal(_),
                    need: TransferNumber::Decimal(_),
                }
            ),
            "{err:?}"
        );
        let all = compute_transfer(&source, None, "balance", exact("0.10"), &decimal_10_2())
            .expect("0.10 covers 0.10");
        assert_eq!(
            stored(&all.new_source, "balance"),
            Value::String("0.00".into())
        );
        assert_eq!(
            stored(&all.new_dest, "balance"),
            Value::String("0.10".into())
        );
    }

    /// An integer amount moves a decimal balance. A float amount converts
    /// through its literal text.
    #[test]
    fn integer_and_float_amounts_move_a_declared_decimal() {
        let source = text_doc("balance", "10.50");
        let int = compute_transfer(&source, None, "balance", exact("3"), &decimal_10_2())
            .expect("integer amount");
        assert_eq!(
            stored(&int.new_source, "balance"),
            Value::String("7.50".into())
        );
        let tenth = compute_transfer(&source, None, "balance", float(0.1), &decimal_10_2())
            .expect("float amount");
        assert_eq!(
            stored(&tenth.new_source, "balance"),
            Value::String("10.40".into())
        );
    }

    /// A plain `DECIMAL` field is not declared. The planner's decimal amount
    /// still moves it exactly.
    #[test]
    fn decimal_amount_moves_a_plain_decimal_field() {
        let source = text_doc("balance", "1.5");
        let result =
            compute_transfer(&source, None, "balance", exact("0.25"), &[]).expect("plain decimal");
        assert_eq!(
            stored(&result.new_source, "balance"),
            Value::String("1.25".into())
        );
        assert_eq!(
            stored(&result.new_dest, "balance"),
            Value::String("0.25".into())
        );
    }

    #[test]
    fn decimal_amount_refuses_non_numeric_text() {
        let source = text_doc("balance", "abc");
        let err = compute_transfer(&source, None, "balance", exact("1"), &[]);
        assert!(
            matches!(err, Err(TransferError::TypeMismatch(_))),
            "{err:?}"
        );
    }
}
