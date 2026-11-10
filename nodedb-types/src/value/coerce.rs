// SPDX-License-Identifier: Apache-2.0

//! Type-coerced equality and ordering for `Value`.
//!
//! Single source of truth for type coercion in filter/sort evaluation.
//! Numbers compare through the shared order in [`crate::numeric_cmp`].

use std::cmp::Ordering;

use super::core::Value;
use crate::numeric_cmp::{Numeric, cmp_numeric, decimal_reading, parse_numeric_str};

impl Value {
    /// Coerced equality: `Value` vs `Value` with numeric/string coercion.
    ///
    /// Single source of truth for type coercion in filter evaluation.
    /// Used by `matches_binary` (msgpack path) and `matches_value` (Value path).
    ///
    /// Two strings are equal by their text or by the instant they denote.
    /// Any other pair where both sides read as numbers is equal when
    /// [`cmp_numeric`] orders it `Equal`: exact for integers and decimals,
    /// and NaN equals NaN. Two UUIDs are equal by the UUID they denote. A
    /// UUID equals a string that is its canonical text, so a typed UUID
    /// matches the text literal that names it.
    pub fn eq_coerced(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a.eq_ignore_ascii_case(b),
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Uuid(u), Value::String(s)) | (Value::String(s), Value::Uuid(u)) => {
                *s == canonical_uuid_text(u)
            }
            (Value::String(a), Value::String(b)) => {
                a == b
                    || matches!(
                        (crate::NdbDateTime::parse(a), crate::NdbDateTime::parse(b)),
                        (Some(x), Some(y)) if x.micros == y.micros
                    )
            }
            // Structural equality on ND cells: same coords and same attrs.
            (Value::ArrayCell(a), Value::ArrayCell(b)) => a == b,
            (a, b) => {
                if let (Some(x), Some(y)) = (numeric_reading(a), numeric_reading(b)) {
                    return cmp_numeric(x, y) == Ordering::Equal;
                }
                match (datetime_micros(a), datetime_micros(b)) {
                    (Some(x), Some(y)) => x == y,
                    _ => false,
                }
            }
        }
    }

    /// The hash bucket of this value under [`Value::eq_coerced`], for an
    /// equi-join or an index probe that must find every pair a WHERE `=`
    /// matches.
    ///
    /// Two values `eq_coerced` reports equal share a bucket. Two values in
    /// one bucket need not be equal (`'1.0'` and `'1'` as text, or two
    /// integers that round to one `f64`), so a join confirms each candidate
    /// with `eq_coerced`. `None` for a value that equals nothing, NULL
    /// included: it joins no row.
    ///
    /// A string reads first as a number, then as an instant, then as text.
    /// A string with both a numeric and an instant reading buckets as a
    /// number.
    pub fn eq_coerced_bucket(&self) -> Option<EqCoercedBucket> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(EqCoercedBucket::Bool(*b)),
            Value::ArrayCell(_) => Some(EqCoercedBucket::Cell),
            other => {
                if let Some(n) = numeric_reading(other) {
                    return Some(EqCoercedBucket::Number(
                        crate::numeric_cmp::numeric_bucket_bits(n),
                    ));
                }
                if let Some(micros) = datetime_micros(other) {
                    return Some(EqCoercedBucket::Instant(micros));
                }
                match other {
                    Value::String(s) => Some(EqCoercedBucket::Text(s.clone())),
                    // A UUID shares the bucket of its canonical text, the
                    // one string it equals.
                    Value::Uuid(u) => Some(EqCoercedBucket::Text(canonical_uuid_text(u))),
                    Value::Bytes(b) => Some(EqCoercedBucket::Bytes(b.clone())),
                    _ => None,
                }
            }
        }
    }

    /// Coerced partial ordering for predicate evaluation.
    ///
    /// Two numbers (or numeric strings) order by [`cmp_numeric`]: exact for
    /// integers and decimals, NaN above every number and equal to NaN, as
    /// in PostgreSQL. Two instants (or ISO-8601 strings) order by epoch
    /// microseconds, two other strings lexicographically, and two ND cells
    /// coordinate-major. A pair with no defined order — an integer against
    /// an instant, text against a number, a bool against a number — is
    /// `None`, so a range predicate over it matches nothing rather than
    /// every row: the row-level counterpart of PostgreSQL refusing to
    /// compare the two types.
    pub fn partial_cmp_coerced(&self, other: &Value) -> Option<Ordering> {
        if let (Value::ArrayCell(a), Value::ArrayCell(b)) = (self, other) {
            for (x, y) in a.coords.iter().zip(b.coords.iter()) {
                match x.partial_cmp_coerced(y)? {
                    Ordering::Equal => continue,
                    non_eq => return Some(non_eq),
                }
            }
            match a.coords.len().cmp(&b.coords.len()) {
                Ordering::Equal => {}
                non_eq => return Some(non_eq),
            }
            for (x, y) in a.attrs.iter().zip(b.attrs.iter()) {
                match x.partial_cmp_coerced(y)? {
                    Ordering::Equal => continue,
                    non_eq => return Some(non_eq),
                }
            }
            return Some(a.attrs.len().cmp(&b.attrs.len()));
        }
        if let (Some(a), Some(b)) = (numeric_reading(self), numeric_reading(other)) {
            return Some(cmp_numeric(a, b));
        }
        if let (Some(a), Some(b)) = (datetime_micros(self), datetime_micros(other)) {
            return Some(a.cmp(&b));
        }
        match (self, other) {
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            // A UUID orders by its canonical text, as against the text that
            // names it.
            (Value::Uuid(a), Value::Uuid(b)) => {
                Some(canonical_uuid_text(a).cmp(&canonical_uuid_text(b)))
            }
            (Value::Uuid(u), Value::String(s)) => Some(canonical_uuid_text(u).as_str().cmp(s)),
            (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
            (Value::String(s), Value::Uuid(u)) => Some(s.as_str().cmp(&canonical_uuid_text(u))),
            _ => None,
        }
    }

    /// Coerced total ordering for sorting.
    ///
    /// [`Value::partial_cmp_coerced`] with an unordered pair placed as
    /// `Equal`, so a stable sort keeps such rows in their input order. Only
    /// for ORDER BY / MIN / MAX style paths that need an `Ordering` for every
    /// pair; a predicate uses `partial_cmp_coerced` so an unordered pair
    /// matches nothing.
    pub fn cmp_coerced(&self, other: &Value) -> Ordering {
        self.partial_cmp_coerced(other).unwrap_or(Ordering::Equal)
    }
}

/// A hash bucket of [`Value::eq_coerced`]. See [`Value::eq_coerced_bucket`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EqCoercedBucket {
    Bool(bool),
    /// The `f64` bits a number rounds to (see
    /// [`crate::numeric_cmp::numeric_bucket_bits`]).
    Number(u64),
    /// Epoch microseconds of an instant.
    Instant(i64),
    /// Text that reads as neither a number nor an instant.
    Text(String),
    /// Every ND cell: cells compare structurally, so they share one bucket.
    Cell,
    /// Bytes: equal to the same bytes only.
    Bytes(Vec<u8>),
}

/// The number a value denotes for coerced comparison: an integer, a float,
/// a decimal, or a string that parses as a number. A bool has no numeric
/// reading here. `None` for anything else.
fn numeric_reading(v: &Value) -> Option<Numeric> {
    match v {
        Value::Integer(i) => Some(Numeric::Int(i128::from(*i))),
        Value::Float(f) => Some(Numeric::Float(*f)),
        Value::Decimal(d) => Some(decimal_reading(d)),
        Value::String(s) => parse_numeric_str(s),
        _ => None,
    }
}

/// Epoch micros for a datetime-typed value, or a string that parses as an
/// ISO-8601 / SQL timestamp. Returns `None` for anything not datetime-like so
/// ordinary strings keep lexicographic ordering.
fn datetime_micros(v: &Value) -> Option<i64> {
    match v {
        Value::NaiveDateTime(dt) | Value::DateTime(dt) => Some(dt.micros),
        Value::String(s) => crate::NdbDateTime::parse(s).map(|dt| dt.micros),
        _ => None,
    }
}

/// The canonical text of a stored UUID. A UUID that does not parse keeps
/// its text.
fn canonical_uuid_text(u: &str) -> String {
    crate::id_gen::canonical_uuid(u).unwrap_or_else(|| u.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_equal_the_same_bytes_and_share_their_bucket() {
        let a = Value::Bytes(vec![1, 2]);
        assert!(a.eq_coerced(&Value::Bytes(vec![1, 2])));
        assert!(!a.eq_coerced(&Value::Bytes(vec![1, 3])));
        assert!(!a.eq_coerced(&Value::String("\\x0102".into())));
        assert_eq!(
            a.eq_coerced_bucket(),
            Value::Bytes(vec![1, 2]).eq_coerced_bucket()
        );
        assert_eq!(
            a.partial_cmp_coerced(&Value::Bytes(vec![1, 3])),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn a_uuid_equals_its_canonical_text_and_shares_its_bucket() {
        let text = "550e8400-e29b-41d4-a716-446655440000";
        let uuid = Value::Uuid(text.into());
        let upper = Value::Uuid(text.to_ascii_uppercase());
        assert!(uuid.eq_coerced(&upper));
        assert!(uuid.eq_coerced(&Value::String(text.into())));
        assert!(Value::String(text.into()).eq_coerced(&upper));
        assert!(!uuid.eq_coerced(&Value::String("other".into())));
        assert_eq!(
            uuid.eq_coerced_bucket(),
            Value::String(text.into()).eq_coerced_bucket()
        );
        assert_eq!(uuid.eq_coerced_bucket(), upper.eq_coerced_bucket());
        assert_eq!(
            uuid.partial_cmp_coerced(&Value::String(text.into())),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn a_decimal_literal_equals_the_float_it_denotes() {
        let d = Value::Decimal(rust_decimal::Decimal::from_str_exact("2.5").expect("decimal"));
        assert!(d.eq_coerced(&Value::Float(2.5)));
        assert!(Value::Float(2.5).eq_coerced(&d));
        assert!(!d.eq_coerced(&Value::Float(2.25)));
        assert_eq!(
            d.partial_cmp_coerced(&Value::Integer(3)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn eq_coerced_same_type() {
        assert!(Value::Null.eq_coerced(&Value::Null));
        assert!(Value::Bool(true).eq_coerced(&Value::Bool(true)));
        assert!(!Value::Bool(true).eq_coerced(&Value::Bool(false)));
        assert!(Value::Integer(42).eq_coerced(&Value::Integer(42)));
        assert!(Value::Float(2.78).eq_coerced(&Value::Float(2.78)));
        assert!(Value::String("hello".into()).eq_coerced(&Value::String("hello".into())));
    }

    #[test]
    fn eq_coerced_int_float() {
        assert!(Value::Integer(5).eq_coerced(&Value::Float(5.0)));
        assert!(Value::Float(5.0).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::Float(5.1)));
    }

    #[test]
    fn eq_coerced_string_number() {
        assert!(Value::String("5".into()).eq_coerced(&Value::Integer(5)));
        assert!(Value::Integer(5).eq_coerced(&Value::String("5".into())));
        assert!(Value::String("2.78".into()).eq_coerced(&Value::Float(2.78)));
        assert!(Value::Float(2.78).eq_coerced(&Value::String("2.78".into())));
        assert!(!Value::String("abc".into()).eq_coerced(&Value::Integer(5)));
        assert!(!Value::Integer(5).eq_coerced(&Value::String("abc".into())));
    }

    /// Every pair `eq_coerced` reports equal shares a bucket, so a join
    /// that probes by bucket and confirms by `eq_coerced` finds every match
    /// a WHERE `=` finds.
    #[test]
    fn equal_values_share_a_bucket() {
        let dec = |s: &str| Value::Decimal(rust_decimal::Decimal::from_str_exact(s).expect("dec"));
        let equal_pairs = [
            (Value::Integer(1), Value::String("1".into())),
            (Value::Integer(1), Value::String("1.0".into())),
            (Value::Integer(1), Value::Float(1.0)),
            (Value::Integer(1), dec("1.00")),
            (dec("2.5"), Value::Float(2.5)),
            (dec("2.50"), Value::String("2.5".into())),
            (Value::Float(0.0), Value::Float(-0.0)),
            (Value::Float(f64::NAN), Value::String("NaN".into())),
            (Value::String("abc".into()), Value::String("abc".into())),
            (Value::Bool(true), Value::Bool(true)),
        ];
        for (a, b) in equal_pairs {
            assert!(a.eq_coerced(&b), "{a:?} = {b:?}");
            assert_eq!(
                a.eq_coerced_bucket(),
                b.eq_coerced_bucket(),
                "{a:?} vs {b:?}"
            );
        }
    }

    #[test]
    fn unequal_values_are_told_apart() {
        assert_ne!(
            Value::Integer(1).eq_coerced_bucket(),
            Value::Integer(2).eq_coerced_bucket()
        );
        assert_ne!(
            Value::Bool(true).eq_coerced_bucket(),
            Value::String("true".into()).eq_coerced_bucket()
        );
        // Same bucket, different text: the confirming `eq_coerced` refuses.
        let a = Value::String("1.0".into());
        let b = Value::String("1".into());
        assert_eq!(a.eq_coerced_bucket(), b.eq_coerced_bucket());
        assert!(!a.eq_coerced(&b));
    }

    #[test]
    fn null_and_containers_join_nothing() {
        assert_eq!(Value::Null.eq_coerced_bucket(), None);
        assert_eq!(
            Value::Array(vec![Value::Integer(1)]).eq_coerced_bucket(),
            None
        );
    }

    #[test]
    fn eq_coerced_cross_type_false() {
        assert!(!Value::Bool(true).eq_coerced(&Value::Integer(1)));
        assert!(!Value::Null.eq_coerced(&Value::Integer(0)));
        assert!(!Value::Null.eq_coerced(&Value::String("".into())));
    }

    #[test]
    fn cmp_coerced_numeric() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::Integer(5).cmp_coerced(&Value::Integer(10)),
            Ordering::Less
        );
        assert_eq!(
            Value::Integer(10).cmp_coerced(&Value::Float(5.0)),
            Ordering::Greater
        );
        assert_eq!(
            Value::String("90".into()).cmp_coerced(&Value::Integer(80)),
            Ordering::Greater
        );
        assert_eq!(
            Value::Float(2.78).cmp_coerced(&Value::String("2.78".into())),
            Ordering::Equal
        );
    }

    #[test]
    fn cmp_coerced_string_fallback() {
        use std::cmp::Ordering;
        assert_eq!(
            Value::String("abc".into()).cmp_coerced(&Value::String("def".into())),
            Ordering::Less
        );
        assert_eq!(
            Value::String("z".into()).cmp_coerced(&Value::String("a".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn cmp_coerced_timestamp_mismatched_string_formats() {
        use std::cmp::Ordering;
        let stored = Value::String("2026-07-02T13:00:00.000000Z".into());
        assert_eq!(
            stored.cmp_coerced(&Value::String("2026-07-02 14:00:00".into())),
            Ordering::Less
        );
        assert_eq!(
            stored.cmp_coerced(&Value::String("2026-07-02 12:00:00".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn eq_coerced_timestamp_mismatched_string_formats() {
        let stored = Value::String("2026-07-02T13:00:00.000000Z".into());
        assert!(stored.eq_coerced(&Value::String("2026-07-02 13:00:00".into())));
    }

    #[test]
    fn cmp_coerced_ordinary_strings_stay_lexicographic() {
        use std::cmp::Ordering;
        // Non-timestamp strings must NOT be datetime-coerced.
        assert_eq!(
            Value::String("apple".into()).cmp_coerced(&Value::String("banana".into())),
            Ordering::Less
        );
        assert_eq!(
            Value::String("zed".into()).cmp_coerced(&Value::String("abc".into())),
            Ordering::Greater
        );
    }

    #[test]
    fn partial_cmp_coerced_orders_instants_against_instants_and_iso_text() {
        use std::cmp::Ordering;
        let earlier = Value::NaiveDateTime(crate::NdbDateTime::from_micros(1_583_402_400_000_000));
        let later = Value::DateTime(crate::NdbDateTime::from_micros(1_583_406_000_000_000));
        assert_eq!(earlier.partial_cmp_coerced(&later), Some(Ordering::Less));
        assert_eq!(
            later.partial_cmp_coerced(&Value::String("2020-03-05 10:00:00".into())),
            Some(Ordering::Greater)
        );
        assert_eq!(
            earlier.partial_cmp_coerced(&Value::String("2020-03-05T10:00:00Z".into())),
            Some(Ordering::Equal)
        );
    }

    /// An integer carries no unit, so it has no order against an instant:
    /// `WHERE at >= 5` over an instant column matches nothing, in both
    /// orientations. The same holds for text against a number.
    #[test]
    fn partial_cmp_coerced_is_none_for_an_unordered_pair() {
        let instant = Value::NaiveDateTime(crate::NdbDateTime::from_micros(1_583_402_400_000_000));
        assert_eq!(instant.partial_cmp_coerced(&Value::Integer(5)), None);
        assert_eq!(Value::Integer(5).partial_cmp_coerced(&instant), None);
        assert_eq!(
            Value::Integer(5).partial_cmp_coerced(&Value::String("abc".into())),
            None
        );
        assert_eq!(
            Value::Bool(true).partial_cmp_coerced(&Value::Integer(1)),
            None
        );
        assert_eq!(Value::Null.partial_cmp_coerced(&Value::Integer(0)), None);
    }

    /// NaN sorts above every number and equals NaN, as in PostgreSQL, so
    /// `WHERE f > 1` matches a NaN row and `WHERE f = 'NaN'` matches it.
    #[test]
    fn nan_orders_above_every_number_and_equals_nan() {
        let nan = Value::Float(f64::NAN);
        assert_eq!(
            nan.partial_cmp_coerced(&Value::Float(1.0)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            nan.partial_cmp_coerced(&Value::Float(f64::INFINITY)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Integer(i64::MAX).partial_cmp_coerced(&nan),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Decimal(rust_decimal::Decimal::MAX).partial_cmp_coerced(&nan),
            Some(Ordering::Less)
        );
        assert_eq!(
            nan.partial_cmp_coerced(&Value::Float(f64::NAN)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            nan.partial_cmp_coerced(&Value::String("NaN".into())),
            Some(Ordering::Equal)
        );
        assert!(nan.eq_coerced(&Value::Float(f64::NAN)));
        assert!(!nan.eq_coerced(&Value::Float(1.0)));
        let mut values = [
            nan.clone(),
            Value::Integer(3),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(0.5),
        ];
        values.sort_by(Value::cmp_coerced);
        assert_eq!(values[0], Value::Float(f64::NEG_INFINITY));
        assert_eq!(values[1], Value::Float(0.5));
        assert_eq!(values[2], Value::Integer(3));
        assert!(matches!(values[3], Value::Float(f) if f.is_nan()));
    }

    /// Two decimals one hundredth apart past `2^53` collapse to one `f64`.
    /// They compare exactly, as decimals and as decimal text.
    #[test]
    fn decimal_pairs_compare_exactly() {
        let dec = |s: &str| rust_decimal::Decimal::from_str_exact(s).expect("decimal");
        let low = Value::Decimal(dec("12345678901234567.01"));
        let high = Value::Decimal(dec("12345678901234567.02"));
        assert_eq!(low.partial_cmp_coerced(&high), Some(Ordering::Less));
        assert_eq!(high.cmp_coerced(&low), Ordering::Greater);
        assert!(!low.eq_coerced(&high));
        let high_text = Value::String("12345678901234567.02".into());
        assert_eq!(low.partial_cmp_coerced(&high_text), Some(Ordering::Less));
        assert!(high.eq_coerced(&high_text));
        assert!(!low.eq_coerced(&high_text));
        let half = Value::Decimal(dec("9007199254740993.5"));
        assert_eq!(
            half.partial_cmp_coerced(&Value::Integer(9_007_199_254_740_993)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Integer(9_007_199_254_740_994).partial_cmp_coerced(&half),
            Some(Ordering::Greater)
        );
    }

    /// An `i64` near the limit compares exactly against a float, with no
    /// rounding of the integer to `f64`.
    #[test]
    fn large_integers_compare_exactly_against_floats() {
        const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
        let max = Value::Integer(i64::MAX);
        assert_eq!(
            max.partial_cmp_coerced(&Value::Float(TWO_POW_63)),
            Some(Ordering::Less)
        );
        assert!(!max.eq_coerced(&Value::Float(TWO_POW_63)));
        assert_eq!(
            Value::Integer(i64::MIN).partial_cmp_coerced(&Value::Float(-TWO_POW_63)),
            Some(Ordering::Equal)
        );
        assert!(Value::Integer(i64::MIN).eq_coerced(&Value::Float(-TWO_POW_63)));
        let above = Value::Integer(9_007_199_254_740_993);
        let float = Value::Float(9_007_199_254_740_992.0);
        assert_eq!(above.partial_cmp_coerced(&float), Some(Ordering::Greater));
        assert_eq!(float.partial_cmp_coerced(&above), Some(Ordering::Less));
        assert!(!above.eq_coerced(&float));
        assert!(!float.eq_coerced(&above));
        assert_eq!(
            Value::Integer(i64::MAX - 1).partial_cmp_coerced(&Value::Integer(i64::MAX)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Integer(i64::MIN).partial_cmp_coerced(&Value::Float(f64::NEG_INFINITY)),
            Some(Ordering::Greater)
        );
        assert!(
            !Value::String("9007199254740993".into())
                .eq_coerced(&Value::Integer(9_007_199_254_740_992))
        );
    }

    /// The sort order places an unordered pair as `Equal` so a stable sort
    /// keeps its input order; an ordered pair sorts as the predicate orders
    /// it.
    #[test]
    fn cmp_coerced_places_an_unordered_pair_as_equal_for_sorting() {
        use std::cmp::Ordering;
        let instant = Value::NaiveDateTime(crate::NdbDateTime::from_micros(1_583_402_400_000_000));
        assert_eq!(instant.cmp_coerced(&Value::Integer(5)), Ordering::Equal);
        assert_eq!(
            Value::Integer(5).cmp_coerced(&Value::Integer(7)),
            Ordering::Less
        );
    }

    #[test]
    fn eq_coerced_symmetry() {
        let cases = [
            (Value::Integer(42), Value::String("42".into())),
            (Value::Float(2.78), Value::String("2.78".into())),
            (Value::Integer(5), Value::Float(5.0)),
        ];
        for (a, b) in &cases {
            assert_eq!(
                a.eq_coerced(b),
                b.eq_coerced(a),
                "symmetry violated for {a:?} vs {b:?}"
            );
        }
    }
}
