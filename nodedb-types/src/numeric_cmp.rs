// SPDX-License-Identifier: Apache-2.0

//! Numeric readings and their orders, shared by every comparison path.
//!
//! - An integer reads as an exact `i128`. `i128` holds every `i64` and
//!   every `u64`.
//! - A fractional `Decimal` reads as an exact `Decimal`. A `DECIMAL` cell is
//!   stored as its text, so numeric text with a fraction reads as a
//!   `Decimal` too.
//! - A float reads as an `f64`.
//! - Integer and decimal pairs compare exactly. An integer against a float
//!   compares exactly.
//! - [`cmp_numeric`] is the predicate order. A decimal against a float
//!   compares as the decimal rounded to the nearest `f64`, as PostgreSQL
//!   casts `numeric` to `float8`, so `float_col = 0.1` matches.
//! - [`cmp_numeric_total`] is the sort order. A decimal against a float
//!   compares exactly, so the order is transitive across every mix.
//! - NaN sorts above every number and equals NaN, as in PostgreSQL.
//!   [`cmp_numeric_total`] is a total order, so a sort over it never panics.

use std::cmp::Ordering;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::decimal_float::{cmp_decimal_float_exact, decimal_to_f64};

/// The numeric reading of a value for comparison and summation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Numeric {
    Int(i128),
    Decimal(Decimal),
    Float(f64),
}

/// Total order of two floats. NaN sorts above every number and equals NaN.
/// `-0.0` equals `0.0`.
pub fn cmp_f64(a: f64, b: f64) -> Ordering {
    match a.partial_cmp(&b) {
        Some(order) => order,
        None => a.is_nan().cmp(&b.is_nan()),
    }
}

/// A numeric string read as a number: integer text as an exact integer,
/// fractional decimal text as an exact `Decimal`, and any other float text
/// (an exponent, a fraction past the `Decimal` precision, `NaN`, `inf`) as
/// an `f64`. `None` for non-numeric text.
pub fn parse_numeric_str(s: &str) -> Option<Numeric> {
    if let Ok(i) = s.parse::<i128>() {
        return Some(Numeric::Int(i));
    }
    let float = s.parse::<f64>().ok()?;
    Some(match Decimal::from_str_exact(s) {
        Ok(d) => decimal_reading(&d),
        Err(_) => Numeric::Float(float),
    })
}

/// An integral `Decimal` as an exact integer, any other as an exact
/// `Decimal`.
pub fn decimal_reading(d: &Decimal) -> Numeric {
    if d.is_integer()
        && let Some(i) = d.to_i128()
    {
        return Numeric::Int(i);
    }
    Numeric::Decimal(*d)
}

/// `2^127` as `f64`: the first float above every `i128`.
const TWO_POW_127: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;

/// Exact order of an `i128` against an `f64`, with no rounding of either.
/// NaN is above every integer.
fn cmp_int_float(i: i128, f: f64) -> Ordering {
    if f.is_nan() || f >= TWO_POW_127 {
        return Ordering::Less;
    }
    if f < -TWO_POW_127 {
        return Ordering::Greater;
    }
    // `f` lies in `[-2^127, 2^127)`, so its integral part is an exact `i128`.
    let whole = f.trunc();
    let by_whole = i.cmp(&(whole as i128));
    if by_whole != Ordering::Equal {
        return by_whole;
    }
    let fraction = f - whole;
    if fraction > 0.0 {
        Ordering::Less
    } else if fraction < 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// Exact order of an `i128` against a `Decimal`. An integer past the
/// `Decimal` range is past every `Decimal` on its side of zero.
fn cmp_int_decimal(i: i128, d: Decimal) -> Ordering {
    match Decimal::try_from_i128_with_scale(i, 0) {
        Ok(as_decimal) => as_decimal.cmp(&d),
        Err(_) if i > 0 => Ordering::Greater,
        Err(_) => Ordering::Less,
    }
}

/// Predicate order of two numeric readings. See the module docs for the
/// rule. A decimal equals the float it rounds to while it can still order
/// apart from an integer that float equals, so this order is not
/// transitive across kinds. Sorts use [`cmp_numeric_total`].
pub fn cmp_numeric(a: Numeric, b: Numeric) -> Ordering {
    match (a, b) {
        (Numeric::Int(x), Numeric::Int(y)) => x.cmp(&y),
        (Numeric::Int(x), Numeric::Decimal(y)) => cmp_int_decimal(x, y),
        (Numeric::Decimal(x), Numeric::Int(y)) => cmp_int_decimal(y, x).reverse(),
        (Numeric::Decimal(x), Numeric::Decimal(y)) => x.cmp(&y),
        (Numeric::Int(x), Numeric::Float(y)) => cmp_int_float(x, y),
        (Numeric::Float(x), Numeric::Int(y)) => cmp_int_float(y, x).reverse(),
        (Numeric::Decimal(x), Numeric::Float(y)) => cmp_f64(decimal_to_f64(x), y),
        (Numeric::Float(x), Numeric::Decimal(y)) => cmp_f64(x, decimal_to_f64(y)),
        (Numeric::Float(x), Numeric::Float(y)) => cmp_f64(x, y),
    }
}

/// Exact total order of two numeric readings, for sorting and extremum
/// selection. [`cmp_numeric`] with a decimal against a float compared
/// exactly instead of as `f64`. Every pair orders exactly, so the order is
/// transitive across integer, decimal and float readings.
pub fn cmp_numeric_total(a: Numeric, b: Numeric) -> Ordering {
    match (a, b) {
        (Numeric::Decimal(x), Numeric::Float(y)) => cmp_decimal_float_exact(x, y),
        (Numeric::Float(x), Numeric::Decimal(y)) => cmp_decimal_float_exact(y, x).reverse(),
        _ => cmp_numeric(a, b),
    }
}

/// The `f64` a numeric reading rounds to, by the same conversion
/// [`cmp_numeric`] uses for a decimal against a float. Two readings
/// [`cmp_numeric`] orders `Equal` round to the same `f64`, with `-0.0` read
/// as `0.0` and every NaN as one NaN. Distinct readings can share an `f64`,
/// so this is a hash bucket, never an equality.
pub fn numeric_bucket_bits(n: Numeric) -> u64 {
    let f = match n {
        Numeric::Int(i) => i as f64,
        Numeric::Decimal(d) => decimal_to_f64(d),
        Numeric::Float(f) => f,
    };
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    }
}

/// Equality of two numeric readings for a coerced `=`: [`cmp_numeric`]
/// orders the pair `Equal`. Exact for integer and decimal pairs, a decimal
/// against a float compares as its nearest `f64`, and NaN equals NaN.
pub fn numeric_eq(a: Numeric, b: Numeric) -> bool {
    cmp_numeric(a, b) == Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }

    #[test]
    fn nan_is_above_every_number_and_equals_nan() {
        assert_eq!(cmp_f64(f64::NAN, f64::INFINITY), Ordering::Greater);
        assert_eq!(cmp_f64(f64::NEG_INFINITY, f64::NAN), Ordering::Less);
        assert_eq!(cmp_f64(f64::NAN, f64::NAN), Ordering::Equal);
        assert_eq!(cmp_f64(-0.0, 0.0), Ordering::Equal);
        let nan = Numeric::Float(f64::NAN);
        assert_eq!(cmp_numeric(nan, Numeric::Int(i128::MAX)), Ordering::Greater);
        assert_eq!(cmp_numeric(Numeric::Int(i128::MIN), nan), Ordering::Less);
        assert_eq!(
            cmp_numeric(nan, Numeric::Decimal(Decimal::MAX)),
            Ordering::Greater
        );
        assert_eq!(
            cmp_numeric(Numeric::Decimal(dec("0.5")), nan),
            Ordering::Less
        );
        assert_eq!(cmp_numeric(nan, nan), Ordering::Equal);
        assert!(numeric_eq(nan, nan));
    }

    /// A sort over NaN, infinities and every reading kind runs to a
    /// consistent order.
    #[test]
    fn mixed_sort_is_total() {
        let mut values = [
            Numeric::Float(f64::NAN),
            Numeric::Int(3),
            Numeric::Float(f64::INFINITY),
            Numeric::Decimal(dec("2.5")),
            Numeric::Float(f64::NAN),
            Numeric::Float(f64::NEG_INFINITY),
            Numeric::Int(-1),
        ];
        values.sort_by(|a, b| cmp_numeric(*a, *b));
        assert_eq!(values[0], Numeric::Float(f64::NEG_INFINITY));
        assert_eq!(values[1], Numeric::Int(-1));
        assert_eq!(values[2], Numeric::Decimal(dec("2.5")));
        assert_eq!(values[3], Numeric::Int(3));
        assert_eq!(values[4], Numeric::Float(f64::INFINITY));
        assert!(matches!(values[5], Numeric::Float(f) if f.is_nan()));
        assert!(matches!(values[6], Numeric::Float(f) if f.is_nan()));
    }

    /// Two decimals one hundredth apart past `2^53` compare exactly.
    #[test]
    fn fractional_decimal_text_compares_exactly() {
        let low = parse_numeric_str("12345678901234567.01").unwrap();
        let high = parse_numeric_str("12345678901234567.02").unwrap();
        assert_eq!(low, Numeric::Decimal(dec("12345678901234567.01")));
        assert_eq!(cmp_numeric(low, high), Ordering::Less);
        assert_eq!(cmp_numeric(high, low), Ordering::Greater);
        assert!(!numeric_eq(low, high));
    }

    #[test]
    fn decimal_against_integer_compares_exactly() {
        let above = Numeric::Decimal(dec("9007199254740993.5"));
        assert_eq!(
            cmp_numeric(above, Numeric::Int(9_007_199_254_740_993)),
            Ordering::Greater
        );
        assert_eq!(
            cmp_numeric(Numeric::Int(9_007_199_254_740_994), above),
            Ordering::Greater
        );
        assert_eq!(
            cmp_numeric(Numeric::Int(i128::MAX), above),
            Ordering::Greater
        );
        assert_eq!(cmp_numeric(Numeric::Int(i128::MIN), above), Ordering::Less);
        assert_eq!(
            cmp_numeric(Numeric::Decimal(dec("-0.5")), Numeric::Int(0)),
            Ordering::Less
        );
    }

    #[test]
    fn numeric_text_reads_by_kind() {
        assert_eq!(parse_numeric_str("12"), Some(Numeric::Int(12)));
        assert_eq!(parse_numeric_str("5.0"), Some(Numeric::Int(5)));
        assert_eq!(parse_numeric_str("0.1"), Some(Numeric::Decimal(dec("0.1"))));
        assert_eq!(parse_numeric_str("1e3"), Some(Numeric::Float(1000.0)));
        assert!(matches!(parse_numeric_str("NaN"), Some(Numeric::Float(f)) if f.is_nan()));
        assert_eq!(parse_numeric_str("x"), None);
        assert_eq!(parse_numeric_str("1_0.5"), None);
    }

    #[test]
    fn integral_decimal_reads_as_integer() {
        assert_eq!(decimal_reading(&dec("42.000")), Numeric::Int(42));
        assert_eq!(
            decimal_reading(&Decimal::MAX),
            Numeric::Int(Decimal::MAX.mantissa())
        );
        assert_eq!(decimal_reading(&dec("0.25")), Numeric::Decimal(dec("0.25")));
    }

    /// The total order keeps a decimal past `2^53` exactly above the float
    /// below it, so integer, float and decimal readings sort transitively.
    #[test]
    fn total_order_compares_decimal_and_float_exactly() {
        let float = Numeric::Float(9_007_199_254_740_992.0);
        let int = Numeric::Int(9_007_199_254_740_993);
        let high = Numeric::Decimal(dec("9007199254740993.0001"));
        assert_eq!(cmp_numeric_total(float, int), Ordering::Less);
        assert_eq!(cmp_numeric_total(int, high), Ordering::Less);
        assert_eq!(cmp_numeric_total(float, high), Ordering::Less);
        assert_eq!(cmp_numeric_total(high, float), Ordering::Greater);
        let tenth = Numeric::Float(0.1);
        assert_eq!(
            cmp_numeric_total(Numeric::Decimal(dec("0.1")), tenth),
            Ordering::Less
        );
        assert_eq!(
            cmp_numeric_total(Numeric::Decimal(dec("0.5")), Numeric::Float(0.5)),
            Ordering::Equal
        );
    }

    /// The predicate order reads a decimal as its nearest `f64`, correctly
    /// rounded, so `0.1` text equals the float `0.1` and a decimal just
    /// above an odd integer past `2^53` rounds up, never down.
    #[test]
    fn predicate_order_rounds_decimal_to_nearest_float() {
        assert_eq!(
            cmp_numeric(Numeric::Decimal(dec("0.1")), Numeric::Float(0.1)),
            Ordering::Equal
        );
        assert_eq!(
            cmp_numeric(
                Numeric::Decimal(dec("9007199254740993.0001")),
                Numeric::Float(9_007_199_254_740_992.0)
            ),
            Ordering::Greater
        );
    }

    /// Readings `cmp_numeric` orders `Equal` share a bucket across kinds.
    #[test]
    fn equal_readings_share_a_bucket() {
        let pairs = [
            (Numeric::Decimal(dec("0.1")), Numeric::Float(0.1)),
            (
                Numeric::Decimal(dec("4503599627370495.5")),
                Numeric::Float(4_503_599_627_370_495.5),
            ),
            (
                Numeric::Int(9_007_199_254_740_992),
                Numeric::Float(9_007_199_254_740_992.0),
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(cmp_numeric(a, b), Ordering::Equal);
            assert_eq!(numeric_bucket_bits(a), numeric_bucket_bits(b));
        }
    }
}
