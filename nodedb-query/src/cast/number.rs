// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL's conversions between `double precision`, `numeric` and
//! `bigint`.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::expr::EvalError;

use super::int_width::IntWidth;
use super::text::parse_numeric;

/// `f` as an integer of `width`, as PostgreSQL's `float8_int2`,
/// `float8_int4` and `float8_int8`: rounded to the nearest integer, a tie
/// to the even one (`rint`). NaN and a result past the width are
/// [`IntWidth::out_of_range`].
pub fn float_to_int(f: f64, width: IntWidth) -> Result<i64, EvalError> {
    let rounded = f.round_ties_even();
    let (min, max) = width.bounds();
    // `min` is exact in `f64`, and `max + 1` is the first value past the
    // range: `2^15`, `2^31` and `2^63` are exact too.
    let past_max = max as f64 + 1.0;
    if rounded.is_nan() || rounded < min as f64 || rounded >= past_max {
        return Err(width.out_of_range());
    }
    Ok(rounded as i64)
}

/// `d` as an integer of `width`, as PostgreSQL's `numeric_int2`,
/// `numeric_int4` and `numeric_int8`: rounded to the nearest integer, a tie
/// away from zero. A result past the width is [`IntWidth::out_of_range`].
pub fn numeric_to_int(d: Decimal, width: IntWidth) -> Result<i64, EvalError> {
    d.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        .to_i64()
        .ok_or_else(|| width.out_of_range())
        .and_then(|i| width.fit(i))
}

/// `f` as a `numeric`, as PostgreSQL's `float8_numeric`: the value `f`
/// prints with 15 significant digits (`DBL_DIG`), trailing zeros dropped.
/// `0.1` is `0.1`, `100.0` is `100`, `1e-5` is `0.00001`.
///
/// NaN and the infinities are numeric values in PostgreSQL, but `Decimal`
/// holds none of them: they are [`EvalError::NumericSpecialValue`]. A value
/// past the `Decimal` range is [`crate::expr::OutOfRange::Numeric`].
pub fn float_to_numeric(f: f64) -> Result<Decimal, EvalError> {
    if f.is_nan() {
        return Err(EvalError::NumericSpecialValue { value: "NaN" });
    }
    if f.is_infinite() {
        let value = if f > 0.0 { "Infinity" } else { "-Infinity" };
        return Err(EvalError::NumericSpecialValue { value });
    }
    // `{:.14e}` is `%.14e`: 15 significant digits, rounded exactly.
    let digits = format!("{f:.14e}");
    Ok(parse_numeric(&digits)?.normalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::OutOfRange;

    #[test]
    fn float_to_int8_rounds_half_to_even() {
        for (f, want) in [
            (3.7, 4),
            (3.2, 3),
            (0.5, 0),
            (1.5, 2),
            (2.5, 2),
            (-0.5, 0),
            (-1.5, -2),
            (-2.5, -2),
            (-9_223_372_036_854_775_808.0, i64::MIN),
        ] {
            assert_eq!(float_to_int(f, IntWidth::Int8), Ok(want), "{f}");
        }
        for f in [f64::NAN, f64::INFINITY, 9_223_372_036_854_775_808.0, -1e19] {
            let err = float_to_int(f, IntWidth::Int8).unwrap_err();
            assert_eq!(err.to_string(), "bigint out of range", "{f}");
        }
    }

    #[test]
    fn numeric_to_int8_rounds_half_away_from_zero() {
        for (text, want) in [
            ("2.5", 3),
            ("-2.5", -3),
            ("0.5", 1),
            ("-0.5", -1),
            ("2.49", 2),
            ("9223372036854775807.4", i64::MAX),
            ("-9223372036854775808.4", i64::MIN),
        ] {
            let d = Decimal::from_str_exact(text).expect("decimal");
            assert_eq!(numeric_to_int(d, IntWidth::Int8), Ok(want), "{text}");
        }
        let past = Decimal::from_str_exact("9223372036854775807.5").expect("decimal");
        assert_eq!(
            numeric_to_int(past, IntWidth::Int8),
            Err(EvalError::OutOfRange(OutOfRange::Bigint))
        );
    }

    #[test]
    fn narrow_integer_casts_check_their_range() {
        assert_eq!(float_to_int(32767.4, IntWidth::Int2), Ok(32767));
        assert_eq!(float_to_int(-32768.5, IntWidth::Int2), Ok(-32768));
        assert_eq!(
            float_to_int(32767.5, IntWidth::Int2)
                .unwrap_err()
                .to_string(),
            "smallint out of range"
        );
        assert_eq!(
            float_to_int(2147483647.0, IntWidth::Int4),
            Ok(i64::from(i32::MAX))
        );
        assert_eq!(
            float_to_int(2147483647.5, IntWidth::Int4)
                .unwrap_err()
                .to_string(),
            "integer out of range"
        );
        let d = |t: &str| Decimal::from_str_exact(t).expect("decimal");
        assert_eq!(numeric_to_int(d("-32768.4"), IntWidth::Int2), Ok(-32768));
        assert_eq!(
            numeric_to_int(d("32767.5"), IntWidth::Int2)
                .unwrap_err()
                .to_string(),
            "smallint out of range"
        );
        assert_eq!(
            numeric_to_int(d("100000000000000000000"), IntWidth::Int4)
                .unwrap_err()
                .to_string(),
            "integer out of range"
        );
    }

    #[test]
    fn float_to_numeric_keeps_15_significant_digits() {
        for (f, want) in [
            (0.1, "0.1"),
            (100.0, "100"),
            (1e-5, "0.00001"),
            (1.5e20, "150000000000000000000"),
            (-2.25, "-2.25"),
            (1.0 / 3.0, "0.333333333333333"),
            (123_456_789.123_456_79, "123456789.123457"),
            (0.0, "0"),
            (-0.0, "0"),
        ] {
            assert_eq!(
                float_to_numeric(f).expect("numeric").to_string(),
                want,
                "{f}"
            );
        }
    }

    #[test]
    fn float_specials_cannot_be_numeric() {
        assert_eq!(
            float_to_numeric(f64::NAN),
            Err(EvalError::NumericSpecialValue { value: "NaN" })
        );
        let err = float_to_numeric(f64::NEG_INFINITY).unwrap_err();
        assert_eq!(err.to_string(), "cannot convert -Infinity to numeric");
        assert_eq!(
            float_to_numeric(1e300),
            Err(EvalError::OutOfRange(OutOfRange::Numeric))
        );
    }
}
