// SPDX-License-Identifier: Apache-2.0

//! Exact SUM / AVG accumulation, shared by every aggregate path.
//!
//! The rule, applied the same way everywhere:
//!
//! - An integer input adds exactly into an `i128`. A `u64` above `i64::MAX`
//!   added through [`ExactSum::add_u64`] is an integer input.
//! - A `Decimal` is a numeric input. It adds exactly into a `Decimal`,
//!   keeping its scale. A scale-0 one outside the `i64` range adds into the
//!   `i128`, which holds it exactly, and still makes the input numeric.
//! - A float input adds into a Neumaier-compensated `f64`. Once the float
//!   total is not finite, compensation stops and the total stays as IEEE
//!   arithmetic gives it: an overflow is `Infinity`, `Infinity` plus
//!   `-Infinity` is NaN, as in PostgreSQL.
//! - A numeric string is the number it spells: an integer, or a `Decimal`
//!   when it has a fraction. A `DECIMAL` cell is stored as its text, so this
//!   keeps a `DECIMAL` column exact.
//! - SUM with only integer inputs is an `Integer` when the total fits `i64`.
//!   A larger total is a `Decimal`.
//! - SUM with a numeric input and no float input is a `Decimal`: the exact
//!   integer part plus the exact decimal part. It is never an `Integer`,
//!   whatever the total, as PostgreSQL's `sum(numeric)` is numeric.
//! - An exact total past the `Decimal` range is
//!   [`EvalError::NumericOverflow`].
//! - SUM with at least one float input is a `Float`: the exact parts plus
//!   the float part.
//! - AVG with no float input is a `Decimal`, as PostgreSQL's `avg` over
//!   integers and numerics: the exact total over the count at PostgreSQL's
//!   division scale ([`crate::numeric_div`]). AVG with a float input is a
//!   `Float`.
//! - SUM and AVG over no input are NULL.
//! - Two partial accumulators merge without loss: the integer parts add
//!   exactly, so a shard or spill merge gives the single-pass result.
//!
//! STDDEV and VARIANCE stay in `f64` (Welford); they are not exact sums.

use nodedb_types::Value;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::expr::EvalError;
use crate::numeric_cmp::{Numeric, parse_numeric_str};

/// Running exact SUM / AVG state. See the module docs for the rule.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExactSum {
    /// Exact total of the integer inputs.
    int: i128,
    /// Exact total of the `Decimal` inputs the `i128` does not hold.
    frac: Decimal,
    /// At least one `Decimal` input was added: SUM and AVG are numeric.
    has_decimal: bool,
    /// An exact total (`int` or `frac`) left its range.
    exact_overflow: bool,
    /// Running total of the float inputs.
    float: f64,
    /// Neumaier compensation term: the rounding error lost from `float`.
    /// The float total is `float + comp` while `float` is finite.
    comp: f64,
    /// At least one float input was added.
    has_float: bool,
    /// Number of inputs added.
    count: u64,
}

impl ExactSum {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of inputs added.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Add one integer input.
    pub fn add_i64(&mut self, v: i64) {
        self.add_int(i128::from(v));
    }

    /// Add one unsigned integer input.
    pub fn add_u64(&mut self, v: u64) {
        self.add_int(i128::from(v));
    }

    /// Add one float input.
    pub fn add_f64(&mut self, v: f64) {
        self.float_add(v);
        self.has_float = true;
        self.count += 1;
    }

    /// Add a numeric `Value`: `Integer`, `Float`, or `Decimal`. Any other
    /// value adds nothing. Returns whether the value was added.
    pub fn add_value(&mut self, v: &Value) -> bool {
        match v {
            Value::Integer(i) => self.add_i64(*i),
            Value::Float(f) => self.add_f64(*f),
            Value::Decimal(d) => self.add_decimal(*d),
            _ => return false,
        }
        true
    }

    /// Add one `Decimal` input, a numeric input. A scale-0 one outside the
    /// `i64` range adds into the exact integer total, which holds it past
    /// the `Decimal` range. Any other adds into the exact decimal total with
    /// its scale.
    pub fn add_decimal(&mut self, d: Decimal) {
        self.has_decimal = true;
        let outside_i64 = d.to_i64().is_none();
        if d.scale() == 0
            && outside_i64
            && let Some(i) = d.to_i128()
        {
            self.add_int(i);
            return;
        }
        match self.frac.checked_add(d) {
            Some(total) => self.frac = total,
            None => self.exact_overflow = true,
        }
        self.count += 1;
    }

    /// Fold another partial accumulator into this one without loss.
    pub fn merge(&mut self, other: &ExactSum) {
        match self.int.checked_add(other.int) {
            Some(total) => self.int = total,
            None => self.exact_overflow = true,
        }
        match self.frac.checked_add(other.frac) {
            Some(total) => self.frac = total,
            None => self.exact_overflow = true,
        }
        self.exact_overflow |= other.exact_overflow;
        self.has_decimal |= other.has_decimal;
        self.float_add(other.float);
        if self.float.is_finite() {
            self.comp += other.comp;
        }
        self.has_float |= other.has_float;
        self.count += other.count;
    }

    /// SUM of the inputs. NULL for no input.
    pub fn sum(&self) -> Result<Value, EvalError> {
        if self.count == 0 {
            return Ok(Value::Null);
        }
        if self.exact_overflow {
            return Err(EvalError::NumericOverflow { function: "sum" });
        }
        if self.has_float {
            return Ok(Value::Float(self.sum_f64()));
        }
        if self.has_decimal {
            return self
                .exact_decimal_total()
                .map(Value::Decimal)
                .ok_or(EvalError::NumericOverflow { function: "sum" });
        }
        if let Ok(i) = i64::try_from(self.int) {
            return Ok(Value::Integer(i));
        }
        Decimal::try_from_i128_with_scale(self.int, 0)
            .map(Value::Decimal)
            .map_err(|_| EvalError::NumericOverflow { function: "sum" })
    }

    /// The total read as an `f64`, for a float-typed result. Rounds an
    /// exact total above 2^53; [`Self::sum`] keeps it exact. `0.0` for no
    /// input. A float total that is not finite (`Infinity`, `-Infinity`,
    /// NaN) is the result alone.
    pub fn sum_f64(&self) -> f64 {
        if !self.float.is_finite() {
            return self.float;
        }
        self.int as f64 + self.frac.as_f64() + (self.float + self.comp)
    }

    /// The integer and decimal totals as one `Decimal`. `None` past the
    /// `Decimal` range.
    fn exact_decimal_total(&self) -> Option<Decimal> {
        Decimal::try_from_i128_with_scale(self.int, 0)
            .ok()?
            .checked_add(self.frac)
    }

    /// SQL AVG of the inputs, typed as PostgreSQL types it. With a float
    /// input it is a `Float`, the total over the count. Otherwise it is a
    /// `Decimal`, the exact integer and numeric total over the count at
    /// PostgreSQL's division scale ([`crate::numeric_div`]). NULL for no
    /// input.
    pub fn avg_value(&self) -> Result<Value, EvalError> {
        if self.count == 0 {
            return Ok(Value::Null);
        }
        if self.exact_overflow {
            return Err(EvalError::NumericOverflow { function: "avg" });
        }
        if self.has_float {
            return Ok(Value::Float(self.sum_f64() / self.count as f64));
        }
        self.exact_decimal_total()
            .and_then(|total| crate::numeric_div::numeric_div(total, Decimal::from(self.count)))
            .map(Value::Decimal)
            .ok_or(EvalError::NumericOverflow { function: "avg" })
    }

    /// AVG of the inputs as an `f64`. `None` for no input.
    pub fn avg_f64(&self) -> Result<Option<f64>, EvalError> {
        if self.count == 0 {
            return Ok(None);
        }
        if self.exact_overflow {
            return Err(EvalError::NumericOverflow { function: "avg" });
        }
        let n = i128::from(self.count);
        let avg = if self.has_float {
            self.sum_f64() / self.count as f64
        } else if self.has_decimal {
            // The exact total divides as a `Decimal`, so only the quotient
            // rounds.
            let total = self
                .exact_decimal_total()
                .ok_or(EvalError::NumericOverflow { function: "avg" })?;
            total
                .checked_div(Decimal::from(self.count))
                .and_then(|avg| avg.to_f64())
                .ok_or(EvalError::NumericOverflow { function: "avg" })?
        } else {
            // Quotient and remainder keep the exact total: no rounding of a
            // total above 2^53 before the division.
            (self.int / n) as f64 + (self.int % n) as f64 / self.count as f64
        };
        Ok(Some(avg))
    }

    fn add_int(&mut self, v: i128) {
        match self.int.checked_add(v) {
            Some(total) => self.int = total,
            None => self.exact_overflow = true,
        }
        self.count += 1;
    }

    /// Neumaier summation step. A total that is not finite takes no
    /// compensation: `Infinity` and NaN propagate as IEEE arithmetic gives
    /// them.
    fn float_add(&mut self, v: f64) {
        let t = self.float + v;
        if t.is_finite() {
            if self.float.abs() >= v.abs() {
                self.comp += (self.float - t) + v;
            } else {
                self.comp += (v - t) + self.float;
            }
        }
        self.float = t;
    }
}

/// A SUM / AVG argument value as the number it contributes: a number as
/// itself, a numeric string as the number it spells (see [`numeric_text`]).
/// `None` for any other value, which contributes nothing.
pub fn sum_input(v: &Value) -> Option<Value> {
    match v {
        Value::Integer(_) | Value::Float(_) | Value::Decimal(_) => Some(v.clone()),
        Value::String(s) => numeric_text(s),
        _ => None,
    }
}

/// The number a numeric string spells, exactly where it can be: an integer
/// as [`numeric_to_value`] gives it, a fraction in decimal notation as a
/// `Decimal`, and any other numeric text (an exponent, a fraction past the
/// `Decimal` precision) as a `Float`. `None` for non-numeric text.
pub fn numeric_text(s: &str) -> Option<Value> {
    parse_numeric_str(s).map(numeric_to_value)
}

/// A numeric reading as a `Value`. An integer past `i64` is a `Decimal`.
/// An integer past the `Decimal` range is the nearest `Float`, the same
/// reading `f64` parsing gives it.
pub(crate) fn numeric_to_value(n: Numeric) -> Value {
    match n {
        Numeric::Int(i) => match i64::try_from(i) {
            Ok(small) => Value::Integer(small),
            Err(_) => Decimal::try_from_i128_with_scale(i, 0)
                .map(Value::Decimal)
                .unwrap_or(Value::Float(i as f64)),
        },
        Numeric::Decimal(d) => Value::Decimal(d),
        Numeric::Float(f) => Value::Float(f),
    }
}

/// Element count of the MessagePack form: the array `[int_hi, int_lo, frac,
/// has_decimal, exact_overflow, float, comp, has_float, count]`. The `i128`
/// total is split into its high `i64` and low `u64` halves, and `frac` is
/// the 16-byte `Decimal` serialization, so both round-trip exactly.
const MSGPACK_FIELDS: usize = 9;

impl zerompk::ToMessagePack for ExactSum {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(MSGPACK_FIELDS)?;
        writer.write_i64((self.int >> 64) as i64)?;
        writer.write_u64(self.int as u64)?;
        writer.write_binary(&self.frac.serialize())?;
        writer.write_boolean(self.has_decimal)?;
        writer.write_boolean(self.exact_overflow)?;
        writer.write_f64(self.float)?;
        writer.write_f64(self.comp)?;
        writer.write_boolean(self.has_float)?;
        writer.write_u64(self.count)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for ExactSum {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        reader.check_array_len(MSGPACK_FIELDS)?;
        let hi = reader.read_i64()?;
        let lo = reader.read_u64()?;
        let frac_bytes = reader.read_binary()?;
        let frac_bytes: [u8; 16] = frac_bytes
            .as_ref()
            .try_into()
            .map_err(|_| zerompk::Error::BufferTooSmall)?;
        Ok(Self {
            int: (i128::from(hi) << 64) | i128::from(lo),
            frac: Decimal::deserialize(frac_bytes),
            has_decimal: reader.read_boolean()?,
            exact_overflow: reader.read_boolean()?,
            float: reader.read_f64()?,
            comp: reader.read_f64()?,
            has_float: reader.read_boolean()?,
            count: reader.read_u64()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABOVE: i64 = 9_007_199_254_740_993;
    const AT: i64 = 9_007_199_254_740_992;

    fn sum_of(values: &[Value]) -> Result<Value, EvalError> {
        let mut acc = ExactSum::new();
        for v in values {
            acc.add_value(v);
        }
        acc.sum()
    }

    #[test]
    fn integers_past_two_pow_53_sum_exactly() {
        let total = sum_of(&[Value::Integer(ABOVE), Value::Integer(AT)]).unwrap();
        assert_eq!(total, Value::Integer(18_014_398_509_481_985));
    }

    #[test]
    fn nanosecond_timestamps_sum_exactly() {
        let t = [
            Value::Integer(1_700_000_000_000_000_001),
            Value::Integer(1_700_000_000_000_000_002),
        ];
        assert_eq!(
            sum_of(&t).unwrap(),
            Value::Integer(3_400_000_000_000_000_003)
        );
    }

    #[test]
    fn total_past_i64_is_decimal() {
        let total = sum_of(&[Value::Integer(i64::MAX), Value::Integer(i64::MAX)]).unwrap();
        assert_eq!(
            total,
            Value::Decimal(Decimal::from_i128_with_scale(2 * i128::from(i64::MAX), 0))
        );
    }

    #[test]
    fn u64_above_i64_max_sums_exactly() {
        let mut acc = ExactSum::new();
        acc.add_u64(u64::MAX);
        acc.add_value(&Value::from_u64(u64::MAX));
        assert_eq!(
            acc.sum().unwrap(),
            Value::Decimal(Decimal::from_i128_with_scale(2 * i128::from(u64::MAX), 0))
        );
        let mut small = ExactSum::new();
        small.add_u64(u64::MAX);
        small.add_i64(-1);
        small.add_i64(i64::MIN);
        assert_eq!(small.sum().unwrap(), Value::Integer(i64::MAX - 1));
    }

    #[test]
    fn total_past_decimal_range_is_overflow_error() {
        let mut acc = ExactSum::new();
        // Four times 2^94 is 2^96, past the 96-bit `Decimal` mantissa.
        for _ in 0..4 {
            acc.add_int(1i128 << 94);
        }
        assert_eq!(
            acc.sum(),
            Err(EvalError::NumericOverflow { function: "sum" })
        );
    }

    #[test]
    fn i128_overflow_is_overflow_error() {
        let mut acc = ExactSum::new();
        acc.add_int(i128::MAX);
        acc.add_int(1);
        assert_eq!(
            acc.sum(),
            Err(EvalError::NumericOverflow { function: "sum" })
        );
        assert_eq!(
            acc.avg_value(),
            Err(EvalError::NumericOverflow { function: "avg" })
        );
    }

    #[test]
    fn mixed_int_float_is_float() {
        let total = sum_of(&[Value::Integer(2), Value::Float(0.5)]).unwrap();
        assert_eq!(total, Value::Float(2.5));
        let total = sum_of(&[Value::Integer(ABOVE), Value::Float(1.0)]).unwrap();
        assert_eq!(total, Value::Float(ABOVE as f64 + 1.0));
    }

    #[test]
    fn nan_input_makes_float_nan() {
        let total = sum_of(&[Value::Integer(1), Value::Float(f64::NAN)]).unwrap();
        assert!(matches!(total, Value::Float(f) if f.is_nan()));
    }

    #[test]
    fn empty_is_null() {
        let acc = ExactSum::new();
        assert_eq!(acc.sum().unwrap(), Value::Null);
        assert_eq!(acc.avg_value().unwrap(), Value::Null);
        assert_eq!(sum_of(&[Value::String("x".into())]).unwrap(), Value::Null);
    }

    #[test]
    fn avg_uses_the_exact_total() {
        let mut acc = ExactSum::new();
        acc.add_i64(ABOVE);
        acc.add_i64(AT);
        // PostgreSQL: avg(9007199254740993, 9007199254740992) is
        // 9007199254740992.5000. The total's weight leaves 4 digits.
        assert_eq!(avg_text_of(&acc), "9007199254740992.5000");
        let mut big = ExactSum::new();
        big.add_i64(i64::MAX);
        big.add_i64(i64::MAX);
        // PostgreSQL: avg(i64::MAX, i64::MAX) is 9223372036854775807.
        assert_eq!(avg_text_of(&big), "9223372036854775807");
        let mut ints = ExactSum::new();
        ints.add_i64(10);
        ints.add_i64(20);
        ints.add_i64(25);
        assert_eq!(ints.avg_f64().unwrap(), Some(55.0 / 3.0));
    }

    #[test]
    fn partial_merge_keeps_exactness() {
        let mut a = ExactSum::new();
        a.add_i64(ABOVE);
        let mut b = ExactSum::new();
        b.add_i64(AT);
        b.add_i64(i64::MAX);
        a.merge(&b);
        let mut single = ExactSum::new();
        for v in [ABOVE, AT, i64::MAX] {
            single.add_i64(v);
        }
        assert_eq!(a.sum().unwrap(), single.sum().unwrap());
        assert_eq!(a.count(), 3);
    }

    fn float_sum(values: &[f64]) -> f64 {
        let mut acc = ExactSum::new();
        for v in values {
            acc.add_f64(*v);
        }
        match acc.sum().unwrap() {
            Value::Float(f) => f,
            other => panic!("expected a float SUM, got {other:?}"),
        }
    }

    /// Float overflow is `Infinity` and `Infinity` plus `-Infinity` is NaN,
    /// as in PostgreSQL.
    #[test]
    fn non_finite_float_totals_follow_ieee() {
        assert_eq!(float_sum(&[1e308, 1e308]), f64::INFINITY);
        assert_eq!(float_sum(&[-1e308, -1e308]), f64::NEG_INFINITY);
        assert_eq!(float_sum(&[f64::INFINITY, 1.0]), f64::INFINITY);
        assert_eq!(float_sum(&[1.0, f64::NEG_INFINITY]), f64::NEG_INFINITY);
        assert!(float_sum(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
        assert!(float_sum(&[f64::NAN, f64::INFINITY]).is_nan());
        let mut avg = ExactSum::new();
        avg.add_f64(1e308);
        avg.add_f64(1e308);
        assert_eq!(avg.avg_value().unwrap(), Value::Float(f64::INFINITY));
        let mut mixed = ExactSum::new();
        mixed.add_i64(5);
        mixed.add_f64(f64::INFINITY);
        assert_eq!(mixed.sum().unwrap(), Value::Float(f64::INFINITY));
    }

    /// Neumaier compensation keeps a small term that a large pair cancels.
    #[test]
    fn compensation_survives_large_cancelling_terms() {
        assert_eq!(float_sum(&[1.0, 1e100, 1.0, -1e100]), 2.0);
        assert_eq!(float_sum(&[0.1; 10]), 1.0);
    }

    #[test]
    fn merge_keeps_non_finite_totals() {
        let mut a = ExactSum::new();
        a.add_f64(1e308);
        let mut b = ExactSum::new();
        b.add_f64(1e308);
        a.merge(&b);
        assert_eq!(a.sum().unwrap(), Value::Float(f64::INFINITY));
        let mut pos = ExactSum::new();
        pos.add_f64(f64::INFINITY);
        let mut neg = ExactSum::new();
        neg.add_f64(f64::NEG_INFINITY);
        pos.merge(&neg);
        assert!(matches!(pos.sum().unwrap(), Value::Float(f) if f.is_nan()));
        let mut small = ExactSum::new();
        small.add_f64(1.0);
        let mut big = ExactSum::new();
        big.add_f64(1e100);
        big.add_f64(1.0);
        big.add_f64(-1e100);
        small.merge(&big);
        assert_eq!(small.sum().unwrap(), Value::Float(2.0));
    }

    #[test]
    fn msgpack_round_trip_keeps_non_finite_floats() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut acc = ExactSum::new();
            acc.add_f64(v);
            let bytes = zerompk::to_msgpack_vec(&acc).unwrap();
            let back: ExactSum = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(back.count(), 1);
            match (back.sum().unwrap(), acc.sum().unwrap()) {
                (Value::Float(x), Value::Float(y)) => {
                    assert!(x == y || (x.is_nan() && y.is_nan()));
                }
                other => panic!("expected float sums, got {other:?}"),
            }
        }
    }

    #[test]
    fn msgpack_round_trip_keeps_every_part() {
        let mut big = ExactSum::new();
        big.add_u64(u64::MAX);
        big.add_u64(u64::MAX);
        let mut negative = ExactSum::new();
        negative.add_i64(i64::MIN);
        negative.add_i64(i64::MIN);
        negative.add_f64(0.1);
        let mut overflow = ExactSum::new();
        overflow.add_int(i128::MAX);
        overflow.add_int(1);
        for acc in [ExactSum::new(), big, negative, overflow] {
            let bytes = zerompk::to_msgpack_vec(&acc).unwrap();
            let back: ExactSum = zerompk::from_msgpack(&bytes).unwrap();
            assert_eq!(back, acc);
            assert_eq!(back.sum(), acc.sum());
        }
    }

    #[test]
    fn coerced_strings_stay_exact() {
        let mut acc = ExactSum::new();
        let text = sum_input(&Value::String("9007199254740993".into())).unwrap();
        assert_eq!(text, Value::Integer(ABOVE));
        assert!(acc.add_value(&text));
        assert!(acc.add_value(&Value::Integer(AT)));
        assert!(!acc.add_value(&Value::String("1".into())));
        assert_eq!(acc.sum().unwrap(), Value::Integer(18_014_398_509_481_985));
        assert_eq!(
            sum_input(&Value::String("18446744073709551615".into())),
            Some(Value::Decimal(Decimal::from(u64::MAX)))
        );
        assert_eq!(sum_input(&Value::String("x".into())), None);
        assert_eq!(sum_input(&Value::Bool(true)), None);
    }

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).unwrap()
    }

    #[test]
    fn fractional_decimals_sum_exactly() {
        let tenths = vec![Value::Decimal(dec("0.1")); 3];
        assert_eq!(sum_of(&tenths).unwrap(), Value::Decimal(dec("0.3")));
        let mixed = [Value::Decimal(dec("0.25")), Value::Integer(i64::MAX)];
        assert_eq!(
            sum_of(&mixed).unwrap(),
            Value::Decimal(Decimal::from(i64::MAX) + dec("0.25"))
        );
        let with_float = [Value::Decimal(dec("0.5")), Value::Float(0.25)];
        assert_eq!(sum_of(&with_float).unwrap(), Value::Float(0.75));
    }

    /// A numeric input makes SUM numeric whatever the total: a scale-0
    /// numeric past `i64` that cancels to a small total stays a `Decimal`.
    /// Integer-only input keeps an `Integer` SUM.
    #[test]
    fn numeric_input_never_sums_to_an_integer() {
        let big = Decimal::from_i128_with_scale(100_000_000_000_000_000_000, 0);
        let mut acc = ExactSum::new();
        acc.add_value(&Value::Decimal(big));
        acc.add_value(&Value::Decimal(-big + Decimal::from(3)));
        assert_eq!(acc.sum().unwrap(), Value::Decimal(Decimal::from(3)));
        let mut small = ExactSum::new();
        small.add_value(&Value::Decimal(Decimal::from(2)));
        small.add_i64(5);
        assert_eq!(small.sum().unwrap(), Value::Decimal(Decimal::from(7)));
        let mut ints = ExactSum::new();
        ints.add_i64(2);
        ints.add_i64(5);
        assert_eq!(ints.sum().unwrap(), Value::Integer(7));
    }

    /// The numeric flag crosses a shard merge and the partial-state
    /// encoding, so a merged SUM types as the single-pass one.
    #[test]
    fn numeric_flag_survives_merge_and_encoding() {
        let big = Decimal::from_i128_with_scale(100_000_000_000_000_000_000, 0);
        let mut shard_a = ExactSum::new();
        shard_a.add_value(&Value::Decimal(big));
        let mut shard_b = ExactSum::new();
        shard_b.add_value(&Value::Decimal(-big));
        shard_b.add_i64(4);
        let bytes = zerompk::to_msgpack_vec(&shard_a).unwrap();
        let mut merged: ExactSum = zerompk::from_msgpack(&bytes).unwrap();
        merged.merge(&shard_b);
        assert_eq!(merged.sum().unwrap(), Value::Decimal(Decimal::from(4)));
        let mut int_shard = ExactSum::new();
        int_shard.add_i64(4);
        let mut int_merged = ExactSum::new();
        int_merged.merge(&int_shard);
        assert_eq!(int_merged.sum().unwrap(), Value::Integer(4));
    }

    #[test]
    fn decimal_avg_divides_the_exact_total() {
        let mut acc = ExactSum::new();
        acc.add_value(&Value::Decimal(dec("0.1")));
        acc.add_value(&Value::Decimal(dec("0.2")));
        // PostgreSQL: avg(0.1, 0.2) is 0.15000000000000000000.
        assert_eq!(avg_text_of(&acc), "0.15000000000000000000");
    }

    #[test]
    fn a_decimal_total_past_the_range_is_overflow_error() {
        let mut acc = ExactSum::new();
        acc.add_value(&Value::Decimal(Decimal::MAX - dec("0.5")));
        acc.add_value(&Value::Decimal(Decimal::MAX - dec("0.5")));
        assert_eq!(
            acc.sum(),
            Err(EvalError::NumericOverflow { function: "sum" })
        );
        let mut joined = ExactSum::new();
        joined.add_value(&Value::Decimal(Decimal::MAX));
        joined.add_value(&Value::Decimal(dec("0.5")));
        assert_eq!(
            joined.sum(),
            Err(EvalError::NumericOverflow { function: "sum" })
        );
    }

    #[test]
    fn numeric_text_is_exact_where_it_can_be() {
        assert_eq!(numeric_text("12"), Some(Value::Integer(12)));
        assert_eq!(numeric_text("0.1"), Some(Value::Decimal(dec("0.1"))));
        assert_eq!(numeric_text("1e3"), Some(Value::Float(1000.0)));
        assert_eq!(numeric_text("x"), None);
        let mut acc = ExactSum::new();
        for text in ["0.1", "0.2"] {
            assert!(acc.add_value(&sum_input(&Value::String(text.into())).unwrap()));
        }
        assert_eq!(acc.sum().unwrap(), Value::Decimal(dec("0.3")));
    }

    #[test]
    fn decimal_parts_merge_and_round_trip() {
        let mut a = ExactSum::new();
        a.add_value(&Value::Decimal(dec("0.1")));
        let mut b = ExactSum::new();
        b.add_value(&Value::Decimal(dec("0.2")));
        b.add_i64(1);
        a.merge(&b);
        assert_eq!(a.sum().unwrap(), Value::Decimal(dec("1.3")));
        let bytes = zerompk::to_msgpack_vec(&a).unwrap();
        let back: ExactSum = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn sum_inputs_sum_exactly() {
        let mut acc = ExactSum::new();
        for v in [
            Value::Integer(ABOVE),
            Value::from_u64(u64::MAX),
            Value::String("2".into()),
        ] {
            assert!(acc.add_value(&sum_input(&v).unwrap()));
        }
        assert_eq!(sum_input(&Value::Null), None);
        assert_eq!(
            acc.sum().unwrap(),
            Value::Decimal(Decimal::from_i128_with_scale(
                i128::from(ABOVE) + i128::from(u64::MAX) + 2,
                0
            ))
        );
    }

    fn avg_text(inputs: &[&str]) -> String {
        let mut acc = ExactSum::new();
        for d in inputs {
            acc.add_decimal(dec(d));
        }
        avg_text_of(&acc)
    }

    /// The text of a numeric AVG.
    fn avg_text_of(acc: &ExactSum) -> String {
        match acc.avg_value().unwrap() {
            Value::Decimal(avg) => avg.to_string(),
            other => panic!("AVG over integers and numerics is a Decimal, got {other:?}"),
        }
    }

    /// AVG over integers and numerics is the exact total over the count at
    /// PostgreSQL's division scale. A float input makes it a `Float`.
    #[test]
    fn avg_over_decimals_is_an_exact_decimal() {
        // PostgreSQL: avg(0.1000000000000000001, 0.2000000000000000002)
        // = 0.15000000000000000015.
        assert_eq!(
            avg_text(&["0.1000000000000000001", "0.2000000000000000002"]),
            "0.15000000000000000015"
        );
        // PostgreSQL: avg(1.0, 2.0) = 1.5000000000000000, avg(1.5, 2.5) =
        // 2.0000000000000000, avg(1, 2, 2) over numeric = 1.6666666666666667.
        assert_eq!(avg_text(&["1.0", "2.0"]), "1.5000000000000000");
        assert_eq!(avg_text(&["1.5", "2.5"]), "2.0000000000000000");
        assert_eq!(avg_text(&["1", "2", "2"]), "1.6666666666666667");
        let mut acc = ExactSum::new();
        acc.add_decimal(dec("0.1"));
        acc.add_f64(1.0);
        assert!(matches!(acc.avg_value().unwrap(), Value::Float(_)));
        let mut ints = ExactSum::new();
        ints.add_i64(1);
        ints.add_i64(2);
        // PostgreSQL: avg(1, 2) over integers is 1.5000000000000000.
        assert_eq!(avg_text_of(&ints), "1.5000000000000000");
        assert_eq!(ExactSum::new().avg_value().unwrap(), Value::Null);
    }
}
