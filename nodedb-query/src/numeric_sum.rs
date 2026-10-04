// SPDX-License-Identifier: Apache-2.0

//! Exact SUM / AVG accumulation, shared by every aggregate path.
//!
//! The rule, applied the same way everywhere:
//!
//! - An integer input adds exactly into an `i128`. A `u64` above `i64::MAX`
//!   and an integral `Decimal` are integer inputs.
//! - A fractional `Decimal` adds exactly into a `Decimal`.
//! - A float input adds into a Kahan-compensated `f64`.
//! - A numeric string is the number it spells: an integer, or a `Decimal`
//!   when it has a fraction. A `DECIMAL` cell is stored as its text, so this
//!   keeps a `DECIMAL` column exact.
//! - SUM with only integer inputs is an `Integer` when the total fits `i64`.
//!   A larger total is a `Decimal`.
//! - SUM with a fractional `Decimal` input and no float input is a
//!   `Decimal`: the exact integer part plus the exact decimal part.
//! - An exact total past the `Decimal` range is
//!   [`EvalError::NumericOverflow`].
//! - SUM with at least one float input is a `Float`: the exact parts plus
//!   the float part.
//! - AVG is a `Float`. With no float input it divides the exact total by the
//!   count, with no rounding of the total first.
//! - SUM and AVG over no input are NULL.
//! - Two partial accumulators merge without loss: the integer parts add
//!   exactly, so a shard or spill merge gives the single-pass result.
//!
//! STDDEV and VARIANCE stay in `f64` (Welford); they are not exact sums.

use nodedb_types::Value;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::expr::EvalError;
use crate::json_ops::{Numeric, parse_numeric_str};

/// Running exact SUM / AVG state. See the module docs for the rule.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExactSum {
    /// Exact total of the integer inputs.
    #[serde(with = "i128_text")]
    int: i128,
    /// Exact total of the fractional `Decimal` inputs.
    frac: Decimal,
    /// At least one fractional `Decimal` input was added.
    has_decimal: bool,
    /// An exact total (`int` or `frac`) left its range.
    exact_overflow: bool,
    /// Kahan-compensated total of the float inputs.
    float: f64,
    /// Kahan compensation term of `float`.
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
        self.kahan_add(v);
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

    /// Add one `Decimal` input: an integral one into the integer total, any
    /// other into the exact decimal total.
    pub fn add_decimal(&mut self, d: Decimal) {
        if d.fract().is_zero()
            && let Some(i) = d.to_i128()
        {
            self.add_int(i);
            return;
        }
        match self.frac.checked_add(d) {
            Some(total) => self.frac = total,
            None => self.exact_overflow = true,
        }
        self.has_decimal = true;
        self.count += 1;
    }

    /// Add a JSON number or numeric string. Any other value adds nothing.
    /// Returns whether the value was added.
    pub fn add_json(&mut self, v: &serde_json::Value) -> bool {
        let reading = match v {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(Numeric::Int(i128::from(i)))
                } else if let Some(u) = n.as_u64() {
                    Some(Numeric::Int(i128::from(u)))
                } else {
                    n.as_f64().map(Numeric::Float)
                }
            }
            serde_json::Value::String(s) => return self.add_value_opt(numeric_text(s)),
            _ => None,
        };
        match reading {
            Some(n) => {
                self.add_reading(n);
                true
            }
            None => false,
        }
    }

    fn add_value_opt(&mut self, v: Option<Value>) -> bool {
        v.is_some_and(|v| self.add_value(&v))
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
        self.kahan_add(other.float);
        self.kahan_add(-other.comp);
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
    /// input.
    pub fn sum_f64(&self) -> f64 {
        self.int as f64 + self.frac.to_f64().unwrap_or(0.0) + (self.float - self.comp)
    }

    /// The integer and decimal totals as one `Decimal`. `None` past the
    /// `Decimal` range.
    fn exact_decimal_total(&self) -> Option<Decimal> {
        Decimal::try_from_i128_with_scale(self.int, 0)
            .ok()?
            .checked_add(self.frac)
    }

    /// AVG of the inputs as a `Float`. NULL for no input.
    pub fn avg(&self) -> Result<Value, EvalError> {
        match self.avg_f64()? {
            Some(avg) => Ok(Value::Float(avg)),
            None => Ok(Value::Null),
        }
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

    pub(crate) fn add_reading(&mut self, n: Numeric) {
        match n {
            Numeric::Int(i) => self.add_int(i),
            Numeric::Float(f) => self.add_f64(f),
        }
    }

    fn add_int(&mut self, v: i128) {
        match self.int.checked_add(v) {
            Some(total) => self.int = total,
            None => self.exact_overflow = true,
        }
        self.count += 1;
    }

    fn kahan_add(&mut self, v: f64) {
        let y = v - self.comp;
        let t = self.float + y;
        self.comp = (t - self.float) - y;
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
    match parse_numeric_str(s)? {
        Numeric::Int(i) => Some(numeric_to_value(Numeric::Int(i))),
        Numeric::Float(f) => Some(match Decimal::from_str_exact(s.trim()) {
            Ok(d) => Value::Decimal(d),
            Err(_) => Value::Float(f),
        }),
    }
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
        Numeric::Float(f) => Value::Float(f),
    }
}

/// An integral `Decimal` as an exact integer, any other as a float.
pub(crate) fn decimal_reading(d: &Decimal) -> Option<Numeric> {
    if d.fract().is_zero()
        && let Some(i) = d.to_i128()
    {
        return Some(Numeric::Int(i));
    }
    d.to_f64().map(Numeric::Float)
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

/// `i128` as a decimal string, so the state survives JSON spill and shard
/// transport, whose number types stop at 64 bits.
mod i128_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &i128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i128, D::Error> {
        let text = String::deserialize(d)?;
        text.parse::<i128>().map_err(serde::de::Error::custom)
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
            acc.avg(),
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
        assert_eq!(acc.avg().unwrap(), Value::Null);
        assert_eq!(sum_of(&[Value::String("x".into())]).unwrap(), Value::Null);
    }

    #[test]
    fn avg_uses_the_exact_total() {
        let mut acc = ExactSum::new();
        acc.add_i64(ABOVE);
        acc.add_i64(AT);
        // Exact mean is 2^53 + 0.5; the f64 nearest is 2^53.
        assert_eq!(acc.avg().unwrap(), Value::Float(AT as f64));
        let mut big = ExactSum::new();
        big.add_i64(i64::MAX);
        big.add_i64(i64::MAX);
        assert_eq!(big.avg().unwrap(), Value::Float(i64::MAX as f64));
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

    #[test]
    fn serde_round_trip_keeps_the_integer_total() {
        let mut acc = ExactSum::new();
        acc.add_u64(u64::MAX);
        acc.add_u64(u64::MAX);
        let text = sonic_rs::to_string(&acc).unwrap();
        let back: ExactSum = sonic_rs::from_str(&text).unwrap();
        assert_eq!(back, acc);
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

    #[test]
    fn decimal_avg_divides_the_exact_total() {
        let mut acc = ExactSum::new();
        acc.add_value(&Value::Decimal(dec("0.1")));
        acc.add_value(&Value::Decimal(dec("0.2")));
        assert_eq!(acc.avg().unwrap(), Value::Float(0.15));
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
        let text = sonic_rs::to_string(&a).unwrap();
        let back: ExactSum = sonic_rs::from_str(&text).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn json_numbers_sum_exactly() {
        let mut acc = ExactSum::new();
        acc.add_json(&serde_json::json!(ABOVE));
        acc.add_json(&serde_json::json!(u64::MAX));
        acc.add_json(&serde_json::json!("2"));
        assert!(!acc.add_json(&serde_json::json!(null)));
        assert_eq!(
            acc.sum().unwrap(),
            Value::Decimal(Decimal::from_i128_with_scale(
                i128::from(ABOVE) + i128::from(u64::MAX) + 2,
                0
            ))
        );
    }
}
