// SPDX-License-Identifier: Apache-2.0

//! Exact SUM / AVG accumulation, shared by every aggregate path.
//!
//! The rule, applied the same way everywhere:
//!
//! - An integer input adds exactly into an `i128`. A `u64` above `i64::MAX`
//!   and an integral `Decimal` are integer inputs.
//! - A float input adds into a Kahan-compensated `f64`. A fractional
//!   `Decimal` is a float input.
//! - SUM with only integer inputs is an `Integer` when the total fits `i64`.
//!   A larger total is a `Decimal`. A total past the `Decimal` range is
//!   [`EvalError::NumericOverflow`].
//! - SUM with at least one float input is a `Float`: the exact integer part
//!   plus the float part.
//! - AVG is a `Float`. With only integer inputs it divides the exact `i128`
//!   total by the count, with no rounding of the total first.
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
    /// The integer total left the `i128` range.
    int_overflow: bool,
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
        match value_reading(v) {
            Some(n) => {
                self.add_reading(n);
                true
            }
            None => false,
        }
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
            serde_json::Value::String(s) => parse_numeric_str(s),
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

    /// Fold another partial accumulator into this one without loss.
    pub fn merge(&mut self, other: &ExactSum) {
        match self.int.checked_add(other.int) {
            Some(total) => self.int = total,
            None => self.int_overflow = true,
        }
        self.int_overflow |= other.int_overflow;
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
        if self.int_overflow {
            return Err(EvalError::NumericOverflow { function: "sum" });
        }
        if self.has_float {
            return Ok(Value::Float(self.sum_f64()));
        }
        if let Ok(i) = i64::try_from(self.int) {
            return Ok(Value::Integer(i));
        }
        Decimal::try_from_i128_with_scale(self.int, 0)
            .map(Value::Decimal)
            .map_err(|_| EvalError::NumericOverflow { function: "sum" })
    }

    /// The total read as an `f64`, for a float-typed result. Rounds an
    /// integer total above 2^53; [`Self::sum`] keeps it exact. `0.0` for no
    /// input.
    pub fn sum_f64(&self) -> f64 {
        self.int as f64 + (self.float - self.comp)
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
        if self.int_overflow {
            return Err(EvalError::NumericOverflow { function: "avg" });
        }
        let n = i128::from(self.count);
        let avg = if self.has_float {
            self.sum_f64() / self.count as f64
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
            None => self.int_overflow = true,
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
/// itself, a numeric string as the number it spells (an integer string
/// stays exact). `None` for any other value, which contributes nothing.
pub fn sum_input(v: &Value) -> Option<Value> {
    match v {
        Value::Integer(_) | Value::Float(_) | Value::Decimal(_) => Some(v.clone()),
        Value::String(s) => parse_numeric_str(s).map(numeric_to_value),
        _ => None,
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

/// The SUM input reading of a numeric `Value`. An integral `Decimal` is an
/// exact integer.
fn value_reading(v: &Value) -> Option<Numeric> {
    match v {
        Value::Integer(i) => Some(Numeric::Int(i128::from(*i))),
        Value::Float(f) => Some(Numeric::Float(*f)),
        Value::Decimal(d) => decimal_reading(d),
        _ => None,
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

/// Element count of the MessagePack form: the array `[int_hi, int_lo,
/// int_overflow, float, comp, has_float, count]`. The `i128` total is split
/// into its high `i64` and low `u64` halves, so it round-trips exactly.
const MSGPACK_FIELDS: usize = 7;

impl zerompk::ToMessagePack for ExactSum {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(MSGPACK_FIELDS)?;
        writer.write_i64((self.int >> 64) as i64)?;
        writer.write_u64(self.int as u64)?;
        writer.write_boolean(self.int_overflow)?;
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
        Ok(Self {
            int: (i128::from(hi) << 64) | i128::from(lo),
            int_overflow: reader.read_boolean()?,
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
            Value::Decimal(Decimal::from_i128_with_scale(
                2 * i128::from(i64::MAX),
                0
            ))
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
        assert_eq!(
            small.sum().unwrap(),
            Value::Integer(i64::MAX - 1)
        );
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
