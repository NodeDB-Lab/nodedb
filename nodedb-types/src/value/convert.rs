// SPDX-License-Identifier: Apache-2.0

//! `From` conversions for [`Value`].

use std::sync::Arc;

use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::Geometry;
use crate::value::core::Value;

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl Value {
    /// An unsigned integer as a `Value` that keeps its exact number.
    ///
    /// A value up to `i64::MAX` is an `Integer`. A larger value is a
    /// `Decimal`, because an `Integer` cannot hold it without wrapping.
    pub fn from_u64(u: u64) -> Self {
        match i64::try_from(u) {
            Ok(i) => Value::Integer(i),
            Err(_) => Value::Decimal(rust_decimal::Decimal::from(u)),
        }
    }

    /// The `u64` that `d` stands for when `d` is a `Decimal`
    /// [`Value::from_u64`] produces: scale `0`, above `i64::MAX`, at most
    /// `u64::MAX`. `None` for any other decimal.
    ///
    /// Msgpack writers encode such a decimal as a `uint64`, the number type
    /// that holds it, and readers decode a `uint64` through `from_u64`, so
    /// the value round-trips to the same `Decimal`. Every other decimal stays
    /// text: an `Integer`-range or scaled decimal would not read back as a
    /// `Decimal` of the same scale.
    pub fn decimal_as_wide_u64(d: &rust_decimal::Decimal) -> Option<u64> {
        use rust_decimal::prelude::ToPrimitive;
        if d.scale() != 0 {
            return None;
        }
        d.to_u64().filter(|u| i64::try_from(*u).is_err())
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl From<NdbDateTime> for Value {
    fn from(dt: NdbDateTime) -> Self {
        Value::DateTime(dt)
    }
}

impl From<NdbDuration> for Value {
    fn from(d: NdbDuration) -> Self {
        Value::Duration(d)
    }
}

impl From<rust_decimal::Decimal> for Value {
    fn from(d: rust_decimal::Decimal) -> Self {
        Value::Decimal(d)
    }
}

impl From<Geometry> for Value {
    fn from(g: Geometry) -> Self {
        Value::Geometry(g)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v.into())
    }
}

impl From<Arc<[f32]>> for Value {
    fn from(v: Arc<[f32]>) -> Self {
        Value::Vector(v)
    }
}

impl From<&[f32]> for Value {
    fn from(v: &[f32]) -> Self {
        Value::Vector(v.into())
    }
}
