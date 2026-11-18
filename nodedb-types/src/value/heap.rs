// SPDX-License-Identifier: Apache-2.0

//! The heap bytes a [`Value`] holds, counted without encoding it.

use super::core::Value;

impl Value {
    /// Heap bytes the value holds beyond its own size.
    pub fn heap_bytes(&self) -> usize {
        let nested = |values: &[Value]| -> usize {
            values
                .iter()
                .map(|value| std::mem::size_of::<Value>() + value.heap_bytes())
                .sum()
        };
        match self {
            Value::Null
            | Value::Bool(_)
            | Value::Integer(_)
            | Value::Float(_)
            | Value::DateTime(_)
            | Value::NaiveDateTime(_)
            | Value::Duration(_)
            | Value::Decimal(_) => 0,
            Value::String(text) | Value::Uuid(text) | Value::Ulid(text) | Value::Regex(text) => {
                text.len()
            }
            Value::Bytes(bytes) => bytes.len(),
            Value::Array(values) | Value::Set(values) => nested(values),
            Value::Object(fields) => fields
                .iter()
                .map(|(name, value)| {
                    std::mem::size_of::<(String, Value)>() + name.len() + value.heap_bytes()
                })
                .sum(),
            Value::Range { start, end, .. } => [start, end]
                .into_iter()
                .flatten()
                .map(|bound| std::mem::size_of::<Value>() + bound.heap_bytes())
                .sum(),
            Value::Record { table, id } => table.len() + id.len(),
            Value::Vector(floats) => std::mem::size_of_val(&**floats),
            Value::Geometry(geometry) => geometry.heap_bytes(),
            Value::ArrayCell(cell) => cell.heap_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::geometry::{Geometry, Srid, SridGeometry};

    #[test]
    fn nested_values_count_their_contents() {
        assert_eq!(Value::Integer(7).heap_bytes(), 0);
        assert_eq!(Value::String("abcd".into()).heap_bytes(), 4);
        let object = Value::Object(HashMap::from([(
            "k".to_string(),
            Value::Bytes(vec![0; 10]),
        )]));
        assert_eq!(
            object.heap_bytes(),
            std::mem::size_of::<(String, Value)>() + 1 + 10
        );
    }

    #[test]
    fn a_geometry_counts_its_coordinates() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]);
        let value = Value::Geometry(SridGeometry::new(Srid::WGS84, line));
        assert_eq!(value.heap_bytes(), 3 * std::mem::size_of::<[f64; 2]>());
    }
}
