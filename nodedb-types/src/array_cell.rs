//! Typed payload for `Value::NdArrayCell`.
//!
//! An `ArrayCell` carries a single N-dimensional array cell across the
//! SQL / wire boundary: its coordinates (one `Value` per dimension) and
//! its attributes (one `Value` per attribute column). The array engine
//! converts between its own typed `CoordValue` / `CellValue` and this
//! generic carrier at engine boundaries.
//!
//! Using `nodedb_types::Value` for both coords and attrs keeps
//! `nodedb-types` free of any dependency on `nodedb-array`.
//!
//! Two cells are equal when their coords and attrs are structurally
//! equal. Ordering is lexicographic on coords first, then attrs — this
//! matches N-d array semantics where cells are ordered by coordinate.

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// One N-dimensional array cell — coordinates plus attribute values.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCell {
    /// One `Value` per dimension. Length = rank of the array.
    pub coords: Vec<Value>,
    /// One `Value` per attribute column.
    pub attrs: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cell() -> ArrayCell {
        ArrayCell {
            coords: vec![Value::Integer(1), Value::Integer(2)],
            attrs: vec![Value::Float(3.5), Value::String("label".into())],
        }
    }

    #[test]
    fn zerompk_roundtrip() {
        let cell = sample_cell();
        let bytes = zerompk::to_msgpack_vec(&cell).expect("encode");
        let decoded: ArrayCell = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, cell);
    }

    #[test]
    fn json_roundtrip() {
        let cell = sample_cell();
        let s = sonic_rs::to_string(&cell).expect("json encode");
        let decoded: ArrayCell = sonic_rs::from_str(&s).expect("json decode");
        assert_eq!(decoded, cell);
    }

    #[test]
    fn equality_structural() {
        let a = sample_cell();
        let b = sample_cell();
        assert_eq!(a, b);

        let mut c = sample_cell();
        c.coords[0] = Value::Integer(999);
        assert_ne!(a, c);
    }
}
