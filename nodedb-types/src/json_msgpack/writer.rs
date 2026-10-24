// SPDX-License-Identifier: Apache-2.0

//! Msgpack serialization for `serde_json::Value` and `nodedb_types::Value`.
//!
//! `Value::DateTime` and `Value::NaiveDateTime` are written as the instant
//! ext (`fixext8`, see `instant_ext`). `Duration`, `Decimal`, and `Geometry`
//! are written as strings, except a `Decimal` that holds a `u64` above
//! `i64::MAX`, which is a `uint64`. `Vector` is a float64 array, `ArrayCell`
//! a map, and `Range` / `Record` are `nil`.

use zerompk::Write;

use super::instant_ext::InstantKind;
use super::json_value::JsonValue;

/// Serialize a `serde_json::Value` to MessagePack bytes.
#[inline]
pub fn json_to_msgpack(value: &serde_json::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&JsonValue(value.clone()))
}

/// Serialize a `serde_json::Value` to MessagePack bytes, returning an empty
/// msgpack map (`0x80`) on failure.
///
/// Suitable for filter evaluation where an empty map causes all field
/// predicates to pass vacuously. Callers that need error propagation
/// should use [`json_to_msgpack`] instead.
#[inline]
pub fn json_to_msgpack_or_empty(value: &serde_json::Value) -> Vec<u8> {
    json_to_msgpack(value).unwrap_or_else(|_| vec![0x80])
}

/// Serialize a `nodedb_types::Value` to standard MessagePack bytes.
///
/// Writes standard msgpack format (fixmap 0x80-0x8F, fixstr 0xA0-0xBF, etc.)
/// directly from `Value` — no zerompk tagged encoding.
pub fn value_to_msgpack(value: &crate::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&NativeRef(value))
}

/// Serialize `fields` as one standard msgpack map whose keys keep the given
/// order.
///
/// A `Value::Object` writes its keys in hash order, which differs between
/// two maps of the same keys. A row whose bytes must be reproducible, or
/// whose columns must read in a fixed order, is written through this.
pub fn ordered_map_to_msgpack(fields: &[(&str, &crate::Value)]) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&OrderedMapRef(fields))
}

/// A borrowed `Value` written in its plain msgpack form.
struct NativeRef<'a>(&'a crate::Value);

impl zerompk::ToMessagePack for NativeRef<'_> {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        write_native_value(writer, self.0)
    }
}

/// Borrowed key/value pairs written as one plain msgpack map, in order.
struct OrderedMapRef<'a>(&'a [(&'a str, &'a crate::Value)]);

impl zerompk::ToMessagePack for OrderedMapRef<'_> {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(self.0.len())?;
        for (key, value) in self.0 {
            writer.write_string(key)?;
            write_native_value(writer, value)?;
        }
        Ok(())
    }
}

/// Write a `nodedb_types::Value` as standard msgpack.
///
/// `Duration` and `Decimal` are strings, except a `Decimal` that holds a
/// `u64` above `i64::MAX` (see [`crate::Value::decimal_as_wide_u64`]),
/// which is a `uint64`. `Geometry` is the geometry ext (type 3). `Vector` is a float64 array, `ArrayCell` a map, and
/// `Range` / `Record` are `nil`.
pub(crate) fn write_native_value<W: Write>(
    writer: &mut W,
    value: &crate::Value,
) -> zerompk::Result<()> {
    match value {
        crate::Value::Null => writer.write_nil(),
        crate::Value::Bool(b) => writer.write_boolean(*b),
        crate::Value::Integer(i) => writer.write_i64(*i),
        crate::Value::Float(f) => writer.write_f64(*f),
        crate::Value::String(s)
        | crate::Value::Uuid(s)
        | crate::Value::Ulid(s)
        | crate::Value::Regex(s) => writer.write_string(s),
        crate::Value::Bytes(b) => writer.write_binary(b),
        crate::Value::Array(arr) | crate::Value::Set(arr) => {
            writer.write_array_len(arr.len())?;
            for v in arr {
                write_native_value(writer, v)?;
            }
            Ok(())
        }
        crate::Value::Object(map) => {
            writer.write_map_len(map.len())?;
            for (k, v) in map {
                writer.write_string(k)?;
                write_native_value(writer, v)?;
            }
            Ok(())
        }
        crate::Value::DateTime(dt) => write_instant_ext(writer, InstantKind::Utc, dt.micros),
        crate::Value::NaiveDateTime(dt) => write_instant_ext(writer, InstantKind::Naive, dt.micros),
        crate::Value::Duration(d) => writer.write_string(&d.to_string()),
        crate::Value::Decimal(d) => match crate::Value::decimal_as_wide_u64(d) {
            Some(u) => writer.write_u64(u),
            None => writer.write_string(&d.to_string()),
        },
        crate::Value::Geometry(g) => super::geometry_ext::write_geometry_ext(writer, g),
        crate::Value::Range { .. } | crate::Value::Record { .. } => writer.write_nil(),
        crate::Value::Vector(v) => {
            // A standard msgpack array of float64 values, so pgwire clients
            // receive a plain JSON number array.
            writer.write_array_len(v.len())?;
            for f in v.iter() {
                writer.write_f64(f64::from(*f))?;
            }
            Ok(())
        }
        // ArrayCell is encoded as a `{coords:[...], attrs:[...]}` map so the
        // pgwire `msgpack_to_json_string` transcoder produces clean JSON for
        // clients reading slice/project rows. On audit-log (AS OF SYSTEM TIME
        // NULL) reads each version carries its system-time, surfaced as the
        // `_ts_system` column (mirrors the document-engine audit-log shape).
        crate::Value::ArrayCell(cell) => {
            let map_len = if cell.system_time.is_some() { 3 } else { 2 };
            writer.write_map_len(map_len)?;
            writer.write_string("coords")?;
            writer.write_array_len(cell.coords.len())?;
            for v in &cell.coords {
                write_native_value(writer, v)?;
            }
            writer.write_string("attrs")?;
            writer.write_array_len(cell.attrs.len())?;
            for v in &cell.attrs {
                write_native_value(writer, v)?;
            }
            if let Some(ts) = cell.system_time {
                writer.write_string("_ts_system")?;
                writer.write_i64(ts)?;
            }
            Ok(())
        }
    }
}

/// Write an instant as the `fixext8` ext `write_instant` produces.
fn write_instant_ext<W: Write>(
    writer: &mut W,
    kind: InstantKind,
    micros: i64,
) -> zerompk::Result<()> {
    writer.write_ext(kind.ext_type(), &micros.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ordered_map_keeps_its_key_order_and_reads_back() {
        let x = crate::Value::Integer(3);
        let nir = crate::Value::Float(0.5);
        let bytes = ordered_map_to_msgpack(&[("x", &x), ("nir", &nir)]).expect("encode");
        // fixmap of 2, then fixstr "x" first.
        assert_eq!(&bytes[..3], &[0x82, 0xa1, b'x']);
        match crate::json_msgpack::value_from_msgpack(&bytes).expect("decode") {
            crate::Value::Object(map) => {
                assert_eq!(map.get("x"), Some(&x));
                assert_eq!(map.get("nir"), Some(&nir));
            }
            other => panic!("expected an object, got {other:?}"),
        }
    }
}
