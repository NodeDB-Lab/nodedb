// SPDX-License-Identifier: BUSL-1.1

//! The binary array parameter decoder.
//!
//! A binary array binds as its elements, each decoded by the scalar rule
//! for the element type. A multi-dimensional array is an array of arrays.

use bytes::Bytes;
use pgwire::api::Type;
use pgwire::error::PgWireResult;

use super::param_composite::malformed;
use super::param_typed::pg_error;

/// The SQLSTATE of an array past a size limit (`program_limit_exceeded`).
const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
/// The SQLSTATE of an array whose element type is not the declared one.
const DATATYPE_MISMATCH: &str = "42804";
/// The most dimensions an array can have (PostgreSQL's `MAXDIM`).
const MAX_DIMS: usize = 6;

/// A binary array of `member` elements, each decoded by `element`.
///
/// The payload is: dimension count, null flag, element OID, then a length
/// and lower bound per dimension, then each element as a length (`-1` for
/// `NULL`) and its bytes. The payload is untrusted, so the header is
/// checked as PostgreSQL's `array_recv` checks it before anything is
/// allocated:
///
/// - More than [`MAX_DIMS`] dimensions is refused (SQLSTATE 54000).
/// - A negative dimension count or length, a flag other than 0 or 1, or a
///   header longer than the payload is refused (SQLSTATE 22P02).
/// - An element OID other than the declared element type is refused
///   (SQLSTATE 42804).
/// - A lower bound whose upper bound overflows `int4`, or an element count
///   that overflows, is refused (SQLSTATE 54000). Lower bounds are
///   otherwise ignored: the bound array is zero-based.
/// - An element count above what the remaining bytes can hold (4 bytes per
///   element at least) is refused (SQLSTATE 22P02).
/// - A dimension of length 0 makes the array empty, as in PostgreSQL.
pub(super) fn binary_array(
    bytes: &[u8],
    member: &Type,
    index: usize,
    element: &dyn Fn(&Bytes, &Type, usize) -> PgWireResult<nodedb_sql::ParamValue>,
) -> PgWireResult<nodedb_sql::ParamValue> {
    let mut reader = Reader {
        bytes,
        at: 0,
        index,
    };
    let dims = reader.i32()?;
    let flags = reader.i32()?;
    let element_oid = reader.u32()?;
    if dims < 0 {
        return Err(malformed(
            index,
            &format!("invalid number of dimensions: {dims}"),
        ));
    }
    let dims =
        usize::try_from(dims).map_err(|_| malformed(index, "invalid number of dimensions"))?;
    if dims > MAX_DIMS {
        return Err(pg_error(
            PROGRAM_LIMIT_EXCEEDED,
            format!(
                "number of array dimensions ({dims}) exceeds the maximum allowed ({MAX_DIMS}) \
                 in parameter ${}",
                index + 1
            ),
        ));
    }
    if flags != 0 && flags != 1 {
        return Err(malformed(index, &format!("invalid array flags {flags}")));
    }
    if element_oid != member.oid() {
        return Err(pg_error(
            DATATYPE_MISMATCH,
            format!(
                "binary data for parameter ${} has array element type {element_oid} \
                 instead of expected {} ({})",
                index + 1,
                member.oid(),
                member.name()
            ),
        ));
    }
    if dims == 0 {
        return finish(&reader, nodedb_sql::ParamValue::Array(Vec::new()));
    }
    if reader.remaining() < dims * 8 {
        return Err(malformed(index, "array dimension header is truncated"));
    }
    let mut lengths = Vec::with_capacity(dims);
    for _ in 0..dims {
        let length = reader.i32()?;
        let lower_bound = reader.i32()?;
        if length < 0 {
            return Err(malformed(
                index,
                &format!("negative dimension length {length}"),
            ));
        }
        if length > 0 && lower_bound.checked_add(length - 1).is_none() {
            return Err(pg_error(
                PROGRAM_LIMIT_EXCEEDED,
                format!(
                    "array upper bound is too large in parameter ${}: lower bound {lower_bound}, \
                     length {length}",
                    index + 1
                ),
            ));
        }
        lengths.push(
            usize::try_from(length).map_err(|_| malformed(index, "negative dimension length"))?,
        );
    }
    let total = lengths
        .iter()
        .try_fold(1usize, |total, &length| total.checked_mul(length))
        .ok_or_else(|| {
            pg_error(
                PROGRAM_LIMIT_EXCEEDED,
                format!(
                    "array size exceeds the maximum allowed in parameter ${}",
                    index + 1
                ),
            )
        })?;
    if total == 0 {
        return finish(&reader, nodedb_sql::ParamValue::Array(Vec::new()));
    }
    if total > reader.remaining() / 4 {
        return Err(malformed(
            index,
            &format!(
                "array declares {total} elements but only {} payload bytes remain",
                reader.remaining()
            ),
        ));
    }
    let value = read_dimension(&mut reader, &lengths, member, element)?;
    finish(&reader, value)
}

/// `value`, once the payload holds no bytes past the array.
fn finish(
    reader: &Reader<'_>,
    value: nodedb_sql::ParamValue,
) -> PgWireResult<nodedb_sql::ParamValue> {
    if reader.remaining() != 0 {
        return Err(malformed(
            reader.index,
            "trailing bytes after the array elements",
        ));
    }
    Ok(value)
}

/// One dimension of the array. `lengths` holds at most [`MAX_DIMS`]
/// entries and their product fits the payload, so the recursion depth and
/// every allocation are bounded by the checked header.
fn read_dimension(
    reader: &mut Reader<'_>,
    lengths: &[usize],
    member: &Type,
    element: &dyn Fn(&Bytes, &Type, usize) -> PgWireResult<nodedb_sql::ParamValue>,
) -> PgWireResult<nodedb_sql::ParamValue> {
    let Some((&length, inner)) = lengths.split_first() else {
        return Err(malformed(reader.index, "array has no dimension"));
    };
    let mut items = Vec::with_capacity(length);
    for _ in 0..length {
        let item = if inner.is_empty() {
            match reader.i32()? {
                -1 => nodedb_sql::ParamValue::Null,
                len => {
                    let len = usize::try_from(len)
                        .map_err(|_| malformed(reader.index, "negative element length"))?;
                    let raw = Bytes::copy_from_slice(reader.take(len)?);
                    element(&raw, member, reader.index)?
                }
            }
        } else {
            read_dimension(reader, inner, member, element)?
        };
        items.push(item);
    }
    Ok(nodedb_sql::ParamValue::Array(items))
}

/// A cursor over a binary array payload.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    index: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> PgWireResult<&'a [u8]> {
        let end = self
            .at
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| malformed(self.index, "array payload is truncated"))?;
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn i32(&mut self) -> PgWireResult<i32> {
        let raw = self.take(4)?;
        Ok(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    fn u32(&mut self) -> PgWireResult<u32> {
        let raw = self.take(4)?;
        Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element(bytes: &Bytes, _: &Type, _: usize) -> PgWireResult<nodedb_sql::ParamValue> {
        let raw: [u8; 8] = bytes[..].try_into().expect("int8 element");
        Ok(nodedb_sql::ParamValue::Int64(i64::from_be_bytes(raw)))
    }

    fn int8_array(items: &[Option<i64>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&i32::from(items.iter().any(Option::is_none)).to_be_bytes());
        out.extend_from_slice(&20i32.to_be_bytes());
        out.extend_from_slice(&(items.len() as i32).to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes());
        for item in items {
            match item {
                Some(n) => {
                    out.extend_from_slice(&8i32.to_be_bytes());
                    out.extend_from_slice(&n.to_be_bytes());
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        out
    }

    #[test]
    fn a_binary_array_binds_its_elements_and_nulls() {
        let value =
            binary_array(&int8_array(&[Some(7), None]), &Type::INT8, 0, &element).expect("array");
        let nodedb_sql::ParamValue::Array(items) = value else {
            panic!("an array");
        };
        assert!(matches!(items[0], nodedb_sql::ParamValue::Int64(7)));
        assert!(matches!(items[1], nodedb_sql::ParamValue::Null));
    }

    #[test]
    fn a_truncated_array_is_refused() {
        let mut payload = int8_array(&[Some(7)]);
        payload.pop();
        let error = binary_array(&payload, &Type::INT8, 0, &element).expect_err("truncated");
        assert!(error.to_string().contains("22P02"), "{error}");
    }

    /// An array header: dimension count, flags, element OID, then a
    /// (length, lower bound) pair per dimension.
    fn header(dims: i32, flags: i32, oid: u32, bounds: &[(i32, i32)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&dims.to_be_bytes());
        out.extend_from_slice(&flags.to_be_bytes());
        out.extend_from_slice(&oid.to_be_bytes());
        for (length, lower_bound) in bounds {
            out.extend_from_slice(&length.to_be_bytes());
            out.extend_from_slice(&lower_bound.to_be_bytes());
        }
        out
    }

    fn sqlstate(payload: &[u8]) -> String {
        match binary_array(payload, &Type::INT8, 0, &element) {
            Err(pgwire::error::PgWireError::UserError(info)) => info.code.clone(),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn more_than_six_dimensions_is_refused_with_54000() {
        let bounds = [(1, 1); 7];
        assert_eq!(sqlstate(&header(7, 0, 20, &bounds)), "54000");
        assert_eq!(sqlstate(&header(i32::MAX, 0, 20, &[])), "54000");
    }

    #[test]
    fn a_negative_dimension_count_or_length_is_refused() {
        assert_eq!(sqlstate(&header(-1, 0, 20, &[])), "22P02");
        assert_eq!(sqlstate(&header(1, 0, 20, &[(-1, 1)])), "22P02");
    }

    #[test]
    fn a_header_shorter_than_its_dimension_count_is_refused() {
        assert_eq!(sqlstate(&header(6, 0, 20, &[(1, 1)])), "22P02");
    }

    #[test]
    fn bad_flags_or_a_foreign_element_type_is_refused() {
        let mut payload = header(1, 2, 20, &[(1, 1)]);
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(sqlstate(&payload), "22P02");
        let mut payload = header(1, 1, 23, &[(1, 1)]);
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(sqlstate(&payload), "42804");
    }

    #[test]
    fn an_element_count_the_payload_cannot_hold_is_refused_before_allocating() {
        let huge = header(2, 0, 20, &[(i32::MAX, 1), (i32::MAX, 1)]);
        assert_eq!(sqlstate(&huge), "22P02");
        let mut payload = header(1, 0, 20, &[(3, 1)]);
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(sqlstate(&payload), "22P02");
    }

    #[test]
    fn a_count_that_overflows_is_refused_with_54000() {
        let bounds = [(i32::MAX, 1); 6];
        assert_eq!(sqlstate(&header(6, 0, 20, &bounds)), "54000");
    }

    #[test]
    fn an_upper_bound_past_int4_is_refused_with_54000() {
        let mut payload = header(1, 0, 20, &[(2, i32::MAX)]);
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(sqlstate(&payload), "54000");
    }

    #[test]
    fn a_zero_length_dimension_binds_an_empty_array() {
        let value = binary_array(
            &header(2, 0, 20, &[(i32::MAX, 1), (0, 1)]),
            &Type::INT8,
            0,
            &element,
        )
        .expect("empty");
        assert!(matches!(value, nodedb_sql::ParamValue::Array(items) if items.is_empty()));
        let mut trailing = header(1, 0, 20, &[(0, 1)]);
        trailing.push(0);
        assert_eq!(sqlstate(&trailing), "22P02");
    }

    #[test]
    fn lower_bounds_are_read_and_ignored() {
        let mut payload = header(2, 0, 20, &[(2, -5), (1, 9)]);
        for n in [3i64, 4] {
            payload.extend_from_slice(&8i32.to_be_bytes());
            payload.extend_from_slice(&n.to_be_bytes());
        }
        let nodedb_sql::ParamValue::Array(rows) =
            binary_array(&payload, &Type::INT8, 0, &element).expect("2-D array")
        else {
            panic!("an array");
        };
        assert_eq!(rows.len(), 2);
        assert!(
            matches!(&rows[1], nodedb_sql::ParamValue::Array(row) if matches!(row[..], [nodedb_sql::ParamValue::Int64(4)]))
        );
    }
}
