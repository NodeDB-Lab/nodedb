// SPDX-License-Identifier: Apache-2.0

//! Typed field-reader helpers shared by every variant in
//! [`super::from_messagepack`].
//!
//! Each variant payload is a MessagePack map of `{u8 key: value}` pairs.
//! The decoder dispatches on the variant tag and then calls one of these
//! helpers to pull the fields. Every helper requires exactly the field count
//! the encoder writes for the variant. A payload with fewer or more fields is
//! a decode error.

use zerompk::{FromMessagePack, Read};

use crate::sync::compensation::CompensationHint;

/// Read the 2-element outer array and return `(tag, field_count)`.
#[inline]
pub(super) fn read_header<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<(u16, usize)> {
    let outer = reader.read_array_len()?;
    if outer != 2 {
        return Err(zerompk::Error::ArrayLengthMismatch {
            expected: 2,
            actual: outer,
        });
    }
    let tag = reader.read_u16()?;
    let field_count = reader.read_map_len()?;
    Ok((tag, field_count))
}

/// Refuse a payload whose field count is not `expected`.
#[inline]
fn expect_fields(field_count: usize, expected: usize) -> zerompk::Result<()> {
    if field_count != expected {
        return Err(zerompk::Error::MapLengthMismatch {
            expected,
            actual: field_count,
        });
    }
    Ok(())
}

/// Check the empty payload of a unit variant.
#[inline]
pub(super) fn read_no_fields(field_count: usize) -> zerompk::Result<()> {
    expect_fields(field_count, 0)
}

/// Read the next field's key and its string value.
#[inline]
fn read_str_field<'a, R: Read<'a>>(reader: &mut R) -> zerompk::Result<String> {
    reader.read_u8()?;
    Ok(reader.read_string()?.into_owned())
}

pub(super) fn read_u8_field<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<u8> {
    expect_fields(field_count, 1)?;
    reader.read_u8()?;
    reader.read_u8()
}

pub(super) fn read1_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String,)> {
    expect_fields(field_count, 1)?;
    Ok((read_str_field(reader)?,))
}

pub(super) fn read2_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String)> {
    expect_fields(field_count, 2)?;
    let v1 = read_str_field(reader)?;
    let v2 = read_str_field(reader)?;
    Ok((v1, v2))
}

pub(super) fn read3_str<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, String, String)> {
    expect_fields(field_count, 3)?;
    let v1 = read_str_field(reader)?;
    let v2 = read_str_field(reader)?;
    let v3 = read_str_field(reader)?;
    Ok((v1, v2, v3))
}

pub(super) fn read_collection_deactivated<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(String, u64, String)> {
    expect_fields(field_count, 3)?;
    let collection = read_str_field(reader)?;
    reader.read_u8()?;
    let retention_expires_at_ns = reader.read_u64()?;
    let undrop_hint = read_str_field(reader)?;
    Ok((collection, retention_expires_at_ns, undrop_hint))
}

/// Read two `u32` fields.
pub(super) fn read2_u32<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u32, u32)> {
    expect_fields(field_count, 2)?;
    reader.read_u8()?;
    let v1 = reader.read_u32()?;
    reader.read_u8()?;
    let v2 = reader.read_u32()?;
    Ok((v1, v2))
}

/// Read two `u64` fields.
pub(super) fn read2_u64<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u64, u64)> {
    expect_fields(field_count, 2)?;
    reader.read_u8()?;
    let v1 = reader.read_u64()?;
    reader.read_u8()?;
    let v2 = reader.read_u64()?;
    Ok((v1, v2))
}

/// Read a single array field containing strings.
/// The field layout is: `{key: u8, value: [str, ...]}`.
pub(super) fn read_string_vec<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<Vec<String>> {
    expect_fields(field_count, 1)?;
    reader.read_u8()?;
    Vec::<String>::read(reader)
}

/// Read `SegmentCorrupted` fields: (segment_id: u64, corruption: String, detail: String).
pub(super) fn read_segment_corrupted<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<(u64, String, String)> {
    expect_fields(field_count, 3)?;
    reader.read_u8()?;
    let segment_id = reader.read_u64()?;
    let corruption = read_str_field(reader)?;
    let detail = read_str_field(reader)?;
    Ok((segment_id, corruption, detail))
}

pub(super) fn read_sync_delta_rejected<'a, R: Read<'a>>(
    reader: &mut R,
    field_count: usize,
) -> zerompk::Result<Option<CompensationHint>> {
    expect_fields(field_count, 1)?;
    reader.read_u8()?;
    Option::<CompensationHint>::read(reader)
}
