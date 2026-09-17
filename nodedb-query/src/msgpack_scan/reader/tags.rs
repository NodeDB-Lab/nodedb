// SPDX-License-Identifier: Apache-2.0

//! MessagePack tag constants and bounds-checked byte helpers.

pub(super) const NIL: u8 = 0xc0;
pub(super) const FALSE: u8 = 0xc2;
pub(super) const TRUE: u8 = 0xc3;
pub(super) const BIN8: u8 = 0xc4;
pub(super) const BIN16: u8 = 0xc5;
pub(super) const BIN32: u8 = 0xc6;
pub(super) const EXT8: u8 = 0xc7;
pub(super) const EXT16: u8 = 0xc8;
pub(super) const EXT32: u8 = 0xc9;
pub(super) const FLOAT32: u8 = 0xca;
pub(super) const FLOAT64: u8 = 0xcb;
pub(super) const UINT8: u8 = 0xcc;
pub(super) const UINT16: u8 = 0xcd;
pub(super) const UINT32: u8 = 0xce;
pub(super) const UINT64: u8 = 0xcf;
pub(super) const INT8: u8 = 0xd0;
pub(super) const INT16: u8 = 0xd1;
pub(super) const INT32: u8 = 0xd2;
pub(super) const INT64: u8 = 0xd3;
pub(super) const FIXEXT1: u8 = 0xd4;
pub(super) const FIXEXT2: u8 = 0xd5;
pub(super) const FIXEXT4: u8 = 0xd6;
pub(super) const FIXEXT8: u8 = 0xd7;
pub(super) const FIXEXT16: u8 = 0xd8;
pub(super) const STR8: u8 = 0xd9;
pub(super) const STR16: u8 = 0xda;
pub(super) const STR32: u8 = 0xdb;
pub(super) const ARRAY16: u8 = 0xdc;
pub(super) const ARRAY32: u8 = 0xdd;
pub(super) const MAP16: u8 = 0xde;
pub(super) const MAP32: u8 = 0xdf;

/// Maximum nesting depth to prevent stack overflow on malicious payloads.
pub(super) const MAX_DEPTH: u16 = 128;

#[inline(always)]
pub(super) fn get(buf: &[u8], pos: usize) -> Option<u8> {
    buf.get(pos).copied()
}

#[inline(always)]
pub(super) fn read_u16_be(buf: &[u8], pos: usize) -> Option<u16> {
    let bytes = buf.get(pos..pos + 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

#[inline(always)]
pub(super) fn read_u32_be(buf: &[u8], pos: usize) -> Option<u32> {
    let bytes = buf.get(pos..pos + 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

#[inline(always)]
pub(super) fn read_u64_be(buf: &[u8], pos: usize) -> Option<u64> {
    let bytes = buf.get(pos..pos + 8)?;
    Some(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// Return `Some(offset + size)` only if the buffer has enough bytes.
#[inline(always)]
pub(super) fn checked_advance(buf: &[u8], offset: usize, size: usize) -> Option<usize> {
    let end = offset + size;
    if end <= buf.len() { Some(end) } else { None }
}
