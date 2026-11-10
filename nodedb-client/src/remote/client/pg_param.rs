// SPDX-License-Identifier: Apache-2.0

//! Binary parameters for the types `tokio_postgres` binds no built-in Rust
//! type to without an optional feature: `uuid`, the PostgreSQL instant
//! encoding of an `NdbDateTime`, `interval`, `jsonb` text, and a tagged
//! NodeDB value.
//!
//! Each writes the PostgreSQL binary form, so the server decodes the typed
//! value by its declared OID, never text.

use std::error::Error;

use bytes::BytesMut;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

/// Microseconds between the Unix epoch and the PostgreSQL timestamp epoch,
/// 2000-01-01T00:00:00Z.
const POSTGRES_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// A `uuid` parameter: its 16 bytes.
#[derive(Debug, Clone, Copy)]
pub(super) struct PgUuid([u8; 16]);

impl PgUuid {
    /// The parameter for a UUID's text, or `None` when the text is no UUID.
    /// Accepts the hyphenated and the 32-digit form, either case.
    pub(super) fn parse(text: &str) -> Option<Self> {
        let raw = text.as_bytes();
        let digits: Vec<u8> = match raw.len() {
            32 => raw.to_vec(),
            36 if HYPHENS.iter().all(|&at| raw.get(at) == Some(&b'-')) => raw
                .iter()
                .enumerate()
                .filter(|(at, _)| !HYPHENS.contains(at))
                .map(|(_, b)| *b)
                .collect(),
            _ => return None,
        };
        let mut bytes = [0u8; 16];
        for (slot, pair) in bytes.iter_mut().zip(digits.as_chunks::<2>().0) {
            *slot = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
        }
        Some(Self(bytes))
    }
}

/// The hyphen positions of the hyphenated UUID form.
const HYPHENS: [usize; 4] = [8, 13, 18, 23];

fn hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

impl ToSql for PgUuid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

/// A `timestamp` or `timestamptz` parameter: microseconds from the
/// PostgreSQL epoch.
#[derive(Debug, Clone, Copy)]
pub(super) struct PgInstant(i64);

impl PgInstant {
    /// The parameter for an instant `unix_micros` after the Unix epoch, or
    /// `None` when the instant is outside the PostgreSQL range. The result
    /// never equals a PostgreSQL infinity sentinel.
    pub(super) fn from_unix_micros(unix_micros: i64) -> Option<Self> {
        let pg_micros = unix_micros.checked_sub(POSTGRES_EPOCH_OFFSET_MICROS)?;
        (pg_micros != i64::MIN && pg_micros != i64::MAX).then_some(Self(pg_micros))
    }
}

impl ToSql for PgInstant {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0.to_be_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::TIMESTAMP || *ty == Type::TIMESTAMPTZ
    }

    to_sql_checked!();
}

/// An `interval` parameter: microseconds, then zero days and zero months.
/// A duration is an exact count of microseconds, so the day and month
/// fields stay empty.
#[derive(Debug, Clone, Copy)]
pub(super) struct PgInterval(pub(super) i64);

impl ToSql for PgInterval {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INTERVAL
    }

    to_sql_checked!();
}

/// A `jsonb` parameter: the version byte (1) and the JSON text.
#[derive(Debug, Clone)]
pub(super) struct PgJsonb(pub(super) String);

impl ToSql for PgJsonb {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&[1]);
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    to_sql_checked!();
}

/// The parameter type of a tagged NodeDB value: `any`, OID
/// [`nodedb_types::pg_compat::TAGGED_VALUE_PARAM_OID`].
pub(super) const TAGGED_TYPE: Type = Type::ANY;

/// A tagged NodeDB value parameter: the tagged MessagePack of the value.
#[derive(Debug, Clone)]
pub(super) struct PgTagged(pub(super) Vec<u8>);

impl ToSql for PgTagged {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == TAGGED_TYPE
    }

    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tagged_type_is_the_shared_oid() {
        assert_eq!(
            TAGGED_TYPE.oid(),
            nodedb_types::pg_compat::TAGGED_VALUE_PARAM_OID
        );
        let mut out = BytesMut::new();
        PgTagged(vec![0x91, 0x00])
            .to_sql(&TAGGED_TYPE, &mut out)
            .expect("encodes");
        assert_eq!(&out[..], &[0x91, 0x00]);
    }

    #[test]
    fn a_uuid_writes_its_sixteen_bytes() {
        let uuid = PgUuid::parse("550E8400-e29b-41d4-a716-446655440000").expect("valid uuid");
        let mut out = BytesMut::new();
        uuid.to_sql(&Type::UUID, &mut out).expect("encodes");
        assert_eq!(out.len(), 16);
        assert_eq!(&out[..4], &[0x55, 0x0e, 0x84, 0x00]);
        assert!(PgUuid::parse("550e8400e29b41d4a716446655440000").is_some());
        for bad in [
            "550e8400",
            "550e8400-e29b-41d4-a716-44665544000z",
            "not a uuid",
        ] {
            assert!(PgUuid::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn an_interval_writes_micros_then_empty_days_and_months() {
        let mut out = BytesMut::new();
        PgInterval(1_500)
            .to_sql(&Type::INTERVAL, &mut out)
            .expect("encodes");
        let mut expected = 1_500i64.to_be_bytes().to_vec();
        expected.extend_from_slice(&[0; 8]);
        assert_eq!(&out[..], expected.as_slice());
    }

    #[test]
    fn jsonb_writes_its_version_byte_then_the_text() {
        let mut out = BytesMut::new();
        PgJsonb("{}".into())
            .to_sql(&Type::JSONB, &mut out)
            .expect("encodes");
        assert_eq!(&out[..], b"\x01{}");
    }

    #[test]
    fn an_instant_writes_postgres_epoch_micros() {
        let at = PgInstant::from_unix_micros(POSTGRES_EPOCH_OFFSET_MICROS + 5).expect("in range");
        let mut out = BytesMut::new();
        at.to_sql(&Type::TIMESTAMPTZ, &mut out).expect("encodes");
        assert_eq!(&out[..], &5i64.to_be_bytes());
        assert!(PgInstant::from_unix_micros(i64::MIN).is_none());
    }
}
