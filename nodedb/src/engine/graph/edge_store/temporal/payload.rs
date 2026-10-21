// SPDX-License-Identifier: BUSL-1.1

//! `EdgeValuePayload` — wraps property blob with bitemporal valid-time bounds
//! and the endpoints' global identities.

use nodedb_types::Surrogate;
use serde::{Deserialize, Serialize};

/// Bitemporal edge value payload. Wraps the caller's MessagePack property
/// blob with valid-time bounds and the surrogates the endpoints held when
/// the version was written.
///
/// `valid_until_ms` uses [`nodedb_types::OPEN_UPPER`] (`i64::MAX`) as the
/// open-upper sentinel. A surrogate of `0` records no identity. A read at
/// an earlier system time takes each endpoint's identity from the version
/// it resolves to, so a node deleted later keeps its identity at that cut.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgeValuePayload {
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub properties: Vec<u8>,
    pub src_surrogate: u32,
    pub dst_surrogate: u32,
}

impl EdgeValuePayload {
    pub fn new(valid_from_ms: i64, valid_until_ms: i64, properties: Vec<u8>) -> Self {
        Self {
            valid_from_ms,
            valid_until_ms,
            properties,
            src_surrogate: 0,
            dst_surrogate: 0,
        }
    }

    /// Record the endpoints' identities. `None` records none.
    pub fn with_surrogates(mut self, src: Option<Surrogate>, dst: Option<Surrogate>) -> Self {
        self.src_surrogate = src.map_or(0, Surrogate::as_u32);
        self.dst_surrogate = dst.map_or(0, Surrogate::as_u32);
        self
    }

    /// The source endpoint's identity at this version, `None` when the
    /// version records none.
    pub fn src(&self) -> Option<Surrogate> {
        (self.src_surrogate != 0).then(|| Surrogate::new(self.src_surrogate))
    }

    /// The destination endpoint's identity at this version, `None` when the
    /// version records none.
    pub fn dst(&self) -> Option<Surrogate> {
        (self.dst_surrogate != 0).then(|| Surrogate::new(self.dst_surrogate))
    }

    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("encode EdgeValuePayload: {e}"),
        })
    }

    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "graph".into(),
            detail: format!("decode EdgeValuePayload: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::keys::is_sentinel;
    use super::*;

    #[test]
    fn payload_msgpack_roundtrip() {
        let p = EdgeValuePayload::new(1_000, 2_000, b"props".to_vec());
        let bytes = p.encode().unwrap();
        // zerompk encodes derived structs as fixarray — 5 fields → 0x95.
        assert_eq!(bytes[0], 0x95);
        // Sentinels (0xFF, 0xFE) stay disjoint from fixarray range (0x90..=0x9f).
        assert!(!is_sentinel(&bytes[..1]));
        let decoded = EdgeValuePayload::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn surrogates_round_trip_and_zero_records_none() {
        let p = EdgeValuePayload::new(0, i64::MAX, Vec::new())
            .with_surrogates(Some(Surrogate::new(7)), None);
        let decoded = EdgeValuePayload::decode(&p.encode().unwrap()).unwrap();
        assert_eq!(decoded.src(), Some(Surrogate::new(7)));
        assert_eq!(decoded.dst(), None);
    }
}
