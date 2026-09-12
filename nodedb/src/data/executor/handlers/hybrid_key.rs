// SPDX-License-Identifier: BUSL-1.1

//! The key the vector, text, and graph legs of a hybrid search fuse on.
//!
//! RRF fuses on key equality, so every leg must rank under one key type. The
//! text and graph legs always carry a surrogate. A vector hit whose HNSW node
//! has no surrogate binding carries none, and must never be misread as one.

use std::fmt;

use nodedb_types::{HEADLESS_SENTINEL_PREFIX, StorageKey, Surrogate};

/// One hybrid-search leg hit, in the key space RRF fuses on.
///
/// `Bound` is a row with a surrogate: it fuses across legs and renders as the
/// storage key the response envelope carries. `Headless` is a vector hit with
/// no surrogate binding: it ranks in the vector leg only, fuses with nothing,
/// and renders as the `__local_<id>` sentinel.
///
/// Wire encoding: `ToMessagePack`/`FromMessagePack` write and read this type
/// as its `Display` string — the hex storage key for `Bound`, or
/// `__local_<id>` for `Headless`. The Control Plane response translator
/// decodes the same two shapes; see `response_translate/hit_key.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::data::executor) enum HybridFusionKey {
    Bound(StorageKey),
    Headless(u32),
}

impl HybridFusionKey {
    /// The key of a row that carries `surrogate`.
    pub fn for_surrogate(surrogate: Surrogate) -> Self {
        Self::Bound(StorageKey::for_surrogate(surrogate))
    }

    /// The storage key of a bound row. `None` for a headless hit.
    pub fn storage_key(&self) -> Option<StorageKey> {
        match self {
            Self::Bound(key) => Some(*key),
            Self::Headless(_) => None,
        }
    }
}

impl fmt::Display for HybridFusionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bound(key) => write!(f, "{key}"),
            Self::Headless(local_id) => write!(f, "{HEADLESS_SENTINEL_PREFIX}{local_id}"),
        }
    }
}

impl serde::Serialize for HybridFusionKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl zerompk::ToMessagePack for HybridFusionKey {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_string(&self.to_string())
    }
}

impl<'de> zerompk::FromMessagePack<'de> for HybridFusionKey {
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        let text = reader.read_string()?;
        if let Some(local_id) = text.strip_prefix(HEADLESS_SENTINEL_PREFIX) {
            let local_id = local_id
                .parse::<u32>()
                .map_err(|_| zerompk::Error::InvalidMarker(0))?;
            return Ok(Self::Headless(local_id));
        }
        StorageKey::parse(&text)
            .map(Self::Bound)
            .ok_or(zerompk::Error::InvalidMarker(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_renders_storage_key_and_headless_renders_sentinel() {
        let bound = HybridFusionKey::for_surrogate(Surrogate::new(42));
        assert_eq!(bound.to_string(), "0000002a");
        assert_eq!(
            bound.storage_key(),
            Some(StorageKey::for_surrogate(Surrogate::new(42)))
        );

        let headless = HybridFusionKey::Headless(7);
        assert_eq!(headless.to_string(), "__local_7");
        assert_eq!(headless.storage_key(), None);
    }

    #[test]
    fn headless_never_equals_a_bound_key_with_the_same_number() {
        assert_ne!(
            HybridFusionKey::Headless(7),
            HybridFusionKey::for_surrogate(Surrogate::new(7))
        );
    }

    #[test]
    fn wire_round_trips_bound_and_headless() {
        for key in [
            HybridFusionKey::for_surrogate(Surrogate::new(42)),
            HybridFusionKey::Headless(7),
        ] {
            let bytes = zerompk::to_msgpack_vec(&key).expect("encode");
            let decoded: HybridFusionKey = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, key);
        }
    }
}
