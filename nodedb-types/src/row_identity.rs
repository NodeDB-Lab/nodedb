// SPDX-License-Identifier: Apache-2.0

//! This module owns both encodings of a row's surrogate identity.
//!
//! [`StorageKey`] is internal: the substrate redb tables (`DOCUMENTS`,
//! `INDEXES`) use string keys, and the Document engine encodes each
//! surrogate as a fixed-width 8-character lowercase hexadecimal string
//! (e.g. `Surrogate(42)` -> `"0000002a"`). It must never reach a client.
//!
//! The format is intentionally fixed width: lexicographic ordering of the
//! hex string matches numeric ordering of the underlying surrogate, so
//! redb range scans can iterate rows in surrogate order without any
//! additional index.
//!
//! [`RowIdentity`] is what a client sees: a predicate matches it, and the
//! catalog binds it. For a minted row it is the surrogate's decimal string.
//! A row with a user-supplied or DDL-declared primary key carries a
//! different identity, the key's body value, bound via the
//! `_system.surrogate_pk{,_rev}` catalog tables.
//!
//! The two types exist so the compiler rejects passing one where the other
//! belongs. A storage key and a user-declared identity can share the same
//! 8-hex-character shape (a primary key `deadbeef` parses as a storage key),
//! so a loose `String` cannot tell them apart. `StorageKey::parse` is the
//! only place that shape gets reinterpreted as a surrogate.
use crate::Surrogate;

/// The redb key a document row is stored under.
///
/// Fixed-width lowercase hex, so lexicographic order matches surrogate order
/// and a range scan iterates rows in surrogate order with no extra index.
/// Internal: a storage key never reaches a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageKey(Surrogate);

impl StorageKey {
    /// Wrap `surrogate` as the key it is stored under. Allocation-free: the
    /// hex text is a rendering, produced only by `Display` or `to_identity`.
    pub fn for_surrogate(surrogate: Surrogate) -> Self {
        Self(surrogate)
    }

    /// Parse a redb key back into a `StorageKey`.
    ///
    /// Returns `None` unless `key` is exactly 8 lowercase hex characters.
    /// This handles legacy non-surrogate document IDs gracefully: a value
    /// that fails to parse is not a minted storage key. Allocation-free.
    pub fn parse(key: &str) -> Option<Self> {
        if key.len() != 8
            || !key
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return None;
        }
        let raw = u32::from_str_radix(key, 16).ok()?;
        Some(Self(Surrogate::new(raw)))
    }

    /// Recover the surrogate this key encodes. Infallible and free: the
    /// surrogate IS the key, not something re-derived from stored text.
    pub fn surrogate(&self) -> Surrogate {
        self.0
    }

    /// The client-visible identity of the row stored under this key.
    ///
    /// Allocates the decimal string — unavoidable, since a client never sees
    /// the hex encoding.
    pub fn to_identity(&self) -> RowIdentity {
        RowIdentity::for_surrogate(self.0)
    }
}

impl std::fmt::Display for StorageKey {
    /// The only place a surrogate becomes a storage key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:08x}", self.0.as_u32())
    }
}

/// The identity a client sees for a row.
///
/// A minted row renders its surrogate in decimal. A row with a declared
/// `PRIMARY KEY` carries the user's own value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowIdentity(String);

impl RowIdentity {
    /// The decimal identity of a minted row.
    pub fn for_surrogate(surrogate: Surrogate) -> Self {
        Self(surrogate.as_u32().to_string())
    }

    /// Wrap a declared or client-supplied key as the row's identity.
    ///
    /// The value is taken verbatim and never interpreted as a storage key
    /// or a surrogate. This is what makes it safe to call with a KV key, a
    /// user-declared primary key, or any other engine-native identifier.
    ///
    /// Takes `impl Into<String>` so an owned `String` moves in without a
    /// copy. A `&str` caller still pays one copy, which is unavoidable.
    pub fn from_user_key(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume this identity and return its inner `String` without copying.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for RowIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Format a surrogate as the 8-character zero-padded lowercase hex string
/// used as the document's redb key.
///
/// Thin wrapper over [`StorageKey::for_surrogate`], kept because 242
/// call sites across the workspace hold the result as a plain `String`
/// (redb key params, msgpack field injection, WAL replay) rather than a
/// `StorageKey`. Converting all of them is a separate ripple from this one.
/// One allocation: the `Display` format.
pub fn surrogate_to_doc_id(surrogate: Surrogate) -> String {
    StorageKey::for_surrogate(surrogate).to_string()
}

/// Parse a hex-encoded document storage key back to a `Surrogate`.
///
/// Returns `None` if the key is not exactly 8 lowercase hex characters —
/// this handles legacy non-surrogate document IDs gracefully.
///
/// Thin wrapper over [`StorageKey::parse`], kept for the same reason as
/// [`surrogate_to_doc_id`]: 35 call sites hold a plain `&str` doc ID.
/// Allocation-free.
pub fn doc_id_to_surrogate(doc_id: &str) -> Option<Surrogate> {
    StorageKey::parse(doc_id).map(|key| key.surrogate())
}

/// The client-visible identity of a row stored under `doc_id`.
///
/// A minted key renders its surrogate in decimal. Any other key is a user's
/// own value and passes through verbatim.
pub fn identity_of(doc_id: &str) -> RowIdentity {
    StorageKey::parse(doc_id)
        .map(|key| key.to_identity())
        .unwrap_or_else(|| RowIdentity::from_user_key(doc_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_zero_padded_lowercase() {
        assert_eq!(
            StorageKey::for_surrogate(Surrogate::new(0)).to_string(),
            "00000000"
        );
        assert_eq!(
            StorageKey::for_surrogate(Surrogate::new(42)).to_string(),
            "0000002a"
        );
        assert_eq!(
            StorageKey::for_surrogate(Surrogate::new(0xDEAD_BEEF)).to_string(),
            "deadbeef"
        );
    }

    #[test]
    fn lex_order_matches_numeric() {
        let a = StorageKey::for_surrogate(Surrogate::new(0x10)).to_string();
        let b = StorageKey::for_surrogate(Surrogate::new(0x100)).to_string();
        assert!(a < b);
    }

    #[test]
    fn parse_rejects_wrong_shape() {
        assert!(StorageKey::parse("").is_none());
        assert!(StorageKey::parse("abc").is_none());
        assert!(StorageKey::parse("DEADBEEF").is_none());
        assert!(StorageKey::parse("zzzzzzzz").is_none());
        assert!(StorageKey::parse("123456789").is_none());
    }

    #[test]
    fn parse_roundtrips_surrogate() {
        let key = StorageKey::for_surrogate(Surrogate::new(0x2a));
        let parsed = StorageKey::parse(&key.to_string()).expect("valid storage key");
        assert_eq!(parsed.surrogate(), Surrogate::new(0x2a));
    }

    #[test]
    fn to_identity_is_decimal() {
        let key = StorageKey::for_surrogate(Surrogate::new(42));
        assert_eq!(key.to_identity().as_str(), "42");
    }

    #[test]
    fn identity_of_minted_key_is_decimal() {
        assert_eq!(identity_of("0000002a").as_str(), "42");
    }

    #[test]
    fn identity_of_user_key_passes_through() {
        assert_eq!(identity_of("user-declared-id").as_str(), "user-declared-id");
    }
}
