// SPDX-License-Identifier: BUSL-1.1

//! The refusals of a KV write the engine stores nothing for.
//!
//! A write-set part journals each stored row in one WAL record, so a KV row
//! holds its key and value together within [`MAX_ROW_IMAGE_BYTES`].

use nodedb_types::{MAX_ROW_IMAGE_BYTES, Surrogate};

use super::engine_write::UnboundKvWrite;

/// How many bytes of a key a refusal keeps to name its row.
const KEY_SHOWN_BYTES: usize = 256;

/// A KV row whose key and value together are over [`MAX_ROW_IMAGE_BYTES`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "KV row '{key}' of '{collection}' is {bytes} bytes, over the row limit of \
     {MAX_ROW_IMAGE_BYTES} bytes"
)]
pub struct KvRowTooLarge {
    pub collection: String,
    /// The start of the row's key, as text.
    pub key: String,
    /// The key and value lengths together.
    pub bytes: usize,
}

impl KvRowTooLarge {
    /// The refusal of a row storing `key` and `value` into `collection`, or
    /// `Ok` when the row is within the limit.
    pub fn check(collection: &str, key: &[u8], value: &[u8]) -> Result<(), Self> {
        let bytes = key.len().saturating_add(value.len());
        if bytes <= MAX_ROW_IMAGE_BYTES {
            return Ok(());
        }
        let shown = key.get(..KEY_SHOWN_BYTES).unwrap_or(key);
        Err(Self {
            collection: collection.to_owned(),
            key: String::from_utf8_lossy(shown).into_owned(),
            bytes,
        })
    }
}

/// A KV write the engine refuses whole, storing nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvWriteRefusal {
    /// The write binds its row to `Surrogate::ZERO`.
    #[error(transparent)]
    Unbound(#[from] UnboundKvWrite),
    /// The row's key and value are over the row limit.
    #[error(transparent)]
    RowTooLarge(#[from] KvRowTooLarge),
}

impl KvWriteRefusal {
    /// The refusal of a write binding `surrogate` that stores `key` and
    /// `value` into `collection`, or `Ok` when the engine stores it. A
    /// multi-row write checks every row with this before it writes any.
    pub fn check(
        collection: &str,
        key: &[u8],
        value: &[u8],
        surrogate: Surrogate,
    ) -> Result<(), Self> {
        UnboundKvWrite::check(collection, surrogate)?;
        KvRowTooLarge::check(collection, key, value)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_at_the_limit_passes_and_one_byte_more_refuses() {
        let key = b"k";
        let value = vec![0u8; MAX_ROW_IMAGE_BYTES - key.len()];
        KvRowTooLarge::check("cache", key, &value).expect("a row at the limit");
        let over = vec![0u8; MAX_ROW_IMAGE_BYTES];
        let refusal = KvRowTooLarge::check("cache", key, &over).expect_err("refused");
        assert_eq!(refusal.bytes, MAX_ROW_IMAGE_BYTES + 1);
        assert_eq!(refusal.key, "k");
    }

    #[test]
    fn a_long_key_is_cut_in_the_refusal() {
        let key = vec![b'k'; MAX_ROW_IMAGE_BYTES + 1];
        let refusal = KvRowTooLarge::check("cache", &key, b"").expect_err("refused");
        assert_eq!(refusal.key.len(), KEY_SHOWN_BYTES);
    }

    #[test]
    fn an_unbound_write_refuses_before_its_size_is_read() {
        let over = vec![0u8; MAX_ROW_IMAGE_BYTES + 1];
        let refusal =
            KvWriteRefusal::check("cache", b"k", &over, Surrogate::ZERO).expect_err("refused");
        assert!(matches!(refusal, KvWriteRefusal::Unbound(_)), "{refusal:?}");
    }

    /// A KV value is opaque bytes: one that parses as a map with an integer
    /// key stores like any other.
    #[test]
    fn an_opaque_value_that_looks_like_a_map_is_stored() {
        // {1: "a"}
        let value = [0x81, 0x01, 0xa1, b'a'];
        KvWriteRefusal::check("cache", b"k", &value, Surrogate::new(1)).expect("stored");
    }
}
