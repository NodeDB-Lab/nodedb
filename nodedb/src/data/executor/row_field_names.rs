// SPDX-License-Identifier: BUSL-1.1

//! The refusal of a stored body whose field name is not text.
//!
//! A document field name is a string. A plain MessagePack body can carry a
//! map key of any kind, at any depth, and such a key has no field name to
//! read back. Every stored document row and edge passes
//! [`check_field_names`] before it installs, so no stored body holds one.

use nodedb_types::{NonStringKey, find_body_non_string_key};

use crate::bridge::envelope::ErrorCode;

use super::row_image_limit::shown_id;

/// The refusal of a body of row `row_id` in `collection` that holds `key`.
pub(crate) fn non_string_key_refusal(
    collection: &str,
    row_id: &str,
    key: &NonStringKey,
) -> ErrorCode {
    ErrorCode::DataException {
        detail: format!(
            "row '{}' of '{collection}': {key}; send every map key as a string",
            shown_id(row_id)
        ),
    }
}

/// Refuse a body of row `row_id` in `collection` that is a plain map with
/// a key that is not a string at any depth.
pub(crate) fn check_field_names(collection: &str, row_id: &str, body: &[u8]) -> crate::Result<()> {
    match find_body_non_string_key(body) {
        Some(key) => Err(crate::Error::DataPlane(non_string_key_refusal(
            collection, row_id, &key,
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal(body: &[u8]) -> String {
        match check_field_names("orders", "o1", body) {
            Err(crate::Error::DataPlane(ErrorCode::DataException { detail })) => detail,
            other => panic!("expected a data exception, got {other:?}"),
        }
    }

    /// An integer key, an array key and a nested map key are refused, each
    /// named by its path.
    #[test]
    fn a_body_with_a_key_that_is_not_text_is_refused() {
        // {1: "a"}
        let integer = refusal(&[0x81, 0x01, 0xa1, b'a']);
        assert!(
            integer.contains("at $ ") && integer.contains("'o1'"),
            "{integer}"
        );
        // {"tags": {[1]: nil}}
        let array = refusal(&[0x81, 0xa4, b't', b'a', b'g', b's', 0x81, 0x91, 0x01, 0xc0]);
        assert!(array.contains("at $.tags "), "{array}");
        // {"a": [{"b": {{}: nil}}]}
        let nested = refusal(&[0x81, 0xa1, b'a', 0x91, 0x81, 0xa1, b'b', 0x81, 0x80, 0xc0]);
        assert!(nested.contains("at $.a[0].b "), "{nested}");
    }

    #[test]
    fn text_keys_tuples_and_tagged_bodies_pass() {
        check_field_names("orders", "o1", &[0x81, 0xa1, b'a', 0x01]).expect("text keys");
        check_field_names("orders", "o1", b"NDST\x01").expect("a tuple");
        check_field_names("orders", "o1", &[0x92, 0x07, 0x80]).expect("a tagged object");
        check_field_names("orders", "o1", &[]).expect("an empty body");
    }
}
