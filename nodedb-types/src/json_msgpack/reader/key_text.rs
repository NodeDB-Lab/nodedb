// SPDX-License-Identifier: Apache-2.0

//! The bounded text a reader gives a map key that is not a string.
//!
//! Write paths refuse such keys (see `key_check`). A body that still holds
//! one renders each key in at most [`MAX_KEY_TEXT_BYTES`] bytes, and escapes
//! nothing, so no read grows past its input.

/// The longest text a key that is not a string renders to, in bytes.
pub(crate) const MAX_KEY_TEXT_BYTES: usize = 64;

/// `text` cut to [`MAX_KEY_TEXT_BYTES`] at a character boundary.
pub(crate) fn bounded(mut text: String) -> String {
    if text.len() > MAX_KEY_TEXT_BYTES {
        let mut end = MAX_KEY_TEXT_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_msgpack::{json_from_msgpack, msgpack_to_json_string, value_to_msgpack};
    use crate::{Value, value_from_msgpack};

    /// `{key: nil}` nested `depth` deep through its key: each level's key is
    /// the map one level down.
    fn nested_key_body(depth: usize) -> Vec<u8> {
        let mut body = vec![0x81, 0xd2, 0x80, 0, 0, 0, 0xc0];
        for _ in 0..depth {
            let mut outer = vec![0x81];
            outer.extend_from_slice(&body);
            outer.push(0xc0);
            body = outer;
        }
        body
    }

    #[test]
    fn a_hostile_nested_key_body_reads_bounded() {
        let body = nested_key_body(495);
        let value = value_from_msgpack(&body).expect("native read");
        let Value::Object(fields) = &value else {
            panic!("an object");
        };
        assert_eq!(fields.keys().collect::<Vec<_>>(), ["<map>"]);
        assert!(value_to_msgpack(&value).expect("render").len() <= 16);
        let json = json_from_msgpack(&body).expect("json read");
        assert_eq!(json, serde_json::json!({"<map>": null}));
        let text = msgpack_to_json_string(&body).expect("transcode");
        assert_eq!(text, r#"{"<map>":null}"#);
    }

    #[test]
    fn a_scalar_key_keeps_its_text_within_the_cap() {
        // {int32 -2147483648: nil}
        let body = [0x81, 0xd2, 0x80, 0, 0, 0, 0xc0];
        let Value::Object(fields) = value_from_msgpack(&body).expect("read") else {
            panic!("an object");
        };
        assert!(fields.contains_key("Integer(-2147483648)"));
        let long = "é".repeat(MAX_KEY_TEXT_BYTES);
        let cut = bounded(long);
        assert!(cut.len() <= MAX_KEY_TEXT_BYTES && cut.chars().all(|c| c == 'é'));
    }
}
