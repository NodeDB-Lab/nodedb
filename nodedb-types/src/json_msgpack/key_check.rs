// SPDX-License-Identifier: Apache-2.0

//! Find a map key that is not a string in a plain MessagePack value.
//!
//! A document field name is text: `Value::Object` keys are strings, and SQL
//! has no other field name. A write path that stores a plain body refuses a
//! body whose map, at any depth, has a key of another kind.

/// The deepest nesting the walk follows. The plain readers refuse deeper
/// values, so a deeper body has no field to render.
const MAX_DEPTH: usize = 500;

/// The longest path a refusal names, in bytes.
const MAX_PATH_BYTES: usize = 256;

/// The longest key a path segment shows, in bytes.
const MAX_SEGMENT_BYTES: usize = 64;

/// A map key that is not a string: the path to its map and its entry
/// ordinal there. `$` is the value itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonStringKey {
    pub path: String,
    pub entry: usize,
}

impl std::fmt::Display for NonStringKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "map key {} at {} is not a string; a document field name is text",
            self.entry, self.path
        )
    }
}

/// The first map key in `bytes` that is not a string, when `bytes` is one
/// well-formed plain MessagePack value and nothing else. `None` when every
/// key is a string, and when `bytes` is not one such value: a write path
/// refuses those on its own terms.
pub fn find_non_string_key(bytes: &[u8]) -> Option<NonStringKey> {
    // The first pass keeps no path, so the common clean body allocates
    // nothing. Only a body with a refusal to name walks again for its path.
    walk_bytes(bytes, false)?;
    walk_bytes(bytes, true)
}

fn walk_bytes(bytes: &[u8], track: bool) -> Option<NonStringKey> {
    let mut walk = Walk {
        bytes,
        track,
        path: if track {
            String::from("$")
        } else {
            String::new()
        },
        found: None,
    };
    match walk.value(0, 0) {
        Some(end) if end == bytes.len() => walk.found,
        Some(_) | None => None,
    }
}

/// [`find_non_string_key`] for a stored body. Only a plain map is walked: a
/// Binary Tuple, a native tagged object and JSON text hold text keys.
pub fn find_body_non_string_key(body: &[u8]) -> Option<NonStringKey> {
    match body.first() {
        Some(0x80..=0x8f | 0xde | 0xdf) => find_non_string_key(body),
        _ => None,
    }
}

struct Walk<'a> {
    bytes: &'a [u8],
    /// Whether the walk builds `path`.
    track: bool,
    path: String,
    found: Option<NonStringKey>,
}

impl Walk<'_> {
    fn byte(&self, pos: usize) -> Option<u8> {
        self.bytes.get(pos).copied()
    }

    /// The big-endian length of `width` bytes at `pos`.
    fn length(&self, pos: usize, width: usize) -> Option<usize> {
        let raw = self.bytes.get(pos..pos.checked_add(width)?)?;
        Some(
            raw.iter()
                .fold(0usize, |len, b| (len << 8) | usize::from(*b)),
        )
    }

    /// The end of `len` payload bytes at `start`.
    fn span(&self, start: usize, len: usize) -> Option<usize> {
        let end = start.checked_add(len)?;
        (end <= self.bytes.len()).then_some(end)
    }

    /// Walk the value at `pos`, returning where it ends.
    fn value(&mut self, pos: usize, depth: usize) -> Option<usize> {
        let marker = self.byte(pos)?;
        let next = pos + 1;
        match marker {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => Some(next),
            0xcc | 0xd0 => self.span(next, 1),
            0xcd | 0xd1 => self.span(next, 2),
            0xce | 0xd2 | 0xca => self.span(next, 4),
            0xcf | 0xd3 | 0xcb => self.span(next, 8),
            0xd4 => self.span(next, 2),
            0xd5 => self.span(next, 3),
            0xd6 => self.span(next, 5),
            0xd7 => self.span(next, 9),
            0xd8 => self.span(next, 17),
            0xa0..=0xbf => self.span(next, usize::from(marker & 0x1f)),
            0xd9 | 0xc4 => self.sized(next, 1, 0),
            0xda | 0xc5 => self.sized(next, 2, 0),
            0xdb | 0xc6 => self.sized(next, 4, 0),
            0xc7 => self.sized(next, 1, 1),
            0xc8 => self.sized(next, 2, 1),
            0xc9 => self.sized(next, 4, 1),
            0x90..=0x9f => self.array(next, usize::from(marker & 0x0f), depth),
            0xdc => self.array(next + 2, self.length(next, 2)?, depth),
            0xdd => self.array(next + 4, self.length(next, 4)?, depth),
            0x80..=0x8f => self.map(next, usize::from(marker & 0x0f), depth),
            0xde => self.map(next + 2, self.length(next, 2)?, depth),
            0xdf => self.map(next + 4, self.length(next, 4)?, depth),
            0xc1 => None,
        }
    }

    /// A string, binary or ext value: a `width`-byte length, `extra` type
    /// bytes, then the payload.
    fn sized(&self, pos: usize, width: usize, extra: usize) -> Option<usize> {
        let len = self.length(pos, width)?;
        self.span(pos.checked_add(width)?.checked_add(extra)?, len)
    }

    fn array(&mut self, mut pos: usize, count: usize, depth: usize) -> Option<usize> {
        if depth >= MAX_DEPTH {
            return None;
        }
        for index in 0..count {
            let mark = self.path.len();
            if self.track && self.found.is_none() {
                self.push(&format!("[{index}]"));
            }
            pos = self.value(pos, depth + 1)?;
            self.path.truncate(mark);
        }
        Some(pos)
    }

    fn map(&mut self, mut pos: usize, count: usize, depth: usize) -> Option<usize> {
        if depth >= MAX_DEPTH {
            return None;
        }
        for entry in 0..count {
            let key = self.byte(pos)?;
            let key_end = self.value(pos, depth + 1)?;
            let mark = self.path.len();
            match key {
                0xa0..=0xbf | 0xd9..=0xdb => {
                    if self.track && self.found.is_none() {
                        let start = key_end - self.key_len(pos, key_end);
                        let raw = self.bytes.get(start..key_end).unwrap_or_default();
                        let text = String::from_utf8_lossy(raw);
                        let shown: String = text
                            .char_indices()
                            .take_while(|(at, c)| at + c.len_utf8() <= MAX_SEGMENT_BYTES)
                            .map(|(_, c)| c)
                            .collect();
                        self.push(&format!(".{shown}"));
                    }
                }
                _ => {
                    if self.found.is_none() {
                        self.found = Some(NonStringKey {
                            path: self.path.clone(),
                            entry,
                        });
                    }
                }
            }
            pos = self.value(key_end, depth + 1)?;
            self.path.truncate(mark);
        }
        Some(pos)
    }

    /// The payload length of the string key spanning `pos..end`.
    fn key_len(&self, pos: usize, end: usize) -> usize {
        let header = match self.byte(pos) {
            Some(0xd9) => 2,
            Some(0xda) => 3,
            Some(0xdb) => 5,
            _ => 1,
        };
        end.saturating_sub(pos).saturating_sub(header)
    }

    /// Append `segment` to the path while it stays within its cap.
    fn push(&mut self, segment: &str) {
        if self.path.len() + segment.len() <= MAX_PATH_BYTES {
            self.path.push_str(segment);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use crate::json_msgpack::value_to_msgpack;

    #[test]
    fn a_body_of_string_keys_passes() {
        let body = Value::Object(
            [(
                "a".to_string(),
                Value::Array(vec![Value::Object(
                    [("b".to_string(), Value::Integer(1))].into(),
                )]),
            )]
            .into(),
        );
        let bytes = value_to_msgpack(&body).expect("encode");
        assert_eq!(find_non_string_key(&bytes), None);
        let tagged = zerompk::to_msgpack_vec(&body).expect("tagged");
        assert_eq!(find_non_string_key(&tagged), None);
    }

    #[test]
    fn integer_array_and_map_keys_are_found_with_their_path() {
        // {1: nil}
        assert_eq!(
            find_non_string_key(&[0x81, 0x01, 0xc0]),
            Some(NonStringKey {
                path: "$".into(),
                entry: 0,
            })
        );
        // {"a": [{"b": nil, [1]: nil}]}
        let bytes = [
            0x81, 0xa1, b'a', 0x91, 0x82, 0xa1, b'b', 0xc0, 0x91, 0x01, 0xc0,
        ];
        assert_eq!(
            find_non_string_key(&bytes),
            Some(NonStringKey {
                path: "$.a[0]".into(),
                entry: 1,
            })
        );
        // {"m": {{}: nil}}
        let bytes = [0x81, 0xa1, b'm', 0x81, 0x80, 0xc0];
        let found = find_non_string_key(&bytes).expect("a map key");
        assert_eq!(found.path, "$.m");
        assert!(found.to_string().contains("$.m"));
    }

    #[test]
    fn a_malformed_or_trailing_value_is_not_judged() {
        assert_eq!(find_non_string_key(&[0x81, 0x01]), None);
        assert_eq!(find_non_string_key(&[0x81, 0x01, 0xc0, 0xc0]), None);
        assert_eq!(find_non_string_key(b"{\"a\":1}"), None);
    }

    #[test]
    fn a_key_past_the_depth_limit_is_not_followed() {
        let mut bytes = vec![0x91; MAX_DEPTH + 1];
        bytes.extend_from_slice(&[0x81, 0x01, 0xc0]);
        assert_eq!(find_non_string_key(&bytes), None);
        let mut shallow = vec![0x91; 100];
        shallow.extend_from_slice(&[0x81, 0x01, 0xc0]);
        assert!(find_non_string_key(&shallow).is_some());
    }
}
