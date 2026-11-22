// SPDX-License-Identifier: BUSL-1.1

//! Composite sort key encoding for sorted indexes.
//!
//! A composite key concatenates one encoded value per sort column, in column
//! order. The [`value`](super::value) encoding is order-preserving and no
//! encoded value is a prefix of another, so byte order of whole keys matches
//! the multi-column sort order.
//!
//! An ascending column stores its value bytes as-is. A descending column
//! stores every byte complemented (~byte), which reverses its order.

use std::borrow::Cow;

use super::value::{MAX_LEADING_BYTE, encode_json, encoded_len};

/// Sort direction for a column in a sorted index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    /// The byte XORed into each value byte of a column with this direction.
    fn flip(self) -> u8 {
        match self {
            SortDirection::Asc => 0x00,
            SortDirection::Desc => 0xFF,
        }
    }
}

/// Definition of one column in a composite sort key.
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column name in the collection schema.
    pub name: String,
    /// Sort direction for this column.
    pub direction: SortDirection,
}

/// Encodes composite sort keys for a sorted index definition.
///
/// Given a list of `(column_name, direction)` pairs, produces a single
/// byte key from encoded field values such that `BTreeMap` ordering
/// matches the desired multi-column sort.
#[derive(Debug, Clone)]
pub struct SortKeyEncoder {
    columns: Vec<SortColumn>,
}

impl SortKeyEncoder {
    pub fn new(columns: Vec<SortColumn>) -> Self {
        Self { columns }
    }

    /// Number of columns in the composite key.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Column definitions.
    pub fn columns(&self) -> &[SortColumn] {
        &self.columns
    }

    /// Encode column values into a composite sort key.
    ///
    /// `values` holds one [`value`](super::value) encoding per column, in
    /// column order.
    pub fn encode(&self, values: &[&[u8]]) -> Vec<u8> {
        debug_assert_eq!(values.len(), self.columns.len());

        let total_len: usize = values.iter().map(|v| v.len()).sum();
        let mut key = Vec::with_capacity(total_len);
        for (value, col) in values.iter().zip(&self.columns) {
            let flip = col.direction.flip();
            key.extend(value.iter().map(|byte| byte ^ flip));
        }
        key
    }

    /// The sort key of a document. Backfill and write maintenance both build
    /// keys here.
    ///
    /// `None` when `document` is not an object, or a sort column is missing
    /// or holds NULL. A NULL column leaves the index the same as a missing one.
    pub fn key_for_document(&self, document: &serde_json::Value) -> Option<Vec<u8>> {
        let fields = document.as_object()?;
        let mut key = Vec::new();
        for col in &self.columns {
            let flip = col.direction.flip();
            let value = fields.get(&col.name).filter(|v| !v.is_null())?;
            key.extend(encode_json(value).iter().map(|b| b ^ flip));
        }
        Some(key)
    }

    /// Translate an inclusive `[min, max]` range over the FIRST sort column
    /// into inclusive bounds in the encoded key space.
    ///
    /// `min` and `max` are [`value`](super::value) encodings of the leading
    /// column. Two rules of the key layout apply:
    ///
    /// * a descending leading column is complemented, which reverses order, so
    ///   the caller's `min` is the GREATER key bound and the two swap;
    /// * a composite key continues past the leading column with more column
    ///   values. The upper bound carries one byte above the first byte of any
    ///   value, so it admits every key whose leading column equals `max` and
    ///   stays below any key with a greater leading value.
    pub fn first_column_range_bounds(
        &self,
        min: Option<&[u8]>,
        max: Option<&[u8]>,
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let direction = self
            .columns
            .first()
            .map_or(SortDirection::Asc, |col| col.direction);
        let flip = direction.flip();
        let framed = |value: &[u8]| -> Vec<u8> {
            let mut out = Vec::with_capacity(value.len() + 1);
            out.extend(value.iter().map(|byte| byte ^ flip));
            out
        };

        let (lower_source, upper_source) = match direction {
            SortDirection::Asc => (min, max),
            SortDirection::Desc => (max, min),
        };
        let lower = lower_source.map(framed);
        let upper = upper_source.map(|value| {
            let mut bound = framed(value);
            bound.push(MAX_LEADING_BYTE + 1);
            bound
        });
        (lower, upper)
    }

    /// The value bytes of column `name` in `sort_key`, as they were passed to
    /// [`Self::encode`].
    ///
    /// Undoes the DESC complement. Returns `None` when the encoder has no
    /// column `name` or the key does not hold whole values up to it.
    pub fn column_value<'k>(&self, sort_key: &'k [u8], name: &str) -> Option<Cow<'k, [u8]>> {
        let mut rest = sort_key;
        for col in &self.columns {
            let flip = col.direction.flip();
            let len = encoded_len(rest, flip)?;
            let (value, tail) = rest.split_at_checked(len)?;
            if col.name == name {
                return Some(match col.direction {
                    SortDirection::Asc => Cow::Borrowed(value),
                    SortDirection::Desc => Cow::Owned(value.iter().map(|b| !b).collect()),
                });
            }
            rest = tail;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::super::value::{encode_float, encode_int, encode_text};
    use super::*;

    fn encoder(direction: SortDirection) -> SortKeyEncoder {
        SortKeyEncoder::new(vec![SortColumn {
            name: "score".into(),
            direction,
        }])
    }

    fn score_and_ts(ts_direction: SortDirection) -> SortKeyEncoder {
        SortKeyEncoder::new(vec![
            SortColumn {
                name: "score".into(),
                direction: SortDirection::Desc,
            },
            SortColumn {
                name: "ts".into(),
                direction: ts_direction,
            },
        ])
    }

    #[test]
    fn composite_key_desc_score_asc_time() {
        let enc = score_and_ts(SortDirection::Asc);
        let key = |score: i64, ts: i64| enc.encode(&[&encode_int(score), &encode_int(ts)]);

        // 200 DESC sorts before 100 DESC.
        assert!(key(200, 1000) < key(100, 1000));
        // Same score, earlier time ASC sorts before later time.
        assert!(key(200, 1000) < key(200, 2000));
    }

    /// An integer and a float in one column order by numeric value.
    #[test]
    fn mixed_integer_and_float_keys_sort_numerically() {
        let asc = encoder(SortDirection::Asc);
        let key = |v: &[u8]| asc.encode(&[v]);
        assert!(key(&encode_int(10)) < key(&encode_float(10.5)));
        assert!(key(&encode_float(10.5)) < key(&encode_int(11)));

        let desc = encoder(SortDirection::Desc);
        let key = |v: &[u8]| desc.encode(&[v]);
        assert!(key(&encode_int(11)) < key(&encode_float(10.5)));
        assert!(key(&encode_float(10.5)) < key(&encode_int(10)));
    }

    /// A document's key equals the key built from its encoded field values.
    #[test]
    fn key_for_document_matches_encode_of_its_fields() {
        let enc = score_and_ts(SortDirection::Desc);
        let doc = serde_json::json!({ "score": 10.5, "ts": "2024-05-02", "other": 1 });
        assert_eq!(
            enc.key_for_document(&doc),
            Some(enc.encode(&[&encode_float(10.5), &encode_text("2024-05-02")]))
        );
        assert_eq!(
            enc.key_for_document(&serde_json::json!({ "score": 1 })),
            None
        );
        assert_eq!(enc.key_for_document(&serde_json::json!([1, 2])), None);
    }

    /// An explicit NULL sort column leaves the index, as a missing column does.
    #[test]
    fn key_for_document_treats_a_null_sort_column_as_missing() {
        let enc = score_and_ts(SortDirection::Desc);
        assert_eq!(
            enc.key_for_document(&serde_json::json!({ "score": null, "ts": "2024-05-02" })),
            None
        );
        assert_eq!(
            enc.key_for_document(&serde_json::json!({ "score": 1, "ts": null })),
            None
        );
    }

    /// Strings order bytewise, not by length, in either direction.
    #[test]
    fn string_keys_sort_lexicographically() {
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            let enc = score_and_ts(direction);
            let key = |s: &str| enc.encode(&[&encode_int(1), &encode_text(s)]);
            let ascending = ["", "a", "a\u{0}", "aa", "b", "zz"];
            for pair in ascending.windows(2) {
                let (lo, hi) = (key(pair[0]), key(pair[1]));
                match direction {
                    SortDirection::Asc => assert!(lo < hi, "{pair:?}"),
                    SortDirection::Desc => assert!(lo > hi, "{pair:?}"),
                }
            }
        }
    }

    #[test]
    fn column_value_returns_the_bytes_passed_to_encode() {
        for direction in [SortDirection::Asc, SortDirection::Desc] {
            let enc = score_and_ts(direction);
            let score = encode_float(7.5);
            for ts in [encode_int(1_714_608_000_000), encode_text("2024-05-02")] {
                let key = enc.encode(&[&score, &ts]);
                assert_eq!(enc.column_value(&key, "ts").as_deref(), Some(&ts[..]));
                assert_eq!(enc.column_value(&key, "score").as_deref(), Some(&score[..]));
            }
        }
    }

    #[test]
    fn column_value_of_a_null_is_the_null_encoding() {
        let enc = score_and_ts(SortDirection::Asc);
        let null = encode_json(&serde_json::Value::Null);
        let key = enc.encode(&[&encode_int(7), &null]);
        assert_eq!(enc.column_value(&key, "ts").as_deref(), Some(&null[..]));
    }

    #[test]
    fn column_value_of_an_unknown_column_or_truncated_key_is_none() {
        let enc = score_and_ts(SortDirection::Asc);
        let key = enc.encode(&[&encode_int(7), &encode_int(1_714_608_000_000)]);

        assert!(enc.column_value(&key, "missing").is_none());
        assert!(enc.column_value(&key[..key.len() - 1], "ts").is_none());
        assert!(enc.column_value(&key[..2], "score").is_none());
    }

    /// A bound expressed in value bytes has to land inside the encoded key
    /// space, or it is compared against the wrong bytes and matches nothing.
    #[test]
    fn ascending_bounds_bracket_the_keys_they_name() {
        let enc = encoder(SortDirection::Asc);
        let key_of = |v: &[u8]| enc.encode(&[v]);
        let (lower, upper) =
            enc.first_column_range_bounds(Some(&encode_int(10)), Some(&encode_int(20)));
        let (lower, upper) = (lower.expect("lower"), upper.expect("upper"));

        for inside in [
            encode_int(10),
            encode_float(10.5),
            encode_int(15),
            encode_float(20.0),
        ] {
            let key = key_of(&inside);
            assert!(key >= lower && key <= upper, "{inside:?} must be in range");
        }
        for outside in [
            encode_int(9),
            encode_float(9.5),
            encode_float(20.5),
            encode_int(21),
        ] {
            let key = key_of(&outside);
            assert!(
                key < lower || key > upper,
                "{outside:?} must be out of range"
            );
        }
    }

    /// Complementing a descending column reverses byte order, so the caller's
    /// `min` becomes the GREATER encoded bound. Failing to swap them yields an
    /// inverted, always-empty window.
    #[test]
    fn descending_bounds_are_swapped_into_key_order() {
        let enc = encoder(SortDirection::Desc);
        let key_of = |v: &[u8]| enc.encode(&[v]);
        let (lower, upper) =
            enc.first_column_range_bounds(Some(&encode_float(10.5)), Some(&encode_int(11)));
        let (lower, upper) = (lower.expect("lower"), upper.expect("upper"));

        assert!(lower <= upper, "bounds must not be inverted");
        for inside in [encode_float(10.5), encode_float(10.75), encode_int(11)] {
            let key = key_of(&inside);
            assert!(key >= lower && key <= upper, "{inside:?} must be in range");
        }
        for outside in [encode_int(10), encode_float(11.25), encode_int(12)] {
            let key = key_of(&outside);
            assert!(
                key < lower || key > upper,
                "{outside:?} must be out of range"
            );
        }
    }

    /// The upper bound must admit every key whose LEADING column equals it,
    /// including composite keys that carry more columns after it.
    #[test]
    fn upper_bound_admits_composite_keys_on_the_boundary() {
        for (lead, tail) in [
            (SortDirection::Asc, SortDirection::Asc),
            (SortDirection::Asc, SortDirection::Desc),
            (SortDirection::Desc, SortDirection::Asc),
            (SortDirection::Desc, SortDirection::Desc),
        ] {
            let enc = SortKeyEncoder::new(vec![
                SortColumn {
                    name: "score".into(),
                    direction: lead,
                },
                SortColumn {
                    name: "tiebreak".into(),
                    direction: tail,
                },
            ]);
            let (lower, upper) =
                enc.first_column_range_bounds(Some(&encode_int(10)), Some(&encode_int(20)));
            let (lower, upper) = (lower.expect("lower"), upper.expect("upper"));

            for tiebreak in [
                encode_json(&serde_json::Value::Null),
                encode_text(""),
                encode_text("zzz"),
                encode_json(&serde_json::json!({"k": "v"})),
            ] {
                for bound in [encode_int(10), encode_int(20)] {
                    let key = enc.encode(&[&bound, &tiebreak]);
                    assert!(key >= lower && key <= upper, "{lead:?}/{tail:?}");
                }
            }

            for beyond in [encode_int(9), encode_float(20.5), encode_int(21)] {
                let key = enc.encode(&[&beyond, &encode_text("")]);
                assert!(
                    key < lower || key > upper,
                    "a score outside the window must stay outside: {lead:?}/{tail:?}"
                );
            }
        }
    }
}
