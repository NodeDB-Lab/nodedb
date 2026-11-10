// SPDX-License-Identifier: BUSL-1.1

//! Flatteners that turn gathered search hit arrays into bare relational rows
//! for the post-processing tail, and the surrogates each one resolves.

use super::raw::encode_binary_rows;

/// Flatten a gathered array of vector-family search hits (dense / sparse /
/// multi-vector) into bare relational rows for the post-processing tail.
///
/// A hit is `{id: <HybridFusionKey wire string>, distance, doc_id?,
/// body?: <doc msgpack>}`. The document `body`'s columns become top-level (so
/// ORDER BY / DISTINCT / projection can reference any document column),
/// `distance` is surfaced, and `_surrogate` carries the internal id (or, for
/// a headless hit with no surrogate binding, the `__local_<id>` sentinel
/// text). The output `id` is, in order: the document body's own `id`; else
/// the user PK from `resolve_pk(surrogate)`; else the `doc_id`; else the raw
/// surrogate (or sentinel). `resolve_pk` lets a body-less hit (sparse /
/// multi-vector, or a `skip_payload_fetch` dense hit) still surface the user
/// PK — mirroring the Control-Plane response translator.
///
/// The body is a document body: a plain map, or a native object
/// `[7, {...}]`. It decodes through [`nodedb_types::decode_object`], which
/// reads either form, and the row re-encodes with `value_to_msgpack`. RLS is
/// already enforced by the Data Plane before the gather, so these hits are
/// post-RLS.
///
/// A hit whose stored body does not decode fails the call with a
/// serialization error: corruption is reported, never named from a fallback.
pub fn flatten_vector_hits_to_relational_rows(
    bytes: &[u8],
    resolve_pk: impl Fn(u32) -> Option<String>,
) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;

    let hits: Vec<GatheredVectorHit> = match zerompk::from_msgpack(bytes) {
        Ok(h) => h,
        // Not a hit array (already flat, or a non-row payload): leave as-is.
        Err(_) => return Ok(bytes.to_vec()),
    };

    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(hits.len());
    for h in hits {
        // A stored body that does not decode is corruption: the hit is
        // refused, never named from a fallback.
        let decoded = match h.body.as_deref() {
            Some(body) => Some(nodedb_types::decode_object(body).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!(
                        "vector hit {} carries a stored body that does not decode: {e}",
                        h.id
                    ),
                }
            })?),
            None => None,
        };
        let mut fields: std::collections::HashMap<String, Value> = match decoded {
            Some(Value::Object(map)) => map,
            _ => std::collections::HashMap::new(),
        };
        fields
            .entry("distance".to_string())
            .or_insert(Value::Float(h.distance as f64));
        // `None` for a headless hit (no surrogate binding) — never
        // resolvable to a user PK.
        let surrogate = h.id.storage_key().map(|k| k.surrogate().as_u32());
        let raw_identity = match surrogate {
            Some(s) => Value::Integer(s as i64),
            None => Value::String(h.id.to_string()),
        };
        if !fields.contains_key("id") {
            // Prefer a catalog-resolved PK, then the DP-set doc_id, then the
            // raw surrogate (or the headless sentinel) as a last resort. The
            // resolver is asked exactly when `vector_hit_surrogates` lists
            // the hit: both decide through `body_needs_resolved_key`.
            let resolved = if body_needs_resolved_key(h.body.as_deref()) {
                surrogate.and_then(&resolve_pk)
            } else {
                None
            };
            match resolved.or(h.doc_id) {
                Some(pk) => {
                    fields.insert("id".to_string(), Value::String(pk));
                }
                None => {
                    fields.insert("id".to_string(), raw_identity.clone());
                }
            }
        }
        fields
            .entry("_surrogate".to_string())
            .or_insert(raw_identity);
        let row = nodedb_types::value_to_msgpack(&Value::Object(fields)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("vector hit row does not encode: {e}"),
            }
        })?;
        rows.push(row);
    }

    Ok(encode_binary_rows(&rows))
}

/// One gathered vector-family hit in the Data-Plane wire shape.
#[derive(zerompk::FromMessagePack)]
#[msgpack(map)]
struct GatheredVectorHit {
    id: crate::data::executor::handlers::hybrid_key::HybridFusionKey,
    distance: f32,
    doc_id: Option<String>,
    body: Option<Vec<u8>>,
}

/// Whether a hit takes its key from the resolver: it has no body, or a whole
/// body with no `id` key. The body is read as an object, plain or native, and
/// its keys are scanned without decoding a value. A body that is not a whole
/// object takes no key: the flatten reports it as corruption.
fn body_needs_resolved_key(body: Option<&[u8]>) -> bool {
    match body {
        None => true,
        Some(body) => nodedb_types::ObjectView::new(body).is_ok_and(|view| {
            view.fields().all(|entry| {
                entry.is_ok_and(|(key, _)| key != nodedb_types::DEFAULT_IDENTITY_COLUMN)
            })
        }),
    }
}

/// The surrogates of the bound hits in a gathered vector hit array, in hit
/// order, that [`flatten_vector_hits_to_relational_rows`] asks its resolver
/// for: those with no body, or a whole body with no `id`. Bodies are scanned,
/// never decoded. A payload that is not a hit array yields none, as the
/// flatten leaves it unchanged.
pub fn vector_hit_surrogates(bytes: &[u8]) -> Vec<u32> {
    let hits: Vec<GatheredVectorHit> = zerompk::from_msgpack(bytes).unwrap_or_default();
    hits.iter()
        .filter(|h| body_needs_resolved_key(h.body.as_deref()))
        .filter_map(|h| h.id.storage_key().map(|k| k.surrogate().as_u32()))
        .collect()
}

/// The surrogates of the bound hits in a gathered hybrid hit array, in hit
/// order: each `doc_id` storage key [`flatten_hybrid_hits_to_relational_rows`]
/// hands its resolver. A headless sentinel names no surrogate. A payload that
/// is not a hit array yields none.
pub fn hybrid_hit_surrogates(bytes: &[u8]) -> Vec<u32> {
    use nodedb_types::Value;

    let Ok(Value::Array(hits)) = nodedb_types::value_from_msgpack(bytes) else {
        return Vec::new();
    };
    hits.iter()
        .filter_map(|hit| match hit {
            Value::Object(fields) => match fields.get("doc_id") {
                Some(Value::String(doc_id)) => {
                    nodedb_types::StorageKey::parse(doc_id).map(|key| key.surrogate().as_u32())
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

/// Flatten a gathered array of hybrid (RRF fusion) search hits into bare
/// relational rows for the post-processing tail.
///
/// A hybrid hit is a bare map `{doc_id: <surrogate hex | __local_ sentinel>,
/// <score alias>: f64, vector_rank?, text_rank?}` — no document body. Every
/// field is preserved; `doc_id` is resolved to the user PK via
/// `resolve_pk_from_hex` and injected as `id` (the column `SELECT id` reads),
/// mirroring the Control-Plane hybrid translator. An unresolvable `doc_id`
/// (sentinel or unknown surrogate) leaves the row without an `id`.
pub fn flatten_hybrid_hits_to_relational_rows(
    bytes: &[u8],
    resolve_pk_from_hex: impl Fn(&str) -> Option<String>,
) -> Vec<u8> {
    use nodedb_types::Value;

    let Ok(Value::Array(hits)) = nodedb_types::value_from_msgpack(bytes) else {
        return bytes.to_vec();
    };

    let rows: Vec<Vec<u8>> = hits
        .into_iter()
        .filter_map(|hit| {
            let Value::Object(mut fields) = hit else {
                return None;
            };
            if let Some(Value::String(hex)) = fields.get("doc_id").cloned()
                && let Some(pk) = resolve_pk_from_hex(&hex)
            {
                fields.insert("id".to_string(), Value::String(pk));
            }
            nodedb_types::value_to_msgpack(&Value::Object(fields)).ok()
        })
        .collect();

    encode_binary_rows(&rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use std::collections::HashMap;

    fn bare_doc(pairs: Vec<(&str, Value)>) -> Vec<u8> {
        let map = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).unwrap()
    }

    /// Decode a bare msgpack array of bare row maps into per-row column maps.
    fn decode_rows(out: &[u8]) -> Vec<HashMap<String, Value>> {
        match nodedb_types::value_from_msgpack(out) {
            Ok(Value::Array(rows)) => rows
                .into_iter()
                .map(|r| match r {
                    Value::Object(m) => m,
                    other => panic!("expected row map, got {other:?}"),
                })
                .collect(),
            other => panic!("expected row array, got {other:?}"),
        }
    }

    /// A vector-family hit in the Data-Plane wire shape (bare `#[msgpack(map)]`).
    /// `id` is the `HybridFusionKey` wire string: an 8-hex-char storage key
    /// for a bound hit, matching `test_storage_key`.
    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct TestHit {
        id: String,
        distance: f32,
        doc_id: Option<String>,
        body: Option<Vec<u8>>,
    }

    /// Render surrogate `n` as the 8-hex-char storage key text a bound
    /// `HybridFusionKey` carries on the wire.
    fn test_storage_key(n: u32) -> String {
        nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(n)).to_string()
    }

    #[test]
    fn vector_hits_merge_body_and_surface_metadata() {
        let body = bare_doc(vec![
            ("id", Value::String("r0".into())),
            ("tag", Value::String("keep".into())),
        ]);
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(7),
            distance: 0.5,
            doc_id: None,
            body: Some(body),
        }])
        .unwrap();

        // Body carries `id`, so the resolver is not consulted.
        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input, |_| None).unwrap());
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get("id"), Some(&Value::String("r0".into())));
        assert_eq!(row.get("tag"), Some(&Value::String("keep".into())));
        assert_eq!(row.get("distance"), Some(&Value::Float(0.5f32 as f64)));
        assert_eq!(row.get("_surrogate"), Some(&Value::Integer(7)));
    }

    #[test]
    fn body_less_vector_hit_resolves_surrogate_to_user_pk() {
        // Sparse / multivec hit: no body → `id` comes from the PK resolver, not
        // the raw surrogate.
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(42),
            distance: 1.25,
            doc_id: None,
            body: None,
        }])
        .unwrap();

        let rows = decode_rows(
            &flatten_vector_hits_to_relational_rows(&input, |s| Some(format!("pk-{s}"))).unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::String("pk-42".into())));
        assert_eq!(rows[0].get("_surrogate"), Some(&Value::Integer(42)));
        assert_eq!(rows[0].get("distance"), Some(&Value::Float(1.25)));
    }

    #[test]
    fn body_less_vector_hit_falls_back_to_surrogate_when_unresolved() {
        let input = zerompk::to_msgpack_vec(&vec![TestHit {
            id: test_storage_key(9),
            distance: 0.0,
            doc_id: None,
            body: None,
        }])
        .unwrap();

        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input, |_| None).unwrap());
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(9)));
    }

    #[test]
    fn hybrid_hits_resolve_doc_id_to_user_pk() {
        // Hybrid hit: bare map with a hex `doc_id` and a dynamic score alias.
        let hit = Value::Object(
            [
                ("doc_id".to_string(), Value::String("0000000a".into())),
                ("score".to_string(), Value::Float(0.9)),
            ]
            .into_iter()
            .collect(),
        );
        let input = nodedb_types::value_to_msgpack(&Value::Array(vec![hit])).unwrap();

        let rows = decode_rows(&flatten_hybrid_hits_to_relational_rows(&input, |hex| {
            (hex == "0000000a").then(|| "doc-ten".to_string())
        }));
        assert_eq!(rows.len(), 1);
        // doc_id resolved and injected as `id`; the score alias is preserved.
        assert_eq!(rows[0].get("id"), Some(&Value::String("doc-ten".into())));
        assert_eq!(rows[0].get("score"), Some(&Value::Float(0.9)));
        assert_eq!(
            rows[0].get("doc_id"),
            Some(&Value::String("0000000a".into()))
        );
    }

    fn hit(surrogate: u32, body: Option<Vec<u8>>) -> TestHit {
        TestHit {
            id: test_storage_key(surrogate),
            distance: 0.1,
            doc_id: None,
            body,
        }
    }

    /// Only the hits the flatten names from the resolver are listed: a body
    /// with an `id` is left out, a body without one and no body are listed.
    #[test]
    fn vector_hit_surrogates_lists_the_hits_the_flatten_resolves() {
        let input = zerompk::to_msgpack_vec(&vec![
            hit(9, Some(bare_doc(vec![("id", Value::String("r9".into()))]))),
            hit(5, Some(bare_doc(vec![("tag", Value::String("t".into()))]))),
            TestHit {
                id: "__local_3".into(),
                distance: 0.2,
                doc_id: None,
                body: None,
            },
            hit(4, None),
        ])
        .unwrap();
        assert_eq!(vector_hit_surrogates(&input), vec![5, 4]);
        assert!(vector_hit_surrogates(b"not msgpack hits").is_empty());

        // The flatten asks the resolver for exactly the listed surrogates.
        let asked = std::cell::RefCell::new(Vec::new());
        flatten_vector_hits_to_relational_rows(&input, |s| {
            asked.borrow_mut().push(s);
            None
        })
        .unwrap();
        assert_eq!(asked.into_inner(), vec![5, 4]);
    }

    /// A truncated stored body is corruption: the flatten refuses it, and
    /// `vector_hit_surrogates` asks no key for it.
    #[test]
    fn a_truncated_body_fails_the_flatten() {
        let mut body = bare_doc(vec![("tag", Value::String("keep".into()))]);
        body.truncate(body.len() - 2);
        let input = zerompk::to_msgpack_vec(&vec![hit(6, Some(body))]).unwrap();
        assert!(vector_hit_surrogates(&input).is_empty());
        let err = flatten_vector_hits_to_relational_rows(&input, |_| Some("resolved".into()))
            .expect_err("a truncated body fails the flatten");
        assert!(
            matches!(err, crate::Error::Serialization { .. }),
            "corruption is a serialization error: {err:?}"
        );
    }

    /// An `id` key after a nested map value is still found.
    #[test]
    fn an_id_after_a_nested_map_is_detected() {
        let nested = Value::Object(
            [("inner".to_string(), Value::Integer(1))]
                .into_iter()
                .collect(),
        );
        // A fixmap of two pairs, written by hand so `meta` precedes `id`.
        let mut body = vec![0x82u8];
        for (key, value) in [("meta", nested), ("id", Value::String("r7".into()))] {
            body.extend(nodedb_types::value_to_msgpack(&Value::String(key.into())).unwrap());
            body.extend(nodedb_types::value_to_msgpack(&value).unwrap());
        }
        assert!(!body_needs_resolved_key(Some(body.as_slice())));
        let without_id = bare_doc(vec![("x", Value::Integer(1))]);
        assert!(body_needs_resolved_key(Some(without_id.as_slice())));
        assert!(body_needs_resolved_key(None));

        let input = zerompk::to_msgpack_vec(&vec![hit(7, Some(body))]).unwrap();
        assert!(vector_hit_surrogates(&input).is_empty());
        let rows = decode_rows(
            &flatten_vector_hits_to_relational_rows(&input, |_| Some("resolved".into())).unwrap(),
        );
        assert_eq!(rows[0].get("id"), Some(&Value::String("r7".into())));
    }

    #[test]
    fn hybrid_hit_surrogates_skips_the_headless_sentinel() {
        let hit = |doc_id: &str| {
            Value::Object(
                [
                    ("doc_id".to_string(), Value::String(doc_id.into())),
                    ("score".to_string(), Value::Float(0.5)),
                ]
                .into_iter()
                .collect(),
            )
        };
        let input = nodedb_types::value_to_msgpack(&Value::Array(vec![
            hit("0000000a"),
            hit("__local_2"),
            hit("0000000b"),
        ]))
        .unwrap();
        assert_eq!(hybrid_hit_surrogates(&input), vec![10, 11]);
    }

    /// A native body flattens to the row its plain form flattens to and
    /// names its own `id`. A plain array field stays an array. A malformed
    /// native body fails the flatten.
    #[test]
    fn a_native_body_flattens_as_its_plain_form() {
        let fields = vec![
            ("id", Value::String("r1".into())),
            ("price", Value::Decimal("12.50".parse().unwrap())),
            (
                "uid",
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "seen",
                Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                    1_700_000_000_000_000,
                )),
            ),
            (
                "meta",
                Value::Object(HashMap::from([("k".to_owned(), Value::Integer(1))])),
            ),
            (
                "tags",
                Value::Array(vec![Value::Integer(7), Value::String("x".into())]),
            ),
        ];
        let typed = Value::Object(
            fields
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
        );
        let native = zerompk::to_msgpack_vec(&typed).unwrap();
        let input = zerompk::to_msgpack_vec(&vec![hit(3, Some(native))]).unwrap();
        assert!(vector_hit_surrogates(&input).is_empty());
        let rows = decode_rows(&flatten_vector_hits_to_relational_rows(&input, |_| None).unwrap());
        let plain_input = zerompk::to_msgpack_vec(&vec![hit(3, Some(bare_doc(fields)))]).unwrap();
        let plain_rows =
            decode_rows(&flatten_vector_hits_to_relational_rows(&plain_input, |_| None).unwrap());
        assert_eq!(rows, plain_rows);
        assert_eq!(rows[0].get("id"), Some(&Value::String("r1".into())));
        assert_eq!(
            rows[0].get("tags"),
            Some(&Value::Array(vec![
                Value::Integer(7),
                Value::String("x".into())
            ]))
        );

        let malformed = vec![0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x63, 0x01];
        let input = zerompk::to_msgpack_vec(&vec![hit(4, Some(malformed))]).unwrap();
        assert!(matches!(
            flatten_vector_hits_to_relational_rows(&input, |_| None),
            Err(crate::Error::Serialization { .. })
        ));
    }
}
