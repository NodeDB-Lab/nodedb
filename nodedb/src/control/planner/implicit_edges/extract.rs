// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge extraction primitives shared by the INSERT / DELETE / UPDATE
//! lifecycle paths: edge-field decode, label-default resolution, and
//! weight-property encoding.

use memchr::memmem;

/// Default edge label when a document omits `_type`. Mirrors the historical
/// Data-Plane `maybe_register_edge` default.
pub(super) const DEFAULT_EDGE_LABEL: &str = "edge";

/// One implicit edge extracted from a document write.
pub(super) struct ImplicitEdge {
    pub(super) collection: String,
    pub(super) src: String,
    pub(super) dst: String,
    pub(super) label: String,
    /// `Some(w)` when the document carried a finite numeric `weight`.
    pub(super) weight: Option<f64>,
}

/// Read a document body `value` and extract an implicit edge when it carries
/// `_from` and `_to` string fields.
///
/// The body is an object context: a plain msgpack map from a SQL write or a
/// native tagged object from a native document put. Both read through
/// [`nodedb_types::ObjectView`].
///
/// A cheap byte pre-filter skips the decode for the overwhelming majority of
/// documents that are not edges. `_type` defaults to `"edge"`; `weight` is
/// carried only when present, numeric and finite.
pub(super) fn extract_edge(collection: &str, value: &[u8]) -> Option<ImplicitEdge> {
    // Pre-filter: an edge document's body always contains the literal key
    // bytes `_from`. Avoid decoding non-edge documents on the hot path.
    memmem::find(value, b"_from")?;

    let view = nodedb_types::ObjectView::new(value).ok()?;
    let mut src: Option<String> = None;
    let mut dst: Option<String> = None;
    let mut label: Option<String> = None;
    let mut weight: Option<f64> = None;
    for entry in view.fields() {
        let (key, bytes) = entry.ok()?;
        if !matches!(key, "_from" | "_to" | "_type" | "weight") {
            continue;
        }
        let value = view.decode_value(bytes).ok()?;
        let text = || match &value {
            nodedb_types::Value::String(s) => Some(s.clone()),
            _ => None,
        };
        match key {
            "_from" => src = text(),
            "_to" => dst = text(),
            "_type" => label = text(),
            _ => {
                weight = nodedb_graph::property_weight(&value).filter(|w| w.is_finite());
            }
        }
    }

    let src = src?;
    let dst = dst?;
    Some(ImplicitEdge {
        collection: collection.to_string(),
        src,
        dst,
        label: resolve_edge_label(label.as_deref()),
        weight,
    })
}

/// Resolve the edge label, substituting [`DEFAULT_EDGE_LABEL`] when a document
/// omits `_type`. Shared by the INSERT, DELETE, and UPDATE paths so an
/// `EdgeDelete` / `EdgePut` always uses the same label the matching write
/// created.
pub(super) fn resolve_edge_label(label: Option<&str>) -> String {
    label.unwrap_or(DEFAULT_EDGE_LABEL).to_owned()
}

/// Encode `{"weight": <w>}` as the stored edge-property object: the native
/// tagged object `nodedb_graph::encode_edge_properties` writes and
/// `extract_weight_from_properties` reads to derive the CSR edge weight.
pub(super) fn weight_properties(weight: f64) -> crate::Result<Vec<u8>> {
    nodedb_graph::encode_edge_properties(std::collections::HashMap::from([(
        "weight".to_string(),
        nodedb_types::Value::Float(weight),
    )]))
    .map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("implicit edge weight properties: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_graph::csr::extract_weight_from_properties;

    /// Build a standard-msgpack map document from string/number fields, mirroring
    /// the on-wire shape produced by the DML `row_to_msgpack` writer.
    fn doc(fields: &[(&str, rmpv::Value)]) -> Vec<u8> {
        let map = rmpv::Value::Map(
            fields
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        );
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &map).expect("encode test doc");
        buf
    }

    #[test]
    fn non_edge_doc_is_skipped() {
        let v = doc(&[("name", rmpv::Value::String("alice".into()))]);
        assert!(extract_edge("people", &v).is_none());
    }

    #[test]
    fn missing_to_is_skipped() {
        let v = doc(&[("_from", rmpv::Value::String("a".into()))]);
        assert!(extract_edge("e", &v).is_none());
    }

    #[test]
    fn basic_edge_defaults_label_and_no_weight() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.src, "a");
        assert_eq!(e.dst, "b");
        assert_eq!(e.label, "edge");
        assert!(e.weight.is_none());
    }

    #[test]
    fn typed_weighted_edge() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
            ("_type", rmpv::Value::String("ROAD".into())),
            ("weight", rmpv::Value::F64(5.0)),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.label, "ROAD");
        assert_eq!(e.weight, Some(5.0));
    }

    #[test]
    fn weight_properties_round_trip_through_extractor() {
        let props = weight_properties(7.5).expect("encode");
        assert_eq!(extract_weight_from_properties(&props), 7.5);
    }

    #[test]
    fn empty_properties_default_to_unit_weight() {
        assert_eq!(extract_weight_from_properties(&[]), 1.0);
    }

    /// A native document put stores a native tagged body. Its edge fields
    /// read as a plain map's do, and a Decimal weight is a weight.
    #[test]
    fn a_native_body_carries_its_edge() {
        use nodedb_types::Value;
        let body = zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([
            ("_from".to_string(), Value::String("a".into())),
            ("_to".to_string(), Value::String("b".into())),
            ("_type".to_string(), Value::String("ROAD".into())),
            (
                "weight".to_string(),
                Value::Decimal("12.50".parse().expect("decimal")),
            ),
        ])))
        .expect("encode native body");
        let e = extract_edge("links", &body).expect("edge");
        assert_eq!((e.src.as_str(), e.dst.as_str()), ("a", "b"));
        assert_eq!(e.label, "ROAD");
        assert_eq!(e.weight, Some(12.5));
    }

    #[test]
    fn label_default_matches_insert_default() {
        assert_eq!(resolve_edge_label(None), "edge");
        assert_eq!(resolve_edge_label(Some("ROAD")), "ROAD");
    }

    #[test]
    fn integer_weight_is_carried() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
            ("weight", rmpv::Value::Integer(3.into())),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.weight, Some(3.0));
        let props = weight_properties(e.weight.unwrap()).expect("encode");
        assert_eq!(extract_weight_from_properties(&props), 3.0);
    }
}
