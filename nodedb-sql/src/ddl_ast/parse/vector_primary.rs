//! Parse `WITH (primary='vector', ...)` options for CREATE COLLECTION DDL.
//!
//! This module is concerned only with extracting and validating the
//! vector-primary options from the raw SQL string. Column-level validation
//! (e.g. that `vector_field` names an actual `VECTOR(n)` column) happens
//! at the DDL execution layer where the column list is available.

use nodedb_types::NodeDbError;
use nodedb_types::collection_config::VectorPrimaryConfig;
use nodedb_types::vector_ann::VectorQuantization;
use nodedb_types::vector_distance::DistanceMetric;

/// Known quantization codec names accepted in DDL.
const VALID_QUANTIZATIONS: &[&str] = &[
    "none", "sq8", "pq", "rabitq", "bbq", "binary", "ternary", "opq",
];

/// Parse vector-primary options from raw CREATE COLLECTION SQL.
///
/// Returns `Ok(None)` if `primary` is absent or set to a non-vector value.
/// Returns `Ok(Some(cfg))` if `primary = 'vector'` and all required options
/// are present and valid.
/// Returns `Err(NodeDbError)` for any validation failure.
pub fn parse_vector_primary_options(sql: &str) -> Result<Option<VectorPrimaryConfig>, NodeDbError> {
    let primary_val = extract_with_str(sql, "primary");

    match primary_val.as_deref() {
        None | Some("document") | Some("strict") | Some("kv") | Some("key_value")
        | Some("columnar") | Some("timeseries") | Some("spatial") => return Ok(None),
        Some("vector") => {}
        Some(other) => {
            return Err(NodeDbError::bad_request(format!(
                "unknown primary engine '{other}'; valid values: document, strict, kv, \
                 columnar, timeseries, spatial, vector"
            )));
        }
    }

    // primary = 'vector' — require vector_field.
    let vector_field = extract_with_str(sql, "vector_field")
        .ok_or_else(|| NodeDbError::bad_request("primary='vector' requires vector_field option"))?;
    if vector_field.is_empty() {
        return Err(NodeDbError::bad_request(
            "vector_field must be a non-empty column name",
        ));
    }

    // Require dim.
    let dim = extract_with_u32(sql, "dim").ok_or_else(|| {
        NodeDbError::bad_request("primary='vector' requires dim option (e.g. dim=1024)")
    })?;

    // Optional: quantization (default: None / Sq8).
    let quantization = match extract_with_str(sql, "quantization").as_deref() {
        None => VectorQuantization::default(),
        Some(q) => parse_quantization(q)?,
    };

    // Optional: m (default 16).
    let m: u8 = extract_with_u32(sql, "m")
        .and_then(|v| u8::try_from(v).ok())
        .unwrap_or(16);

    // Optional: ef_construction (default 200).
    let ef_construction: u16 = extract_with_u32(sql, "ef_construction")
        .and_then(|v| u16::try_from(v).ok())
        .unwrap_or(200);

    // Optional: metric (default Cosine).
    let metric = match extract_with_str(sql, "metric").as_deref() {
        None => DistanceMetric::Cosine,
        Some(m) => parse_metric(m)?,
    };

    // Optional: payload_indexes (array of quoted strings). Parser emits
    // names only; the DDL handler infers the kind from each column's type
    // before storing the final config.
    let payload_indexes = extract_payload_indexes(sql)
        .into_iter()
        .map(|f| (f, nodedb_types::PayloadIndexKind::Equality))
        .collect();

    Ok(Some(VectorPrimaryConfig {
        vector_field,
        dim,
        quantization,
        m,
        ef_construction,
        metric,
        payload_indexes,
    }))
}

/// Validate that `vector_field` names a `VECTOR(n)` column in the provided
/// column list. Call this after the column list is available.
///
/// `columns` is a slice of `(column_name, type_str)` pairs as stored in
/// `StoredCollection::fields` (lowercased names, original-case type strings).
pub fn validate_vector_field(
    cfg: &VectorPrimaryConfig,
    columns: &[(String, String)],
) -> Result<(), NodeDbError> {
    let col = columns
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(&cfg.vector_field));

    let (_, type_str) = col.ok_or_else(|| {
        NodeDbError::bad_request(format!(
            "vector_field '{}' does not exist in the collection's column list",
            cfg.vector_field
        ))
    })?;

    if !type_str.to_uppercase().starts_with("VECTOR") {
        return Err(NodeDbError::bad_request(format!(
            "vector_field '{}' is of type '{}'; must be VECTOR(n)",
            cfg.vector_field, type_str
        )));
    }

    Ok(())
}

/// Map a SQL column type (uppercased) to its payload bitmap kind.
fn infer_payload_kind(upper_type: &str) -> nodedb_types::PayloadIndexKind {
    use nodedb_types::PayloadIndexKind as K;
    let head = upper_type
        .split_once('(')
        .map(|(p, _)| p)
        .unwrap_or(upper_type)
        .trim();
    match head {
        "BIGINT" | "INT" | "INTEGER" | "SMALLINT" | "TINYINT" | "BIGSERIAL" | "SERIAL"
        | "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" | "TIMESTAMP" | "TIMESTAMPTZ"
        | "DATE" | "TIME" | "INSTANT" | "DATETIME" => K::Range,
        "BOOL" | "BOOLEAN" => K::Boolean,
        _ => K::Equality,
    }
}

/// Validate that each `payload_indexes` field exists, is not a VECTOR/BLOB/BYTES
/// type, and is bitmap-eligible (text/int/bool/timestamp). Also infers the
/// per-field `PayloadIndexKind` from the column type — numeric / timestamp
/// → `Range` (sorted BTreeMap), bool → `Boolean`, everything else →
/// `Equality`. Mutates `cfg.payload_indexes` to attach the inferred kinds.
pub fn validate_payload_indexes(
    cfg: &mut VectorPrimaryConfig,
    columns: &[(String, String)],
) -> Result<(), NodeDbError> {
    for slot in cfg.payload_indexes.iter_mut() {
        let field = slot.0.clone();
        let col = columns
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&field));

        let (_, type_str) = col.ok_or_else(|| {
            NodeDbError::bad_request(format!(
                "payload_indexes field '{field}' does not exist in the collection's column list"
            ))
        })?;

        let upper_type = type_str.to_uppercase();
        if upper_type.starts_with("VECTOR")
            || upper_type == "BLOB"
            || upper_type == "BYTES"
            || upper_type == "BYTEA"
        {
            return Err(NodeDbError::bad_request(format!(
                "payload_indexes field '{field}' has type '{type_str}' which is not bitmap-eligible; \
                 only text, integer, boolean, and timestamp types are supported"
            )));
        }
        slot.1 = infer_payload_kind(&upper_type);
    }
    Ok(())
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Find the substring inside the outermost `WITH (...)` clause, if any.
/// Falls back to the whole SQL when no WITH clause is present.
fn with_clause(sql: &str) -> &str {
    let upper = sql.to_uppercase();
    let Some(pos) = upper.find("WITH") else {
        return sql;
    };
    // Whole-word check on WITH.
    if pos > 0 {
        let before = sql.as_bytes()[pos - 1];
        if before.is_ascii_alphanumeric() || before == b'_' {
            return sql;
        }
    }
    let after = &sql[pos + 4..];
    let Some(open) = after.find('(') else {
        return sql;
    };
    let inner = &after[open + 1..];
    let Some(close) = inner.rfind(')') else {
        return inner;
    };
    &inner[..close]
}

/// Extract a `key = 'value'` or `key = "value"` string from SQL WITH options.
fn extract_with_str(sql: &str, key: &str) -> Option<String> {
    let scope = with_clause(sql);
    let upper = scope.to_uppercase();
    let key_upper = key.to_uppercase();

    // Find a whole-word, '='-followed occurrence; skip false matches like
    // "m" inside "metric" or inside "dim".
    let mut start = 0usize;
    let pos = loop {
        let rel = upper[start..].find(&key_upper)?;
        let abs = start + rel;
        let before_ok = abs == 0 || {
            let b = scope.as_bytes()[abs - 1];
            !(b.is_ascii_alphanumeric() || b == b'_')
        };
        let after_byte = scope
            .as_bytes()
            .get(abs + key.len())
            .copied()
            .unwrap_or(b' ');
        let after_ok = !(after_byte.is_ascii_alphanumeric() || after_byte == b'_');
        if before_ok && after_ok {
            break abs;
        }
        start = abs + key.len();
    };

    let after = scope[pos + key.len()..].trim_start();
    let after = after.strip_prefix('=')?;
    let after = after.trim_start();

    // Value may be quoted with single or double quotes.
    if let Some(rest) = after.strip_prefix('\'') {
        let end = rest.find('\'')?;
        let v = rest[..end].trim().to_lowercase();
        return if v.is_empty() { None } else { Some(v) };
    }
    if let Some(rest) = after.strip_prefix('"') {
        let end = rest.find('"')?;
        let v = rest[..end].trim().to_lowercase();
        return if v.is_empty() { None } else { Some(v) };
    }

    // Bare value (numeric-looking or unquoted identifier).
    let end = after
        .find(|c: char| c == ',' || c == ')' || c.is_whitespace())
        .unwrap_or(after.len());
    let v = after[..end].trim().to_lowercase();
    if v.is_empty() { None } else { Some(v) }
}

/// Extract a `key = <integer>` value from SQL WITH options.
fn extract_with_u32(sql: &str, key: &str) -> Option<u32> {
    let raw = extract_with_str(sql, key)?;
    raw.parse::<u32>().ok()
}

/// Extract `payload_indexes = ['a', 'b', ...]` from SQL.
///
/// Returns an empty `Vec` if the key is absent.
fn extract_payload_indexes(sql: &str) -> Vec<String> {
    let scope = with_clause(sql);
    let upper = scope.to_uppercase();
    let pos = match upper.find("PAYLOAD_INDEXES") {
        Some(p) => p,
        None => return Vec::new(),
    };

    let after = scope[pos + "payload_indexes".len()..].trim_start();
    let after = match after.strip_prefix('=') {
        Some(a) => a.trim_start(),
        None => return Vec::new(),
    };

    // Expect '[' ... ']'.
    let after = match after.strip_prefix('[') {
        Some(a) => a,
        None => return Vec::new(),
    };
    let end = match after.find(']') {
        Some(e) => e,
        None => return Vec::new(),
    };
    let inner = &after[..end];

    // Split by commas, strip quotes.
    inner
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            let s = s
                .strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .or_else(|| s.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
                .unwrap_or(s);
            let s = s.trim().to_lowercase();
            if s.is_empty() { None } else { Some(s) }
        })
        .collect()
}

/// Parse a quantization string to `VectorQuantization`.
fn parse_quantization(q: &str) -> Result<VectorQuantization, NodeDbError> {
    match q.to_lowercase().as_str() {
        "none" => Ok(VectorQuantization::None),
        "sq8" => Ok(VectorQuantization::Sq8),
        "pq" => Ok(VectorQuantization::Pq),
        "rabitq" => Ok(VectorQuantization::RaBitQ),
        "bbq" => Ok(VectorQuantization::Bbq),
        "binary" => Ok(VectorQuantization::Binary),
        "ternary" => Ok(VectorQuantization::Ternary),
        "opq" => Ok(VectorQuantization::Opq),
        other => Err(NodeDbError::bad_request(format!(
            "unknown quantization '{other}'; valid values: {}",
            VALID_QUANTIZATIONS.join(", ")
        ))),
    }
}

/// Parse a metric string to `DistanceMetric`.
fn parse_metric(m: &str) -> Result<DistanceMetric, NodeDbError> {
    match m.to_lowercase().as_str() {
        "l2" | "euclidean" => Ok(DistanceMetric::L2),
        "cosine" => Ok(DistanceMetric::Cosine),
        "ip" | "inner_product" | "innerproduct" | "dot" => Ok(DistanceMetric::InnerProduct),
        "manhattan" | "l1" => Ok(DistanceMetric::Manhattan),
        "chebyshev" | "linf" | "l_inf" => Ok(DistanceMetric::Chebyshev),
        "hamming" => Ok(DistanceMetric::Hamming),
        "jaccard" => Ok(DistanceMetric::Jaccard),
        "pearson" => Ok(DistanceMetric::Pearson),
        other => Err(NodeDbError::bad_request(format!(
            "unknown distance metric '{other}'; valid values: l2, cosine, ip, manhattan, \
             chebyshev, hamming, jaccard, pearson"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Happy path ────────────────────────────────────────────────────────

    #[test]
    fn happy_path_full_options() {
        let sql = "CREATE COLLECTION embeds \
            (id BIGINT PRIMARY KEY, vec VECTOR(1024), category TEXT) \
            WITH (primary='vector', vector_field='vec', dim=1024, \
                  quantization='rabitq', m=32, ef_construction=200, \
                  metric='cosine', payload_indexes=['category'])";
        let cfg = parse_vector_primary_options(sql)
            .expect("parse ok")
            .expect("should be Some");
        assert_eq!(cfg.vector_field, "vec");
        assert_eq!(cfg.dim, 1024);
        assert_eq!(cfg.quantization, VectorQuantization::RaBitQ);
        assert_eq!(cfg.m, 32);
        assert_eq!(cfg.ef_construction, 200);
        assert_eq!(cfg.metric, DistanceMetric::Cosine);
        assert_eq!(
            cfg.payload_indexes,
            vec![(
                "category".to_string(),
                nodedb_types::PayloadIndexKind::Equality
            )]
        );
    }

    #[test]
    fn happy_path_minimal_options() {
        let sql = "CREATE COLLECTION v (id BIGINT PRIMARY KEY, vec VECTOR(128)) \
            WITH (primary='vector', vector_field='vec', dim=128)";
        let cfg = parse_vector_primary_options(sql)
            .expect("parse ok")
            .expect("should be Some");
        assert_eq!(cfg.vector_field, "vec");
        assert_eq!(cfg.dim, 128);
        assert_eq!(cfg.m, 16);
        assert_eq!(cfg.ef_construction, 200);
        assert_eq!(cfg.metric, DistanceMetric::Cosine);
        assert!(cfg.payload_indexes.is_empty());
    }

    #[test]
    fn happy_path_multiple_payload_indexes() {
        let sql = "CREATE COLLECTION v (id BIGINT PRIMARY KEY, vec VECTOR(128), a TEXT, b INT) \
            WITH (primary='vector', vector_field='vec', dim=128, \
                  payload_indexes=['a', 'b'])";
        let cfg = parse_vector_primary_options(sql)
            .expect("parse ok")
            .expect("should be Some");
        use nodedb_types::PayloadIndexKind as K;
        assert_eq!(
            cfg.payload_indexes,
            vec![
                ("a".to_string(), K::Equality),
                ("b".to_string(), K::Equality)
            ]
        );
    }

    // ── primary absent / non-vector returns None ──────────────────────────

    #[test]
    fn no_primary_returns_none() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY)";
        let result = parse_vector_primary_options(sql).expect("parse ok");
        assert!(result.is_none());
    }

    #[test]
    fn primary_document_returns_none() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY) WITH (primary='document')";
        let result = parse_vector_primary_options(sql).expect("parse ok");
        assert!(result.is_none());
    }

    #[test]
    fn primary_strict_returns_none() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY) WITH (primary='strict')";
        let result = parse_vector_primary_options(sql).expect("parse ok");
        assert!(result.is_none());
    }

    #[test]
    fn primary_columnar_returns_none() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY) WITH (primary='columnar')";
        let result = parse_vector_primary_options(sql).expect("parse ok");
        assert!(result.is_none());
    }

    // ── Missing required options ──────────────────────────────────────────

    #[test]
    fn missing_vector_field_returns_error() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY, v VECTOR(64)) \
            WITH (primary='vector', dim=64)";
        let err = parse_vector_primary_options(sql).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("vector_field"),
            "expected vector_field in error: {msg}"
        );
    }

    #[test]
    fn missing_dim_returns_error() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY, v VECTOR(64)) \
            WITH (primary='vector', vector_field='v')";
        let err = parse_vector_primary_options(sql).expect_err("should error");
        let msg = format!("{err}");
        assert!(msg.contains("dim"), "expected dim in error: {msg}");
    }

    // ── Invalid quantization ──────────────────────────────────────────────

    #[test]
    fn unknown_quantization_returns_error() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY, v VECTOR(64)) \
            WITH (primary='vector', vector_field='v', dim=64, quantization='ivfflat')";
        let err = parse_vector_primary_options(sql).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("ivfflat"),
            "expected codec name in error: {msg}"
        );
    }

    // ── All valid quantization strings ───────────────────────────────────

    #[test]
    fn all_valid_quantizations_accepted() {
        for q in VALID_QUANTIZATIONS {
            let sql = format!(
                "CREATE COLLECTION c (id BIGINT PRIMARY KEY, v VECTOR(64)) \
                 WITH (primary='vector', vector_field='v', dim=64, quantization='{q}')"
            );
            let result = parse_vector_primary_options(&sql);
            assert!(
                result.is_ok(),
                "quantization '{q}' should be accepted, got: {result:?}"
            );
        }
    }

    // ── validate_vector_field ─────────────────────────────────────────────

    #[test]
    fn validate_vector_field_ok() {
        let cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 128,
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![
            ("id".to_string(), "BIGINT".to_string()),
            ("vec".to_string(), "VECTOR(128)".to_string()),
        ];
        validate_vector_field(&cfg, &cols).expect("should be ok");
    }

    #[test]
    fn validate_vector_field_nonexistent_column_errors() {
        let cfg = VectorPrimaryConfig {
            vector_field: "missing".to_string(),
            dim: 128,
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![("id".to_string(), "BIGINT".to_string())];
        let err = validate_vector_field(&cfg, &cols).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("missing"),
            "expected column name in error: {msg}"
        );
    }

    #[test]
    fn validate_vector_field_wrong_type_errors() {
        let cfg = VectorPrimaryConfig {
            vector_field: "name".to_string(),
            dim: 128,
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![("name".to_string(), "TEXT".to_string())];
        let err = validate_vector_field(&cfg, &cols).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("VECTOR"),
            "expected VECTOR mention in error: {msg}"
        );
    }

    // ── validate_payload_indexes ──────────────────────────────────────────

    #[test]
    fn validate_payload_indexes_ok() {
        let mut cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 128,
            payload_indexes: vec![(
                "category".to_string(),
                nodedb_types::PayloadIndexKind::Equality,
            )],
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![
            ("vec".to_string(), "VECTOR(128)".to_string()),
            ("category".to_string(), "TEXT".to_string()),
        ];
        validate_payload_indexes(&mut cfg, &cols).expect("should be ok");
    }

    #[test]
    fn validate_payload_indexes_nonexistent_errors() {
        let mut cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 128,
            payload_indexes: vec![(
                "ghost".to_string(),
                nodedb_types::PayloadIndexKind::Equality,
            )],
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![("vec".to_string(), "VECTOR(128)".to_string())];
        let err = validate_payload_indexes(&mut cfg, &cols).expect_err("should error");
        let msg = format!("{err}");
        assert!(msg.contains("ghost"), "expected field name in error: {msg}");
    }

    #[test]
    fn validate_payload_indexes_vector_type_rejected() {
        let mut cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 128,
            payload_indexes: vec![("vec".to_string(), nodedb_types::PayloadIndexKind::Equality)],
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![("vec".to_string(), "VECTOR(128)".to_string())];
        let err = validate_payload_indexes(&mut cfg, &cols).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("bitmap-eligible"),
            "expected bitmap-eligible in error: {msg}"
        );
    }

    #[test]
    fn validate_payload_indexes_blob_type_rejected() {
        let mut cfg = VectorPrimaryConfig {
            vector_field: "vec".to_string(),
            dim: 128,
            payload_indexes: vec![("data".to_string(), nodedb_types::PayloadIndexKind::Equality)],
            ..VectorPrimaryConfig::default()
        };
        let cols = vec![
            ("vec".to_string(), "VECTOR(128)".to_string()),
            ("data".to_string(), "BLOB".to_string()),
        ];
        let err = validate_payload_indexes(&mut cfg, &cols).expect_err("should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("bitmap-eligible"),
            "expected bitmap-eligible in error: {msg}"
        );
    }
}
