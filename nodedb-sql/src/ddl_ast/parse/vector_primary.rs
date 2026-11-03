// SPDX-License-Identifier: Apache-2.0

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
use nodedb_types::vector_dtype::VectorStorageDtype;

use super::clause::{ClauseCursor, Tok, Token, tokenize};

/// Known quantization codec names accepted in DDL.
const VALID_QUANTIZATIONS: &[&str] = &[
    "none", "sq8", "pq", "rabitq", "bbq", "binary", "ternary", "opq",
];

/// Parse vector-primary options from raw CREATE COLLECTION SQL.
///
/// The options are the pairs of the statement's top-level `WITH (...)`
/// clause; a `WITH` or an option name inside the column list or a literal is
/// never read. Values are lower-cased, and a `[...]` list value becomes its
/// comma-joined items.
///
/// Returns `Ok(None)` if `primary` is absent or set to a non-vector value.
/// Returns `Ok(Some(cfg))` if `primary = 'vector'` and all required options
/// are present and valid.
/// Returns `Err(NodeDbError)` for any validation failure, including a
/// malformed WITH clause.
pub fn parse_vector_primary_options(sql: &str) -> Result<Option<VectorPrimaryConfig>, NodeDbError> {
    let options = with_option_pairs(sql)
        .map_err(|e| NodeDbError::bad_request(format!("WITH clause: {e}")))?;
    parse_vector_primary_options_from_kvs(&options)
}

/// Index of the first `WITH` token at parenthesis depth zero that opens a
/// parenthesized list.
fn top_level_with(toks: &[Token<'_>]) -> Option<usize> {
    let mut depth = 0i32;
    for (i, tok) in toks.iter().enumerate() {
        match &tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Word(w)
                if depth == 0
                    && w.eq_ignore_ascii_case("WITH")
                    && toks.get(i + 1).is_some_and(|t| t.tok == Tok::Punct('(')) =>
            {
                return Some(i);
            }
            _ => {}
        }
    }
    None
}

/// The `(key, value)` pairs of the top-level `WITH (...)` clause, values
/// lower-cased. Empty when the statement has no such clause.
fn with_option_pairs(sql: &str) -> Result<Vec<(String, String)>, crate::error::SqlError> {
    let toks = tokenize(sql);
    let Some(at) = top_level_with(&toks) else {
        return Ok(Vec::new());
    };
    let mut c = ClauseCursor::new(sql, &toks[at + 1..], "WITH");
    c.paren_list("WITH options", |c| {
        let key = c.word("option name")?.to_lowercase();
        c.expect_punct('=')?;
        let value = if c.eat_punct('[') {
            let mut items = Vec::new();
            if !c.eat_punct(']') {
                loop {
                    let item = match c.string("list item") {
                        Ok(item) => item,
                        Err(_) => c.name("list item")?,
                    };
                    items.push(item);
                    if c.eat_punct(',') {
                        continue;
                    }
                    c.expect_punct(']')?;
                    break;
                }
            }
            items.join(",")
        } else {
            match c.string(&key) {
                Ok(value) => value,
                Err(_) => {
                    let run = c.raw_run(&key)?.trim_matches('"').to_string();
                    // A call-shaped value such as `time(1h)` keeps its argument list.
                    if c.peek_punct('(') {
                        format!("{run}({})", c.raw_paren(&key)?)
                    } else {
                        run
                    }
                }
            }
        };
        Ok((key, value.trim().to_lowercase()))
    })
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

/// Parse vector-primary options from pre-extracted `(key, value)` pairs.
///
/// This is the typed-AST entry point, used when the CREATE COLLECTION parser
/// has already split the WITH clause into `Vec<(String, String)>`. The raw-SQL
/// entry point (`parse_vector_primary_options`) delegates here after extracting
/// its own pairs.
pub fn parse_vector_primary_options_from_kvs(
    options: &[(String, String)],
) -> Result<Option<VectorPrimaryConfig>, NodeDbError> {
    let get = |key: &str| -> Option<String> {
        options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    };

    let primary_val = get("primary");
    match primary_val.as_deref() {
        None
        | Some("document_schemaless")
        | Some("document_strict")
        | Some("kv")
        | Some("columnar")
        | Some("timeseries")
        | Some("spatial") => return Ok(None),
        Some("vector") => {}
        Some(other) => {
            return Err(NodeDbError::bad_request(format!(
                "unknown primary engine '{other}'; valid values: \
                 document_schemaless, document_strict, kv, columnar, timeseries, spatial, vector"
            )));
        }
    }

    let vector_field = get("vector_field")
        .ok_or_else(|| NodeDbError::bad_request("primary='vector' requires vector_field option"))?;
    if vector_field.is_empty() {
        return Err(NodeDbError::bad_request(
            "vector_field must be a non-empty column name",
        ));
    }

    let dim_raw = get("dim").ok_or_else(|| {
        NodeDbError::bad_request("primary='vector' requires dim option (e.g. dim=1024)")
    })?;
    let dim = dim_raw.trim().parse::<u32>().map_err(|_| {
        NodeDbError::bad_request(format!(
            "dim must be a non-negative integer (e.g. dim=1024), got '{dim_raw}'"
        ))
    })?;

    let quantization = match get("quantization").as_deref() {
        None => VectorQuantization::default(),
        Some(q) => parse_quantization(q)?,
    };

    // A value that does not parse or does not fit is refused, never replaced
    // by the default.
    let m: u8 = match get("m") {
        None => 16,
        Some(raw) => raw.trim().parse::<u8>().map_err(|_| {
            NodeDbError::bad_request(format!("m must be an integer in 0..=255, got '{raw}'"))
        })?,
    };

    let ef_construction: u16 = match get("ef_construction") {
        None => 200,
        Some(raw) => raw.trim().parse::<u16>().map_err(|_| {
            NodeDbError::bad_request(format!(
                "ef_construction must be an integer in 0..=65535, got '{raw}'"
            ))
        })?,
    };

    let metric = match get("metric").as_deref() {
        None => DistanceMetric::Cosine,
        Some(m) => parse_metric(m)?,
    };

    let storage_dtype = parse_storage_dtype(get("storage_dtype").as_deref())?;

    // payload_indexes is stored as a single value by the collection parser
    // as a comma-separated list (stripped of bracket syntax).
    let payload_indexes = get("payload_indexes")
        .map(|v| {
            v.split(',')
                .filter_map(|s| {
                    let s = s
                        .trim()
                        .trim_matches('\'')
                        .trim_matches('"')
                        .trim()
                        .to_lowercase();
                    if s.is_empty() {
                        None
                    } else {
                        Some((s, nodedb_types::PayloadIndexKind::Equality))
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(Some(VectorPrimaryConfig {
        vector_field,
        dim,
        quantization,
        m,
        ef_construction,
        metric,
        storage_dtype,
        payload_indexes,
    }))
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

/// Parse the optional `storage_dtype` DDL option. `None` (option omitted)
/// resolves to the default `F32`; unknown values produce a typed
/// `bad_request` naming the offending value.
fn parse_storage_dtype(s: Option<&str>) -> Result<VectorStorageDtype, NodeDbError> {
    let Some(s) = s else {
        return Ok(VectorStorageDtype::default());
    };
    VectorStorageDtype::parse(s).ok_or_else(|| {
        NodeDbError::bad_request(format!(
            "unknown storage_dtype '{s}'; valid values: f32, f16, bf16"
        ))
    })
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

    fn pairs(sql: &str) -> Vec<(String, String)> {
        with_option_pairs(sql).expect("WITH parses")
    }

    #[test]
    fn with_clause_after_unicode_schema_preserves_original_offsets() {
        assert_eq!(
            pairs("CREATE COLLECTION v (name ﬀﬀ) WITH (primary='vector')"),
            vec![("primary".to_string(), "vector".to_string())]
        );
    }

    #[test]
    fn option_after_unicode_value_preserves_original_offsets() {
        assert_eq!(
            pairs("CREATE COLLECTION v WITH (note='ﬀﬀ', metric='cosine')")[1],
            ("metric".to_string(), "cosine".to_string())
        );
    }

    #[test]
    fn payload_indexes_list_is_comma_joined() {
        assert_eq!(
            pairs("CREATE COLLECTION v WITH (note='ﬀﬀ', payload_indexes=['category', \"Tag\"])")[1],
            ("payload_indexes".to_string(), "category,tag".to_string())
        );
    }

    #[test]
    fn option_name_inside_a_literal_or_column_list_is_not_read() {
        // The old reader matched `dim` inside the column default and the
        // literal, and so saw `dim=7`.
        let sql = "CREATE COLLECTION v (note TEXT DEFAULT 'dim=7', vec VECTOR(64)) \
            WITH (primary='vector', vector_field='vec', dim=64)";
        let cfg = parse_vector_primary_options(sql)
            .expect("parse ok")
            .expect("should be Some");
        assert_eq!(cfg.dim, 64);
    }

    #[test]
    fn with_inside_column_list_is_not_the_options_clause() {
        let toks = tokenize("CREATE COLLECTION v (a TEXT CHECK (a WITH (x))) WITH (dim=3)");
        let at = top_level_with(&toks).expect("top-level WITH");
        assert_eq!(toks[at + 2].tok, Tok::Word("dim"));
    }

    #[test]
    fn call_shaped_value_keeps_its_arguments() {
        assert_eq!(
            pairs("CREATE COLLECTION v WITH (partition_by=time(1h), dim=3)")[0],
            ("partition_by".to_string(), "time(1h)".to_string())
        );
    }

    #[test]
    fn malformed_with_clause_is_an_error() {
        assert!(parse_vector_primary_options("CREATE COLLECTION v WITH (dim 3)").is_err());
        assert!(parse_vector_primary_options("CREATE COLLECTION v WITH (dim=3").is_err());
    }

    #[test]
    fn out_of_range_hnsw_options_are_refused() {
        // The old reader replaced an out-of-range m or ef_construction with
        // the default.
        let base = "CREATE COLLECTION c (v VECTOR(64)) WITH (primary='vector', \
            vector_field='v', dim=64";
        for extra in [", m=300)", ", ef_construction=70000)", ", m=abc)"] {
            let sql = format!("{base}{extra}");
            assert!(parse_vector_primary_options(&sql).is_err(), "{sql}");
        }
        let bad_dim = "CREATE COLLECTION c (v VECTOR(64)) WITH (primary='vector', \
            vector_field='v', dim=-1)";
        assert!(parse_vector_primary_options(bad_dim).is_err());
    }

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
        let sql =
            "CREATE COLLECTION c (id BIGINT PRIMARY KEY) WITH (primary='document_schemaless')";
        let result = parse_vector_primary_options(sql).expect("parse ok");
        assert!(result.is_none());
    }

    #[test]
    fn primary_strict_returns_none() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY) WITH (primary='document_strict')";
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

    #[test]
    fn quantization_omitted_defaults_to_sq8() {
        let sql = "CREATE COLLECTION c (id BIGINT PRIMARY KEY, v VECTOR(64)) \
            WITH (primary='vector', vector_field='v', dim=64)";
        let cfg = parse_vector_primary_options(sql)
            .expect("parse ok")
            .expect("vector-primary config");
        assert_eq!(cfg.quantization, VectorQuantization::Sq8);
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
