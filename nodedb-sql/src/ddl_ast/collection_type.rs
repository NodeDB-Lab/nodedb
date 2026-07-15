// SPDX-License-Identifier: Apache-2.0

//! Engine-name → `CollectionType` mapping for `CREATE COLLECTION` / `CREATE TABLE` DDL.
//!
//! The single public entry point is [`build_collection_type`]. It consumes the
//! pre-parsed fields from the typed DDL AST and returns a fully-constructed
//! `CollectionType` plus the columnar schema columns that the caller must use
//! to populate `StoredCollection::fields`.

use std::str::FromStr;

use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::kv_parsing;

use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive;

/// Build a `CollectionType` from the pre-parsed DDL fields.
///
/// # Parameters
///
/// - `engine`: value of `engine=` from the WITH clause (already lowercased and
///   validated against the canonical list), or `None` for the caller's default.
/// - `columns`: `(name, type_str)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `bitemporal`: whether the `BITEMPORAL` modifier flag was present.
/// - `default_to_strict`: controls the `None` branch:
///   - `true`  → `None` engine maps to `document_strict` (CREATE TABLE semantics).
///   - `false` → `None` engine maps to `document_schemaless` (CREATE COLLECTION semantics).
///
/// # Returns
///
/// `(collection_type, columnar_schema_columns)`. The second element is only
/// non-empty for `columnar`, `timeseries`, and `spatial` engines; the caller
/// should use it to populate `StoredCollection::fields` when `fields` would
/// otherwise be empty.
pub fn build_collection_type(
    engine: Option<&str>,
    columns: &[(String, String)],
    options: &[(String, String)],
    bitemporal: bool,
    default_to_strict: bool,
) -> Result<(nodedb_types::CollectionType, Vec<(String, String)>), SqlError> {
    let opt_val = |key: &str| -> Option<String> {
        options
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.clone())
    };

    match engine {
        Some("kv") => {
            let ct = build_kv_collection_type(columns, options)?;
            Ok((ct, Vec::new()))
        }
        Some("document_strict") => {
            let schema = build_strict_schema(columns, bitemporal)?;
            Ok((nodedb_types::CollectionType::strict(schema), Vec::new()))
        }
        Some("columnar") => Ok((nodedb_types::CollectionType::columnar(), columns.to_vec())),
        Some("timeseries") => {
            let partition_by = opt_val("partition_by").unwrap_or_else(|| "1h".to_string());
            let time_key = find_time_key(columns).ok_or_else(|| SqlError::MissingField {
                field: "time_key".to_string(),
                context: "timeseries engine".to_string(),
            })?;
            Ok((
                nodedb_types::CollectionType::timeseries(time_key, partition_by),
                columns.to_vec(),
            ))
        }
        Some("spatial") => {
            let geom_col = find_geom_col(columns).ok_or_else(|| SqlError::MissingField {
                field: "geometry column (GEOMETRY type or SPATIAL_INDEX modifier)".to_string(),
                context: "spatial engine".to_string(),
            })?;
            Ok((
                nodedb_types::CollectionType::spatial(geom_col),
                columns.to_vec(),
            ))
        }
        Some("document_schemaless") | Some("vector") => {
            Ok((nodedb_types::CollectionType::document(), Vec::new()))
        }
        None => {
            if default_to_strict {
                let schema = build_strict_schema(columns, bitemporal)?;
                Ok((nodedb_types::CollectionType::strict(schema), Vec::new()))
            } else {
                Ok((nodedb_types::CollectionType::document(), Vec::new()))
            }
        }
        Some(other) => Err(SqlError::Parse {
            detail: format!("internal: unhandled canonical engine '{other}'"),
        }),
    }
}

// ── Internal builders ─────────────────────────────────────────────────────────

/// Build a `StrictSchema` from pre-extracted `(name, type_str)` column pairs.
///
/// Auto-inserts a `_rowid INT64 PRIMARY KEY` if no PRIMARY KEY is declared.
/// Returns `Err` if `columns` is empty or any type string is unknown.
pub(crate) fn build_strict_schema(
    columns: &[(String, String)],
    bitemporal: bool,
) -> Result<StrictSchema, SqlError> {
    if columns.is_empty() {
        return Err(SqlError::Parse {
            detail: "document_strict requires at least one column".to_string(),
        });
    }

    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(columns.len());
    for (name, type_str) in columns {
        let (bare_type, is_pk, is_not_null, default_expr) = parse_column_type_str_full(type_str);
        let column_type = ColumnType::from_str(&bare_type).map_err(
            |e: nodedb_types::columnar::ColumnTypeParseError| SqlError::Parse {
                detail: e.to_string(),
            },
        )?;
        let nullable = !is_not_null && !is_pk;
        let mut col = if nullable {
            ColumnDef::nullable(name.clone(), column_type)
        } else {
            ColumnDef::required(name.clone(), column_type)
        };
        if is_pk {
            col = col.with_primary_key();
        }
        if let Some(expr) = default_expr {
            col = col.with_default(expr);
        }

        // GENERATED ALWAYS AS: extract and store the expression when present in type_str.
        let upper_type = type_str.to_uppercase();
        let gen_kw = if upper_type.contains("GENERATED ALWAYS AS") {
            Some("GENERATED ALWAYS AS")
        } else if upper_type.contains("GENERATED AS") {
            Some("GENERATED AS")
        } else {
            None
        };
        if let Some(kw) = gen_kw {
            let gen_pos = find_ascii_case_insensitive(type_str, kw).expect(
                "invariant: kw was found via upper_type.contains(kw) in the enclosing if-let",
            );
            let after_gen = type_str[gen_pos + kw.len()..].trim();
            if after_gen.starts_with('(') {
                let mut depth = 0usize;
                let mut end = 0usize;
                for (i, ch) in after_gen.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = i;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if end > 1 {
                    let expr_text = &after_gen[1..end];
                    match nodedb_query::expr_parse::parse_generated_expr(expr_text) {
                        Ok((parsed_expr, deps)) => {
                            if let Ok(expr_json) = sonic_rs::to_string(&parsed_expr) {
                                col.generated_expr = Some(expr_json);
                                col.generated_deps = deps;
                                // Generated columns are nullable (computed value may be null).
                                col.nullable = true;
                            }
                        }
                        Err(e) => {
                            return Err(SqlError::Parse {
                                detail: format!("invalid GENERATED expression: {e}"),
                            });
                        }
                    }
                }
            }
        }

        col_defs.push(col);
    }

    if !col_defs.iter().any(|c| c.primary_key) {
        col_defs.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    if bitemporal {
        StrictSchema::new_bitemporal(col_defs).map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })
    } else {
        StrictSchema::new(col_defs).map_err(|e| SqlError::Parse {
            detail: e.to_string(),
        })
    }
}

/// Build a `CollectionType::KeyValue` from pre-parsed column pairs and options.
///
/// Validates:
/// - Exactly one `PRIMARY KEY` column present (detected from the type_str token).
/// - PRIMARY KEY type is a valid hash key.
/// - TTL option is well-formed when present.
/// - Capacity hint is a positive integer when present.
fn build_kv_collection_type(
    columns: &[(String, String)],
    options: &[(String, String)],
) -> Result<nodedb_types::CollectionType, SqlError> {
    // Build ColumnDef list from pre-parsed (name, type_str) pairs.
    // The type_str may contain modifiers like "PRIMARY KEY", "NOT NULL", etc.
    // We strip those to get the bare type, then apply primary_key flag.
    let mut col_defs: Vec<ColumnDef> = Vec::with_capacity(columns.len());
    for (name, type_str) in columns {
        let (bare_type, is_pk, not_null) = parse_column_type_str(type_str);
        let column_type = ColumnType::from_str(&bare_type).map_err(
            |e: nodedb_types::columnar::ColumnTypeParseError| SqlError::Parse {
                detail: format!("column '{}': {}", name, e),
            },
        )?;
        let nullable = !not_null && !is_pk;
        let mut col = if nullable {
            ColumnDef::nullable(name.clone(), column_type)
        } else {
            ColumnDef::required(name.clone(), column_type)
        };
        if is_pk {
            col = col.with_primary_key();
        }
        col_defs.push(col);
    }

    // Validate: exactly one PRIMARY KEY column.
    let pk_count = col_defs.iter().filter(|c| c.primary_key).count();
    if pk_count == 0 {
        return Err(SqlError::Parse {
            detail: "KV collections require a PRIMARY KEY column (the hash key)".to_string(),
        });
    }
    if pk_count > 1 {
        return Err(SqlError::Parse {
            detail: "KV collections support exactly one PRIMARY KEY column".to_string(),
        });
    }

    // Validate: PK type must be hashable.
    let pk = col_defs
        .iter()
        .find(|c| c.primary_key)
        .expect("invariant: pk_count validated to be exactly 1 in the checks above");
    if !nodedb_types::is_valid_kv_key_type(&pk.column_type) {
        return Err(SqlError::Parse {
            detail: format!(
                "KV PRIMARY KEY type '{}' is not supported; \
                 use TEXT, UUID, INT, BIGINT, BYTES, or TIMESTAMP",
                pk.column_type
            ),
        });
    }

    let schema = StrictSchema::new(col_defs).map_err(|e| SqlError::Parse {
        detail: e.to_string(),
    })?;

    let ttl = build_kv_ttl(options, &schema)?;
    let capacity_hint = build_kv_capacity(options);

    let config = nodedb_types::KvConfig {
        schema,
        ttl,
        capacity_hint,
        inline_threshold: nodedb_types::KV_DEFAULT_INLINE_THRESHOLD,
    };

    Ok(nodedb_types::CollectionType::KeyValue(config))
}

/// Parse a raw type_str token that may contain SQL modifiers.
///
/// Returns `(bare_type, is_primary_key, is_not_null)`.
fn parse_column_type_str(type_str: &str) -> (String, bool, bool) {
    let (bare, is_pk, is_not_null, _) = parse_column_type_str_full(type_str);
    (bare, is_pk, is_not_null)
}

/// Parse a raw type_str token that may contain SQL modifiers and a DEFAULT clause.
///
/// Returns `(bare_type, is_primary_key, is_not_null, default_expr)`.
/// The `default_expr` is the raw expression text following the `DEFAULT` keyword,
/// trimmed of surrounding whitespace.
pub fn parse_column_type_str_full(type_str: &str) -> (String, bool, bool, Option<String>) {
    let upper = type_str.to_uppercase();
    let is_pk = upper.contains("PRIMARY KEY");
    let is_not_null = upper.contains("NOT NULL");

    // Extract the DEFAULT clause from the type_str.
    // type_str may look like: "TEXT DEFAULT upper('x')" or "INT NOT NULL DEFAULT 1 + 2".
    let default_expr = if let Some(def_pos) = find_ascii_case_insensitive(type_str, "DEFAULT") {
        let after = type_str[def_pos + "DEFAULT".len()..].trim();
        if after.is_empty() {
            None
        } else {
            Some(after.to_string())
        }
    } else {
        None
    };

    // Strip modifiers to get bare type token.
    let bare = type_str
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(',');

    (bare.to_string(), is_pk, is_not_null, default_expr)
}

/// Parse TTL from the options list.
///
/// Looks for a key `ttl` whose value is either:
/// - An INTERVAL literal: `INTERVAL '15 minutes'` → FixedDuration
/// - A field-based expression: `last_active + INTERVAL '1 hour'` → FieldBased
fn build_kv_ttl(
    options: &[(String, String)],
    schema: &StrictSchema,
) -> Result<Option<nodedb_types::KvTtlPolicy>, SqlError> {
    let ttl_val = match options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("ttl"))
        .map(|(_, v)| v.as_str())
    {
        Some(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };

    let expr = ttl_val.trim();

    // Field-based: <field_name> + INTERVAL '...'
    if let Some(plus_pos) = expr.find('+') {
        let field_name = expr[..plus_pos].trim().to_lowercase();
        let interval_part = expr[plus_pos + 1..].trim();

        if !schema.columns.iter().any(|c| c.name == field_name) {
            return Err(SqlError::Parse {
                detail: format!("TTL field '{field_name}' not found in schema"),
            });
        }

        let offset_ms =
            kv_parsing::parse_interval_to_ms(interval_part).map_err(|e| SqlError::Parse {
                detail: e.to_string(),
            })?;

        return Ok(Some(nodedb_types::KvTtlPolicy::FieldBased {
            field: field_name,
            offset_ms,
        }));
    }

    // Fixed duration: INTERVAL '...' or bare short-form.
    let duration_ms = kv_parsing::parse_interval_to_ms(expr).map_err(|e| SqlError::Parse {
        detail: format!("invalid TTL expression '{expr}': {e}"),
    })?;

    Ok(Some(nodedb_types::KvTtlPolicy::FixedDuration {
        duration_ms,
    }))
}

/// Parse optional `capacity` from the options list.
fn build_kv_capacity(options: &[(String, String)]) -> u32 {
    options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("capacity"))
        .and_then(|(_, v)| v.trim().parse::<u32>().ok())
        .unwrap_or(0)
}

/// Find the time key column from the column list.
///
/// Matches columns whose type_str contains `TIME_KEY` modifier or whose bare
/// type is `TIMESTAMP` or `TIMESTAMPTZ`.
pub(crate) fn find_time_key(columns: &[(String, String)]) -> Option<String> {
    columns
        .iter()
        .find(|(_, t)| {
            let u = t.to_uppercase();
            u.contains("TIME_KEY") || u.starts_with("TIMESTAMP")
        })
        .map(|(n, _)| n.clone())
}

/// Find the geometry column from the column list.
///
/// Matches columns whose type_str contains `SPATIAL_INDEX` modifier or whose
/// bare type is `GEOMETRY` or `GEOM`.
pub(crate) fn find_geom_col(columns: &[(String, String)]) -> Option<String> {
    columns
        .iter()
        .find(|(_, t)| {
            let u = t.to_uppercase();
            u.contains("SPATIAL_INDEX") || u == "GEOMETRY" || u == "GEOM"
        })
        .map(|(n, _)| n.clone())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| (n.to_string(), t.to_string()))
            .collect()
    }

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn default_after_unicode_type_text_preserves_original_offsets() {
        let (bare_type, is_pk, is_not_null, default_expr) =
            parse_column_type_str_full("CUSTOMﬀﬀ DEFAULT 42");
        assert_eq!(bare_type, "CUSTOMﬀﬀ");
        assert!(!is_pk);
        assert!(!is_not_null);
        assert_eq!(default_expr.as_deref(), Some("42"));
    }

    // ── engine name → CollectionType variant ─────────────────────────────

    #[test]
    fn engine_document_schemaless() {
        let (ct, schema_cols) = build_collection_type(
            Some("document_schemaless"),
            &cols(&[("id", "BIGINT")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
        assert!(schema_cols.is_empty());
    }

    #[test]
    fn engine_vector_maps_to_document() {
        let (ct, _) = build_collection_type(
            Some("vector"),
            &cols(&[("emb", "VECTOR(128)")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
    }

    #[test]
    fn engine_document_strict_produces_strict() {
        let (ct, schema_cols) = build_collection_type(
            Some("document_strict"),
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(_))
        ));
        assert!(schema_cols.is_empty());
    }

    #[test]
    fn engine_strict_requires_columns() {
        let err =
            build_collection_type(Some("document_strict"), &[], &[], false, false).unwrap_err();
        assert!(err.to_string().contains("at least one column"), "{err}");
    }

    #[test]
    fn engine_columnar_returns_schema_cols() {
        let input_cols = cols(&[("ts", "TIMESTAMP"), ("val", "FLOAT64")]);
        let (ct, schema_cols) =
            build_collection_type(Some("columnar"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
    }

    #[test]
    fn engine_timeseries_auto_detects_time_key() {
        let input_cols = cols(&[("ts", "TIMESTAMP"), ("val", "FLOAT64")]);
        let (ct, schema_cols) =
            build_collection_type(Some("timeseries"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
        // Verify time_key wired into the type.
        if let nodedb_types::CollectionType::Columnar(profile) = &ct {
            if let nodedb_types::ColumnarProfile::Timeseries { time_key, .. } = profile {
                assert_eq!(time_key, "ts");
            } else {
                panic!("expected Timeseries profile, got {profile:?}");
            }
        }
    }

    #[test]
    fn engine_timeseries_rejects_missing_time_key() {
        let err = build_collection_type(
            Some("timeseries"),
            &cols(&[("val", "FLOAT64")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("time_key"), "{err}");
    }

    #[test]
    fn engine_spatial_auto_detects_geom_col() {
        let input_cols = cols(&[("id", "BIGINT"), ("geom", "GEOMETRY")]);
        let (ct, schema_cols) =
            build_collection_type(Some("spatial"), &input_cols, &[], false, false).unwrap();
        assert!(matches!(ct, nodedb_types::CollectionType::Columnar(_)));
        assert_eq!(schema_cols, input_cols);
        if let nodedb_types::CollectionType::Columnar(profile) = &ct {
            if let nodedb_types::ColumnarProfile::Spatial {
                geometry_column, ..
            } = profile
            {
                assert_eq!(geometry_column, "geom");
            } else {
                panic!("expected Spatial profile, got {profile:?}");
            }
        }
    }

    #[test]
    fn engine_spatial_rejects_missing_geom_col() {
        let err = build_collection_type(
            Some("spatial"),
            &cols(&[("id", "BIGINT"), ("val", "FLOAT64")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("geometry column"), "{err}");
    }

    // ── default_to_strict flag ────────────────────────────────────────────

    #[test]
    fn none_engine_default_to_strict_true() {
        let (ct, _) = build_collection_type(
            None,
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            false,
            true,
        )
        .unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(_))
        ));
    }

    #[test]
    fn none_engine_default_to_strict_false() {
        let (ct, _) =
            build_collection_type(None, &cols(&[("id", "BIGINT")]), &[], false, false).unwrap();
        assert!(matches!(
            ct,
            nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        ));
    }

    // ── bitemporal flag ───────────────────────────────────────────────────

    #[test]
    fn bitemporal_flag_flows_through_strict() {
        let (ct, _) = build_collection_type(
            Some("document_strict"),
            &cols(&[("id", "BIGINT"), ("name", "TEXT")]),
            &[],
            true,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) =
            &ct
        {
            assert!(schema.bitemporal, "expected bitemporal schema");
        } else {
            panic!("expected Strict variant");
        }
    }

    // ── KV engine ─────────────────────────────────────────────────────────

    #[test]
    fn kv_requires_primary_key() {
        let err = build_collection_type(
            Some("kv"),
            &cols(&[("session_id", "TEXT"), ("data", "BYTES")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("PRIMARY KEY"), "{err}");
    }

    #[test]
    fn kv_correct_construction() {
        let (ct, schema_cols) = build_collection_type(
            Some("kv"),
            &cols(&[("session_id", "TEXT PRIMARY KEY"), ("data", "BYTES")]),
            &[],
            false,
            false,
        )
        .unwrap();
        assert!(schema_cols.is_empty());
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            let pk = config.primary_key_column().unwrap();
            assert_eq!(pk.name, "session_id");
            assert!(pk.primary_key);
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_rejects_invalid_pk_type() {
        let err = build_collection_type(
            Some("kv"),
            &cols(&[("geom_key", "GEOMETRY PRIMARY KEY"), ("val", "TEXT")]),
            &[],
            false,
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not supported"), "{err}");
    }

    #[test]
    fn kv_capacity_option_parsed() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("v", "BYTES")]),
            &opts(&[("capacity", "10000")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(config.capacity_hint, 10000);
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_ttl_fixed_duration() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("v", "BYTES")]),
            &opts(&[("ttl", "INTERVAL '1h'")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(
                config.ttl,
                Some(nodedb_types::KvTtlPolicy::FixedDuration {
                    duration_ms: 3_600_000
                })
            );
        } else {
            panic!("expected KeyValue");
        }
    }

    #[test]
    fn kv_ttl_field_based() {
        let (ct, _) = build_collection_type(
            Some("kv"),
            &cols(&[("k", "TEXT PRIMARY KEY"), ("last_active", "TIMESTAMP")]),
            &opts(&[("ttl", "last_active + INTERVAL '30m'")]),
            false,
            false,
        )
        .unwrap();
        if let nodedb_types::CollectionType::KeyValue(config) = ct {
            assert_eq!(
                config.ttl,
                Some(nodedb_types::KvTtlPolicy::FieldBased {
                    field: "last_active".to_string(),
                    offset_ms: 1_800_000,
                })
            );
        } else {
            panic!("expected KeyValue");
        }
    }
}
