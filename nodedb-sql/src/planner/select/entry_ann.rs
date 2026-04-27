//! ANN options parsing for the `vector_distance` SQL function.
//!
//! Parses the optional third JSON-string argument to
//! `vector_distance(field, query, '{"quantization":"rabitq","oversample":3}')`.

use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use sqlparser::ast;

use super::helpers::extract_string_literal;
use crate::error::{Result, SqlError};
use crate::types::{VectorAnnOptions, VectorQuantization};

/// Parse the optional third argument of `vector_distance(...)` into
/// `VectorAnnOptions`. Returns `VectorAnnOptions::default()` when `arg`
/// is `None`. Unknown JSON keys are silently ignored for forward-compat.
pub fn parse_ann_options(arg: Option<&ast::Expr>) -> Result<VectorAnnOptions> {
    let Some(expr) = arg else {
        return Ok(VectorAnnOptions::default());
    };

    let json_str = extract_string_literal(expr)?;
    parse_ann_options_str(&json_str)
}

/// Parse a JSON-encoded options string directly. Exposed so unit tests
/// can exercise the JSON contract without constructing `ast::Expr` values.
pub fn parse_ann_options_str(json_str: &str) -> Result<VectorAnnOptions> {
    let root: sonic_rs::Value =
        sonic_rs::from_str(json_str).map_err(|e| SqlError::Unsupported {
            detail: format!("invalid vector_distance options JSON: {e}"),
        })?;

    let obj = root.as_object().ok_or_else(|| SqlError::Unsupported {
        detail: "invalid vector_distance options JSON: expected a JSON object".into(),
    })?;

    let mut opts = VectorAnnOptions::default();

    for (key, val) in obj.iter() {
        match key {
            "quantization" => {
                if let Some(s) = val.as_str() {
                    opts.quantization = VectorQuantization::parse(s);
                }
            }
            "oversample" => {
                if let Some(n) = val.as_u64() {
                    opts.oversample = Some(u8::try_from(n).map_err(|_| {
                        SqlError::Unsupported {
                            detail: format!(
                                "vector_distance options: `oversample` must fit in u8 (0..={}); got {n}",
                                u8::MAX
                            ),
                        }
                    })?);
                }
            }
            "query_dim" => {
                if let Some(n) = val.as_u64() {
                    opts.query_dim = Some(u32::try_from(n).map_err(|_| {
                        SqlError::Unsupported {
                            detail: format!(
                                "vector_distance options: `query_dim` must fit in u32 (0..={}); got {n}",
                                u32::MAX
                            ),
                        }
                    })?);
                }
            }
            "meta_token_budget" => {
                if let Some(n) = val.as_u64() {
                    opts.meta_token_budget = Some(u8::try_from(n).map_err(|_| {
                        SqlError::Unsupported {
                            detail: format!(
                                "vector_distance options: `meta_token_budget` must fit in u8 (0..={}); got {n}",
                                u8::MAX
                            ),
                        }
                    })?);
                }
            }
            "ef_search_override" => {
                if let Some(n) = val.as_u64() {
                    opts.ef_search_override = Some(usize::try_from(n).map_err(|_| {
                        SqlError::Unsupported {
                            detail: format!(
                                "vector_distance options: `ef_search_override` exceeds usize range; got {n}"
                            ),
                        }
                    })?);
                }
            }
            "target_recall" => {
                if let Some(f) = val.as_f64() {
                    if !(0.0..=1.0).contains(&f) {
                        return Err(SqlError::Unsupported {
                            detail: format!(
                                "vector_distance options: `target_recall` must be in [0.0, 1.0]; got {f}"
                            ),
                        });
                    }
                    opts.target_recall = Some(f as f32);
                }
            }
            _ => {}
        }
    }

    Ok(opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_arg_returns_default() {
        let result = parse_ann_options(None).unwrap();
        assert_eq!(result, VectorAnnOptions::default());
    }

    #[test]
    fn parses_quantization_and_oversample() {
        let result = parse_ann_options_str(r#"{"quantization":"rabitq","oversample":3}"#).unwrap();
        assert_eq!(result.quantization, Some(VectorQuantization::RaBitQ));
        assert_eq!(result.oversample, Some(3));
    }

    #[test]
    fn invalid_json_returns_unsupported_error() {
        let err = parse_ann_options_str("not valid json{{{").unwrap_err();
        assert!(
            matches!(err, SqlError::Unsupported { .. }),
            "expected SqlError::Unsupported, got {err:?}"
        );
    }

    #[test]
    fn unknown_keys_silently_ignored() {
        let result =
            parse_ann_options_str(r#"{"quantization":"sq8","future_key":"value"}"#).unwrap();
        assert_eq!(result.quantization, Some(VectorQuantization::Sq8));
    }

    #[test]
    fn all_fields_parse() {
        let result = parse_ann_options_str(
            r#"{"quantization":"pq","oversample":5,"query_dim":128,"meta_token_budget":10,"ef_search_override":200,"target_recall":0.95}"#,
        )
        .unwrap();
        assert_eq!(result.quantization, Some(VectorQuantization::Pq));
        assert_eq!(result.oversample, Some(5));
        assert_eq!(result.query_dim, Some(128));
        assert_eq!(result.meta_token_budget, Some(10));
        assert_eq!(result.ef_search_override, Some(200));
        assert!((result.target_recall.unwrap() - 0.95f32).abs() < 0.001);
    }
}
