// SPDX-License-Identifier: Apache-2.0

//! Scalar function evaluation dispatch across per-family sub-modules.

use nodedb_crs::CrsContext;
use nodedb_types::Value;

use crate::expr::EvalError;

use super::{
    array, conditional, datetime, fts, id, json, math, string, system, text_chunk, types, vector,
};

/// Evaluate a scalar function call.
///
/// A `NULL` argument gives `Ok(Value::Null)` (SQL NULL propagation). These
/// calls fail instead:
/// - `mod`'s zero-modulus arm (`math::try_eval`) returns
///   `Err(EvalError::DivisionByZero)`;
/// - a vector distance over operands of different dimensions or over a
///   non-vector operand (`vector::try_eval`);
/// - a document function given a malformed JSONPath (`json::try_eval`);
/// - a name no family module implements returns
///   `Err(EvalError::UnknownFunction)`, never a silent `NULL`.
///
/// The fallible families (`math`, `vector`, `json`, geo) return
/// `Option<Result<Value, EvalError>>`. The rest stay `Option<Value>`-shaped
/// and are wrapped in `Ok` at this dispatch boundary.
///
/// Geo functions resolve SRIDs through `crs`.
pub fn eval_function(name: &str, args: &[Value], crs: &CrsContext<'_>) -> Result<Value, EvalError> {
    if let Some(v) = string::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(r) = math::try_eval(name, args) {
        return r;
    }
    if let Some(v) = conditional::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = id::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = datetime::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(r) = json::try_eval(name, args) {
        return r;
    }
    if let Some(v) = types::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = array::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(v) = fts::try_eval_fts(name, args) {
        return Ok(v);
    }
    if let Some(v) = system::try_eval(name, args) {
        return Ok(v);
    }
    if let Some(r) = vector::try_eval(name, args) {
        return r;
    }
    if let Some(v) = text_chunk::try_eval(name, args) {
        return Ok(v);
    }
    // Geo / Spatial functions — delegated to geo_functions module. A geo
    // fault (undefined or mixed SRID, projection domain) fails the call.
    match crate::geo_functions::eval_geo_function(name, args, crs) {
        Some(r) => r,
        None => Err(EvalError::UnknownFunction {
            name: name.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::SqlExpr;

    fn crs() -> CrsContext<'static> {
        CrsContext::builtin_only()
    }

    fn eval_fn(name: &str, args: Vec<Value>) -> Value {
        eval_function(name, &args, &crs()).unwrap()
    }

    #[test]
    fn mod_by_zero_errors() {
        let err =
            eval_function("mod", &[Value::Integer(5), Value::Integer(0)], &crs()).unwrap_err();
        assert_eq!(err, EvalError::DivisionByZero);
    }

    #[test]
    fn an_unknown_function_is_an_error_not_null() {
        let err = eval_function("no_such_function", &[Value::Integer(1)], &crs()).unwrap_err();
        assert_eq!(
            err,
            EvalError::UnknownFunction {
                name: "no_such_function".into()
            }
        );
    }

    /// Registered SQL scalars with a per-row meaning dispatch to a real
    /// evaluator, never to the unknown-function error.
    #[test]
    fn registered_row_scalars_have_evaluators() {
        let doc = Value::Object(
            [(
                "tags".to_string(),
                Value::Array(vec![Value::String("a".into())]),
            )]
            .into_iter()
            .collect(),
        );
        let path = Value::String("$.tags[0]".into());
        let vector = Value::Array(vec![Value::Float(1.0), Value::Float(0.0)]);
        let calls: [(&str, Vec<Value>); 8] = [
            ("doc_get", vec![doc.clone(), path.clone()]),
            ("doc_exists", vec![doc.clone(), path.clone()]),
            (
                "doc_array_contains",
                vec![
                    doc.clone(),
                    Value::String("$.tags".into()),
                    Value::String("a".into()),
                ],
            ),
            ("nav", vec![doc, path]),
            ("vector_distance", vec![vector.clone(), vector.clone()]),
            (
                "vector_cosine_distance",
                vec![vector.clone(), vector.clone()],
            ),
            ("vector_neg_inner_product", vec![vector.clone(), vector]),
            (
                "ndb_chunk_text",
                vec![Value::String("abc".into()), Value::Integer(2)],
            ),
        ];
        for (name, args) in calls {
            let value = eval_function(name, &args, &crs())
                .unwrap_or_else(|e| panic!("{name}() must evaluate, got {e}"));
            assert_ne!(value, Value::Null, "{name}() gave NULL");
        }
    }

    fn text(s: &str) -> Value {
        Value::String(s.into())
    }

    #[test]
    fn like_matches_percent_and_underscore() {
        assert_eq!(
            eval_fn("like", vec![text("alice"), text("a%")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("like", vec![text("alice"), text("a_ice")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("like", vec![text("alice"), text("b%")]),
            Value::Bool(false)
        );
        assert_eq!(
            eval_fn("like", vec![text("Alice"), text("a%")]),
            Value::Bool(false)
        );
    }

    #[test]
    fn like_honours_the_escape_character() {
        assert_eq!(
            eval_fn("like", vec![text("50%"), text("50\\%")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("like", vec![text("500"), text("50\\%")]),
            Value::Bool(false)
        );
        assert_eq!(
            eval_fn("like", vec![text("50%"), text("50!%"), text("!")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("like", vec![text("a\\b"), text("a\\b"), text("")]),
            Value::Bool(true)
        );
    }

    #[test]
    fn like_with_a_null_operand_is_null() {
        assert_eq!(eval_fn("like", vec![Value::Null, text("a%")]), Value::Null);
        assert_eq!(eval_fn("ilike", vec![text("a"), Value::Null]), Value::Null);
    }

    #[test]
    fn ilike_folds_unicode_case() {
        assert_eq!(
            eval_fn("ilike", vec![text("ÉCOLE"), text("éc%")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("ilike", vec![text("Straße"), text("STRA%")]),
            Value::Bool(true)
        );
        assert_eq!(
            eval_fn("like", vec![text("ÉCOLE"), text("éc%")]),
            Value::Bool(false)
        );
    }

    #[test]
    fn not_like_negates_the_call() {
        let expr = SqlExpr::Negate(Box::new(SqlExpr::Function {
            name: "like".into(),
            args: vec![SqlExpr::Literal(text("bob")), SqlExpr::Literal(text("a%"))],
        }));
        assert_eq!(expr.eval(&Value::Null, &crs()).unwrap(), Value::Bool(true));
    }

    #[test]
    fn make_array_keeps_every_evaluated_element() {
        assert_eq!(
            eval_fn(
                "make_array",
                vec![Value::Integer(5), Value::Null, text("x")]
            ),
            Value::Array(vec![Value::Integer(5), Value::Null, text("x")])
        );
    }

    #[test]
    fn upper() {
        assert_eq!(
            eval_fn("upper", vec![Value::String("hello".into())]),
            Value::String("HELLO".into())
        );
    }

    #[test]
    fn upper_null_propagation() {
        assert_eq!(eval_fn("upper", vec![Value::Null]), Value::Null);
    }

    #[test]
    fn substring() {
        assert_eq!(
            eval_fn(
                "substr",
                vec![
                    Value::String("hello".into()),
                    Value::Integer(2),
                    Value::Integer(3)
                ]
            ),
            Value::String("ell".into())
        );
    }

    #[test]
    fn round_with_decimals() {
        assert_eq!(
            eval_fn("round", vec![Value::Float(3.15159), Value::Integer(2)]),
            Value::Float(3.15)
        );
    }

    #[test]
    fn typeof_int() {
        assert_eq!(
            eval_fn("typeof", vec![Value::Integer(42)]),
            Value::String("int".into())
        );
    }

    #[test]
    fn function_via_expr() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        let doc = Value::Object(
            [("name".to_string(), Value::String("alice".into()))]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            expr.eval(&doc, &crs()).unwrap(),
            Value::String("ALICE".into())
        );
    }

    #[test]
    fn geo_geohash_encode() {
        let result = eval_fn(
            "geo_geohash",
            vec![
                Value::Float(-73.9857),
                Value::Float(40.758),
                Value::Integer(6),
            ],
        );
        let hash = result.as_str().unwrap();
        assert_eq!(hash.len(), 6);
        assert!(hash.starts_with("dr5ru"), "got {hash}");
    }

    #[test]
    fn geo_geohash_decode() {
        let hash = eval_fn(
            "geo_geohash",
            vec![Value::Float(0.0), Value::Float(0.0), Value::Integer(6)],
        );
        let result = eval_fn("geo_geohash_decode", vec![hash]);
        assert!(!result.is_null());
        assert!(result.get("min_lng").is_some());
    }

    #[test]
    fn geo_geohash_neighbors_returns_8() {
        let hash = eval_fn(
            "geo_geohash",
            vec![Value::Float(10.0), Value::Float(50.0), Value::Integer(6)],
        );
        let result = eval_fn("geo_geohash_neighbors", vec![hash]);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 8);
    }

    fn point_value(lng: f64, lat: f64) -> Value {
        let geom = nodedb_types::geometry::Geometry::point(lng, lat);
        Value::Geometry(nodedb_types::geometry::SridGeometry::wgs84(geom))
    }

    fn square_value() -> Value {
        let geom = nodedb_types::geometry::Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        Value::Geometry(nodedb_types::geometry::SridGeometry::wgs84(geom))
    }

    #[test]
    fn st_contains_sql() {
        let result = eval_fn("st_contains", vec![square_value(), point_value(5.0, 5.0)]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_intersects_sql() {
        let result = eval_fn("st_intersects", vec![square_value(), point_value(5.0, 0.0)]);
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_distance_sql() {
        let result = eval_fn(
            "st_distance",
            vec![point_value(0.0, 0.0), point_value(0.0, 1.0)],
        );
        let d = result.as_f64().unwrap();
        // WGS 84 geodesic, pyproj Geod(ellps="WGS84").inv(0, 0, 0, 1).
        assert!((d - 110_574.388_557_798_78).abs() < 1e-6, "got {d}");
    }

    #[test]
    fn st_dwithin_sql() {
        let result = eval_fn(
            "st_dwithin",
            vec![
                point_value(0.0, 0.0),
                point_value(0.001, 0.0),
                Value::Float(200.0),
            ],
        );
        assert_eq!(result, Value::Bool(true));
    }

    #[test]
    fn st_buffer_sql() {
        let result = eval_fn(
            "st_buffer",
            vec![
                point_value(0.0, 0.0),
                Value::Float(1000.0),
                Value::Integer(8),
            ],
        );
        // Result should be a Geometry (Polygon)
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn st_envelope_sql() {
        let result = eval_fn("st_envelope", vec![square_value()]);
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_length_sql() {
        let line = Value::Geometry(nodedb_types::geometry::SridGeometry::wgs84(
            nodedb_types::geometry::Geometry::line_string(vec![[0.0, 0.0], [0.0, 1.0]]),
        ));
        let result = eval_fn("geo_length", vec![line]);
        let d = result.as_f64().unwrap();
        assert!((d - 110_574.388_557_798_78).abs() < 1e-6, "got {d}");
    }

    #[test]
    fn geo_x_y() {
        assert_eq!(
            eval_fn("geo_x", vec![point_value(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            5.0
        );
        assert_eq!(
            eval_fn("geo_y", vec![point_value(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            10.0
        );
    }

    #[test]
    fn geo_type_sql() {
        assert_eq!(
            eval_fn("geo_type", vec![point_value(0.0, 0.0)]),
            Value::String("Point".into())
        );
        assert_eq!(
            eval_fn("geo_type", vec![square_value()]),
            Value::String("Polygon".into())
        );
    }

    #[test]
    fn geo_num_points_sql() {
        assert_eq!(
            eval_fn("geo_num_points", vec![point_value(0.0, 0.0)]),
            Value::Integer(1)
        );
        assert_eq!(
            eval_fn("geo_num_points", vec![square_value()]),
            Value::Integer(5)
        );
    }

    #[test]
    fn geo_is_valid_sql() {
        assert_eq!(
            eval_fn("geo_is_valid", vec![square_value()]),
            Value::Bool(true)
        );
    }

    #[test]
    fn geo_as_wkt_sql() {
        let result = eval_fn("geo_as_wkt", vec![point_value(5.0, 10.0)]);
        assert_eq!(result, Value::String("POINT(5 10)".into()));
    }

    #[test]
    fn geo_from_wkt_sql() {
        let result = eval_fn("geo_from_wkt", vec![Value::String("POINT(5 10)".into())]);
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_circle_sql() {
        let result = eval_fn(
            "geo_circle",
            vec![
                Value::Float(0.0),
                Value::Float(0.0),
                Value::Float(1000.0),
                Value::Integer(16),
            ],
        );
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn geo_bbox_sql() {
        let result = eval_fn(
            "geo_bbox",
            vec![
                Value::Float(0.0),
                Value::Float(0.0),
                Value::Float(10.0),
                Value::Float(10.0),
            ],
        );
        assert!(result.as_geometry().is_some());
    }

    #[test]
    fn version_returns_postgres_compatible_string() {
        match eval_fn("version", vec![]) {
            Value::String(s) => assert!(s.contains("PostgreSQL")),
            other => panic!("expected Value::String, got {other:?}"),
        }
    }
}
