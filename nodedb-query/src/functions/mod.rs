//! Scalar function evaluation for SqlExpr.
//!
//! All functions return `serde_json::Value::Null` on invalid/missing
//! arguments (SQL NULL propagation semantics).

mod array;
mod conditional;
mod datetime;
mod id;
mod json;
mod math;
pub(crate) mod shared;
mod string;
mod types;

/// Evaluate a scalar function call.
pub fn eval_function(name: &str, args: &[serde_json::Value]) -> serde_json::Value {
    if let Some(v) = string::try_eval(name, args) {
        return v;
    }
    if let Some(v) = math::try_eval(name, args) {
        return v;
    }
    if let Some(v) = conditional::try_eval(name, args) {
        return v;
    }
    if let Some(v) = id::try_eval(name, args) {
        return v;
    }
    if let Some(v) = datetime::try_eval(name, args) {
        return v;
    }
    if let Some(v) = json::try_eval(name, args) {
        return v;
    }
    if let Some(v) = types::try_eval(name, args) {
        return v;
    }
    if let Some(v) = array::try_eval(name, args) {
        return v;
    }
    // Geo / Spatial functions — delegated to geo_functions module.
    crate::geo_functions::eval_geo_function(name, args).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::SqlExpr;
    use serde_json::json;

    fn eval_fn(name: &str, args: Vec<serde_json::Value>) -> serde_json::Value {
        eval_function(name, &args)
    }

    #[test]
    fn upper() {
        assert_eq!(eval_fn("upper", vec![json!("hello")]), json!("HELLO"));
    }

    #[test]
    fn upper_null_propagation() {
        assert_eq!(eval_fn("upper", vec![json!(null)]), json!(null));
    }

    #[test]
    fn substring() {
        assert_eq!(
            eval_fn("substr", vec![json!("hello"), json!(2), json!(3)]),
            json!("ell")
        );
    }

    #[test]
    fn round_with_decimals() {
        assert_eq!(
            eval_fn("round", vec![json!(3.15159), json!(2)]),
            json!(3.15)
        );
    }

    #[test]
    fn typeof_int() {
        assert_eq!(eval_fn("typeof", vec![json!(42)]), json!("int"));
    }

    #[test]
    fn function_via_expr() {
        let expr = SqlExpr::Function {
            name: "upper".into(),
            args: vec![SqlExpr::Column("name".into())],
        };
        let doc = json!({"name": "alice"});
        assert_eq!(expr.eval(&doc), json!("ALICE"));
    }

    #[test]
    fn geo_geohash_encode() {
        let result = eval_fn(
            "geo_geohash",
            vec![json!(-73.9857), json!(40.758), json!(6)],
        );
        let hash = result.as_str().unwrap();
        assert_eq!(hash.len(), 6);
        assert!(hash.starts_with("dr5ru"), "got {hash}");
    }

    #[test]
    fn geo_geohash_decode() {
        let hash = eval_fn("geo_geohash", vec![json!(0.0), json!(0.0), json!(6)]);
        let result = eval_fn("geo_geohash_decode", vec![hash]);
        assert!(result.is_object());
        assert!(result["min_lng"].as_f64().is_some());
        assert!(result["max_lat"].as_f64().is_some());
    }

    #[test]
    fn geo_geohash_neighbors_returns_8() {
        let hash = eval_fn("geo_geohash", vec![json!(10.0), json!(50.0), json!(6)]);
        let result = eval_fn("geo_geohash_neighbors", vec![hash]);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 8);
    }

    fn point_json(lng: f64, lat: f64) -> serde_json::Value {
        json!({"type": "Point", "coordinates": [lng, lat]})
    }

    fn square_json() -> serde_json::Value {
        json!({"type": "Polygon", "coordinates": [[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]]]})
    }

    #[test]
    fn st_contains_sql() {
        let result = eval_fn("st_contains", vec![square_json(), point_json(5.0, 5.0)]);
        assert_eq!(result, json!(true));
    }

    #[test]
    fn st_intersects_sql() {
        let result = eval_fn("st_intersects", vec![square_json(), point_json(5.0, 0.0)]);
        assert_eq!(result, json!(true));
    }

    #[test]
    fn st_distance_sql() {
        let result = eval_fn(
            "st_distance",
            vec![point_json(0.0, 0.0), point_json(0.0, 1.0)],
        );
        let d = result.as_f64().unwrap();
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn st_dwithin_sql() {
        let result = eval_fn(
            "st_dwithin",
            vec![point_json(0.0, 0.0), point_json(0.001, 0.0), json!(200.0)],
        );
        assert_eq!(result, json!(true));
    }

    #[test]
    fn st_buffer_sql() {
        let result = eval_fn(
            "st_buffer",
            vec![point_json(0.0, 0.0), json!(1000.0), json!(8)],
        );
        assert!(result.is_object());
        assert_eq!(result["type"], "Polygon");
    }

    #[test]
    fn st_envelope_sql() {
        let result = eval_fn("st_envelope", vec![square_json()]);
        assert_eq!(result["type"], "Polygon");
    }

    #[test]
    fn geo_length_sql() {
        let line = json!({"type": "LineString", "coordinates": [[0.0,0.0],[0.0,1.0]]});
        let result = eval_fn("geo_length", vec![line]);
        let d = result.as_f64().unwrap();
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn geo_x_y() {
        assert_eq!(
            eval_fn("geo_x", vec![point_json(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            5.0
        );
        assert_eq!(
            eval_fn("geo_y", vec![point_json(5.0, 10.0)])
                .as_f64()
                .unwrap(),
            10.0
        );
    }

    #[test]
    fn geo_type_sql() {
        assert_eq!(
            eval_fn("geo_type", vec![point_json(0.0, 0.0)]),
            json!("Point")
        );
        assert_eq!(eval_fn("geo_type", vec![square_json()]), json!("Polygon"));
    }

    #[test]
    fn geo_num_points_sql() {
        assert_eq!(
            eval_fn("geo_num_points", vec![point_json(0.0, 0.0)]),
            json!(1)
        );
        assert_eq!(eval_fn("geo_num_points", vec![square_json()]), json!(5));
    }

    #[test]
    fn geo_is_valid_sql() {
        assert_eq!(eval_fn("geo_is_valid", vec![square_json()]), json!(true));
    }

    #[test]
    fn geo_as_wkt_sql() {
        let result = eval_fn("geo_as_wkt", vec![point_json(5.0, 10.0)]);
        assert_eq!(result, json!("POINT(5 10)"));
    }

    #[test]
    fn geo_from_wkt_sql() {
        let result = eval_fn("geo_from_wkt", vec![json!("POINT(5 10)")]);
        assert_eq!(result["type"], "Point");
    }

    #[test]
    fn geo_circle_sql() {
        let result = eval_fn(
            "geo_circle",
            vec![json!(0.0), json!(0.0), json!(1000.0), json!(16)],
        );
        assert_eq!(result["type"], "Polygon");
    }

    #[test]
    fn geo_bbox_sql() {
        let result = eval_fn(
            "geo_bbox",
            vec![json!(0.0), json!(0.0), json!(10.0), json!(10.0)],
        );
        assert_eq!(result["type"], "Polygon");
    }
}
