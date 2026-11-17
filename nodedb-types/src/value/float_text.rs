// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL text and JSON forms of a float, including the non-finite
//! values JSON numbers cannot hold.
//!
//! PostgreSQL renders a non-finite float as `NaN`, `Infinity` or
//! `-Infinity`, and `to_json` renders it as that text in a JSON string.
//! A JSON number cannot hold these values, and `serde_json` turns them into
//! `null`. Every path that renders a float to text or JSON calls these
//! functions instead.

/// The PostgreSQL text of a non-finite float: `NaN`, `Infinity` or
/// `-Infinity`. `None` for a finite float.
pub fn non_finite_float_text(f: f64) -> Option<&'static str> {
    (!f.is_finite()).then(|| non_finite_text(f))
}

/// The PostgreSQL text of a float8, as `float8out` writes it with the
/// default `extra_float_digits`: the shortest digits that read back as the
/// same float. A decimal exponent from -4 to 14 prints in fixed notation
/// (`7`, `0.001`, `123456789012345`), any other in exponent notation with a
/// sign and at least two exponent digits (`1e+15`, `1.5e-05`). NaN and the
/// infinities print as `NaN`, `Infinity` and `-Infinity`.
pub fn float8_text(f: f64) -> String {
    if let Some(text) = non_finite_float_text(f) {
        return text.to_owned();
    }
    // `{:e}` writes the shortest round-trip digits: `1.5e-5`, `7e0`.
    shortest_text(f.is_sign_negative(), &format!("{:e}", f.abs()), 15)
}

/// The PostgreSQL text of a float4, as `float4out` writes it with the
/// default `extra_float_digits`: the shortest digits that read back as the
/// same `f32`. A decimal exponent from -4 to 5 prints in fixed notation
/// (`100000`, `0.25`), any other in exponent notation (`1.234567e+06`).
pub fn float4_text(f: f32) -> String {
    if let Some(text) = non_finite_float_text(f64::from(f)) {
        return text.to_owned();
    }
    shortest_text(f.is_sign_negative(), &format!("{:e}", f.abs()), 6)
}

/// `shortest`, the `{:e}` text of a float's magnitude, in PostgreSQL's
/// notation: fixed for a decimal exponent from -4 below `fixed_limit`,
/// exponent notation otherwise.
fn shortest_text(negative: bool, shortest: &str, fixed_limit: i32) -> String {
    let sign = if negative { "-" } else { "" };
    let (mantissa, exponent) = shortest.split_once('e').unwrap_or((shortest, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if (-4..fixed_limit).contains(&exponent) {
        let point = exponent + 1;
        let body = if point <= 0 {
            format!("0.{}{digits}", "0".repeat(point.unsigned_abs() as usize))
        } else {
            let point = point as usize;
            if digits.len() <= point {
                format!("{digits}{}", "0".repeat(point - digits.len()))
            } else {
                format!("{}.{}", &digits[..point], &digits[point..])
            }
        };
        return format!("{sign}{body}");
    }
    let exp_sign = if exponent < 0 { '-' } else { '+' };
    format!("{sign}{mantissa}e{exp_sign}{:02}", exponent.unsigned_abs())
}

/// A float as a JSON value. A finite float is a JSON number. A non-finite
/// float is its PostgreSQL text in a JSON string, as `to_json` renders it.
pub fn float_to_json(f: f64) -> serde_json::Value {
    match serde_json::Number::from_f64(f) {
        Some(n) => serde_json::Value::Number(n),
        // `from_f64` refuses exactly the non-finite floats.
        None => serde_json::Value::String(non_finite_text(f).to_owned()),
    }
}

/// The PostgreSQL text of a float the caller knows is non-finite.
fn non_finite_text(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_finite_floats_render_postgres_text() {
        assert_eq!(non_finite_float_text(f64::NAN), Some("NaN"));
        assert_eq!(non_finite_float_text(f64::INFINITY), Some("Infinity"));
        assert_eq!(non_finite_float_text(f64::NEG_INFINITY), Some("-Infinity"));
        assert_eq!(non_finite_float_text(1.5), None);
        assert_eq!(non_finite_float_text(f64::MAX), None);
    }

    #[test]
    fn float8_text_matches_float8out() {
        for (f, want) in [
            (7.0, "7"),
            (-7.0, "-7"),
            (0.0, "0"),
            (-0.0, "-0"),
            (2.5, "2.5"),
            (0.1, "0.1"),
            (1.0 / 3.0, "0.3333333333333333"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (123_456_789_012_345.0, "123456789012345"),
            (1e15, "1e+15"),
            (1.25e20, "1.25e+20"),
            (1e100, "1e+100"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::NAN, "NaN"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(float8_text(f), want, "{f:e}");
        }
    }

    #[test]
    fn float4_text_matches_float4out() {
        for (f, want) in [
            (7.0f32, "7"),
            (0.1, "0.1"),
            (1.1, "1.1"),
            (100000.0, "100000"),
            (1234567.0, "1.234567e+06"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (-2.5, "-2.5"),
            (f32::MAX, "3.4028235e+38"),
            (f32::NAN, "NaN"),
            (f32::INFINITY, "Infinity"),
        ] {
            assert_eq!(float4_text(f), want, "{f:e}");
        }
    }

    #[test]
    fn non_finite_floats_are_json_strings() {
        assert_eq!(float_to_json(f64::NAN), serde_json::json!("NaN"));
        assert_eq!(float_to_json(f64::INFINITY), serde_json::json!("Infinity"));
        assert_eq!(
            float_to_json(f64::NEG_INFINITY),
            serde_json::json!("-Infinity")
        );
        assert_eq!(float_to_json(2.5), serde_json::json!(2.5));
    }
}
