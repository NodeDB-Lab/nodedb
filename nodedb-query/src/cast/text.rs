// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL's text input rules for `boolean`, `bigint`, `double
//! precision` and `numeric`.
//!
//! Each parser accepts what PostgreSQL's input function accepts, with
//! leading and trailing whitespace. Text that spells no value is
//! [`EvalError::InvalidTextRepresentation`] (`22P02`), and text that spells
//! a value past the type's range is [`OutOfRange`] (`22003`).

use rust_decimal::Decimal;

use crate::expr::{EvalError, OutOfRange};

use super::int_width::IntWidth;

/// The largest exponent magnitude numeric input accepts.
const NUMERIC_MAX_EXPONENT: i64 = 1000;

/// Whether `b` is whitespace as C's `isspace` reads it.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// `s` without leading and trailing C whitespace.
fn trim_space(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_ascii() && is_space(c as u8))
}

fn invalid(type_name: &'static str, text: &str) -> EvalError {
    EvalError::InvalidTextRepresentation {
        type_name,
        text: text.to_owned(),
    }
}

/// The boolean `s` spells, as PostgreSQL's `boolin` reads it: `true`,
/// `yes`, `on`, `1` and `false`, `no`, `off`, `0`, any case, or a prefix of
/// one that no other word shares (`t`, `tr`, `y`, `n`, `of`). A lone `o` is
/// ambiguous and refused.
pub fn parse_bool(s: &str) -> Result<bool, EvalError> {
    parse_bool_with_len(trim_space(s).as_bytes()).ok_or_else(|| invalid("boolean", s))
}

/// PostgreSQL's `parse_bool_with_len` over the trimmed bytes `v`.
fn parse_bool_with_len(v: &[u8]) -> Option<bool> {
    // `v` is a case-insensitive prefix of `word`.
    let prefix_of =
        |word: &str| v.len() <= word.len() && v.eq_ignore_ascii_case(&word.as_bytes()[..v.len()]);
    match v.first()?.to_ascii_lowercase() {
        b't' if prefix_of("true") => Some(true),
        b'f' if prefix_of("false") => Some(false),
        b'y' if prefix_of("yes") => Some(true),
        b'n' if prefix_of("no") => Some(false),
        // `o` alone could be `on` or `off`: two characters at least.
        b'o' if v.len() >= 2 && prefix_of("on") => Some(true),
        b'o' if v.len() >= 2 && prefix_of("off") => Some(false),
        b'1' if v.len() == 1 => Some(true),
        b'0' if v.len() == 1 => Some(false),
        _ => None,
    }
}

/// The integer of `width` that `s` spells, as PostgreSQL's `int2in`,
/// `int4in` and `int8in` read it: an optional sign and decimal digits. A
/// value past the width is [`OutOfRange::IntegerText`].
pub fn parse_int(s: &str, width: IntWidth) -> Result<i64, EvalError> {
    let out_of_range = || {
        EvalError::OutOfRange(OutOfRange::IntegerText {
            type_name: width.type_name(),
            text: s.to_owned(),
        })
    };
    let bytes = s.as_bytes();
    let mut i = bytes.iter().take_while(|b| is_space(**b)).count();
    let negative = match bytes.get(i) {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    let digits_start = i;
    let mut total: i64 = 0;
    while let Some(d) = bytes.get(i).filter(|b| b.is_ascii_digit()) {
        let digit = i64::from(d - b'0');
        // A negative total accumulates downward, so the minimum parses.
        total = total
            .checked_mul(10)
            .and_then(|t| {
                if negative {
                    t.checked_sub(digit)
                } else {
                    t.checked_add(digit)
                }
            })
            .ok_or_else(out_of_range)?;
        i += 1;
    }
    if i == digits_start || !bytes[i..].iter().all(|b| is_space(*b)) {
        return Err(invalid(width.type_name(), s));
    }
    width.fit(total).map_err(|_| out_of_range())
}

/// The `double precision` `s` spells, as PostgreSQL's `float8in` reads it
/// through glibc's `strtod`: a decimal number with an optional exponent, a
/// hex float (`0x1.8p3`), or `inf`, `infinity`, `nan`, `nan(chars)`, each
/// with an optional sign, any case. A finite spelling whose value overflows
/// or underflows to zero is [`OutOfRange::FloatText`].
pub fn parse_float8(s: &str) -> Result<f64, EvalError> {
    let text = trim_space(s);
    let out_of_range = || EvalError::OutOfRange(OutOfRange::FloatText(s.to_owned()));
    if let Some((value, nonzero)) = super::hex_float::parse_hex_float(text) {
        if value.is_infinite() || (value == 0.0 && nonzero) {
            return Err(out_of_range());
        }
        return Ok(value);
    }
    if is_nan_with_payload(text) {
        return Ok(f64::NAN);
    }
    let value: f64 = text.parse().map_err(|_| invalid("double precision", s))?;
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    let spells_digits = mantissa.bytes().any(|b| b.is_ascii_digit());
    if spells_digits {
        let overflowed = value.is_infinite();
        let underflowed = value == 0.0 && mantissa.bytes().any(|b| matches!(b, b'1'..=b'9'));
        if overflowed || underflowed {
            return Err(out_of_range());
        }
    }
    Ok(value)
}

/// Whether `text` is `nan(chars)` with an optional sign, any case: the
/// NaN payload form `strtod` reads, the chars letters, digits and `_`.
fn is_nan_with_payload(text: &str) -> bool {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let Some(head) = unsigned.get(..4) else {
        return false;
    };
    head.eq_ignore_ascii_case("nan(")
        && unsigned.ends_with(')')
        && unsigned[4..unsigned.len() - 1]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The `numeric` `s` spells, as PostgreSQL's `numeric_in` reads it: an
/// optional sign, digits with an optional decimal point, and an optional
/// exponent `e[sign]digits` of magnitude at most 1000. The scale is the
/// fraction digits less the exponent, at least 0: `'1.50e1'` is `15.0`,
/// `'1e5'` is `100000`.
///
/// `NaN`, `Infinity` and `inf` are numeric input in PostgreSQL, but
/// `Decimal` holds no such value: they are
/// [`EvalError::NumericSpecialValue`]. A value whose integer part `Decimal`
/// cannot hold is [`OutOfRange::Numeric`]. A fraction past 28 digits rounds
/// to 28, the most `Decimal` holds.
pub fn parse_numeric(s: &str) -> Result<Decimal, EvalError> {
    let text = trim_space(s);
    if let Some(value) = numeric_special(text) {
        return Err(EvalError::NumericSpecialValue { value });
    }
    let parts = numeric_parts(text).ok_or_else(|| invalid("numeric", s))?;
    numeric_from_parts(&parts).ok_or(EvalError::OutOfRange(OutOfRange::Numeric))
}

/// The special value `text` spells, when it spells one.
fn numeric_special(text: &str) -> Option<&'static str> {
    let lower = text.to_ascii_lowercase();
    match lower.as_str() {
        "nan" => Some("NaN"),
        "infinity" | "+infinity" | "inf" | "+inf" => Some("Infinity"),
        "-infinity" | "-inf" => Some("-Infinity"),
        _ => None,
    }
}

/// A numeric spelling taken apart.
struct NumericParts {
    negative: bool,
    /// Every digit, integer and fraction, without the point.
    digits: String,
    /// Digits after the point.
    fraction_len: i64,
    exponent: i64,
}

/// `text` taken apart by PostgreSQL's numeric grammar. `None` when it is not
/// one.
fn numeric_parts(text: &str) -> Option<NumericParts> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let negative = match bytes.first() {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    let mut digits = String::new();
    let mut fraction_len = 0i64;
    let mut seen_point = false;
    while let Some(&b) = bytes.get(i) {
        if b.is_ascii_digit() {
            digits.push(char::from(b));
            if seen_point {
                fraction_len += 1;
            }
        } else if b == b'.' && !seen_point {
            seen_point = true;
        } else {
            break;
        }
        i += 1;
    }
    if digits.is_empty() {
        return None;
    }
    let mut exponent = 0i64;
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        // `strtol` reads the exponent: leading whitespace, a sign, digits.
        i += bytes[i..].iter().take_while(|b| is_space(**b)).count();
        let exp_negative = match bytes.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let exp_start = i;
        while let Some(b) = bytes.get(i).filter(|b| b.is_ascii_digit()) {
            exponent = exponent
                .saturating_mul(10)
                .saturating_add(i64::from(b - b'0'));
            i += 1;
        }
        if i == exp_start || exponent > NUMERIC_MAX_EXPONENT {
            return None;
        }
        if exp_negative {
            exponent = -exponent;
        }
    }
    if i != bytes.len() {
        return None;
    }
    Some(NumericParts {
        negative,
        digits,
        fraction_len,
        exponent,
    })
}

/// The `Decimal` of `parts` at PostgreSQL's scale. `None` when its integer
/// part is past the `Decimal` range.
fn numeric_from_parts(parts: &NumericParts) -> Option<Decimal> {
    let scale = (parts.fraction_len - parts.exponent).max(0);
    let significant = parts.digits.trim_start_matches('0');
    let mut value = if significant.is_empty() {
        Decimal::new(
            0,
            u32::try_from(scale.min(i64::from(Decimal::MAX_SCALE))).ok()?,
        )
    } else {
        // The value is `significant * 10^shift`.
        let shift = parts.exponent - parts.fraction_len;
        let len = i64::try_from(significant.len()).ok()?;
        if shift >= 0 {
            // More than 29 integer digits never fit the 96-bit mantissa.
            if len + shift > 29 {
                return None;
            }
            let zeros = "0".repeat(usize::try_from(shift).ok()?);
            Decimal::from_str_exact(&format!("{significant}{zeros}")).ok()?
        } else {
            let frac = -shift;
            let plain = if len > frac {
                let point = usize::try_from(len - frac).ok()?;
                format!("{}.{}", &significant[..point], &significant[point..])
            } else {
                let zeros = "0".repeat(usize::try_from(frac - len).ok()?);
                format!("0.{zeros}{significant}")
            };
            exact_or_rounded(&plain, len - frac)?
        }
    };
    if parts.negative && !value.is_zero() {
        value.set_sign_negative(true);
    }
    Some(value)
}

/// `plain` as an exact `Decimal`, or rounded to the digits `Decimal` holds
/// when its fraction is too long. `None` when its `integer_digits` are past
/// the `Decimal` range.
fn exact_or_rounded(plain: &str, integer_digits: i64) -> Option<Decimal> {
    if let Ok(exact) = Decimal::from_str_exact(plain) {
        return Some(exact);
    }
    if integer_digits > 29 {
        return None;
    }
    plain.parse::<Decimal>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec_text(s: &str) -> String {
        parse_numeric(s).expect("numeric").to_string()
    }

    #[test]
    fn bool_spellings_follow_boolin() {
        for (text, want) in [
            ("t", true),
            ("TRUE", true),
            ("tr", true),
            ("  yes ", true),
            ("y", true),
            ("on", true),
            ("ON", true),
            ("1", true),
            ("f", false),
            ("fa", false),
            ("False", false),
            ("n", false),
            ("no", false),
            ("of", false),
            ("off", false),
            ("0", false),
            ("\t0\n", false),
        ] {
            assert_eq!(parse_bool(text), Ok(want), "{text:?}");
        }
        for text in [
            "", "  ", "o", "truex", "yess", "onn", "offf", "10", "00", "x", "-1",
        ] {
            let err = parse_bool(text).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("invalid input syntax for type boolean: \"{text}\"")
            );
        }
    }

    #[test]
    fn int8_input_follows_int8in() {
        let int8 = |t: &str| parse_int(t, IntWidth::Int8);
        assert_eq!(int8(" 42 "), Ok(42));
        assert_eq!(int8("+7"), Ok(7));
        assert_eq!(int8("-9223372036854775808"), Ok(i64::MIN));
        assert_eq!(int8("9223372036854775807"), Ok(i64::MAX));
        for text in ["", "-", "3.5", "1e3", "12a", "0x10", "1 2"] {
            assert!(
                matches!(
                    int8(text),
                    Err(EvalError::InvalidTextRepresentation {
                        type_name: "bigint",
                        ..
                    })
                ),
                "{text:?}"
            );
        }
        let err = int8("9223372036854775808").unwrap_err();
        assert_eq!(
            err.to_string(),
            "value \"9223372036854775808\" is out of range for type bigint"
        );
    }

    #[test]
    fn int2_and_int4_input_follow_int2in_and_int4in() {
        assert_eq!(parse_int("-32768", IntWidth::Int2), Ok(-32768));
        assert_eq!(parse_int(" 32767", IntWidth::Int2), Ok(32767));
        assert_eq!(
            parse_int("32768", IntWidth::Int2).unwrap_err().to_string(),
            "value \"32768\" is out of range for type smallint"
        );
        assert_eq!(
            parse_int("-2147483648", IntWidth::Int4),
            Ok(i64::from(i32::MIN))
        );
        assert_eq!(
            parse_int("2147483648", IntWidth::Int4)
                .unwrap_err()
                .to_string(),
            "value \"2147483648\" is out of range for type integer"
        );
        assert_eq!(
            parse_int("1.5", IntWidth::Int4).unwrap_err().to_string(),
            "invalid input syntax for type integer: \"1.5\""
        );
        assert_eq!(
            parse_int("x", IntWidth::Int2).unwrap_err().to_string(),
            "invalid input syntax for type smallint: \"x\""
        );
    }

    #[test]
    fn float8_input_follows_float8in() {
        assert_eq!(parse_float8(" 1.5 "), Ok(1.5));
        assert_eq!(parse_float8("1e3"), Ok(1000.0));
        assert_eq!(parse_float8("-Infinity"), Ok(f64::NEG_INFINITY));
        assert_eq!(parse_float8("inf"), Ok(f64::INFINITY));
        assert!(parse_float8("NaN").is_ok_and(f64::is_nan));
        assert!(matches!(
            parse_float8("abc"),
            Err(EvalError::InvalidTextRepresentation { .. })
        ));
        assert_eq!(
            parse_float8("1e999").unwrap_err().to_string(),
            "\"1e999\" is out of range for type double precision"
        );
        assert!(matches!(
            parse_float8("1e-400"),
            Err(EvalError::OutOfRange(OutOfRange::FloatText(_)))
        ));
        assert_eq!(parse_float8("0e-400"), Ok(0.0));
        // strtod forms: hex floats, every infinity and NaN spelling.
        assert_eq!(parse_float8(" 0x1.8p3 "), Ok(12.0));
        assert_eq!(parse_float8("-0X10"), Ok(-16.0));
        assert_eq!(parse_float8("INFINITY"), Ok(f64::INFINITY));
        assert_eq!(parse_float8("+Inf"), Ok(f64::INFINITY));
        assert_eq!(parse_float8("-inf"), Ok(f64::NEG_INFINITY));
        assert!(parse_float8("nAn").is_ok_and(f64::is_nan));
        assert!(parse_float8("-NaN").is_ok_and(f64::is_nan));
        assert!(parse_float8("nan(0x7f_ab)").is_ok_and(f64::is_nan));
        assert_eq!(
            parse_float8("0x1p2000").unwrap_err().to_string(),
            "\"0x1p2000\" is out of range for type double precision"
        );
        assert!(matches!(
            parse_float8("0x1p-2000"),
            Err(EvalError::OutOfRange(OutOfRange::FloatText(_)))
        ));
        for text in ["0x", "0x1p", "infinit", "nan(", "nan(a b)", "1e", "--1"] {
            assert!(
                matches!(
                    parse_float8(text),
                    Err(EvalError::InvalidTextRepresentation { .. })
                ),
                "{text:?}"
            );
        }
    }

    #[test]
    fn numeric_input_follows_numeric_in() {
        assert_eq!(dec_text("12.50"), "12.50");
        assert_eq!(dec_text(" -3 "), "-3");
        assert_eq!(dec_text("1e5"), "100000");
        assert_eq!(dec_text("1.50e1"), "15.0");
        assert_eq!(dec_text("1.5E-3"), "0.0015");
        assert_eq!(dec_text("+.5"), "0.5");
        assert_eq!(dec_text("5."), "5");
        assert_eq!(dec_text("-0.00"), "0.00");
        assert_eq!(dec_text("0e-3"), "0.000");
        for text in [
            "", ".", "1.2.3", "e5", "1e", "1e1001", "-NaN", "12x", "1_000",
        ] {
            assert!(
                matches!(
                    parse_numeric(text),
                    Err(EvalError::InvalidTextRepresentation {
                        type_name: "numeric",
                        ..
                    })
                ),
                "{text:?}"
            );
        }
        assert_eq!(
            parse_numeric("1e40"),
            Err(EvalError::OutOfRange(OutOfRange::Numeric))
        );
        assert_eq!(
            parse_numeric(" NaN "),
            Err(EvalError::NumericSpecialValue { value: "NaN" })
        );
        assert_eq!(
            parse_numeric("-inf").unwrap_err().to_string(),
            "cannot convert -Infinity to numeric"
        );
    }
}
