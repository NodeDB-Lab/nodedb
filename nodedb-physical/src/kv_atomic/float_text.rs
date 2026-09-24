// SPDX-License-Identifier: Apache-2.0

//! `INCRBYFLOAT` on a raw KV body: decimal text in, decimal text out.
//!
//! Redis adds in `long double` and prints the sum with 17 fractional digits,
//! trailing zeros trimmed, so `"0.1"` plus `0.2` stores `"0.3"`. Rust has no
//! `long double`. Exact decimal addition gives the same text for every sum
//! that fits a [`Decimal`]: 28 significant digits, magnitude below 7.9e28.
//! An operand or a sum outside that range is added in `f64` instead.

use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::counter_fault::CounterFault;
use super::error::AtomicComputeError;

/// Add `delta` to the raw body `stored`. Returns the new value and the text
/// to store.
///
/// `stored` and `delta` must both be decimal numbers (see
/// [`is_decimal_number`]). Anything else is `Counter(NotAFloat)`. A sum that
/// is not finite is `Counter(NonFinite)`.
pub(super) fn add(stored: &[u8], delta: &str) -> Result<(f64, Vec<u8>), AtomicComputeError> {
    let text = std::str::from_utf8(stored)
        .ok()
        .filter(|text| is_decimal_number(text))
        .ok_or(AtomicComputeError::Counter(CounterFault::NotAFloat))?;
    let delta_f64 = delta_to_f64(delta)?;
    if let (Some(base), Some(step)) = (parse_decimal(text), parse_decimal(delta))
        && let Some(sum) = base.checked_add(step)
    {
        let sum = sum.normalize();
        let value = sum
            .to_f64()
            .ok_or(AtomicComputeError::Counter(CounterFault::NonFinite))?;
        return Ok((value, sum.to_string().into_bytes()));
    }
    let base: f64 = text
        .parse()
        .map_err(|_| AtomicComputeError::Counter(CounterFault::NotAFloat))?;
    let value = base + delta_f64;
    if !value.is_finite() {
        return Err(AtomicComputeError::Counter(CounterFault::NonFinite));
    }
    Ok((value, float_text(value)))
}

/// The text for a fresh float counter: `0` plus `delta`.
pub(super) fn fresh(delta: &str) -> Result<(f64, Vec<u8>), AtomicComputeError> {
    add(b"0", delta)
}

/// `delta` as a finite `f64`, for a typed column. A delta that is not a
/// decimal number is `Counter(NotAFloat)`. One outside the `f64` range is
/// `Counter(NonFinite)`.
pub fn delta_to_f64(delta: &str) -> Result<f64, AtomicComputeError> {
    if !is_decimal_number(delta) {
        return Err(AtomicComputeError::Counter(CounterFault::NotAFloat));
    }
    let value: f64 = delta
        .parse()
        .map_err(|_| AtomicComputeError::Counter(CounterFault::NotAFloat))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(AtomicComputeError::Counter(CounterFault::NonFinite))
    }
}

/// The exact value of `text`, or `None` when it does not fit a [`Decimal`].
fn parse_decimal(text: &str) -> Option<Decimal> {
    if text.contains(['e', 'E']) {
        Decimal::from_scientific(text).ok()
    } else {
        Decimal::from_str(text).ok()
    }
}

/// The text for an `f64` sum outside the [`Decimal`] range: plain decimal
/// digits with no exponent, the form Redis prints.
fn float_text(value: f64) -> Vec<u8> {
    let text = value.to_string();
    if text == "-0" {
        b"0".to_vec()
    } else {
        text.into_bytes()
    }
}

/// The number grammar `INCRBYFLOAT` accepts, for a stored body and for the
/// client's increment: an optional sign, then digits with an optional point
/// (at least one digit), then an optional exponent `e` or `E` with an
/// optional sign and at least one digit. No whitespace, digit separators,
/// `inf`, or `nan`.
pub fn is_decimal_number(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let int_digits = count_digits(&bytes[i..]);
    i += int_digits;
    let mut frac_digits = 0;
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        frac_digits = count_digits(&bytes[i..]);
        i += frac_digits;
    }
    if int_digits + frac_digits == 0 {
        return false;
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let exp_digits = count_digits(&bytes[i..]);
        if exp_digits == 0 {
            return false;
        }
        i += exp_digits;
    }
    i == bytes.len()
}

fn count_digits(bytes: &[u8]) -> usize {
    bytes.iter().take_while(|b| b.is_ascii_digit()).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_of(stored: &str, delta: &str) -> String {
        let (_, bytes) = add(stored.as_bytes(), delta).expect("add");
        String::from_utf8(bytes).expect("UTF-8")
    }

    #[test]
    fn decimal_text_adds_exactly_like_redis() {
        assert_eq!(text_of("0.1", "0.2"), "0.3");
        assert_eq!(text_of("10.5", "0.1"), "10.6");
        assert_eq!(text_of("5.0e3", "200"), "5200");
        assert_eq!(text_of("3.0", "0"), "3");
        assert_eq!(text_of("-1.5", "1.5"), "0");
        assert_eq!(text_of("1.5", "1"), "2.5");
        assert_eq!(text_of("+2", "-0.5"), "1.5");
        assert_eq!(text_of("1E-2", "0"), "0.01");
        assert_eq!(text_of("1", "1e1"), "11");
    }

    #[test]
    fn a_twenty_digit_delta_adds_exactly() {
        assert_eq!(
            text_of("1", "0.12345678901234567891"),
            "1.12345678901234567891"
        );
        assert_eq!(text_of("10000000000000000000", "1"), "10000000000000000001");
    }

    #[test]
    fn the_returned_value_matches_the_stored_text() {
        let (value, bytes) = add(b"0.1", "0.2").expect("add");
        assert_eq!(value, 0.3);
        assert_eq!(bytes, b"0.3".to_vec());
    }

    #[test]
    fn a_fresh_counter_stores_the_delta_text() {
        assert_eq!(fresh("2.5").expect("fresh").1, b"2.5".to_vec());
        assert_eq!(fresh("0").expect("fresh").1, b"0".to_vec());
        assert_eq!(fresh("-0.0").expect("fresh").1, b"0".to_vec());
    }

    #[test]
    fn a_sum_outside_the_decimal_range_adds_in_f64() {
        let (value, bytes) = add(b"1e300", "1").expect("add");
        assert_eq!(value, 1e300);
        assert_eq!(bytes, 1e300f64.to_string().into_bytes());
    }

    #[test]
    fn text_that_is_not_a_number_is_not_a_float() {
        for stored in [
            "abc", "", "NaN", "inf", " 1.5", "1.5 ", "1_000", ".", "1e", "e5", "0x10",
        ] {
            assert!(
                matches!(
                    add(stored.as_bytes(), "1"),
                    Err(AtomicComputeError::Counter(CounterFault::NotAFloat))
                ),
                "{stored:?}"
            );
        }
    }

    #[test]
    fn a_non_finite_sum_is_refused() {
        let max = f64::MAX.to_string();
        assert!(matches!(
            add(max.as_bytes(), &max),
            Err(AtomicComputeError::Counter(CounterFault::NonFinite))
        ));
        assert!(matches!(
            add(b"1", "1e400"),
            Err(AtomicComputeError::Counter(CounterFault::NonFinite))
        ));
    }

    #[test]
    fn a_delta_that_is_not_a_number_is_not_a_float() {
        for delta in ["abc", "", "inf", "NaN", " 1"] {
            assert!(
                matches!(
                    add(b"1", delta),
                    Err(AtomicComputeError::Counter(CounterFault::NotAFloat))
                ),
                "{delta:?}"
            );
        }
    }
}
