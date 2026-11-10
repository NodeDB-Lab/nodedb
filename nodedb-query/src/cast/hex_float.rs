// SPDX-License-Identifier: Apache-2.0

//! C99 hexadecimal float input, as `strtod` reads it: `0x1.8p3`, `-0X.4P-2`,
//! `0xff`. PostgreSQL's `float8in` reads float text through `strtod`, so it
//! takes these too.

/// Significant hex digits kept exactly. Past these, a digit only decides
/// the rounding.
const KEPT_DIGITS: usize = 16;

/// The float `text` spells as a hex float, correctly rounded to nearest,
/// ties to even, and whether its mantissa has a nonzero digit. `None` when
/// `text` is not a whole hex float: an optional sign, `0x` or `0X`, hex
/// digits with at most one point and at least one digit, and an optional
/// binary exponent `p[sign]digits`. An overflow is an infinity and an
/// underflow is zero; the caller reads the mantissa flag to tell an
/// underflow from a zero.
pub(super) fn parse_hex_float(text: &str) -> Option<(f64, bool)> {
    let bytes = text.as_bytes();
    let (negative, rest) = match bytes.first() {
        Some(b'-') => (true, &bytes[1..]),
        Some(b'+') => (false, &bytes[1..]),
        _ => (false, bytes),
    };
    let rest = rest
        .strip_prefix(b"0x")
        .or_else(|| rest.strip_prefix(b"0X"))?;
    let mut i = 0;
    let mut nibbles: Vec<u8> = Vec::new();
    let mut fraction_len: i64 = 0;
    let mut seen_point = false;
    while let Some(&b) = rest.get(i) {
        if let Some(n) = char::from(b).to_digit(16) {
            nibbles.push(n as u8);
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
    if nibbles.is_empty() {
        return None;
    }
    let mut exponent: i64 = 0;
    if matches!(rest.get(i), Some(b'p' | b'P')) {
        i += 1;
        let exp_negative = match rest.get(i) {
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
        let start = i;
        while let Some(b) = rest.get(i).filter(|b| b.is_ascii_digit()) {
            // Past this magnitude every value overflows or underflows.
            exponent = (exponent * 10 + i64::from(b - b'0')).min(1 << 20);
            i += 1;
        }
        if i == start {
            return None;
        }
        if exp_negative {
            exponent = -exponent;
        }
    }
    if i != rest.len() {
        return None;
    }
    let signed = |v: f64| if negative { -v } else { v };
    let first = nibbles.iter().position(|&n| n != 0);
    let Some(first) = first else {
        return Some((signed(0.0), false));
    };
    let significant = &nibbles[first..];
    let kept = &significant[..significant.len().min(KEPT_DIGITS)];
    let dropped = &significant[kept.len()..];
    let mantissa = kept.iter().fold(0u64, |m, &n| (m << 4) | u64::from(n));
    let sticky = dropped.iter().any(|&n| n != 0);
    // The value is `mantissa * 2^scale`, plus the sticky remainder.
    let scale = 4 * dropped.len() as i64 - 4 * fraction_len + exponent;
    Some((signed(round_to_f64(mantissa, scale, sticky)), true))
}

/// `mantissa * 2^scale`, with `sticky` a nonzero remainder below the
/// mantissa's last bit, rounded to the nearest `f64`, ties to even.
/// `mantissa` is nonzero.
fn round_to_f64(mantissa: u64, scale: i64, sticky: bool) -> f64 {
    let shift = mantissa.leading_zeros();
    let normalized = u128::from(mantissa << shift);
    // The exponent of the leading bit.
    let top = scale - i64::from(shift) + 63;
    if top > 1023 {
        return f64::INFINITY;
    }
    // Bits the result holds: 53 when normal, fewer when subnormal, whose
    // last bit is `2^-1074`.
    let precision = if top >= -1022 { 53 } else { top + 1075 };
    if precision < 0 {
        return 0.0;
    }
    let drop = 64 - precision as u32;
    let mut kept = normalized >> drop;
    let remainder = normalized & ((1u128 << drop) - 1);
    let half = 1u128 << (drop - 1);
    if remainder > half || (remainder == half && (sticky || kept & 1 == 1)) {
        kept += 1;
    }
    let last_bit = top - precision + 1;
    // `kept` is at most `2^53`, exact in `f64`; scaling by a power of two is
    // exact while the result is representable.
    (kept as f64) * power_of_two(last_bit)
}

/// `2^k` for `k` in `-1074..=1023`, built from its bits.
fn power_of_two(k: i64) -> f64 {
    if k >= -1022 {
        f64::from_bits(((k + 1023) as u64) << 52)
    } else {
        f64::from_bits(1u64 << (k + 1074))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> f64 {
        parse_hex_float(text).expect("hex float").0
    }

    #[test]
    fn hex_floats_read_as_strtod_reads_them() {
        assert_eq!(hex("0x1p3"), 8.0);
        assert_eq!(hex("0x1.8p1"), 3.0);
        assert_eq!(hex("0XFF"), 255.0);
        assert_eq!(hex("-0x.4P-2"), -0.0625);
        assert_eq!(hex("+0x10"), 16.0);
        assert_eq!(hex("0x1.fffffffffffffp1023"), f64::MAX);
        assert_eq!(hex("0x1p-1074"), f64::from_bits(1));
        assert_eq!(hex("0x1p-1022"), f64::MIN_POSITIVE);
        // Past 53 bits the value rounds to nearest, ties to even.
        assert_eq!(hex("0x1.00000000000008p0"), 1.0);
        assert_eq!(hex("0x1.00000000000018p0"), 1.0 + 2.0 * f64::EPSILON);
        assert_eq!(hex("0x1.000000000000080001p0"), 1.0 + f64::EPSILON);
        assert_eq!(hex("0x1p1024"), f64::INFINITY);
        assert_eq!(parse_hex_float("0x1p-1080"), Some((0.0, true)));
        assert_eq!(parse_hex_float("0x0p5"), Some((0.0, false)));
    }

    #[test]
    fn malformed_hex_floats_are_refused() {
        for text in [
            "0x", "0x.", "0x1p", "0x1p+", "0xg", "1p3", "0x1.2.3", "0x1 ",
        ] {
            assert_eq!(parse_hex_float(text), None, "{text:?}");
        }
    }
}
