// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL's result-scale rules for numeric `sqrt`, `exp`, `ln`, `log`
//! and `power`, and the estimates they read.
//!
//! PostgreSQL stores a numeric as base-10000 digit groups with the weight
//! of the first group, and estimates a result's decimal weight from those.
//! These functions rebuild the same groups from a `Decimal`.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// The fewest significant digits a result keeps.
pub(super) const MIN_SIG_DIGITS: i64 = 16;
/// The largest scale a result takes.
pub(super) const MAX_DISPLAY_SCALE: i64 = 1000;
/// The bound `exp` and `power` clamp an estimated weight to.
pub(super) const MAX_RESULT_SCALE: i64 = 2000;
/// `log10(e)`, truncated as PostgreSQL spells it. Scale estimates must
/// match PostgreSQL's digit for digit, so `std::f64::consts::LOG10_E` is
/// not a substitute.
#[expect(
    clippy::approx_constant,
    reason = "PostgreSQL's truncated constant drives its scale estimates"
)]
pub(super) const LOG10_E: f64 = 0.434294481903252;

/// The base-10000 groups of `|d|` and the weight of the first, trailing
/// zero groups dropped. Zero is weight 0 with no groups.
pub(super) fn base_groups(d: Decimal) -> (i64, Vec<u32>) {
    let mantissa = d.mantissa().unsigned_abs();
    if mantissa == 0 {
        return (0, Vec::new());
    }
    let digits = mantissa.to_string();
    let lead = digits.len() as i64 - 1 - i64::from(d.scale());
    let weight = lead.div_euclid(4);
    let last = (lead - (digits.len() as i64 - 1)).div_euclid(4);
    let mut groups = vec![0u32; (weight - last + 1) as usize];
    for (i, b) in digits.bytes().enumerate() {
        let exponent = lead - i as i64;
        let slot = (weight - exponent.div_euclid(4)) as usize;
        groups[slot] += u32::from(b - b'0') * 10u32.pow(exponent.rem_euclid(4) as u32);
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    (weight, groups)
}

/// The decimal exponent of the leading digit of nonzero `d`.
fn leading_exponent(d: Decimal) -> i64 {
    d.mantissa().unsigned_abs().to_string().len() as i64 - 1 - i64::from(d.scale())
}

/// `sqrt`'s result scale for `x`: 16 significant digits for the root's
/// weight, and never less than `x`'s scale.
pub(super) fn sqrt_scale(x: Decimal) -> i64 {
    let (weight, _) = base_groups(x);
    let root_weight = (weight + 1) * 4 / 2 - 1;
    clamp(MIN_SIG_DIGITS - root_weight, &[x])
}

/// `exp`'s result scale for `x`.
pub(super) fn exp_scale(x: Decimal) -> i64 {
    let weight = (x.to_f64().unwrap_or(0.0) * LOG10_E)
        .clamp(-(MAX_RESULT_SCALE as f64), MAX_RESULT_SCALE as f64);
    clamp(MIN_SIG_DIGITS - weight as i64, &[x])
}

/// PostgreSQL's `estimate_ln_dweight`: the decimal weight of `ln(x)` for
/// positive `x`. Zero for any other `x`.
pub(super) fn estimate_ln_dweight(x: Decimal) -> i64 {
    if !x.is_sign_positive() || x.is_zero() {
        return 0;
    }
    let near_one = Decimal::new(9, 1) <= x && x <= Decimal::new(11, 1);
    if near_one {
        // ln(1 + t) is about t, whose weight is the weight of `x - 1`.
        let t = x - Decimal::ONE;
        return if t.is_zero() { 0 } else { leading_exponent(t) };
    }
    let (weight, groups) = base_groups(x);
    let mut digits = f64::from(groups[0]);
    let mut dweight = weight * 4;
    if let Some(&next) = groups.get(1) {
        digits = digits * 10000.0 + f64::from(next);
        dweight -= 4;
    }
    let ln = digits.ln() + dweight as f64 * std::f64::consts::LN_10;
    ln.abs().log10() as i64
}

/// `ln`'s result scale for `x`.
pub(super) fn ln_scale(x: Decimal) -> i64 {
    clamp(MIN_SIG_DIGITS - estimate_ln_dweight(x), &[x])
}

/// `log(base, x)`'s result scale.
pub(super) fn log_scale(base: Decimal, x: Decimal) -> i64 {
    let weight = estimate_ln_dweight(x) - estimate_ln_dweight(base);
    clamp(MIN_SIG_DIGITS - weight, &[base, x])
}

/// The decimal weight of `base^exp` for an integer `exp`, as PostgreSQL's
/// `power_var_int` estimates it from up to 16 digits of `base`.
pub(super) fn int_power_weight(base: Decimal, exp: i64) -> f64 {
    let (weight, groups) = base_groups(base);
    let Some(&first) = groups.first() else {
        return 0.0;
    };
    let mut f = f64::from(first);
    let mut p = weight * 4;
    for &group in groups.iter().skip(1).take(3) {
        f = f * 10000.0 + f64::from(group);
        p -= 4;
    }
    exp as f64 * (f.log10() + p as f64)
}

/// `rscale` at least every input's scale, within `0..=1000`.
pub(super) fn clamp(rscale: i64, inputs: &[Decimal]) -> i64 {
    inputs
        .iter()
        .fold(rscale, |r, d| r.max(i64::from(d.scale())))
        .clamp(0, MAX_DISPLAY_SCALE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str_exact(text).expect("decimal")
    }

    #[test]
    fn groups_match_postgres_storage() {
        assert_eq!(base_groups(dec("16.0")), (0, vec![16]));
        assert_eq!(base_groups(dec("12345.678")), (1, vec![1, 2345, 6780]));
        assert_eq!(base_groups(dec("0.5")), (-1, vec![5000]));
        assert_eq!(base_groups(dec("0.00001")), (-2, vec![1000]));
        assert_eq!(base_groups(dec("100000000")), (2, vec![1]));
        assert_eq!(base_groups(Decimal::ZERO), (0, vec![]));
    }

    /// The scales PostgreSQL gives: `sqrt(16.0)` is `4.000000000000000`,
    /// `exp(1.0)` `2.7182818284590452`, `ln(2.0)` `0.6931471805599453`,
    /// `log(10, 100)` `2.0000000000000000`.
    #[test]
    fn result_scales_match_postgres() {
        assert_eq!(sqrt_scale(dec("16.0")), 15);
        assert_eq!(sqrt_scale(dec("2")), 15);
        assert_eq!(sqrt_scale(dec("1000000")), 13);
        assert_eq!(exp_scale(dec("1.0")), 16);
        assert_eq!(exp_scale(dec("50")), 0);
        assert_eq!(ln_scale(dec("2.0")), 16);
        assert_eq!(ln_scale(dec("1.05")), 18);
        assert_eq!(log_scale(dec("10"), dec("100")), 16);
    }
}
