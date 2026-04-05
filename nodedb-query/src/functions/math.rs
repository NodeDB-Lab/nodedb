//! Math scalar functions.

use super::shared::num_arg;
use crate::json_ops::to_json_number;

pub(super) fn try_eval(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let v = match name {
        "abs" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.abs())),
        "round" => {
            let Some(n) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let decimals = num_arg(args, 1).unwrap_or(0.0) as u32;
            let mode_str = super::shared::str_arg(args, 2).unwrap_or_default();
            let strategy = match mode_str.to_uppercase().as_str() {
                "HALF_UP" => rust_decimal::RoundingStrategy::MidpointAwayFromZero,
                "HALF_DOWN" => rust_decimal::RoundingStrategy::MidpointTowardZero,
                "TRUNCATE" | "TRUNC" => rust_decimal::RoundingStrategy::ToZero,
                "CEILING" | "CEIL" => rust_decimal::RoundingStrategy::AwayFromZero,
                "FLOOR" => rust_decimal::RoundingStrategy::ToNegativeInfinity,
                _ => rust_decimal::RoundingStrategy::MidpointNearestEven, // HALF_EVEN default
            };
            // Use Decimal path for exact rounding.
            match rust_decimal::Decimal::try_from(n) {
                Ok(d) => {
                    let rounded = d.round_dp_with_strategy(decimals, strategy);
                    // Convert back to f64 for JSON compatibility.
                    use rust_decimal::prelude::ToPrimitive;
                    rounded
                        .to_f64()
                        .map_or(serde_json::Value::Null, to_json_number)
                }
                Err(_) => serde_json::Value::Null,
            }
        }
        "ceil" | "ceiling" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ceil()))
        }
        "floor" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.floor())),
        "power" | "pow" => {
            let Some(base) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let exp = num_arg(args, 1).unwrap_or(1.0);
            to_json_number(base.powf(exp))
        }
        "sqrt" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.sqrt())),
        "mod" => {
            let Some(a) = num_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let b = num_arg(args, 1).unwrap_or(1.0);
            if b == 0.0 {
                serde_json::Value::Null
            } else {
                to_json_number(a % b)
            }
        }
        "sign" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.signum())),
        "log" | "ln" => {
            num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.ln()))
        }
        "log10" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log10())),
        "log2" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.log2())),
        "exp" => num_arg(args, 0).map_or(serde_json::Value::Null, |n| to_json_number(n.exp())),
        _ => return None,
    };
    Some(v)
}
