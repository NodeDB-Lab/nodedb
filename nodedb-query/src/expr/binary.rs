//! Binary-operator evaluation on `Value` operands.

use nodedb_types::Value;

use crate::value_ops::{
    coerced_eq, compare_values, is_truthy, to_value_number, value_to_display_string, value_to_f64,
};

use super::types::BinaryOp;

pub(super) fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Value {
    match op {
        BinaryOp::Add => match (value_to_f64(left, true), value_to_f64(right, true)) {
            (Some(a), Some(b)) => to_value_number(a + b),
            _ => Value::Null,
        },
        BinaryOp::Sub => match (value_to_f64(left, true), value_to_f64(right, true)) {
            (Some(a), Some(b)) => to_value_number(a - b),
            _ => Value::Null,
        },
        BinaryOp::Mul => match (value_to_f64(left, true), value_to_f64(right, true)) {
            (Some(a), Some(b)) => to_value_number(a * b),
            _ => Value::Null,
        },
        BinaryOp::Div => match (value_to_f64(left, true), value_to_f64(right, true)) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    Value::Null
                } else {
                    to_value_number(a / b)
                }
            }
            _ => Value::Null,
        },
        BinaryOp::Mod => match (value_to_f64(left, true), value_to_f64(right, true)) {
            (Some(a), Some(b)) => {
                if b == 0.0 {
                    Value::Null
                } else {
                    to_value_number(a % b)
                }
            }
            _ => Value::Null,
        },
        BinaryOp::Concat => {
            let ls = value_to_display_string(left);
            let rs = value_to_display_string(right);
            Value::String(format!("{ls}{rs}"))
        }
        BinaryOp::Eq => Value::Bool(coerced_eq(left, right)),
        BinaryOp::NotEq => Value::Bool(!coerced_eq(left, right)),
        BinaryOp::Gt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater),
        BinaryOp::GtEq => {
            let c = compare_values(left, right);
            Value::Bool(c == std::cmp::Ordering::Greater || c == std::cmp::Ordering::Equal)
        }
        BinaryOp::Lt => Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less),
        BinaryOp::LtEq => {
            let c = compare_values(left, right);
            Value::Bool(c == std::cmp::Ordering::Less || c == std::cmp::Ordering::Equal)
        }
        BinaryOp::And => Value::Bool(is_truthy(left) && is_truthy(right)),
        BinaryOp::Or => Value::Bool(is_truthy(left) || is_truthy(right)),
    }
}
