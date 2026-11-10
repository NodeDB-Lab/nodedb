// SPDX-License-Identifier: Apache-2.0

//! An object literal as written: values, and `$N` parameter references
//! that a prepared statement binds later.
//!
//! The scanner reads a literal into this tree. A caller that binds no
//! parameters converts it to plain values with [`Literal::into_value`],
//! which refuses a parameter reference. The `INSERT ... { }` rewrite keeps
//! each reference as `$N` in the `VALUES` it builds, so the statement's
//! parameter binder binds it typed, exactly as in a `VALUES` clause.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::error::SqlError;

/// One object-literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// A value written in the literal.
    Value(Value),
    /// A `$N` parameter reference, 1-based.
    Param(usize),
    /// An array whose elements may hold parameter references.
    Array(Vec<Literal>),
    /// A nested object whose values may hold parameter references.
    Object(HashMap<String, Literal>),
}

impl Literal {
    /// The plain value, or a refusal naming the first parameter reference.
    pub fn into_value(self) -> Result<Value, SqlError> {
        match self {
            Self::Value(value) => Ok(value),
            Self::Param(n) => Err(unbound(n)),
            Self::Array(items) => items
                .into_iter()
                .map(Literal::into_value)
                .collect::<Result<_, _>>()
                .map(Value::Array),
            Self::Object(fields) => into_value_map(fields).map(Value::Object),
        }
    }

    /// The parameter reference this value holds at any depth, if any.
    pub fn first_param(&self) -> Option<usize> {
        match self {
            Self::Value(_) => None,
            Self::Param(n) => Some(*n),
            Self::Array(items) => items.iter().find_map(Literal::first_param),
            Self::Object(fields) => fields.values().find_map(Literal::first_param),
        }
    }
}

/// The plain values of a parsed object, refusing a parameter reference.
pub fn into_value_map(
    fields: HashMap<String, Literal>,
) -> Result<HashMap<String, Value>, SqlError> {
    fields
        .into_iter()
        .map(|(key, value)| value.into_value().map(|value| (key, value)))
        .collect()
}

fn unbound(n: usize) -> SqlError {
    SqlError::Parse {
        detail: format!(
            "parameter ${n} in an object literal has no bound value here; \
             bind it in a prepared INSERT or UPSERT"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parameter_reference_has_no_plain_value() {
        let literal = Literal::Array(vec![Literal::Value(Value::Integer(1)), Literal::Param(2)]);
        assert_eq!(literal.first_param(), Some(2));
        assert!(literal.into_value().is_err());
        assert_eq!(
            Literal::Value(Value::Integer(1)).into_value(),
            Ok(Value::Integer(1))
        );
    }
}
