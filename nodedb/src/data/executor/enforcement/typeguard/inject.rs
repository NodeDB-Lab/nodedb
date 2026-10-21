// SPDX-License-Identifier: BUSL-1.1

//! Typeguard `DEFAULT` / `VALUE` injection into the fields one write stores.
//!
//! - **DEFAULT** fills a field that is absent or null.
//! - **VALUE** writes the field on every write, overwriting a supplied value.
//!
//! Both expressions are evaluated per row against the document, so a
//! cross-field reference (`LOWER(REPLACE(title, ' ', '-'))`) reads the
//! document's own fields.

use std::collections::HashMap;

use nodedb_types::Value;

use super::compiled::CompiledTypeGuard;
use crate::bridge::envelope::ErrorCode;

/// Inject each guard's DEFAULT and VALUE into `fields`.
///
/// Returns the names of the fields it wrote. An evaluation error (division by
/// zero, an unknown function) keeps its own SQLSTATE, not the guard's
/// violation code, matching generated-column enforcement.
pub fn inject_defaults(
    guards: &[CompiledTypeGuard],
    fields: &mut HashMap<String, Value>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<Vec<String>, ErrorCode> {
    let mut injected = Vec::new();
    for guard in guards {
        let clauses = guard.clauses()?;
        let expr = if let Some(value) = clauses.value.as_ref() {
            value
        } else if let Some(default) = clauses.default.as_ref() {
            let absent = fields
                .get(&guard.field)
                .is_none_or(|v| matches!(v, Value::Null));
            if !absent {
                continue;
            }
            default
        } else {
            continue;
        };
        let doc = Value::Object(fields.clone());
        let value = expr.eval(&doc, crs).map_err(ErrorCode::from)?;
        fields.insert(guard.field.clone(), value);
        injected.push(guard.field.clone());
    }
    Ok(injected)
}
