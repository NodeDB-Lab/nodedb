// SPDX-License-Identifier: BUSL-1.1

//! PostgreSQL compatibility catalogs used by driver and ORM introspection.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog::oid::stable_collection_oid;
use crate::control::state::SharedState;

use super::collections::{encode_row, load_collections};

/// Column defaults, one row for each declared `DEFAULT` expression.
pub fn pg_attrdef(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    for coll in load_collections(state, identity) {
        let rel_oid = stable_collection_oid(coll.tenant_id, &coll.name);
        for (index, (_, type_decl)) in coll.fields.iter().enumerate() {
            let (_, _, _, default_expr) =
                nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full(type_decl);
            let Some(default_expr) = default_expr else {
                continue;
            };
            let attnum = (index + 1) as i64;
            let mut row = HashMap::with_capacity(4);
            row.insert(
                "oid".into(),
                Value::Integer(rel_oid.wrapping_mul(1024).wrapping_add(attnum)),
            );
            row.insert("adrelid".into(), Value::Integer(rel_oid));
            row.insert("adnum".into(), Value::Integer(attnum));
            row.insert("adbin".into(), Value::String(default_expr));
            rows.push(encode_row(row)?);
        }
    }
    Ok(rows)
}

/// NodeDB currently exposes no PostgreSQL range types. The relation still
/// exists so type-map bootstrap queries can LEFT JOIN it.
pub fn pg_range() -> crate::Result<Vec<Vec<u8>>> {
    Ok(Vec::new())
}

/// PostgreSQL's built-in database-default collation. Collatable types and
/// columns reference this row by OID 100; non-collatable types use OID 0.
pub fn pg_collation() -> crate::Result<Vec<Vec<u8>>> {
    let mut row = HashMap::with_capacity(2);
    row.insert("oid".into(), Value::Integer(100));
    row.insert("collname".into(), Value::String("default".into()));
    Ok(vec![encode_row(row)?])
}
