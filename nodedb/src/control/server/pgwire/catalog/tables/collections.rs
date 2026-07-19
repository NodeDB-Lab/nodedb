// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for catalog row producers: collection loading, field-type →
//! OID mapping, and msgpack row encoding.

use std::collections::HashMap;

use nodedb_types::DatabaseId;
use nodedb_types::Value;
use nodedb_types::columnar::ColumnType;

use crate::control::security::catalog::types::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

/// Encode one catalog row (column-name → value) as msgpack `Value::Object`
/// bytes. The map keys must be the relation's schema column names, matching
/// the order declared in `super::super::schema::catalog_columns`.
pub fn encode_row(row: HashMap<String, Value>) -> crate::Result<Vec<u8>> {
    nodedb_types::value_to_msgpack(&Value::Object(row)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: e.to_string(),
    })
}

/// Load the collections visible to `identity` (all active collections for a
/// superuser, tenant-scoped otherwise).
pub fn load_collections(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Vec<StoredCollection> {
    let catalog = state.credentials.catalog();
    if identity.is_superuser {
        catalog
            .load_all_collections(DatabaseId::DEFAULT)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.is_active)
            .collect()
    } else {
        catalog
            .load_collections_for_tenant(DatabaseId::DEFAULT, identity.tenant_id.as_u64())
            .unwrap_or_default()
    }
}

/// True if the collection has at least one secondary index (drives
/// `pg_class.relhasindex`, consistent with what `pg_index` reports).
pub fn has_secondary_index(coll: &StoredCollection) -> bool {
    !coll.indexes.is_empty()
}

pub fn field_type_to_oid(field_type: &str) -> i64 {
    let normalized = field_type.trim().to_ascii_lowercase();
    let starts_with_type = |name: &str| {
        normalized == name
            || normalized.strip_prefix(name).is_some_and(|rest| {
                rest.starts_with('(') || rest.chars().next().is_some_and(char::is_whitespace)
            })
    };

    if starts_with_type("timestamp with time zone") || starts_with_type("timestamptz") {
        1184
    } else if starts_with_type("timestamp without time zone") || starts_with_type("timestamp") {
        1114
    } else if starts_with_type("time without time zone") || starts_with_type("time") {
        1083
    } else if starts_with_type("character varying") || starts_with_type("varchar") {
        1043
    } else if starts_with_type("double precision")
        || starts_with_type("double")
        || starts_with_type("float8")
    {
        701
    } else if starts_with_type("integer") || starts_with_type("int4") || starts_with_type("int") {
        23
    } else if starts_with_type("smallint") || starts_with_type("int2") {
        21
    } else if starts_with_type("bigint") || starts_with_type("int8") {
        20
    } else if starts_with_type("float") || starts_with_type("float4") || starts_with_type("real") {
        700
    } else if starts_with_type("bool") || starts_with_type("boolean") {
        16
    } else if starts_with_type("text") {
        25
    } else if starts_with_type("date") {
        1082
    } else if starts_with_type("uuid") {
        2950
    } else if starts_with_type("jsonb") {
        3802
    } else if starts_with_type("json") {
        114
    } else {
        field_type
            .parse::<ColumnType>()
            .map_or(25, |column_type| column_type.to_pg_oid() as i64)
    }
}

pub fn type_oid_is_collatable(oid: i64) -> bool {
    matches!(oid, 18 | 19 | 25 | 1042 | 1043)
}
