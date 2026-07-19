// SPDX-License-Identifier: BUSL-1.1

//! `pg_type` row producer and the canonical built-in type table.

use std::collections::HashMap;

use nodedb_types::Value;

use super::collections::{encode_row, type_oid_is_collatable};

/// One built-in PostgreSQL type row.
struct PgTypeRow {
    oid: i64,
    name: &'static str,
    len: i32,
    byval: bool,
    /// `b` = base, `A`-category arrays are also `b` in `typtype`.
    typtype: &'static str,
    /// `typcategory`: N numeric, S string, B boolean, D datetime, T timespan,
    /// U user/other, A array.
    category: &'static str,
    /// Element type OID (0 for scalars; the base type OID for arrays).
    elem: i64,
    /// Array type OID whose element is this type (0 for array types).
    array: i64,
}

/// Canonical built-in types (base types followed by their array types). OIDs
/// match PostgreSQL so `::regtype` and driver type caches interoperate.
const TYPES: &[PgTypeRow] = &[
    row(16, "bool", 1, true, "B", 0, 1000),
    row(17, "bytea", -1, false, "U", 0, 1001),
    row(18, "char", 1, true, "Z", 0, 1002),
    row(19, "name", 64, false, "S", 0, 1003),
    row(20, "int8", 8, true, "N", 0, 1016),
    row(21, "int2", 2, true, "N", 0, 1005),
    row(23, "int4", 4, true, "N", 0, 1007),
    row(25, "text", -1, false, "S", 0, 1009),
    row(26, "oid", 4, true, "N", 0, 1028),
    row(114, "json", -1, false, "U", 0, 199),
    row(700, "float4", 4, true, "N", 0, 1021),
    row(701, "float8", 8, true, "N", 0, 1022),
    row(1042, "bpchar", -1, false, "S", 0, 1014),
    row(1043, "varchar", -1, false, "S", 0, 1015),
    row(1082, "date", 4, true, "D", 0, 1182),
    row(1083, "time", 8, true, "D", 0, 1183),
    row(1114, "timestamp", 8, true, "D", 0, 1115),
    row(1184, "timestamptz", 8, true, "D", 0, 1185),
    row(1186, "interval", 16, false, "T", 0, 1187),
    row(1700, "numeric", -1, false, "N", 0, 1231),
    row(2950, "uuid", 16, false, "U", 0, 2951),
    row(3802, "jsonb", -1, false, "U", 0, 3807),
    // Array types.
    row(1000, "_bool", -1, false, "A", 16, 0),
    row(1001, "_bytea", -1, false, "A", 17, 0),
    row(1002, "_char", -1, false, "A", 18, 0),
    row(1003, "_name", -1, false, "A", 19, 0),
    row(1005, "_int2", -1, false, "A", 21, 0),
    row(1007, "_int4", -1, false, "A", 23, 0),
    row(1009, "_text", -1, false, "A", 25, 0),
    row(1016, "_int8", -1, false, "A", 20, 0),
    row(1028, "_oid", -1, false, "A", 26, 0),
    row(199, "_json", -1, false, "A", 114, 0),
    row(1021, "_float4", -1, false, "A", 700, 0),
    row(1022, "_float8", -1, false, "A", 701, 0),
    row(1014, "_bpchar", -1, false, "A", 1042, 0),
    row(1015, "_varchar", -1, false, "A", 1043, 0),
    row(1182, "_date", -1, false, "A", 1082, 0),
    row(1183, "_time", -1, false, "A", 1083, 0),
    row(1115, "_timestamp", -1, false, "A", 1114, 0),
    row(1185, "_timestamptz", -1, false, "A", 1184, 0),
    row(1187, "_interval", -1, false, "A", 1186, 0),
    row(1231, "_numeric", -1, false, "A", 1700, 0),
    row(2951, "_uuid", -1, false, "A", 2950, 0),
    row(3807, "_jsonb", -1, false, "A", 3802, 0),
];

const fn row(
    oid: i64,
    name: &'static str,
    len: i32,
    byval: bool,
    category: &'static str,
    elem: i64,
    array: i64,
) -> PgTypeRow {
    PgTypeRow {
        oid,
        name,
        len,
        byval,
        typtype: "b",
        category,
        elem,
        array,
    }
}

pub fn pg_type() -> crate::Result<Vec<Vec<u8>>> {
    let mut rows = Vec::with_capacity(TYPES.len());
    for r in TYPES {
        let mut m: HashMap<String, Value> = HashMap::with_capacity(14);
        m.insert("oid".into(), Value::Integer(r.oid));
        m.insert("typname".into(), Value::String(r.name.into()));
        m.insert("typnamespace".into(), Value::Integer(11));
        m.insert("typlen".into(), Value::Integer(r.len as i64));
        m.insert("typbyval".into(), Value::Bool(r.byval));
        m.insert("typtype".into(), Value::String(r.typtype.into()));
        m.insert("typcategory".into(), Value::String(r.category.into()));
        m.insert("typispreferred".into(), Value::Bool(false));
        m.insert("typisdefined".into(), Value::Bool(true));
        m.insert("typdelim".into(), Value::String(",".into()));
        m.insert("typrelid".into(), Value::Integer(0));
        m.insert("typelem".into(), Value::Integer(r.elem));
        m.insert("typarray".into(), Value::Integer(r.array));
        m.insert("typnotnull".into(), Value::Bool(false));
        m.insert(
            "typinput".into(),
            Value::String(if r.elem == 0 {
                format!("{}in", r.name)
            } else {
                "array_in".into()
            }),
        );
        m.insert("typbasetype".into(), Value::Integer(0));
        m.insert(
            "typcollation".into(),
            Value::Integer(if type_oid_is_collatable(r.oid) {
                100
            } else {
                0
            }),
        );
        rows.push(encode_row(m)?);
    }
    Ok(rows)
}

/// Name → OID map for `::regtype` resolution, including common aliases.
pub fn type_oid_map() -> HashMap<String, i64> {
    let mut m: HashMap<String, i64> = TYPES.iter().map(|r| (r.name.to_string(), r.oid)).collect();
    for (alias, oid) in [
        ("integer", 23),
        ("int", 23),
        ("bigint", 20),
        ("smallint", 21),
        ("boolean", 16),
        ("real", 700),
        ("float", 701),
        ("double precision", 701),
        ("double", 701),
        ("character varying", 1043),
        ("character", 1042),
        ("timestamp without time zone", 1114),
        ("timestamp with time zone", 1184),
        ("time without time zone", 1083),
    ] {
        m.insert(alias.to_string(), oid);
    }
    m
}
