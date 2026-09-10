// SPDX-License-Identifier: BUSL-1.1

//! Conversion helpers: `StoredCollection` → planner-facing catalog types.

use nodedb_sql::types::{ColumnInfo, EngineType, SqlDataType};
use nodedb_types::columnar::{FloatWidth, IntWidth};

/// Convert a StoredCollection to engine type, columns, and primary key.
pub(super) fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    // Declared numeric widths, resolved once per collection from the raw DDL
    // type strings the catalog records in `fields` for *every* engine.
    //
    // Strict and KV columns are typed by a resolved `ColumnType`, which
    // deliberately has one `Int64` variant for every declared integer width
    // and one `Float64` variant for every declared float width (nodedb stores
    // all integers as i64 and all floats as f64). `fields` is therefore the
    // only surviving record of what the author actually wrote, and it is
    // populated for strict/KV exactly as it is for schemaless/columnar — see
    // `ddl::neutral::collection::create::build`, which fills it from the raw
    // column list before the typed schema is built. Resolving from it here
    // keeps declared-width fidelity uniform across all engines without
    // widening any persisted structure.
    let declared_int = declared_widths(&stored.fields, IntWidth::from_declared_type);
    let declared_float = declared_widths(&stored.fields, FloatWidth::from_declared_type);

    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let columns = schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                    raw_type: None,
                    int_width: lookup_width(&declared_int, &c.name),
                    float_width: lookup_width(&declared_float, &c.name),
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            (EngineType::DocumentStrict, columns, pk)
        }

        CollectionType::Document(DocumentMode::Schemaless) => {
            // Schemaless collections normally key documents off the
            // built-in `id` field, but `CREATE COLLECTION` may have
            // declared an explicit `PRIMARY KEY` column instead (e.g.
            // `sku STRING PRIMARY KEY`); fall back to `id` when absent.
            let pk_name = stored
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string());
            // `stored.fields` carries every declared column by name, the pk
            // included, so its DEFAULT clause is recovered the same way the
            // columnar arm recovers one for its synthetic-pk field.
            let pk_default = stored
                .fields
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&pk_name))
                .and_then(|(_, type_str)| declared_default(type_str));
            let mut columns = vec![ColumnInfo {
                name: pk_name.clone(),
                data_type: SqlDataType::String,
                nullable: false,
                is_primary_key: true,
                default: pk_default,
                raw_type: None,
                int_width: None,
                float_width: None,
            }];
            // Add tracked fields from catalog.
            for (name, type_str) in &stored.fields {
                if name.eq_ignore_ascii_case(&pk_name) {
                    continue;
                }
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: declared_default(type_str),
                    raw_type: None,
                    int_width: IntWidth::from_declared_type(type_str),
                    float_width: FloatWidth::from_declared_type(type_str),
                });
            }
            (EngineType::DocumentSchemaless, columns, Some(pk_name))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                    raw_type: None,
                    int_width: lookup_width(&declared_int, &c.name),
                    float_width: lookup_width(&declared_float, &c.name),
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            (EngineType::KeyValue, columns, pk)
        }

        CollectionType::Columnar(profile) => {
            let engine = if profile.is_timeseries() {
                EngineType::Timeseries
            } else if profile.is_spatial() {
                EngineType::Spatial
            } else {
                EngineType::Columnar
            };
            let pk_name = "id";
            // If the DDL declared its own `id` field, the synthetic primary key
            // adopts that declared type and is client-supplied — an explicit
            // `id INT PRIMARY KEY` must stay INT rather than being dropped in
            // favor of a String surrogate (which would make every insert fail a
            // type check). With no declared `id`, synthesize a UUID_V7 String
            // surrogate primary key.
            let declared_pk = stored
                .fields
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(pk_name));
            let mut columns = Vec::new();
            if !profile.is_timeseries() {
                let (pk_type, pk_default, pk_raw) = match declared_pk {
                    // A declared `id` keeps whatever DEFAULT the DDL gave it.
                    // Dropping it here would accept the declaration and then
                    // ignore it on every insert — the column would read back
                    // empty with nothing to point at as the cause.
                    Some((_, type_str)) => (
                        parse_type_str(type_str),
                        declared_default(type_str),
                        Some(type_str.clone()),
                    ),
                    None => (SqlDataType::String, Some("UUID_V7".into()), None),
                };
                let pk_int_width = pk_raw.as_deref().and_then(IntWidth::from_declared_type);
                let pk_float_width = pk_raw.as_deref().and_then(FloatWidth::from_declared_type);
                columns.push(ColumnInfo {
                    name: pk_name.into(),
                    data_type: pk_type,
                    nullable: false,
                    is_primary_key: true,
                    default: pk_default,
                    raw_type: pk_raw,
                    int_width: pk_int_width,
                    float_width: pk_float_width,
                });
            }
            for (name, type_str) in &stored.fields {
                if !profile.is_timeseries() && name.eq_ignore_ascii_case(pk_name) {
                    continue;
                }
                columns.push(ColumnInfo {
                    name: name.clone(),
                    data_type: parse_type_str(type_str),
                    nullable: true,
                    is_primary_key: false,
                    default: declared_default(type_str),
                    raw_type: Some(type_str.clone()),
                    int_width: IntWidth::from_declared_type(type_str),
                    float_width: FloatWidth::from_declared_type(type_str),
                });
            }
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some(pk_name.into())
            };
            (engine, columns, pk)
        }
    }
}

/// Extract the `DEFAULT <expr>` clause a columnar-family column declared.
///
/// The columnar catalog stores each column as the raw DDL type string with its
/// modifiers still attached, so the default has to be recovered from that text.
/// It goes through the SAME parser the strict-document and key-value schema
/// builders use, so `DEFAULT concat('a', 'b')` delimits identically on every
/// engine rather than each one guessing where the expression ends.
fn declared_default(type_str: &str) -> Option<String> {
    let (_, _, _, default_expr) =
        nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full(type_str);
    default_expr
}

/// Resolve the declared width of every catalog field `resolve` recognizes,
/// keyed by column name.
///
/// Generic over the width family so the integer and float passes share one
/// implementation: `resolve` is `IntWidth::from_declared_type` or
/// `FloatWidth::from_declared_type`, each of which is the single source of
/// truth for its own keyword set.
///
/// Fields `resolve` does not recognize are dropped rather than stored as
/// `None`, so the result is usually empty and the common case costs one
/// allocation of zero capacity.
fn declared_widths<W>(
    fields: &[(String, String)],
    resolve: fn(&str) -> Option<W>,
) -> Vec<(&str, W)> {
    fields
        .iter()
        .filter_map(|(name, type_str)| resolve(type_str).map(|w| (name.as_str(), w)))
        .collect()
}

/// Look up a column's declared width by name, case-insensitively to match the
/// rest of this module's column-name comparisons.
///
/// `None` means either "not a column of this numeric family" or "the catalog
/// has no record of this column's declared type" — for example a column added
/// by `ALTER ADD COLUMN`, whose declared width was never recorded in `fields`.
/// Both degrade to the widest wire type of the family (`BIGINT` /
/// `double precision`), which is the only lossless fallback.
fn lookup_width<W: Copy>(widths: &[(&str, W)], column: &str) -> Option<W> {
    widths
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(column))
        .map(|(_, w)| *w)
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry | ColumnType::Json => SqlDataType::Bytes,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Timestamptz => SqlDataType::Timestamptz,
        ColumnType::Decimal { .. } => SqlDataType::Decimal,
        ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex | ColumnType::SparseVector => {
            SqlDataType::String
        }
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
            SqlDataType::Bytes
        }
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
        // ColumnType is #[non_exhaustive]; unknown types surface as Bytes
        // until the planner learns about them.
        _ => SqlDataType::Bytes,
    }
}

/// Resolve the declared SQL type of a catalog `fields` entry.
///
/// The catalog records the raw DDL text that followed the column name, so an
/// entry reads `INT DEFAULT 5` or `INT NOT NULL`, not `INT`.
/// [`nodedb_types::columnar::ColumnType::from_declared_type`] is the single
/// classifier both planes resolve that text through, so a trailing modifier
/// never changes the resolved type and the Data Plane cannot disagree about
/// which columns carry an instant.
///
/// A token that names no known type resolves to `SqlDataType::String`, the
/// widest rendering a catalog column can fall back to.
fn parse_type_str(s: &str) -> SqlDataType {
    match nodedb_types::columnar::ColumnType::from_declared_type(s) {
        Some(declared) => declared_column_type_to_sql(declared),
        None => SqlDataType::String,
    }
}

/// Map a declared column type onto the SQL type a `fields` column advertises.
///
/// Every type whose declared spelling and whose resolved strict/kv schema
/// type advertise the same SQL type defers to [`convert_column_type`], so the
/// two mappings cannot drift. The arms above that tail are the exceptions: a
/// schemaless or columnar-family column stores these as the text the client
/// wrote, not in the strict engine's binary encoding, so it renders as text.
fn declared_column_type_to_sql(declared: nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match declared {
        // A declared TIMESTAMPTZ column reads back in the naive timestamp
        // shape these engines store, so it advertises OID 1114.
        ColumnType::Timestamptz => SqlDataType::Timestamp,
        // Bitemporal system time is engine-assigned and renders as text.
        ColumnType::SystemTimestamp => SqlDataType::String,
        // Stored as the client's own text: WKT geometry, JSON text, a vector
        // or duration literal, and the collection literals.
        ColumnType::Geometry
        | ColumnType::Json
        | ColumnType::Vector(_)
        | ColumnType::Duration
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => SqlDataType::String,
        other => convert_column_type(&other),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::CollectionType;

    use super::{SqlDataType, convert_collection_type, parse_type_str};
    use crate::control::security::catalog::StoredCollection;

    /// The planner reads a cell as an instant for exactly the declared types
    /// `ColumnType::is_instant` names, which is the same predicate the Data
    /// Plane scales emission by. Pinning the equivalence here is what a
    /// comment could not do: a spelling added to one side and not the other
    /// fails this test instead of shipping a millisecond value labelled as
    /// microseconds.
    #[test]
    fn parse_type_str_reads_exactly_the_instant_declared_types_as_timestamps() {
        use nodedb_types::columnar::ColumnType;
        for declared in [
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "timestamp",
            "TIMESTAMP TIME_KEY",
            "TIMESTAMPTZ NOT NULL",
            "SYSTEM_TIMESTAMP",
            "BIGINT TIME_KEY",
            "INT",
            "TEXT",
            "GEOMETRY",
            "DECIMAL(10, 2)",
            "VECTOR(768)",
            "SOMETHING_ELSE",
            "",
        ] {
            let is_instant =
                ColumnType::from_declared_type(declared).is_some_and(|ty| ty.is_instant());
            assert_eq!(
                is_instant,
                parse_type_str(declared) == SqlDataType::Timestamp,
                "{declared}: the instant predicate and the planner type must agree"
            );
        }
    }

    /// `SMALLINT`/`INT2` are valid PostgreSQL wire-width integer keywords
    /// that must resolve to the same `SqlDataType::Int64` arm as
    /// `INT`/`INTEGER`/`INT4`/`INT8`/`BIGINT` — previously they were unlisted
    /// and fell through to the `_ => SqlDataType::String` default, which is
    /// what produced the wire OID 25 (text) bug for `SMALLINT` columns.
    #[test]
    fn parse_type_str_smallint_and_int2_map_to_int64() {
        assert_eq!(parse_type_str("SMALLINT"), SqlDataType::Int64);
        assert_eq!(parse_type_str("INT2"), SqlDataType::Int64);
        // Case-insensitivity, matching every other arm in this function.
        assert_eq!(parse_type_str("smallint"), SqlDataType::Int64);
        assert_eq!(parse_type_str("int2"), SqlDataType::Int64);
    }

    /// Every float spelling `FloatWidth::from_declared_type` recognizes must
    /// also resolve to `SqlDataType::Float64` here — `FLOAT4`/`FLOAT8` were
    /// rejected by DDL entirely, and a spelling this function does not list
    /// falls through to `_ => SqlDataType::String` and advertises OID 25.
    #[test]
    fn parse_type_str_maps_every_float_spelling_to_float64() {
        for declared in [
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "FLOAT32",
            "FLOAT64",
            "DOUBLE",
            "DOUBLE PRECISION",
            "REAL",
        ] {
            assert_eq!(
                parse_type_str(declared),
                SqlDataType::Float64,
                "{declared} must resolve to Float64"
            );
            assert!(
                nodedb_types::columnar::FloatWidth::from_declared_type(declared).is_some(),
                "{declared} must also resolve to a declared FloatWidth"
            );
        }
    }

    /// Declared float widths must be recovered for *every* engine, from the
    /// same `fields` entries the integer widths come from. A strict collection
    /// is the case that motivated this: its typed schema collapses `REAL` and
    /// `DOUBLE` to one `Float64` column type, so `fields` is the only record of
    /// what was declared.
    #[test]
    fn declared_float_widths_are_recovered_for_strict_columns() {
        use nodedb_types::columnar::{
            ColumnDef, ColumnType, DocumentMode, FloatWidth, StrictSchema,
        };

        let schema = StrictSchema::new(
            ["r", "d", "f"]
                .into_iter()
                .map(|name| ColumnDef::nullable(name, ColumnType::Float64))
                .collect(),
        )
        .expect("three nullable float columns are a valid strict schema");

        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = CollectionType::Document(DocumentMode::Strict(schema));
        stored.fields = vec![
            ("r".to_string(), "REAL".to_string()),
            ("d".to_string(), "DOUBLE".to_string()),
            ("f".to_string(), "FLOAT".to_string()),
        ];

        let (_, columns, _) = convert_collection_type(&stored);
        let width_of = |name: &str| {
            columns
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("column {name} must be present"))
                .float_width
        };
        assert_eq!(width_of("r"), Some(FloatWidth::F32));
        assert_eq!(width_of("d"), Some(FloatWidth::F64));
        // Bare FLOAT is double precision, not single.
        assert_eq!(width_of("f"), Some(FloatWidth::F64));
    }

    /// A columnar (or spatial, which shares the same non-timeseries
    /// synthetic-PK path) collection whose DDL declares an explicit
    /// `id` field must not surface two `id` columns to the planner —
    /// the synthetic primary-key column and the user-declared field
    /// must collapse into a single entry.
    fn assert_single_id_column(collection_type: CollectionType) {
        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = collection_type;
        stored.fields = vec![
            ("id".to_string(), "STRING".to_string()),
            ("ID".to_string(), "STRING".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];

        let (_, columns, _) = convert_collection_type(&stored);
        let id_count = columns
            .iter()
            .filter(|c| c.name.eq_ignore_ascii_case("id"))
            .count();
        assert_eq!(
            id_count,
            1,
            "expected exactly one `id` column, got: {:?}",
            columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn columnar_declared_id_field_does_not_duplicate_synthetic_pk() {
        assert_single_id_column(CollectionType::columnar());
    }

    #[test]
    fn spatial_declared_id_field_does_not_duplicate_synthetic_pk() {
        assert_single_id_column(CollectionType::spatial("geom"));
    }

    /// A columnar collection that declares an explicitly typed `id` primary
    /// key (`id INT PRIMARY KEY`) must surface that column with the declared
    /// type — not the String surrogate default. Collapsing it to String makes
    /// every integer insert fail a type check.
    #[test]
    fn declared_typed_id_pk_keeps_its_declared_type() {
        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = CollectionType::columnar();
        stored.fields = vec![
            ("id".to_string(), "INT".to_string()),
            ("v".to_string(), "INT".to_string()),
        ];

        let (_, columns, pk) = convert_collection_type(&stored);
        let id_col = columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case("id"))
            .expect("id column present");
        assert!(id_col.is_primary_key, "declared id must remain the pk");
        assert_eq!(
            id_col.data_type,
            SqlDataType::Int64,
            "declared `id INT` pk must keep its INT type, not the String surrogate"
        );
        assert!(
            id_col.default.is_none(),
            "a client-supplied typed id pk must not carry the UUID_V7 surrogate default"
        );
        assert_eq!(pk.as_deref(), Some("id"));
    }
}
