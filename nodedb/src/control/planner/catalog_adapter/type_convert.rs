// SPDX-License-Identifier: BUSL-1.1

//! Conversion helpers: `StoredCollection` → planner-facing catalog types.

use nodedb_sql::types::{ColumnInfo, EngineType, SqlDataType};
use nodedb_types::columnar::{FloatWidth, IntWidth};

/// The declared key column of a document collection, per
/// `nodedb_types::declared_key` over the primary key
/// [`convert_collection_type`] resolves. `None` for a collection keyed by the
/// implicit `id` or `_rowid`, and for every non-document collection: a KV row
/// names its key by its own rule, and a columnar row is not a sparse row.
///
/// The one source the Data-Plane register config and the Control-Plane write
/// admission both read, so a scan row and a write image name the identity
/// column alike.
pub(crate) fn document_declared_key(
    stored: &crate::control::security::catalog::StoredCollection,
) -> Option<String> {
    match &stored.collection_type {
        nodedb_types::CollectionType::Document(_) => {
            let (_, _, primary_key) = convert_collection_type(stored);
            nodedb_types::declared_key(primary_key.as_deref()).map(str::to_string)
        }
        nodedb_types::CollectionType::KeyValue(_) | nodedb_types::CollectionType::Columnar(_) => {
            None
        }
    }
}

/// Convert a StoredCollection to engine type, columns, and primary key.
pub(crate) fn convert_collection_type(
    stored: &crate::control::security::catalog::StoredCollection,
) -> (EngineType, Vec<ColumnInfo>, Option<String>) {
    use nodedb_types::CollectionType;
    use nodedb_types::columnar::DocumentMode;

    // Strict and KV columns take their declared numeric width from the typed
    // schema, the same width the Data Plane enforces on write. Schemaless and
    // columnar-family columns resolve it from the declared text in `fields`,
    // the text their write rule is built from.
    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let columns = schema.columns.iter().map(schema_column_info).collect();
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
                columns.push(declared_column_info(name, type_str));
            }
            (EngineType::DocumentSchemaless, columns, Some(pk_name))
        }

        CollectionType::KeyValue(config) => {
            let columns = config
                .schema
                .columns
                .iter()
                .map(schema_column_info)
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
            let pk_name = crate::control::planner::sql_plan_convert::dml::DEFAULT_IDENTITY_COLUMN;
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
                columns.push(declared_column_info(name, type_str));
            }
            // A declared natural key on a column other than the synthetic
            // surrogate (`sku TEXT PRIMARY KEY`, `declared_primary_key =
            // Some("sku")`) is the collection's real primary key, not `id`.
            // Timeseries never declares one (`timeseries/create.rs` always
            // records `None`), so this never fires for it.
            //
            // Design: a collection reports exactly one primary key, matching
            // every consumer of `CollectionInfo.primary_key` and `ColumnInfo
            // .is_primary_key` elsewhere in the planner, which read a single
            // answer (`Option<String>` / one flagged column), not a set. The
            // declared column is "the" key: its `is_primary_key` is set, and
            // the synthetic surrogate's is cleared so exactly one column
            // carries the flag, the same shape a document-schemaless
            // collection with a declared key already has.
            let declared_natural_key = stored
                .declared_primary_key
                .as_deref()
                .filter(|declared| !declared.eq_ignore_ascii_case(pk_name));
            let pk = if profile.is_timeseries() {
                None
            } else if let Some(declared) = declared_natural_key {
                if let Some(col) = columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(declared))
                {
                    col.is_primary_key = true;
                }
                if let Some(surrogate) = columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(pk_name))
                {
                    surrogate.is_primary_key = false;
                }
                Some(declared.to_string())
            } else {
                Some(pk_name.into())
            };
            (engine, columns, pk)
        }
    }
}

/// The planner-facing column a raw DDL declaration (`name`, `type_str`)
/// resolves to: its SQL type, declared numeric width, DEFAULT text, and the
/// declared text itself.
///
/// `type_str` is the text that followed the column name in the DDL, modifiers
/// included (`SMALLINT DEFAULT 5`, `TIMESTAMP TIME_KEY`). The schemaless and
/// columnar-family catalog arms read their tracked fields through this, and
/// the DDL gate checks a declared DEFAULT against the same resolution, so a
/// default is judged against exactly the type its column will carry at
/// INSERT time.
pub(crate) fn declared_column_info(name: &str, type_str: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: parse_type_str(type_str),
        nullable: true,
        is_primary_key: false,
        default: declared_default(type_str),
        raw_type: Some(type_str.to_string()),
        int_width: IntWidth::from_declared_type(type_str),
        float_width: FloatWidth::from_declared_type(type_str),
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

/// The planner-facing column a typed strict or KV schema column is.
///
/// The declared numeric width comes from the schema column, the width the
/// Data Plane enforces on write. An absent width is the widest wire type of
/// its family (`BIGINT` / `double precision`).
fn schema_column_info(column: &nodedb_types::columnar::ColumnDef) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        data_type: convert_column_type(&column.column_type),
        nullable: column.nullable,
        is_primary_key: column.primary_key,
        default: column.default.clone(),
        raw_type: None,
        int_width: column.int_width,
        float_width: column.float_width,
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes => SqlDataType::Bytes,
        // A structured column reads back as its JSON text.
        ColumnType::Json
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => SqlDataType::Json,
        ColumnType::Geometry => SqlDataType::Geometry,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Timestamptz => SqlDataType::Timestamptz,
        ColumnType::Decimal(typmod) => SqlDataType::Decimal(*typmod),
        ColumnType::Uuid => SqlDataType::Uuid,
        ColumnType::Ulid | ColumnType::Regex | ColumnType::SparseVector => SqlDataType::String,
        ColumnType::Duration => SqlDataType::Int64,
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

    /// A declared `DECIMAL(p,s)` field carries its typmod to the planner, so
    /// the schemaless and key-value write paths fit values to it.
    #[test]
    fn parse_type_str_keeps_the_decimal_typmod() {
        let typmod = nodedb_types::columnar::DecimalTypmod::new(5, 2).expect("valid typmod");
        assert_eq!(
            parse_type_str("DECIMAL(5, 2) NOT NULL"),
            SqlDataType::Decimal(Some(typmod))
        );
        assert_eq!(
            parse_type_str("NUMERIC(5,2)"),
            SqlDataType::Decimal(Some(typmod))
        );
        assert_eq!(parse_type_str("DECIMAL"), SqlDataType::Decimal(None));
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

    /// A strict column advertises the numeric width its schema column
    /// declares, the width the Data Plane enforces on write. The catalog
    /// `fields` text plays no part.
    #[test]
    fn strict_columns_take_their_width_from_the_schema() {
        use nodedb_types::columnar::{
            ColumnDef, ColumnType, DocumentMode, FloatWidth, IntWidth, StrictSchema,
        };

        let schema = StrictSchema::new(vec![
            ColumnDef::nullable("r", ColumnType::Float64).with_declared_width("REAL"),
            ColumnDef::nullable("d", ColumnType::Float64).with_declared_width("DOUBLE"),
            ColumnDef::nullable("f", ColumnType::Float64).with_declared_width("FLOAT"),
            ColumnDef::nullable("s", ColumnType::Int64).with_declared_width("SMALLINT"),
        ])
        .expect("nullable numeric columns are a valid strict schema");

        let mut stored = StoredCollection::new(1, "coll", "owner");
        stored.collection_type = CollectionType::Document(DocumentMode::Strict(schema));

        let (_, columns, _) = convert_collection_type(&stored);
        let width_of = |name: &str| {
            let column = columns
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("column {name} must be present"));
            (column.int_width, column.float_width)
        };
        assert_eq!(width_of("r"), (None, Some(FloatWidth::F32)));
        assert_eq!(width_of("d"), (None, Some(FloatWidth::F64)));
        // Bare FLOAT is double precision, not single.
        assert_eq!(width_of("f"), (None, Some(FloatWidth::F64)));
        assert_eq!(width_of("s"), (Some(IntWidth::I16), None));
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

    /// A strict or KV structured column is `Json`, so it advertises `json`
    /// and reads back as JSON text. A `BYTEA` column stays `Bytes`.
    #[test]
    fn structured_schema_columns_are_json() {
        use nodedb_types::columnar::ColumnType;
        for structured in [
            ColumnType::Json,
            ColumnType::Array,
            ColumnType::Set,
            ColumnType::Range,
            ColumnType::Record,
        ] {
            assert_eq!(
                super::convert_column_type(&structured),
                SqlDataType::Json,
                "{structured}"
            );
        }
        assert_eq!(
            super::convert_column_type(&ColumnType::Bytes),
            SqlDataType::Bytes
        );
    }
}
