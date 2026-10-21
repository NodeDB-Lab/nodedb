// SPDX-License-Identifier: BUSL-1.1

//! Column types of the relations a `WITH` clause or a derived table names.
//!
//! The outer query of a CTE plan reads each definition by its name, and the
//! stored catalog holds no relation under that name. [`CteTypesCatalog`]
//! answers for those names with the columns the definition body's own output
//! schema announces, and passes every other name to the stored catalog. A
//! column read through a CTE or a derived table then carries the type its
//! body gives it, exactly as a plain `SELECT` of the body does.

use nodedb_sql::SqlError;
use nodedb_sql::catalog::{ArrayCatalogView, SqlCatalog, SqlCatalogError};
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types::{CollectionInfo, ColumnInfo, SqlPlan};
use nodedb_sql::types_expr::SqlDataType;
use nodedb_types::columnar::{FloatWidth, IntWidth};

use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::response_shape::types::DdlColType;

use super::build::build_output_schema;

/// A borrowed catalog of any type, held as a sized value.
///
/// Schema derivation recurses through nested CTE plans. Each level reads its
/// catalog as `&dyn SqlCatalog`, so the recursion instantiates a finite set
/// of generic functions. Schema derivation reads relations and arrays, so
/// those lookups are passed through.
pub(super) struct CatalogRef<'a, C: ?Sized>(pub(super) &'a C);

impl<C: SqlCatalog + ?Sized> SqlCatalog for CatalogRef<'_, C> {
    fn get_collection(
        &self,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        self.0.get_collection(database_id, name)
    }

    fn resolve_relation(
        &self,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        self.0.resolve_relation(database_id, name)
    }

    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        self.0.lookup_array(name)
    }

    fn crs_context(&self) -> nodedb_crs::CrsContext<'_> {
        self.0.crs_context()
    }
}

/// A catalog that answers for each CTE definition name first.
///
/// A definition name shadows a stored collection of the same name, as it
/// does in SQL. Every other name goes to the stored catalog.
pub(super) struct CteTypesCatalog<'a> {
    inner: &'a dyn SqlCatalog,
    relations: Vec<(String, CollectionInfo)>,
    /// Each definition's body output schema, by definition name.
    bodies: Vec<(String, OutputSchema)>,
}

impl<'a> CteTypesCatalog<'a> {
    /// Types each definition from its body's output schema, in order.
    ///
    /// A definition reads the definitions before it, so a later `WITH` entry
    /// that selects from an earlier one carries its types too. A catalog
    /// lookup error is returned.
    pub(super) fn build(
        inner: &'a dyn SqlCatalog,
        definitions: &[(String, SqlPlan)],
        database_id: nodedb_types::DatabaseId,
    ) -> Result<Self, SqlError> {
        let mut overlay = Self {
            inner,
            relations: Vec::with_capacity(definitions.len()),
            bodies: Vec::with_capacity(definitions.len()),
        };
        for (name, body) in definitions {
            let schema =
                build_output_schema(std::slice::from_ref(body), &overlay, database_id, None)?;
            let schema = overlay.read_through(body, schema);
            overlay
                .relations
                .push((name.clone(), relation_from_schema(name, &schema)));
            overlay.bodies.push((name.clone(), schema));
        }
        Ok(overlay)
    }

    /// `schema`, the schema of `plan`, with each column reading the key the
    /// rows carry.
    ///
    /// A plan that reads a definition body unchanged runs as that body, so
    /// its rows are the body's rows. A body column can be announced under its
    /// display name but carried under another lookup key: a join body
    /// carries `o.id` under the qualified `o.id` and announces it as `id`.
    /// Each column that names a body column by display name reads that body
    /// column's lookup key. Any other plan returns `schema` unchanged.
    pub(super) fn read_through(&self, plan: &SqlPlan, mut schema: OutputSchema) -> OutputSchema {
        let Some((_, body)) = self
            .bodies
            .iter()
            .find(|(name, _)| nodedb_sql::planner::cte::reads_cte_body_unchanged(plan, name))
        else {
            return schema;
        };
        for column in &mut schema.columns {
            let bare = column
                .lookup_key
                .rsplit('.')
                .next()
                .unwrap_or(column.lookup_key.as_str());
            if let Some(source) = body.columns.iter().find(|c| c.display_name == bare) {
                column.lookup_key = source.lookup_key.clone();
            }
        }
        schema
    }
}

impl SqlCatalog for CteTypesCatalog<'_> {
    fn get_collection(
        &self,
        database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        if let Some((_, info)) = self.relations.iter().find(|(key, _)| key == name) {
            return Ok(Some(info.clone()));
        }
        self.inner.get_collection(database_id, name)
    }

    fn lookup_array(&self, name: &str) -> Option<ArrayCatalogView> {
        self.inner.lookup_array(name)
    }

    fn crs_context(&self) -> nodedb_crs::CrsContext<'_> {
        self.inner.crs_context()
    }
}

/// The relation one definition body exposes: one column per announced output
/// column, named as the body announces it.
///
/// A name the body announces twice keeps its first type. The relation
/// declares no key column, so a row read through it renders its identity
/// under the implicit `id`. A body that announces no columns, such as a
/// `SELECT *` over a schemaless collection, exposes none, and every column
/// read through it stays `Text`.
fn relation_from_schema(name: &str, schema: &OutputSchema) -> CollectionInfo {
    let mut columns: Vec<ColumnInfo> = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        if columns.iter().any(|c| c.name == column.display_name) {
            continue;
        }
        columns.push(column_info(&column.display_name, column.ty));
    }
    CollectionInfo {
        name: name.to_string(),
        engine: EngineType::DocumentSchemaless,
        columns,
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: true,
    }
}

/// A catalog column that maps back to `ty` on the wire.
///
/// Integer and float widths are recorded, so `Int4` and `Float4` stay
/// narrow. Schema derivation never announces `Varchar`, `Jsonb` or
/// `Float8Array`. Those three map to `String`, `Json` and `Vector`, which
/// announce `Text`, `Json` and `Float4Array`.
fn column_info(name: &str, ty: DdlColType) -> ColumnInfo {
    let (data_type, int_width, float_width) = match ty {
        DdlColType::Int8 => (SqlDataType::Int64, Some(IntWidth::I64), None),
        DdlColType::Int4 => (SqlDataType::Int64, Some(IntWidth::I32), None),
        DdlColType::Int2 => (SqlDataType::Int64, Some(IntWidth::I16), None),
        DdlColType::Float8 => (SqlDataType::Float64, None, Some(FloatWidth::F64)),
        DdlColType::Float4 => (SqlDataType::Float64, None, Some(FloatWidth::F32)),
        DdlColType::Text | DdlColType::Varchar => (SqlDataType::String, None, None),
        DdlColType::Bool => (SqlDataType::Bool, None, None),
        DdlColType::Bytea => (SqlDataType::Bytes, None, None),
        DdlColType::Json | DdlColType::Jsonb => (SqlDataType::Json, None, None),
        DdlColType::Timestamp => (SqlDataType::Timestamp, None, None),
        DdlColType::Timestamptz => (SqlDataType::Timestamptz, None, None),
        DdlColType::Numeric => (SqlDataType::Decimal(None), None, None),
        DdlColType::Uuid => (SqlDataType::Uuid, None, None),
        DdlColType::Float4Array | DdlColType::Float8Array => (SqlDataType::Vector(0), None, None),
    };
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        is_primary_key: false,
        default: None,
        raw_type: None,
        int_width,
        float_width,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::schema::sql_data_type_to_ddl_col_type_with_width;

    /// Every type schema derivation announces maps back to itself.
    #[test]
    fn every_announced_type_round_trips() {
        let announced = [
            DdlColType::Text,
            DdlColType::Int8,
            DdlColType::Int4,
            DdlColType::Int2,
            DdlColType::Float8,
            DdlColType::Float4,
            DdlColType::Bool,
            DdlColType::Bytea,
            DdlColType::Json,
            DdlColType::Timestamp,
            DdlColType::Timestamptz,
            DdlColType::Numeric,
            DdlColType::Uuid,
            DdlColType::Float4Array,
        ];
        for ty in announced {
            let column = column_info("c", ty);
            let back = sql_data_type_to_ddl_col_type_with_width(
                &column.data_type,
                column.int_width,
                column.float_width,
            );
            assert_eq!(back, ty);
        }
    }
}
