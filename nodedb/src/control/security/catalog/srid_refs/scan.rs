// SPDX-License-Identifier: BUSL-1.1

//! The catalog scan `DROP SPATIAL_REF_SYS` runs: every catalog object whose
//! definition names a user-defined SRID in an SRID role.
//!
//! Every definition the write path evaluates counts: field and type-guard
//! types, DEFAULT / VALUE / ASSERT expressions, generated columns, type
//! guards, CHECK constraints, transition checks, materialized sums, partial
//! index predicates, events, and an array GEOREF. A number counts only in
//! an SRID role, as `nodedb_query::geo_functions::srid_refs` defines it, so
//! `CHECK (amount < 910001)` does not hold SRID 910001.
//!
//! Stored rows are data, not definitions. A geometry a row stores keeps its
//! code after the drop, and a read of it reports the code as dropped, as
//! PostGIS does for a row whose `spatial_ref_sys` entry is gone.

use std::borrow::Cow;

use nodedb_array::schema::ArraySchema;
use nodedb_crs::Srid;
use nodedb_query::expr::SqlExpr;
use nodedb_query::geo_functions::{expr_names_srid, type_names_srid};
use nodedb_types::columnar::ColumnDef;
use nodedb_types::{CollectionType, DocumentMode};

use super::text::text_names_srid;
use crate::control::security::catalog::types::{StoredCollection, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Every catalog object whose definition names `srid`, one line each,
    /// sorted: `collection '<name>' (tenant <t>, database <d>): <part>` or
    /// `array '<name>' (tenant <t>, database <d>): GEOREF`.
    pub fn srid_referrers(&self, srid: Srid) -> crate::Result<Vec<String>> {
        let code = srid.code();
        let mut referrers = Vec::new();
        for coll in self.load_all_collections_across_databases()? {
            for part in collection_parts_naming(&coll, code) {
                referrers.push(format!(
                    "collection '{}' (tenant {}, database {}): {part}",
                    coll.name,
                    coll.tenant_id,
                    coll.database_id.as_u64()
                ));
            }
        }
        for array in self.load_all_arrays()? {
            let schema: ArraySchema = zerompk::from_msgpack(&array.schema_msgpack)
                .map_err(|e| catalog_err("decode array schema", e))?;
            if schema.georef.as_ref().is_some_and(|g| g.srid == srid) {
                referrers.push(format!(
                    "array '{}' (tenant {}, database {}): GEOREF",
                    array.name,
                    array.array_id.tenant_id.as_u64(),
                    array.array_id.database_id.as_u64()
                ));
            }
        }
        referrers.sort();
        referrers.dedup();
        Ok(referrers)
    }
}

/// What one definition part holds.
enum Part<'a> {
    /// A type declaration.
    Type(&'a str),
    /// Expression or statement text.
    Text(&'a str),
    /// A stored expression tree.
    Expr(Cow<'a, SqlExpr>),
}

impl Part<'_> {
    fn names(&self, code: u32) -> bool {
        match self {
            Part::Type(text) => type_names_srid(text, code),
            Part::Text(text) => text_names_srid(text, code),
            Part::Expr(expr) => expr_names_srid(expr, code),
        }
    }
}

/// The parts of `coll`'s definition that name `code`.
fn collection_parts_naming(coll: &StoredCollection, code: u32) -> Vec<String> {
    definition_parts(coll)
        .into_iter()
        .filter(|(_, part)| part.names(code))
        .map(|(label, _)| label)
        .collect()
}

/// Every evaluated part of `coll`'s definition, labelled for the error.
fn definition_parts(coll: &StoredCollection) -> Vec<(String, Part<'_>)> {
    let mut parts = Vec::new();
    for (name, field_type) in &coll.fields {
        parts.push((format!("field '{name}' type"), Part::Type(field_type)));
    }
    for def in &coll.field_defs {
        let name = &def.name;
        parts.push((format!("field '{name}' type"), Part::Type(&def.field_type)));
        parts.push((
            format!("field '{name}' DEFAULT"),
            Part::Text(&def.default_expr),
        ));
        // A generated field stores a serialized tree. Other value text
        // parses as an expression.
        let value = if def.is_generated {
            "generated column"
        } else {
            "VALUE"
        };
        parts.push((
            format!("field '{name}' {value}"),
            stored_or_text(&def.value_expr),
        ));
        parts.push((
            format!("field '{name}' ASSERT"),
            Part::Text(&def.assert_expr),
        ));
    }
    for column in strict_columns(&coll.collection_type) {
        let name = &column.name;
        if let Some(default) = &column.default {
            parts.push((format!("column '{name}' DEFAULT"), Part::Text(default)));
        }
        if let Some(generated) = &column.generated_expr {
            parts.push((
                format!("column '{name}' generated column"),
                stored_or_text(generated),
            ));
        }
    }
    for guard in &coll.type_guards {
        let field = &guard.field;
        parts.push((
            format!("type guard '{field}' type"),
            Part::Type(&guard.type_expr),
        ));
        for (clause, text) in [
            ("CHECK", &guard.check_expr),
            ("DEFAULT", &guard.default_expr),
            ("VALUE", &guard.value_expr),
        ] {
            if let Some(text) = text {
                parts.push((format!("type guard '{field}' {clause}"), Part::Text(text)));
            }
        }
    }
    for constraint in &coll.check_constraints {
        parts.push((
            format!("CHECK constraint '{}'", constraint.name),
            Part::Text(&constraint.check_sql),
        ));
    }
    for transition in &coll.transition_checks {
        parts.push((
            format!("transition check '{}'", transition.name),
            Part::Expr(Cow::Borrowed(&transition.predicate)),
        ));
    }
    for sum in &coll.materialized_sums {
        parts.push((
            format!(
                "materialized sum into '{}.{}'",
                sum.target_collection, sum.target_column
            ),
            Part::Expr(Cow::Borrowed(&sum.value_expr)),
        ));
    }
    for index in &coll.indexes {
        if let Some(predicate) = &index.predicate {
            parts.push((
                format!("index '{}' predicate", index.name),
                Part::Text(predicate),
            ));
        }
    }
    for event in &coll.event_defs {
        parts.push((
            format!("event '{}' WHEN", event.name),
            Part::Text(&event.when_condition),
        ));
        parts.push((
            format!("event '{}' THEN", event.name),
            Part::Text(&event.then_action),
        ));
    }
    parts
}

/// A part that holds a serialized expression tree, else expression text.
fn stored_or_text(text: &str) -> Part<'_> {
    match sonic_rs::from_str::<SqlExpr>(text) {
        Ok(tree) => Part::Expr(Cow::Owned(tree)),
        Err(_) => Part::Text(text),
    }
}

/// The columns of a strict document or key-value schema.
fn strict_columns(collection_type: &CollectionType) -> &[ColumnDef] {
    match collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => &schema.columns,
        CollectionType::KeyValue(config) => &config.schema.columns,
        CollectionType::Document(DocumentMode::Schemaless) | CollectionType::Columnar(_) => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::collection_constraints::{
        CheckConstraintDef, FieldDefinition,
    };

    const CODE: u32 = 910_042;

    fn field(name: &str, value_expr: &str, generated: bool) -> FieldDefinition {
        FieldDefinition {
            name: name.into(),
            field_type: String::new(),
            default_expr: String::new(),
            value_expr: value_expr.into(),
            assert_expr: String::new(),
            readonly: false,
            sequence_name: None,
            is_generated: generated,
            generated_deps: Vec::new(),
        }
    }

    fn check(name: &str, sql: &str) -> CheckConstraintDef {
        CheckConstraintDef {
            name: name.into(),
            check_sql: sql.into(),
            has_subquery: false,
        }
    }

    #[test]
    fn every_srid_role_is_reported() {
        let (tree, _) =
            nodedb_query::expr_parse::parse_generated_expr("ST_Transform(geom, 910042)")
                .expect("parses");
        let mut coll = StoredCollection::new(1, "parcels", "admin");
        coll.fields = vec![("site".into(), "GEOMETRY(Point, 910042)".into())];
        coll.field_defs = vec![
            field(
                "geom_local",
                &sonic_rs::to_string(&tree).expect("encodes"),
                true,
            ),
            field("label", "'parcel'", false),
        ];
        coll.check_constraints = vec![check("in_zone", "ST_SRID(ST_SetSRID(NEW.g, 910042)) > 0")];
        coll.type_guards = vec![nodedb_types::TypeGuardFieldDef {
            field: "origin".into(),
            type_expr: "GEOMETRY".into(),
            required: false,
            check_expr: None,
            default_expr: Some("'SRID=910042;POINT(0 0)'".into()),
            value_expr: None,
        }];
        assert_eq!(
            collection_parts_naming(&coll, CODE),
            vec![
                "field 'site' type".to_string(),
                "field 'geom_local' generated column".to_string(),
                "type guard 'origin' DEFAULT".to_string(),
                "CHECK constraint 'in_zone'".to_string(),
            ]
        );
        assert!(collection_parts_naming(&coll, 910_043).is_empty());
    }

    /// The code as a plain number holds nothing: the drop proceeds.
    #[test]
    fn the_same_number_in_another_role_is_not_a_reference() {
        let mut coll = StoredCollection::new(1, "ledger", "admin");
        coll.check_constraints = vec![
            check("small", "NEW.amount < 910042"),
            check("srid_is", "ST_SRID(NEW.g) = 910042"),
        ];
        coll.field_defs = vec![field("limit", "910042", false)];
        assert!(collection_parts_naming(&coll, CODE).is_empty());
    }
}
