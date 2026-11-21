// SPDX-License-Identifier: BUSL-1.1

//! Translate a stored collection descriptor into the CRDT constraint set the
//! commit-time validator enforces.
//!
//! Scope today: `UNIQUE` (from secondary indexes), `NOT NULL` (from strict /
//! KV typed schemas, columnar type strings, and schemaless type guards), and
//! `CHECK` (from schemaless type-guard predicates). Foreign keys are
//! deliberately out of scope here.
//!
//! Each constraint's `collection` is the stored name, `QualifiedCollection::new`
//! of the row's database. The validator installs and finds a constraint under
//! that name. A derived constraint name uses the bare name, as in
//! `{name}_{field}_notnull`.

use nodedb_crdt::{Constraint, ConstraintKind};
use nodedb_types::columnar::{ColumnDef, DocumentMode};
use nodedb_types::{CollectionType, QualifiedCollection};

use super::collection::StoredCollection;

/// Derive every `UNIQUE` / `NOT NULL` constraint implied by a catalog row.
///
/// The result is sorted by constraint name so two derivations of the same
/// catalog row encode byte-for-byte identically — the delivery path compares
/// encoded constraint sets to decide whether a re-publish is needed.
pub fn collection_constraints(stored: &StoredCollection) -> Vec<Constraint> {
    let mut out: Vec<Constraint> = Vec::new();
    let names = CollectionNames {
        stored: QualifiedCollection::new(stored.database_id, &stored.name),
        bare: stored.name.as_str(),
    };
    let collection = names.stored.as_str();

    // UNIQUE — one constraint per unique secondary index.
    //
    // A secondary index stores its field as an extraction path (`$.email` for a
    // top-level column), but the validator matches a constraint's field by exact
    // equality against the row's stored field names, which are the bare column
    // names (`email`). Strip the leading `$.` so a UNIQUE constraint keys the
    // same field identifier as the row data and the column-derived NOT NULL
    // constraints.
    for idx in &stored.indexes {
        if idx.unique {
            let field = idx.field.strip_prefix("$.").unwrap_or(&idx.field);
            out.push(Constraint {
                name: idx.name.clone(),
                collection: collection.to_string(),
                field: field.to_string(),
                kind: ConstraintKind::Unique,
            });
        }
    }

    // NOT NULL — typed schema (strict document + KV) carries structured
    // `ColumnDef`s; columnar columns survive only as type strings in `fields`.
    match typed_schema_columns(&stored.collection_type) {
        Some(columns) => {
            for col in columns {
                if !col.nullable {
                    out.push(not_null(&names, &col.name));
                }
            }
        }
        None => {
            for (name, type_str) in &stored.fields {
                let (_, is_pk, is_not_null, _) =
                    nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full(type_str);
                if is_not_null || is_pk {
                    out.push(not_null(&names, name));
                }
            }
        }
    }

    // NOT NULL / CHECK — schemaless type guards.
    for guard in &stored.type_guards {
        if guard.required {
            out.push(not_null(&names, &guard.field));
        }
        if let Some(expr) = &guard.check_expr {
            out.push(Constraint {
                name: format!("{}_{}_check", names.bare, guard.field),
                collection: collection.to_string(),
                field: guard.field.clone(),
                kind: ConstraintKind::Check {
                    expr: expr.clone(),
                    description: expr.clone(),
                },
            });
        }
    }

    // Determinism: identical catalog rows must encode identically.
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Structured column defs for engines that carry a typed schema (strict
/// document and key-value). Returns `None` for schemaless and columnar-family
/// collections, whose nullability lives elsewhere (`fields` / `type_guards`).
fn typed_schema_columns(collection_type: &CollectionType) -> Option<&[ColumnDef]> {
    match collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => Some(&schema.columns),
        CollectionType::KeyValue(config) => Some(&config.schema.columns),
        _ => None,
    }
}

/// The two names of the collection a constraint set derives from.
struct CollectionNames<'a> {
    /// The stored name each constraint's `collection` holds.
    stored: QualifiedCollection,
    /// The bare name each derived constraint name starts with.
    bare: &'a str,
}

/// Build a `NOT NULL` constraint with the canonical name scheme.
fn not_null(names: &CollectionNames<'_>, field: &str) -> Constraint {
    Constraint {
        name: format!("{}_{field}_notnull", names.bare),
        collection: names.stored.as_str().to_string(),
        field: field.to_string(),
        kind: ConstraintKind::NotNull,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::TypeGuardFieldDef;
    use nodedb_types::columnar::{ColumnType, StrictSchema};

    use crate::control::security::catalog::collection::StoredIndex;

    fn base(name: &str) -> StoredCollection {
        StoredCollection::new(1, name, "owner")
    }

    fn idx(name: &str, field: &str, unique: bool) -> StoredIndex {
        StoredIndex {
            name: name.to_string(),
            field: field.to_string(),
            unique,
            case_insensitive: false,
            predicate: None,
            state: Default::default(),
            owner: "owner".to_string(),
        }
    }

    fn guard(field: &str, required: bool) -> TypeGuardFieldDef {
        TypeGuardFieldDef {
            field: field.to_string(),
            type_expr: "STRING".to_string(),
            required,
            check_expr: None,
            default_expr: None,
            value_expr: None,
        }
    }

    #[test]
    fn empty_catalog_yields_no_constraints() {
        let c = base("empty");
        assert_eq!(collection_constraints(&c), Vec::<Constraint>::new());
    }

    #[test]
    fn unique_index_yields_one_unique() {
        let mut c = base("users");
        // Indexes store the field as an extraction path; the translator must
        // normalize `$.email` to the bare column name `email` so it matches the
        // row's field keys and the column-derived NOT NULL constraints.
        c.indexes.push(idx("users_email_unique", "$.email", true));
        c.indexes.push(idx("users_age_idx", "$.age", false)); // non-unique ignored
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "users_email_unique");
        assert_eq!(got[0].field, "email");
        assert_eq!(got[0].kind, ConstraintKind::Unique);
    }

    #[test]
    fn strict_not_null_column_yields_notnull() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("bio", ColumnType::String),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut c = base("people");
        c.collection_type = CollectionType::strict(schema);
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "people_name_notnull");
        assert_eq!(got[0].kind, ConstraintKind::NotNull);
    }

    #[test]
    fn primary_key_column_yields_notnull() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::nullable("note", ColumnType::String),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut c = base("rows");
        c.collection_type = CollectionType::strict(schema);
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "rows_id_notnull");
        assert_eq!(got[0].kind, ConstraintKind::NotNull);
    }

    #[test]
    fn schemaless_required_type_guard_yields_notnull() {
        let mut c = base("docs");
        c.type_guards.push(guard("title", true));
        c.type_guards.push(guard("subtitle", false)); // not required → ignored
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "docs_title_notnull");
        assert_eq!(got[0].kind, ConstraintKind::NotNull);
    }

    #[test]
    fn schemaless_check_expr_yields_check() {
        let mut c = base("people");
        c.type_guards.push(TypeGuardFieldDef {
            field: "age".to_string(),
            type_expr: "INT".to_string(),
            required: false,
            check_expr: Some("age > 0".to_string()),
            default_expr: None,
            value_expr: None,
        });
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "people_age_check");
        assert_eq!(got[0].field, "age");
        match &got[0].kind {
            ConstraintKind::Check { expr, .. } => assert_eq!(expr, "age > 0"),
            other => panic!("expected Check, got {other:?}"),
        }
    }

    #[test]
    fn schemaless_required_and_check_yields_two_constraints() {
        let mut c = base("people");
        c.type_guards.push(TypeGuardFieldDef {
            field: "age".to_string(),
            type_expr: "INT".to_string(),
            required: true,
            check_expr: Some("age > 0".to_string()),
            default_expr: None,
            value_expr: None,
        });
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 2);
        let names: Vec<&str> = got.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"people_age_notnull"));
        assert!(names.contains(&"people_age_check"));
    }

    #[test]
    fn field_can_yield_both_unique_and_notnull() {
        // email NOT NULL UNIQUE — strict NOT NULL column + unique index.
        let schema = StrictSchema {
            columns: vec![ColumnDef::required("email", ColumnType::String)],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut c = base("accounts");
        c.collection_type = CollectionType::strict(schema);
        c.indexes.push(idx("accounts_email_unique", "email", true));
        let got = collection_constraints(&c);
        assert_eq!(got.len(), 2);
        // Sorted by name: "accounts_email_notnull" < "accounts_email_unique".
        assert_eq!(got[0].name, "accounts_email_notnull");
        assert_eq!(got[0].kind, ConstraintKind::NotNull);
        assert_eq!(got[1].name, "accounts_email_unique");
        assert_eq!(got[1].kind, ConstraintKind::Unique);
    }

    #[test]
    fn a_named_database_scopes_each_constraint_to_the_stored_name() {
        let mut c = base("people");
        c.database_id = nodedb_types::DatabaseId::new(7);
        c.indexes.push(idx("people_email_unique", "$.email", true));
        c.type_guards.push(TypeGuardFieldDef {
            field: "age".to_string(),
            type_expr: "INT".to_string(),
            required: true,
            check_expr: Some("age > 0".to_string()),
            default_expr: None,
            value_expr: None,
        });
        let got = collection_constraints(&c);
        let names: Vec<&str> = got.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "people_age_check",
                "people_age_notnull",
                "people_email_unique"
            ]
        );
        for constraint in &got {
            assert_eq!(constraint.collection, "7/people", "{}", constraint.name);
        }
    }

    #[test]
    fn the_default_database_scopes_each_constraint_to_the_bare_name() {
        let mut c = base("people");
        c.indexes.push(idx("people_email_unique", "$.email", true));
        c.type_guards.push(guard("title", true));
        for constraint in collection_constraints(&c) {
            assert_eq!(constraint.collection, "people", "{}", constraint.name);
        }
    }

    #[test]
    fn output_is_deterministically_sorted_across_calls() {
        let mut c = base("t");
        c.indexes.push(idx("zeta_unique", "z", true));
        c.indexes.push(idx("alpha_unique", "a", true));
        c.type_guards.push(guard("m", true));
        let first = collection_constraints(&c);
        let second = collection_constraints(&c);
        assert_eq!(first, second);
        let names: Vec<&str> = first.iter().map(|c| c.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    #[test]
    fn each_constraint_roundtrips_zerompk() {
        let schema = StrictSchema {
            columns: vec![ColumnDef::required("email", ColumnType::String)],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut c = base("accounts");
        c.collection_type = CollectionType::strict(schema);
        c.indexes.push(idx("accounts_email_unique", "email", true));
        for constraint in collection_constraints(&c) {
            let bytes =
                zerompk::to_msgpack_vec(&constraint).expect("constraint encodes to msgpack");
            let decoded: Constraint =
                zerompk::from_msgpack(&bytes).expect("constraint decodes from msgpack");
            assert_eq!(decoded, constraint);
        }
    }
}
