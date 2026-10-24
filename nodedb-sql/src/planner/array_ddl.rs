// SPDX-License-Identifier: Apache-2.0

//! Planner for `CREATE ARRAY` and `DROP ARRAY`.
//!
//! Validation is engine-agnostic: name non-empty, dims non-empty, attrs
//! non-empty, `tile_extents.len() == dims.len()`, no duplicate dim/attr
//! names, and per-dim domain bounds are well formed (lo <= hi). The
//! schema-hash + the typed `ArraySchema` are computed in the Origin
//! converter where `nodedb-array` is available.

use crate::types::{AlterArrayPlan, CreateArrayPlan};
use std::collections::HashSet;

use crate::catalog::SqlCatalog;
use crate::error::{Result, SqlError};
use crate::parser::array_stmt::{AlterArrayAction, AlterArrayAst, CreateArrayAst, DropArrayAst};
use crate::planner::array_georef::check_georef;
use crate::types::SqlPlan;
use crate::types_array::{
    ArrayAttrMetaAlter, ArrayAttrMetaChange, ArrayDimType, ArrayDomainBound, ArrayGeorefAlter,
};

pub fn plan_create_array(ast: &CreateArrayAst, catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if ast.name.is_empty() {
        return Err(SqlError::Parse {
            detail: "CREATE ARRAY: array name must not be empty".into(),
        });
    }
    if ast.dims.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("CREATE ARRAY {}: at least one dim is required", ast.name),
        });
    }
    if ast.attrs.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("CREATE ARRAY {}: at least one attr is required", ast.name),
        });
    }
    if ast.tile_extents.len() != ast.dims.len() {
        return Err(SqlError::Parse {
            detail: format!(
                "CREATE ARRAY {}: TILE_EXTENTS arity {} != DIMS arity {}",
                ast.name,
                ast.tile_extents.len(),
                ast.dims.len()
            ),
        });
    }
    let mut seen_dims: HashSet<&str> = HashSet::new();
    for d in &ast.dims {
        if d.name.is_empty() {
            return Err(SqlError::Parse {
                detail: format!("CREATE ARRAY {}: empty dim name", ast.name),
            });
        }
        if !seen_dims.insert(d.name.as_str()) {
            return Err(SqlError::Parse {
                detail: format!("CREATE ARRAY {}: duplicate dim name `{}`", ast.name, d.name),
            });
        }
        if !domain_bounds_match_type(&d.lo, &d.hi, d.dtype) {
            return Err(SqlError::Parse {
                detail: format!(
                    "CREATE ARRAY {}: domain bounds for dim `{}` do not match dim type",
                    ast.name, d.name
                ),
            });
        }
        if !domain_lo_le_hi(&d.lo, &d.hi) {
            return Err(SqlError::Parse {
                detail: format!("CREATE ARRAY {}: dim `{}` lo > hi", ast.name, d.name),
            });
        }
    }
    let mut seen_attrs: HashSet<&str> = HashSet::new();
    for a in &ast.attrs {
        if a.name.is_empty() {
            return Err(SqlError::Parse {
                detail: format!("CREATE ARRAY {}: empty attr name", ast.name),
            });
        }
        if !seen_attrs.insert(a.name.as_str()) {
            return Err(SqlError::Parse {
                detail: format!(
                    "CREATE ARRAY {}: duplicate attr name `{}`",
                    ast.name, a.name
                ),
            });
        }
    }
    for (i, te) in ast.tile_extents.iter().enumerate() {
        if *te <= 0 {
            return Err(SqlError::Parse {
                detail: format!(
                    "CREATE ARRAY {}: tile extent for dim `{}` must be > 0 (got {te})",
                    ast.name, ast.dims[i].name
                ),
            });
        }
    }

    if !(1..=16).contains(&ast.prefix_bits) {
        return Err(SqlError::Parse {
            detail: format!(
                "CREATE ARRAY {}: prefix_bits {} is not in range 1–16",
                ast.name, ast.prefix_bits
            ),
        });
    }
    if let Some(georef) = &ast.georef {
        check_georef(&ast.name, georef, &ast.dims, &catalog.crs_context())?;
    }
    Ok(SqlPlan::CreateArray(CreateArrayPlan {
        name: ast.name.clone(),
        dims: ast.dims.clone(),
        attrs: ast.attrs.clone(),
        tile_extents: ast.tile_extents.clone(),
        cell_order: ast.cell_order,
        tile_order: ast.tile_order,
        georef: ast.georef.clone(),
        prefix_bits: ast.prefix_bits,
        audit_retain_ms: ast.audit_retain_ms,
        minimum_audit_retain_ms: ast.minimum_audit_retain_ms,
    }))
}

pub fn plan_alter_array(ast: &AlterArrayAst, catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    if ast.name.is_empty() {
        return Err(SqlError::Parse {
            detail: "ALTER ARRAY: array name must not be empty".into(),
        });
    }
    match &ast.action {
        AlterArrayAction::SetOptions(set) => plan_alter_options(&ast.name, set),
        AlterArrayAction::Georef(change) => plan_alter_georef(&ast.name, change, catalog),
        AlterArrayAction::AttrMeta(change) => plan_alter_attr_meta(&ast.name, change, catalog),
    }
}

/// `ALTER ARRAY <name> ALTER ATTR <attr> SET (...)` or `DROP ...`, checked
/// against the array's attrs. The engine checks each value against the
/// attr type when it applies the change.
fn plan_alter_attr_meta(
    name: &str,
    change: &ArrayAttrMetaAlter,
    catalog: &dyn SqlCatalog,
) -> Result<SqlPlan> {
    let view = catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })?;
    let attr = view
        .attrs
        .iter()
        .find(|a| a.name == change.attr)
        .ok_or_else(|| SqlError::UnknownColumn {
            table: name.to_string(),
            column: change.attr.clone(),
        })?;
    if let ArrayAttrMetaChange::Drop(keys) = &change.change
        && let Some(unset) = keys.iter().find(|k| !k.is_set(&attr.meta))
    {
        return Err(SqlError::ObjectNotInPrerequisiteState {
            object: format!("attr '{}' of array '{name}'", change.attr),
            detail: format!(
                "ALTER ARRAY {name} ALTER ATTR {} DROP {}: the attr has no {}",
                change.attr,
                unset.sql_name(),
                unset.sql_name()
            ),
        });
    }
    Ok(SqlPlan::AlterArray(AlterArrayPlan {
        name: name.to_string(),
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
        georef: None,
        attr_meta: Some(change.clone()),
    }))
}

/// `ALTER ARRAY <name> SET GEOREF (...)` or `DROP GEOREF`, checked against
/// the array's current dims and georef.
fn plan_alter_georef(
    name: &str,
    change: &ArrayGeorefAlter,
    catalog: &dyn SqlCatalog,
) -> Result<SqlPlan> {
    let view = catalog
        .lookup_array(name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: name.to_string(),
        })?;
    match change {
        ArrayGeorefAlter::Set(georef) => {
            check_georef(name, georef, &view.dims, &catalog.crs_context())?
        }
        ArrayGeorefAlter::Drop => {
            if view.georef.is_none() {
                return Err(SqlError::ObjectNotInPrerequisiteState {
                    object: format!("array '{name}'"),
                    detail: format!("ALTER ARRAY {name} DROP GEOREF: the array has no GEOREF"),
                });
            }
        }
    }
    Ok(SqlPlan::AlterArray(AlterArrayPlan {
        name: name.to_string(),
        audit_retain_ms: None,
        minimum_audit_retain_ms: None,
        georef: Some(change.clone()),
        attr_meta: None,
    }))
}

/// `ALTER ARRAY <name> SET (key = value, ...)`.
fn plan_alter_options(name: &str, set: &[(String, Option<i64>)]) -> Result<SqlPlan> {
    if set.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("ALTER ARRAY {name}: SET clause is empty"),
        });
    }

    let mut audit_retain_ms: Option<Option<i64>> = None;
    let mut minimum_audit_retain_ms: Option<u64> = None;

    for (key, value) in set {
        if value.is_some_and(|v| v < 0) {
            return Err(SqlError::Parse {
                detail: format!("ALTER ARRAY {name}: {key} must not be negative"),
            });
        }
        match key.as_str() {
            "audit_retain_ms" => {
                audit_retain_ms = Some(*value);
            }
            "minimum_audit_retain_ms" => {
                let n = value.ok_or_else(|| SqlError::Parse {
                    detail: format!("ALTER ARRAY {name}: minimum_audit_retain_ms cannot be NULL"),
                })?;
                minimum_audit_retain_ms = Some(n as u64);
            }
            other => {
                return Err(SqlError::Parse {
                    detail: format!(
                        "ALTER ARRAY {name}: unknown SET key `{other}`; \
                         expected `audit_retain_ms` or `minimum_audit_retain_ms`"
                    ),
                });
            }
        }
    }

    Ok(SqlPlan::AlterArray(AlterArrayPlan {
        name: name.to_string(),
        audit_retain_ms,
        minimum_audit_retain_ms,
        georef: None,
        attr_meta: None,
    }))
}

pub fn plan_drop_array(ast: &DropArrayAst) -> Result<SqlPlan> {
    if ast.name.is_empty() {
        return Err(SqlError::Parse {
            detail: "DROP ARRAY: name must not be empty".into(),
        });
    }
    Ok(SqlPlan::DropArray {
        name: ast.name.clone(),
        if_exists: ast.if_exists,
    })
}

fn domain_bounds_match_type(
    lo: &ArrayDomainBound,
    hi: &ArrayDomainBound,
    dtype: ArrayDimType,
) -> bool {
    matches!(
        (lo, hi, dtype),
        (
            ArrayDomainBound::Int64(_),
            ArrayDomainBound::Int64(_),
            ArrayDimType::Int64
        ) | (
            ArrayDomainBound::Float64(_),
            ArrayDomainBound::Float64(_),
            ArrayDimType::Float64
        ) | (
            ArrayDomainBound::TimestampMs(_),
            ArrayDomainBound::TimestampMs(_),
            ArrayDimType::TimestampMs,
        ) | (
            ArrayDomainBound::String(_),
            ArrayDomainBound::String(_),
            ArrayDimType::String
        )
    )
}

fn domain_lo_le_hi(lo: &ArrayDomainBound, hi: &ArrayDomainBound) -> bool {
    match (lo, hi) {
        (ArrayDomainBound::Int64(a), ArrayDomainBound::Int64(b)) => a <= b,
        (ArrayDomainBound::Float64(a), ArrayDomainBound::Float64(b)) => a <= b,
        (ArrayDomainBound::TimestampMs(a), ArrayDomainBound::TimestampMs(b)) => a <= b,
        (ArrayDomainBound::String(a), ArrayDomainBound::String(b)) => a <= b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ArrayCatalogView;
    use crate::types_array::{ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayGeorefAst};

    fn ok_ast() -> CreateArrayAst {
        CreateArrayAst {
            name: "g".into(),
            dims: vec![ArrayDimAst {
                name: "x".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(10),
            }],
            attrs: vec![ArrayAttrAst {
                name: "v".into(),
                dtype: ArrayAttrType::Int64,
                nullable: false,
                meta: Default::default(),
            }],
            tile_extents: vec![4],
            cell_order: Default::default(),
            tile_order: Default::default(),
            georef: None,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    fn int_dim(name: &str) -> ArrayDimAst {
        ArrayDimAst {
            name: name.into(),
            dtype: ArrayDimType::Int64,
            lo: ArrayDomainBound::Int64(0),
            hi: ArrayDomainBound::Int64(1023),
        }
    }

    fn georef(x: &str, y: &str, transform: Option<[f64; 6]>) -> ArrayGeorefAst {
        ArrayGeorefAst {
            srid: 32647,
            x_dim: x.into(),
            y_dim: y.into(),
            transform,
        }
    }

    const GT: [f64; 6] = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];

    fn raster_ast(georef: Option<ArrayGeorefAst>) -> CreateArrayAst {
        CreateArrayAst {
            dims: vec![int_dim("row"), int_dim("col")],
            tile_extents: vec![256, 256],
            georef,
            ..ok_ast()
        }
    }

    #[test]
    fn create_carries_georef() {
        let g = georef("col", "row", Some(GT));
        let SqlPlan::CreateArray(plan) =
            plan_create_array(&raster_ast(Some(g.clone())), &Catalog(None)).unwrap()
        else {
            panic!("expected CreateArray");
        };
        assert_eq!(plan.georef, Some(g));
    }

    #[test]
    fn create_refuses_bad_georef_with_sqlstate_class() {
        let err = plan_create_array(
            &raster_ast(Some(georef("col", "lat", None))),
            &Catalog(None),
        )
        .unwrap_err();
        assert!(matches!(err, SqlError::UnknownColumn { .. }), "{err:?}");
        let mut srid = georef("col", "row", None);
        srid.srid = 999;
        let err = plan_create_array(&raster_ast(Some(srid)), &Catalog(None)).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        let singular = georef("col", "row", Some([0.0, 1.0, 1.0, 0.0, 1.0, 1.0]));
        let err = plan_create_array(&raster_ast(Some(singular)), &Catalog(None)).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
    }

    struct Catalog(Option<ArrayCatalogView>);

    impl SqlCatalog for Catalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            _: &str,
        ) -> std::result::Result<
            Option<crate::types::CollectionInfo>,
            crate::catalog::SqlCatalogError,
        > {
            Ok(None)
        }
        fn lookup_array(&self, _: &str) -> Option<ArrayCatalogView> {
            self.0.clone()
        }
    }

    fn view(georef: Option<ArrayGeorefAst>) -> ArrayCatalogView {
        ArrayCatalogView {
            name: "scene".into(),
            dims: vec![int_dim("row"), int_dim("col")],
            attrs: ok_ast().attrs,
            tile_extents: vec![256, 256],
            georef,
        }
    }

    fn alter(action: AlterArrayAction, catalog: &Catalog) -> Result<SqlPlan> {
        plan_alter_array(
            &AlterArrayAst {
                name: "scene".into(),
                action,
            },
            catalog,
        )
    }

    #[test]
    fn alter_set_georef_plans_the_change() {
        let g = georef("col", "row", Some(GT));
        let change = ArrayGeorefAlter::Set(g);
        let SqlPlan::AlterArray(plan) = alter(
            AlterArrayAction::Georef(change.clone()),
            &Catalog(Some(view(None))),
        )
        .unwrap() else {
            panic!("expected AlterArray");
        };
        assert_eq!(plan.georef, Some(change));
        assert_eq!(plan.audit_retain_ms, None);
        assert_eq!(plan.minimum_audit_retain_ms, None);
    }

    #[test]
    fn alter_set_georef_refusals() {
        let catalog = Catalog(Some(view(None)));
        let unknown = ArrayGeorefAlter::Set(georef("col", "lat", None));
        let err = alter(AlterArrayAction::Georef(unknown), &catalog).unwrap_err();
        assert!(matches!(err, SqlError::UnknownColumn { .. }), "{err:?}");
        let mut inf = GT;
        inf[5] = f64::NEG_INFINITY;
        let bad = ArrayGeorefAlter::Set(georef("col", "row", Some(inf)));
        let err = alter(AlterArrayAction::Georef(bad), &catalog).unwrap_err();
        assert!(matches!(err, SqlError::DataException { .. }), "{err:?}");
        let missing = alter(
            AlterArrayAction::Georef(ArrayGeorefAlter::Drop),
            &Catalog(None),
        )
        .unwrap_err();
        assert!(
            matches!(missing, SqlError::UnknownTable { .. }),
            "{missing:?}"
        );
    }

    #[test]
    fn alter_drop_georef_needs_a_georef() {
        let err = alter(
            AlterArrayAction::Georef(ArrayGeorefAlter::Drop),
            &Catalog(Some(view(None))),
        )
        .unwrap_err();
        assert!(
            matches!(err, SqlError::ObjectNotInPrerequisiteState { .. }),
            "{err:?}"
        );
        let plan = alter(
            AlterArrayAction::Georef(ArrayGeorefAlter::Drop),
            &Catalog(Some(view(Some(georef("col", "row", None))))),
        )
        .unwrap();
        assert!(matches!(
            plan,
            SqlPlan::AlterArray(AlterArrayPlan {
                georef: Some(ArrayGeorefAlter::Drop),
                ..
            })
        ));
    }

    #[test]
    fn alter_options_keep_georef_unchanged() {
        let plan = alter(
            AlterArrayAction::SetOptions(vec![("audit_retain_ms".into(), Some(5))]),
            &Catalog(None),
        )
        .unwrap();
        assert!(matches!(
            plan,
            SqlPlan::AlterArray(AlterArrayPlan {
                georef: None,
                audit_retain_ms: Some(Some(5)),
                ..
            })
        ));
    }

    #[test]
    fn alter_attr_meta_checks_the_attr_and_dropped_keys() {
        use crate::types_array::{ArrayAttrMetaAst, ArrayAttrMetaKey};
        let catalog = Catalog(Some(view(None)));
        let set = ArrayAttrMetaAlter {
            attr: "v".into(),
            change: ArrayAttrMetaChange::Set(ArrayAttrMetaAst {
                nodata: Some(0.0),
                ..ArrayAttrMetaAst::default()
            }),
        };
        let SqlPlan::AlterArray(plan) =
            alter(AlterArrayAction::AttrMeta(set.clone()), &catalog).unwrap()
        else {
            panic!("expected AlterArray");
        };
        assert_eq!(plan.attr_meta, Some(set));
        assert_eq!(plan.georef, None);

        let unknown = ArrayAttrMetaAlter {
            attr: "nope".into(),
            change: ArrayAttrMetaChange::Drop(vec![ArrayAttrMetaKey::Nodata]),
        };
        let err = alter(AlterArrayAction::AttrMeta(unknown), &catalog).unwrap_err();
        assert!(matches!(err, SqlError::UnknownColumn { .. }), "{err:?}");

        let unset = ArrayAttrMetaAlter {
            attr: "v".into(),
            change: ArrayAttrMetaChange::Drop(vec![ArrayAttrMetaKey::Scale]),
        };
        let err = alter(AlterArrayAction::AttrMeta(unset), &catalog).unwrap_err();
        assert!(
            matches!(err, SqlError::ObjectNotInPrerequisiteState { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn alter_options_refuse_negative_values() {
        for key in ["audit_retain_ms", "minimum_audit_retain_ms"] {
            let err = alter(
                AlterArrayAction::SetOptions(vec![(key.into(), Some(-1))]),
                &Catalog(None),
            )
            .expect_err("negative refused");
            assert!(matches!(err, SqlError::Parse { .. }), "{key}: {err:?}");
        }
    }

    #[test]
    fn happy_path() {
        let plan = plan_create_array(&ok_ast(), &Catalog(None)).unwrap();
        assert!(matches!(plan, SqlPlan::CreateArray { .. }));
    }

    #[test]
    fn rejects_extent_arity() {
        let mut a = ok_ast();
        a.tile_extents = vec![1, 2];
        assert!(plan_create_array(&a, &Catalog(None)).is_err());
    }

    #[test]
    fn rejects_dup_dim() {
        let mut a = ok_ast();
        a.dims.push(a.dims[0].clone());
        a.tile_extents = vec![4, 4];
        assert!(plan_create_array(&a, &Catalog(None)).is_err());
    }

    #[test]
    fn rejects_lo_gt_hi() {
        let mut a = ok_ast();
        a.dims[0].lo = ArrayDomainBound::Int64(5);
        a.dims[0].hi = ArrayDomainBound::Int64(1);
        assert!(plan_create_array(&a, &Catalog(None)).is_err());
    }
}
