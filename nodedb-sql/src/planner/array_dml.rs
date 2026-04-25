//! Planner for `INSERT INTO ARRAY` and `DELETE FROM ARRAY`.
//!
//! Validation against the catalog: array exists, coord arity matches
//! dim count, attr arity matches attr count, type tags coerce. Type
//! coercion is purposely loose at the SQL level — `Int → Float` is
//! accepted, the converter performs the actual cast on the way to the
//! engine's typed `CoordValue` / `CellValue`.

use crate::catalog::{ArrayCatalogView, SqlCatalog};
use crate::error::{Result, SqlError};
use crate::parser::array_stmt::{DeleteArrayAst, InsertArrayAst};
use crate::types::SqlPlan;
use crate::types_array::{ArrayAttrLiteral, ArrayAttrType, ArrayCoordLiteral, ArrayDimType};

pub fn plan_insert_array(ast: &InsertArrayAst, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let view = catalog
        .lookup_array(&ast.name)
        .ok_or_else(|| SqlError::Parse {
            detail: format!("INSERT INTO ARRAY {}: array not found", ast.name),
        })?;
    if ast.rows.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("INSERT INTO ARRAY {}: at least one row required", ast.name),
        });
    }
    for (ri, row) in ast.rows.iter().enumerate() {
        validate_coords(&ast.name, ri, &row.coords, &view)?;
        validate_attrs(&ast.name, ri, &row.attrs, &view)?;
    }
    Ok(vec![SqlPlan::InsertArray {
        name: ast.name.clone(),
        rows: ast.rows.clone(),
    }])
}

pub fn plan_delete_array(ast: &DeleteArrayAst, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let view = catalog
        .lookup_array(&ast.name)
        .ok_or_else(|| SqlError::Parse {
            detail: format!("DELETE FROM ARRAY {}: array not found", ast.name),
        })?;
    if ast.coords.is_empty() {
        return Err(SqlError::Parse {
            detail: format!(
                "DELETE FROM ARRAY {}: at least one coord tuple required",
                ast.name
            ),
        });
    }
    for (ri, row) in ast.coords.iter().enumerate() {
        validate_coords(&ast.name, ri, row, &view)?;
    }
    Ok(vec![SqlPlan::DeleteArray {
        name: ast.name.clone(),
        coords: ast.coords.clone(),
    }])
}

fn validate_coords(
    array: &str,
    row: usize,
    coords: &[ArrayCoordLiteral],
    view: &ArrayCatalogView,
) -> Result<()> {
    if coords.len() != view.dims.len() {
        return Err(SqlError::Parse {
            detail: format!(
                "ARRAY {array} row {row}: coord arity {} != dim count {}",
                coords.len(),
                view.dims.len()
            ),
        });
    }
    for (i, c) in coords.iter().enumerate() {
        if !coord_compatible(c, view.dims[i].dtype) {
            return Err(SqlError::TypeMismatch {
                detail: format!(
                    "ARRAY {array} row {row}: coord for dim `{}` (declared {:?}) is incompatible",
                    view.dims[i].name, view.dims[i].dtype
                ),
            });
        }
    }
    Ok(())
}

fn validate_attrs(
    array: &str,
    row: usize,
    attrs: &[ArrayAttrLiteral],
    view: &ArrayCatalogView,
) -> Result<()> {
    if attrs.len() != view.attrs.len() {
        return Err(SqlError::Parse {
            detail: format!(
                "ARRAY {array} row {row}: attr arity {} != attr count {}",
                attrs.len(),
                view.attrs.len()
            ),
        });
    }
    for (i, a) in attrs.iter().enumerate() {
        let spec = &view.attrs[i];
        match a {
            ArrayAttrLiteral::Null if !spec.nullable => {
                return Err(SqlError::TypeMismatch {
                    detail: format!("ARRAY {array} row {row}: attr `{}` is NOT NULL", spec.name),
                });
            }
            ArrayAttrLiteral::Null => {}
            other if !attr_compatible(other, spec.dtype) => {
                return Err(SqlError::TypeMismatch {
                    detail: format!(
                        "ARRAY {array} row {row}: attr `{}` (declared {:?}) is incompatible",
                        spec.name, spec.dtype
                    ),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn coord_compatible(c: &ArrayCoordLiteral, dtype: ArrayDimType) -> bool {
    matches!(
        (c, dtype),
        (ArrayCoordLiteral::Int64(_), ArrayDimType::Int64)
            | (ArrayCoordLiteral::Int64(_), ArrayDimType::TimestampMs)
            | (ArrayCoordLiteral::Int64(_), ArrayDimType::Float64)
            | (ArrayCoordLiteral::Float64(_), ArrayDimType::Float64)
            | (ArrayCoordLiteral::String(_), ArrayDimType::String)
    )
}

fn attr_compatible(a: &ArrayAttrLiteral, dtype: ArrayAttrType) -> bool {
    matches!(
        (a, dtype),
        (ArrayAttrLiteral::Int64(_), ArrayAttrType::Int64)
            | (ArrayAttrLiteral::Int64(_), ArrayAttrType::Float64)
            | (ArrayAttrLiteral::Float64(_), ArrayAttrType::Float64)
            | (ArrayAttrLiteral::String(_), ArrayAttrType::String)
            | (ArrayAttrLiteral::Bytes(_), ArrayAttrType::Bytes)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::types::CollectionInfo;
    use crate::types_array::{ArrayAttrAst, ArrayDimAst};

    struct StubCatalog {
        view: Option<ArrayCatalogView>,
    }
    impl SqlCatalog for StubCatalog {
        fn get_collection(
            &self,
            _: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(None)
        }
        fn lookup_array(&self, _: &str) -> Option<ArrayCatalogView> {
            self.view.clone()
        }
    }

    fn view_2d() -> ArrayCatalogView {
        ArrayCatalogView {
            name: "g".into(),
            dims: vec![
                ArrayDimAst {
                    name: "chrom".into(),
                    dtype: ArrayDimType::Int64,
                    lo: crate::types_array::ArrayDomainBound::Int64(1),
                    hi: crate::types_array::ArrayDomainBound::Int64(23),
                },
                ArrayDimAst {
                    name: "pos".into(),
                    dtype: ArrayDimType::Int64,
                    lo: crate::types_array::ArrayDomainBound::Int64(0),
                    hi: crate::types_array::ArrayDomainBound::Int64(10_000_000),
                },
            ],
            attrs: vec![ArrayAttrAst {
                name: "v".into(),
                dtype: ArrayAttrType::Float64,
                nullable: true,
            }],
            tile_extents: vec![1, 1_000_000],
        }
    }

    #[test]
    fn insert_unknown_array() {
        let cat = StubCatalog { view: None };
        let ast = InsertArrayAst {
            name: "g".into(),
            rows: vec![],
        };
        assert!(plan_insert_array(&ast, &cat).is_err());
    }

    #[test]
    fn insert_arity_mismatch_rejected() {
        let cat = StubCatalog {
            view: Some(view_2d()),
        };
        let ast = InsertArrayAst {
            name: "g".into(),
            rows: vec![crate::types_array::ArrayInsertRow {
                coords: vec![ArrayCoordLiteral::Int64(1)],
                attrs: vec![ArrayAttrLiteral::Float64(1.0)],
            }],
        };
        assert!(plan_insert_array(&ast, &cat).is_err());
    }

    #[test]
    fn insert_happy() {
        let cat = StubCatalog {
            view: Some(view_2d()),
        };
        let ast = InsertArrayAst {
            name: "g".into(),
            rows: vec![crate::types_array::ArrayInsertRow {
                coords: vec![ArrayCoordLiteral::Int64(1), ArrayCoordLiteral::Int64(100)],
                attrs: vec![ArrayAttrLiteral::Float64(99.5)],
            }],
        };
        let plans = plan_insert_array(&ast, &cat).unwrap();
        assert_eq!(plans.len(), 1);
        assert!(matches!(plans[0], SqlPlan::InsertArray { .. }));
    }

    #[test]
    fn delete_happy() {
        let cat = StubCatalog {
            view: Some(view_2d()),
        };
        let ast = DeleteArrayAst {
            name: "g".into(),
            coords: vec![vec![
                ArrayCoordLiteral::Int64(1),
                ArrayCoordLiteral::Int64(100),
            ]],
        };
        let plans = plan_delete_array(&ast, &cat).unwrap();
        assert_eq!(plans.len(), 1);
        assert!(matches!(plans[0], SqlPlan::DeleteArray { .. }));
    }
}
