//! Planner for `CREATE ARRAY` and `DROP ARRAY`.
//!
//! Validation is engine-agnostic: name non-empty, dims non-empty, attrs
//! non-empty, `tile_extents.len() == dims.len()`, no duplicate dim/attr
//! names, and per-dim domain bounds are well formed (lo <= hi). The
//! schema-hash + the typed `ArraySchema` are computed in the Origin
//! converter where `nodedb-array` is available.

use std::collections::HashSet;

use crate::error::{Result, SqlError};
use crate::parser::array_stmt::{AlterArrayAst, CreateArrayAst, DropArrayAst};
use crate::types::SqlPlan;
use crate::types_array::{ArrayDimType, ArrayDomainBound};

pub fn plan_create_array(ast: &CreateArrayAst) -> Result<SqlPlan> {
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
    Ok(SqlPlan::CreateArray {
        name: ast.name.clone(),
        dims: ast.dims.clone(),
        attrs: ast.attrs.clone(),
        tile_extents: ast.tile_extents.clone(),
        cell_order: ast.cell_order,
        tile_order: ast.tile_order,
        prefix_bits: ast.prefix_bits,
        audit_retain_ms: ast.audit_retain_ms,
        minimum_audit_retain_ms: ast.minimum_audit_retain_ms,
    })
}

pub fn plan_alter_array(ast: &AlterArrayAst) -> Result<SqlPlan> {
    if ast.name.is_empty() {
        return Err(SqlError::Parse {
            detail: "ALTER NDARRAY: array name must not be empty".into(),
        });
    }
    if ast.set.is_empty() {
        return Err(SqlError::Parse {
            detail: format!("ALTER NDARRAY {}: SET clause is empty", ast.name),
        });
    }

    let mut audit_retain_ms: Option<Option<i64>> = None;
    let mut minimum_audit_retain_ms: Option<u64> = None;

    for (key, value) in &ast.set {
        match key.as_str() {
            "audit_retain_ms" => {
                audit_retain_ms = Some(*value);
            }
            "minimum_audit_retain_ms" => {
                let n = value.ok_or_else(|| SqlError::Parse {
                    detail: format!(
                        "ALTER NDARRAY {}: minimum_audit_retain_ms cannot be NULL",
                        ast.name
                    ),
                })?;
                minimum_audit_retain_ms = Some(n as u64);
            }
            other => {
                return Err(SqlError::Parse {
                    detail: format!(
                        "ALTER NDARRAY {}: unknown SET key `{other}`; \
                         expected `audit_retain_ms` or `minimum_audit_retain_ms`",
                        ast.name
                    ),
                });
            }
        }
    }

    Ok(SqlPlan::AlterArray {
        name: ast.name.clone(),
        audit_retain_ms,
        minimum_audit_retain_ms,
    })
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
    use crate::types_array::{ArrayAttrAst, ArrayAttrType, ArrayDimAst};

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
            }],
            tile_extents: vec![4],
            cell_order: Default::default(),
            tile_order: Default::default(),
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        }
    }

    #[test]
    fn happy_path() {
        let plan = plan_create_array(&ok_ast()).unwrap();
        assert!(matches!(plan, SqlPlan::CreateArray { .. }));
    }

    #[test]
    fn rejects_extent_arity() {
        let mut a = ok_ast();
        a.tile_extents = vec![1, 2];
        assert!(plan_create_array(&a).is_err());
    }

    #[test]
    fn rejects_dup_dim() {
        let mut a = ok_ast();
        a.dims.push(a.dims[0].clone());
        a.tile_extents = vec![4, 4];
        assert!(plan_create_array(&a).is_err());
    }

    #[test]
    fn rejects_lo_gt_hi() {
        let mut a = ok_ast();
        a.dims[0].lo = ArrayDomainBound::Int64(5);
        a.dims[0].hi = ArrayDomainBound::Int64(1);
        assert!(plan_create_array(&a).is_err());
    }
}
