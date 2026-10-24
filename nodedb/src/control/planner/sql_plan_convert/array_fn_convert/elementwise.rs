// SPDX-License-Identifier: BUSL-1.1

//! ARRAY_ELEMENTWISE → PhysicalPlan::ClusterArray(Elementwise) or
//! Array(Elementwise).
//!
//! A cell routes by its tile key, and the tile key is a function of the
//! schema's dims and tile extents alone. Two arrays with one schema hash and
//! one `prefix_bits` therefore place each coordinate's cells on one shard,
//! and every shard pairs its own cells. Arrays whose routing differs keep a
//! coordinate's two cells on different shards, so they are refused.

use nodedb_array::types::ArrayId;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types_array::ArrayBinaryOpAst;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{decode_schema, load_entry, map_binary_op};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// One `ARRAY_ELEMENTWISE` call as the planner names it.
pub(crate) struct ElementwiseCall<'a> {
    pub left_name: &'a str,
    pub right_name: &'a str,
    pub op: ArrayBinaryOpAst,
    pub attr: &'a str,
    /// The `AS OF` scope both operands are read at.
    pub temporal: TemporalScope,
}

pub(crate) fn convert_elementwise(
    call: ElementwiseCall<'_>,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let ElementwiseCall {
        left_name,
        right_name,
        op,
        attr,
        temporal,
    } = call;
    let (system_time, valid_at_ms) =
        super::helpers::resolve_array_temporal(temporal, "ARRAY_ELEMENTWISE")?;
    let lentry = load_entry(left_name, tenant_id, ctx)?;
    let rentry = load_entry(right_name, tenant_id, ctx)?;
    let lschema = decode_schema(&lentry)?;
    let rschema = decode_schema(&rentry)?;
    if lentry.schema_hash != rentry.schema_hash {
        return Err(crate::Error::PlanError {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left_name}' and '{right_name}' have different \
                 schemas; both need the same dims, attrs, tile extents and orders"
            ),
        });
    }
    if lentry.prefix_bits != rentry.prefix_bits {
        return Err(crate::Error::PlanError {
            detail: format!(
                "ARRAY_ELEMENTWISE: arrays '{left_name}' (prefix_bits {}) and '{right_name}' \
                 (prefix_bits {}) route their tiles differently; create both with the same \
                 prefix_bits",
                lentry.prefix_bits, rentry.prefix_bits
            ),
        });
    }
    let attr_idx = lschema
        .attrs
        .iter()
        .position(|a| a.name == attr)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_ELEMENTWISE: array '{left_name}' has no attr '{attr}'"),
        })? as u32;
    if !rschema.attrs.iter().any(|a| a.name == attr) {
        return Err(crate::Error::PlanError {
            detail: format!("ARRAY_ELEMENTWISE: array '{right_name}' has no attr '{attr}'"),
        });
    }
    let left = ArrayId::in_database(tenant_id, ctx.database_id, left_name);
    let right = ArrayId::in_database(tenant_id, ctx.database_id, right_name);
    let vshard = ctx.collection_key(left_name).vshard();
    let op = map_binary_op(op);
    let plan = if ctx.cluster_enabled {
        PhysicalPlan::ClusterArray(ClusterArrayOp::Elementwise {
            left,
            right,
            op,
            attr_idx,
            prefix_bits: lentry.prefix_bits,
            system_time,
            valid_at_ms,
        })
    } else {
        PhysicalPlan::Array(ArrayOp::Elementwise {
            left,
            right,
            op,
            attr_idx,
            cell_filter: None,
            shard_bucket: None,
            system_time,
            valid_at_ms,
        })
    };
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::array_catalog::{ArrayCatalog, ArrayCatalogEntry};
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    /// A catalog holding `(name, schema_hash, prefix_bits)` arrays of one
    /// 1-D shape.
    fn make_ctx(cluster_enabled: bool, arrays: &[(&str, u64, u8)]) -> ConvertContext {
        let schema = ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "x".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v".to_string(), AttrType::Float64, true))
            .tile_extents(vec![4])
            .build()
            .expect("schema build");
        let schema_msgpack = zerompk::to_msgpack_vec(&schema).expect("encode");
        let handle = ArrayCatalog::handle();
        {
            let mut cat = handle.write().expect("lock");
            for &(name, schema_hash, prefix_bits) in arrays {
                cat.register(ArrayCatalogEntry {
                    array_id: ArrayId::new(TenantId::new(1), name),
                    name: name.into(),
                    schema_msgpack: schema_msgpack.clone(),
                    schema_hash,
                    created_at_ms: 0,
                    prefix_bits,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                    modification_hlc: nodedb_types::Hlc::ZERO,
                    incarnation: nodedb_types::Hlc::ZERO,
                })
                .expect("register");
            }
        }
        ConvertContext {
            purpose: super::super::super::convert::PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: Some(handle),
            credentials: None,
            wal: None,
            surrogate_assigner:
                crate::control::planner::sql_plan_convert::test_support::test_assigner(),
            cluster_enabled,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 8 * 1024 * 1024,
            shuffle_agg_threshold: 10_000,
            prefetched: Default::default(),
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: crate::types::TenantId::new(0),
        }
    }

    fn convert_at(
        ctx: &ConvertContext,
        temporal: TemporalScope,
    ) -> crate::Result<Vec<PhysicalTask>> {
        convert_elementwise(
            ElementwiseCall {
                left_name: "l",
                right_name: "r",
                op: ArrayBinaryOpAst::Add,
                attr: "v",
                temporal,
            },
            TenantId::new(1),
            ctx,
        )
    }

    fn convert(ctx: &ConvertContext) -> crate::Result<Vec<PhysicalTask>> {
        convert_at(ctx, TemporalScope::default())
    }

    fn as_of(system_ms: i64, valid_ms: i64) -> TemporalScope {
        TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AsOf(system_ms),
            valid_time: nodedb_sql::temporal::ValidTime::At(valid_ms),
        }
    }

    #[test]
    fn a_cluster_op_carries_the_as_of_scope() {
        let ctx = make_ctx(true, &[("l", 7, 8), ("r", 7, 8)]);
        let tasks = convert_at(&ctx, as_of(1_000, 2_000)).expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::ClusterArray(ClusterArrayOp::Elementwise {
                system_time: nodedb_types::SystemTimeScope::AsOf(1_000),
                valid_at_ms: Some(2_000),
                ..
            })
        ));
    }

    #[test]
    fn a_local_op_carries_the_as_of_scope() {
        let ctx = make_ctx(false, &[("l", 7, 8), ("r", 7, 8)]);
        let tasks = convert_at(&ctx, as_of(1_000, 2_000)).expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Array(ArrayOp::Elementwise {
                system_time: nodedb_types::SystemTimeScope::AsOf(1_000),
                valid_at_ms: Some(2_000),
                ..
            })
        ));
    }

    #[test]
    fn all_system_versions_are_refused() {
        let ctx = make_ctx(true, &[("l", 7, 8), ("r", 7, 8)]);
        let temporal = TemporalScope {
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            valid_time: nodedb_sql::temporal::ValidTime::Any,
        };
        let err = convert_at(&ctx, temporal).expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("not meaningful for ARRAY_ELEMENTWISE")),
            "{err:?}"
        );
    }

    #[test]
    fn a_cluster_plans_a_fan_out_with_the_shared_routing() {
        let ctx = make_ctx(true, &[("l", 7, 8), ("r", 7, 8)]);
        let tasks = convert(&ctx).expect("convert");
        match &tasks[0].plan {
            PhysicalPlan::ClusterArray(ClusterArrayOp::Elementwise {
                left,
                right,
                prefix_bits,
                attr_idx,
                ..
            }) => {
                assert_eq!((left.name.as_str(), right.name.as_str()), ("l", "r"));
                assert_eq!(*prefix_bits, 8);
                assert_eq!(*attr_idx, 0);
            }
            other => panic!("expected ClusterArray Elementwise, got {other:?}"),
        }
    }

    #[test]
    fn a_single_node_plans_a_local_op() {
        let ctx = make_ctx(false, &[("l", 7, 8), ("r", 7, 8)]);
        let tasks = convert(&ctx).expect("convert");
        assert!(matches!(
            &tasks[0].plan,
            PhysicalPlan::Array(ArrayOp::Elementwise {
                shard_bucket: None,
                ..
            })
        ));
    }

    #[test]
    fn arrays_with_different_routing_are_refused() {
        let ctx = make_ctx(true, &[("l", 7, 8), ("r", 7, 10)]);
        let err = convert(&ctx).expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("prefix_bits")),
            "{err:?}"
        );
    }

    #[test]
    fn arrays_with_different_schemas_are_refused() {
        let ctx = make_ctx(true, &[("l", 7, 8), ("r", 9, 8)]);
        let err = convert(&ctx).expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("different schemas")),
            "{err:?}"
        );
    }
}
