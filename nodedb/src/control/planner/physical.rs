use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};

/// Post-execution set operation for merging multi-task results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostSetOp {
    /// No post-merge (single task or independent tasks).
    None,
    /// UNION DISTINCT: deduplicate rows across sub-queries.
    UnionDistinct,
    /// INTERSECT: keep rows that appear in all sub-query results.
    Intersect,
    /// INTERSECT ALL: keep rows appearing in both (with multiplicity).
    IntersectAll,
    /// EXCEPT: keep rows from first that don't appear in second.
    Except,
    /// EXCEPT ALL: keep rows from first not in second (with multiplicity).
    ExceptAll,
}

/// A physical execution task ready for dispatch to the Data Plane.
///
/// The planner produces these after converting a DataFusion logical plan
/// into a concrete physical operation targeting a specific vShard.
#[derive(Debug, Clone)]
pub struct PhysicalTask {
    /// Target tenant.
    pub tenant_id: TenantId,

    /// Target vShard (determines which Data Plane core handles this).
    pub vshard_id: VShardId,

    /// The physical operation to execute.
    pub plan: PhysicalPlan,

    /// Post-execution merge operation for set operations (UNION, INTERSECT, EXCEPT).
    pub post_set_op: PostSetOp,
}
