use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};

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

    /// When true, results from this task batch must be deduplicated
    /// (e.g. UNION DISTINCT across multiple sub-queries).
    pub post_dedup: bool,
}
