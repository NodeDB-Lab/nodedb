//! Query operations (joins, aggregates) dispatched to the Data Plane.

/// Query-level physical operations (joins, aggregates).
#[derive(Debug, Clone)]
pub enum QueryOp {
    /// Aggregate: GROUP BY + aggregate functions.
    Aggregate {
        collection: String,
        group_by: Vec<String>,
        aggregates: Vec<(String, String)>,
        filters: Vec<u8>,
        /// HAVING predicates applied post-aggregation.
        having: Vec<u8>,
        limit: usize,
        sub_group_by: Vec<String>,
        sub_aggregates: Vec<(String, String)>,
    },

    /// Partial aggregate: each core computes locally, Control Plane merges.
    PartialAggregate {
        collection: String,
        group_by: Vec<String>,
        aggregates: Vec<(String, String)>,
        filters: Vec<u8>,
    },

    /// Hash join: build hash map on right, probe with left.
    HashJoin {
        left_collection: String,
        right_collection: String,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
    },

    /// Broadcast join: small side serialized in the plan.
    BroadcastJoin {
        large_collection: String,
        broadcast_data: Vec<u8>,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
    },

    /// Shuffle join: repartition by join key via SPSC.
    ShuffleJoin {
        left_collection: String,
        right_collection: String,
        on: Vec<(String, String)>,
        join_type: String,
        limit: usize,
        target_core: usize,
    },

    /// Nested loop join: fallback for non-equi joins.
    NestedLoopJoin {
        left_collection: String,
        right_collection: String,
        /// Join condition as serialized `Vec<ScanFilter>`.
        condition: Vec<u8>,
        join_type: String,
        limit: usize,
    },
}
