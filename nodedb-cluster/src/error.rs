use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClusterError>;

#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("raft error: {0}")]
    Raft(#[from] nodedb_raft::RaftError),

    #[error("vshard {vshard_id} not mapped to any raft group")]
    VShardNotMapped { vshard_id: u16 },

    #[error("raft group {group_id} not found on this node")]
    GroupNotFound { group_id: u64 },

    #[error("migration in progress for vshard {vshard_id}")]
    MigrationInProgress { vshard_id: u16 },

    #[error("migration refused: estimated pause {estimated_us}µs exceeds budget {budget_us}µs")]
    MigrationPauseBudgetExceeded { estimated_us: u64, budget_us: u64 },

    #[error("node {node_id} not reachable")]
    NodeUnreachable { node_id: u64 },

    #[error("ghost stub not found: node={node_id} on shard={shard_id}")]
    GhostNotFound { node_id: String, shard_id: u16 },

    #[error("transport error: {detail}")]
    Transport { detail: String },

    #[error("codec error: {detail}")]
    Codec { detail: String },
}
