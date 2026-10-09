// SPDX-License-Identifier: BUSL-1.1

//! Typed errors of a data-group snapshot install.

/// The step of an install that runs after every core acknowledged its share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleStep {
    /// Rebinding the snapshot's PK→surrogate identities in the catalog.
    SurrogateRebind,
    /// Persisting the group's tenant write marks.
    WriteMarks,
    /// Persisting the proposal keys of the entries the snapshot covers.
    ProposalKeys,
    /// Holding the snapshot's trigger actions and messages, and raising
    /// their cursors.
    EventLane,
    /// Replacing the group vShards' Calvin applied state and base.
    CalvinState,
    /// Replacing the group's open chunked redo streams.
    RedoStreams,
}

impl std::fmt::Display for SettleStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::SurrogateRebind => "surrogate rebind",
            Self::WriteMarks => "write-mark persist",
            Self::ProposalKeys => "proposal-key persist",
            Self::EventLane => "event lane install",
            Self::CalvinState => "Calvin state install",
            Self::RedoStreams => "redo stream install",
        })
    }
}

/// Why a data-group snapshot install did not complete.
///
/// Every variant leaves the Raft boundary and durable applied floor where
/// they were, and the staged install on disk. A re-install of the same
/// snapshot clears each core before it installs, so it converges from any
/// partial state. [`Self::is_retryable`] names the variants a re-install of
/// the same bytes can clear.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotInstallError {
    #[error("snapshot install of group {group_id}: the routing table lock is poisoned")]
    RoutingPoisoned { group_id: u64 },

    #[error(
        "snapshot install of group {group_id}: the snapshot needs this node's metadata group \
         applied through index {floor}, and it applied through {applied}; the leader resends \
         the snapshot"
    )]
    MetadataBehind {
        group_id: u64,
        floor: u64,
        applied: u64,
    },

    #[error("snapshot install of group {group_id}: the payload does not decode: {detail}")]
    Decode { group_id: u64, detail: String },

    #[error(
        "snapshot install of group {group_id}: {section} key starting {key_prefix:?} names no \
         routable collection or endpoint"
    )]
    UnroutableKey {
        group_id: u64,
        section: &'static str,
        key_prefix: String,
    },

    #[error("snapshot install of group {group_id}: vShard {vshard} routes to no local core")]
    NoCoreForVShard { group_id: u64, vshard: u32 },

    #[error(
        "snapshot install of group {group_id}: the snapshot carries no Calvin cut, so this \
         node cannot tell which Calvin transactions its storage holds; the leader resends a \
         snapshot built with one"
    )]
    NoCalvinCut { group_id: u64 },

    #[error("snapshot install of group {group_id}: encode the share of core {core_id}: {detail}")]
    Encode {
        group_id: u64,
        core_id: usize,
        detail: String,
    },

    #[error("snapshot install of group {group_id}: read the local catalog: {source}")]
    Catalog {
        group_id: u64,
        #[source]
        source: crate::Error,
    },

    #[error("snapshot install of group {group_id}: append the WAL install barrier: {source}")]
    Barrier {
        group_id: u64,
        #[source]
        source: crate::Error,
    },

    #[error(
        "snapshot install of group {group_id}: core {core_id} did not install its share: {source}"
    )]
    CoreInstall {
        group_id: u64,
        core_id: usize,
        #[source]
        source: crate::Error,
    },

    #[error(
        "snapshot install of group {group_id}: {step} after the core installs failed: {source}"
    )]
    Settle {
        group_id: u64,
        step: SettleStep,
        #[source]
        source: crate::Error,
    },
}

impl SnapshotInstallError {
    /// True when a re-install of the same snapshot bytes can succeed: the
    /// error came from local state, not from the payload.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::MetadataBehind { .. }
            | Self::Catalog { .. }
            | Self::Barrier { .. }
            | Self::CoreInstall { .. }
            | Self::Settle { .. } => true,
            Self::RoutingPoisoned { .. }
            | Self::Decode { .. }
            | Self::UnroutableKey { .. }
            | Self::NoCoreForVShard { .. }
            | Self::NoCalvinCut { .. }
            | Self::Encode { .. } => false,
        }
    }
}

impl From<SnapshotInstallError> for crate::Error {
    fn from(e: SnapshotInstallError) -> Self {
        match e {
            SnapshotInstallError::Catalog { source, .. }
            | SnapshotInstallError::Barrier { source, .. }
            | SnapshotInstallError::CoreInstall { source, .. }
            | SnapshotInstallError::Settle { source, .. } => source,
            other @ (SnapshotInstallError::RoutingPoisoned { .. }
            | SnapshotInstallError::MetadataBehind { .. }
            | SnapshotInstallError::NoCoreForVShard { .. }) => crate::Error::Dispatch {
                detail: other.to_string(),
            },
            other @ (SnapshotInstallError::Decode { .. }
            | SnapshotInstallError::UnroutableKey { .. }
            | SnapshotInstallError::NoCalvinCut { .. }
            | SnapshotInstallError::Encode { .. }) => crate::Error::Codec {
                detail: other.to_string(),
            },
        }
    }
}
