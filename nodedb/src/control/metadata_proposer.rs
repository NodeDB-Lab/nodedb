//! Synchronous `propose-and-wait-for-local-apply` helper for
//! replicated catalog DDL.
//!
//! This is the single entry point pgwire DDL handlers use to write a
//! `MetadataEntry` through the metadata raft group (group 0). It is
//! deliberately sync — pgwire DDL handlers today are not async, and
//! `tokio::task::block_in_place`-style workarounds inside tokio tasks
//! are a footgun. The blocking wait is implemented with
//! [`crate::control::cluster::AppliedIndexWatcher`] (a plain
//! `Condvar`), so it composes cleanly with tokio's current-thread
//! and multi-thread runtimes alike.
//!
//! Semantics:
//!
//! 1. If the node is in single-node / no-cluster mode, the proposer
//!    returns `Ok(0)` immediately. The caller's legacy direct-write
//!    path (e.g. `SystemCatalog::put_collection`) remains
//!    authoritative until batch 1c migrates pgwire handlers fully.
//! 2. If this node is the metadata-group leader, it proposes the
//!    entry, records the log index, and blocks until its local
//!    applied watermark reaches that index (5s default timeout).
//! 3. If this node is NOT the leader, the proposer returns
//!    `NodeDbError::Cluster { detail: "not metadata-group leader ..." }`.
//!    Phase C will replace this with a gateway-side redirect so every
//!    node can accept DDL. Until then, clients must target the leader.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
use crate::control::state::SharedState;
use crate::error::Error;

/// Default upper bound on how long a single `propose_metadata_and_wait`
/// call will block before returning an error.
pub const DEFAULT_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Type-erased handle for proposing to the metadata raft group.
///
/// The concrete implementation ([`RaftLoopProposerHandle`]) wraps an
/// `Arc<RaftLoop<..>>`. The trait exists so `SharedState` can store a
/// non-generic handle — the `RaftLoop` type has a pair of generic
/// parameters (applier + forwarder) that would otherwise leak into
/// `SharedState`.
pub trait MetadataRaftHandle: Send + Sync {
    /// Propose a raw encoded `MetadataEntry` to the metadata group and
    /// return its assigned log index on success.
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error>;

    /// The applied-index watcher backing this handle.
    fn watcher(&self) -> Arc<AppliedIndexWatcher>;
}

/// Concrete implementation that wraps the `nodedb-cluster` `RaftLoop`.
pub struct RaftLoopProposerHandle {
    raft_loop: Arc<
        nodedb_cluster::RaftLoop<
            crate::control::cluster::SpscCommitApplier,
            crate::control::LocalForwarder,
        >,
    >,
    watcher: OnceLock<Arc<AppliedIndexWatcher>>,
}

impl RaftLoopProposerHandle {
    pub fn new(
        raft_loop: Arc<
            nodedb_cluster::RaftLoop<
                crate::control::cluster::SpscCommitApplier,
                crate::control::LocalForwarder,
            >,
        >,
    ) -> Self {
        Self {
            raft_loop,
            watcher: OnceLock::new(),
        }
    }

    pub fn with_watcher(self, watcher: Arc<AppliedIndexWatcher>) -> Self {
        let _ = self.watcher.set(watcher);
        self
    }
}

impl MetadataRaftHandle for RaftLoopProposerHandle {
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error> {
        self.raft_loop
            .propose_to_metadata_group(bytes)
            .map_err(|e| Error::Config {
                detail: format!("metadata propose: {e}"),
            })
    }

    fn watcher(&self) -> Arc<AppliedIndexWatcher> {
        self.watcher
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(AppliedIndexWatcher::new()))
    }
}

/// Propose a `MetadataEntry` and block until the local applied-index
/// watcher confirms the entry has been applied on this node.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed on
/// `SharedState`), returns `Ok(0)` immediately — the caller is expected
/// to fall back to the legacy direct-write path.
pub fn propose_metadata_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
) -> Result<u64, Error> {
    propose_metadata_with_timeout(shared, entry, DEFAULT_PROPOSE_TIMEOUT)
}

/// Same as [`propose_metadata_and_wait`] but with an explicit timeout.
pub fn propose_metadata_with_timeout(
    shared: &SharedState,
    entry: &MetadataEntry,
    timeout: Duration,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Single-node / no-cluster mode: caller falls back to direct
        // `put_collection` / etc. Return `0` as the "no replicated
        // log index" sentinel.
        return Ok(0);
    };

    let bytes = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("metadata encode: {e}"),
    })?;

    let log_index = handle.propose(bytes)?;

    let watcher = shared.applied_index_watcher();
    // `wait_for` blocks the calling thread on a `Condvar`. When the
    // caller is already inside a tokio task (pgwire handlers always
    // are), parking the worker without telling tokio starves every
    // other task that happens to land on the same worker — including
    // the raft tick that would otherwise bump the watcher. Wrap the
    // blocking section in `block_in_place` so tokio reassigns a
    // fresh worker to keep driving the runtime.
    let timed_out = tokio::task::block_in_place(|| !watcher.wait_for(log_index, timeout));
    if timed_out {
        return Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {:?} waiting for log index {} (current: {})",
                timeout,
                log_index,
                watcher.current()
            ),
        });
    }

    Ok(log_index)
}

// ── Typed builders for CollectionDdl entries ────────────────────────────
//
// pgwire handlers build a `StoredCollection` in their existing parsing
// flow, then call these helpers to construct the matching
// `MetadataEntry::CollectionDdl` variant. Keeping the builders here
// (rather than inline in each handler) centralizes the
// StoredCollection → `CollectionDescriptor` projection so every
// collection-DDL handler produces a consistent cache view.

use nodedb_cluster::metadata_group::actions::{CollectionAction, CollectionAlter};
use nodedb_cluster::metadata_group::descriptors::collection::{
    CollectionDescriptor, ColumnDef as DescColumnDef,
};
use nodedb_cluster::metadata_group::descriptors::common::{
    DescriptorHeader, DescriptorId, DescriptorKind,
};
use nodedb_cluster::metadata_group::state::DescriptorState;
use nodedb_types::{CollectionType, Hlc, columnar::DocumentMode};

use crate::control::security::catalog::StoredCollection;

/// Build a `MetadataEntry::CollectionDdl { Create, host_payload }`
/// from a fully-populated `StoredCollection`. The returned entry is
/// the canonical input to [`propose_metadata_and_wait`] for
/// `CREATE COLLECTION`.
pub fn collection_create_entry(stored: &StoredCollection) -> Result<MetadataEntry, Error> {
    let descriptor = project_descriptor(stored);
    let host_payload = zerompk::to_msgpack_vec(stored).map_err(|e| Error::Config {
        detail: format!("encode StoredCollection: {e}"),
    })?;
    Ok(MetadataEntry::CollectionDdl {
        tenant_id: stored.tenant_id,
        action: CollectionAction::Create(Box::new(descriptor)),
        host_payload,
    })
}

/// Build a drop entry for an existing collection. The `host_payload`
/// is empty — the applier's drop branch only needs the descriptor id.
pub fn collection_drop_entry(tenant_id: u32, name: &str) -> MetadataEntry {
    MetadataEntry::CollectionDdl {
        tenant_id,
        action: CollectionAction::Drop {
            id: DescriptorId::new(tenant_id, DescriptorKind::Collection, name),
        },
        host_payload: vec![],
    }
}

/// Build an alter entry for a collection. Carries the full
/// post-alter `StoredCollection` as `host_payload` so the applier
/// overwrites the record atomically on every node — same semantics
/// as Create, just using the Alter action so the cache-side version
/// counter bumps.
pub fn collection_alter_entry(
    stored: &StoredCollection,
    change: CollectionAlter,
) -> Result<MetadataEntry, Error> {
    let host_payload = zerompk::to_msgpack_vec(stored).map_err(|e| Error::Config {
        detail: format!("encode StoredCollection: {e}"),
    })?;
    Ok(MetadataEntry::CollectionDdl {
        tenant_id: stored.tenant_id,
        action: CollectionAction::Alter {
            id: DescriptorId::new(stored.tenant_id, DescriptorKind::Collection, &stored.name),
            change,
        },
        host_payload,
    })
}

fn project_descriptor(stored: &StoredCollection) -> CollectionDescriptor {
    let id = DescriptorId::new(stored.tenant_id, DescriptorKind::Collection, &stored.name);
    let header = DescriptorHeader {
        id,
        version: 1,
        modification_time: Hlc::new(stored.created_at.saturating_mul(1_000_000_000), 0),
        state: DescriptorState::Public,
    };
    let (collection_type_str, columns, primary_key) = project_collection_type(stored);
    CollectionDescriptor {
        header,
        collection_type: collection_type_str,
        columns,
        with_options: vec![],
        primary_key,
    }
}

fn project_collection_type(
    stored: &StoredCollection,
) -> (String, Vec<DescColumnDef>, Option<String>) {
    match &stored.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            let cols = schema
                .columns
                .iter()
                .map(|c| DescColumnDef {
                    name: c.name.clone(),
                    data_type: c.column_type.to_string(),
                    nullable: c.nullable,
                    default: c.default.clone(),
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            ("document_strict".into(), cols, pk)
        }
        CollectionType::Document(DocumentMode::Schemaless) => {
            let cols = stored
                .fields
                .iter()
                .map(|(name, ty)| DescColumnDef {
                    name: name.clone(),
                    data_type: ty.clone(),
                    nullable: true,
                    default: None,
                })
                .collect();
            ("document_schemaless".into(), cols, Some("id".into()))
        }
        CollectionType::KeyValue(config) => {
            let cols = config
                .schema
                .columns
                .iter()
                .map(|c| DescColumnDef {
                    name: c.name.clone(),
                    data_type: c.column_type.to_string(),
                    nullable: c.nullable,
                    default: c.default.clone(),
                })
                .collect();
            let pk = config
                .schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone())
                .or_else(|| Some("key".into()));
            ("key_value".into(), cols, pk)
        }
        CollectionType::Columnar(profile) => {
            let cols = stored
                .fields
                .iter()
                .map(|(name, ty)| DescColumnDef {
                    name: name.clone(),
                    data_type: ty.clone(),
                    nullable: true,
                    default: None,
                })
                .collect();
            let type_str = if profile.is_timeseries() {
                "timeseries"
            } else if profile.is_spatial() {
                "spatial"
            } else {
                "columnar"
            };
            let pk = if profile.is_timeseries() {
                None
            } else {
                Some("id".into())
            };
            (type_str.into(), cols, pk)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_helper_returns_true_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(1)));
    }

    #[test]
    fn collection_create_entry_roundtrips_host_payload() {
        let stored = StoredCollection::new(42, "orders", "alice");
        let entry = collection_create_entry(&stored).expect("build");
        match &entry {
            MetadataEntry::CollectionDdl {
                tenant_id,
                action: CollectionAction::Create(desc),
                host_payload,
            } => {
                assert_eq!(*tenant_id, 42);
                assert_eq!(desc.header.id.name, "orders");
                assert_eq!(desc.collection_type, "document_schemaless");
                // host_payload roundtrips to the same StoredCollection.
                let decoded: StoredCollection =
                    zerompk::from_msgpack(host_payload).expect("decode");
                assert_eq!(decoded.name, "orders");
                assert_eq!(decoded.owner, "alice");
                assert_eq!(decoded.tenant_id, 42);
            }
            other => panic!("expected Create, got {other:?}"),
        }
    }

    #[test]
    fn collection_drop_entry_has_empty_payload() {
        let entry = collection_drop_entry(1, "users");
        match entry {
            MetadataEntry::CollectionDdl {
                action: CollectionAction::Drop { id },
                host_payload,
                ..
            } => {
                assert_eq!(id.name, "users");
                assert!(host_payload.is_empty());
            }
            other => panic!("expected Drop, got {other:?}"),
        }
    }
}
