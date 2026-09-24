// SPDX-License-Identifier: BUSL-1.1

//! Engine side effects of transactional index DDL, held until COMMIT.
//!
//! Index DDL inside an explicit transaction buffers its catalog entries (see
//! [`super::ddl_buffer`]). Some index kinds also change Data Plane state: a
//! secondary index is backfilled from existing rows, a dropped one is purged,
//! a full-text index binds or resets its analyzer, a sorted index builds
//! or drops its order-statistic tree, and a key-value collection's index is
//! built or dropped in the KV engine. That engine
//! work must follow the catalog: it runs at COMMIT, once the buffered entries
//! landed, and never after a ROLLBACK. Each effect rides on the buffered entry
//! its statement wrote last, so a savepoint rollback drops it with the entry.

use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::PhysicalPlan;

/// One engine side effect a buffered index statement owes at COMMIT.
#[derive(Debug, Clone)]
pub enum DeferredDdlEffect {
    /// Backfill a secondary index the transaction created, then mark it
    /// `Ready`.
    SecondaryIndexBuild(SecondaryIndexBuild),
    /// Apply an index's engine configuration, such as a full-text analyzer
    /// binding. A refusal fails the COMMIT with `sqlstate`.
    EngineApply {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
        plan: PhysicalPlan,
        sqlstate: String,
        context: String,
    },
    /// Remove a dropped index's engine state: a secondary index's entries,
    /// or a full-text collection's analyzer binding.
    IndexTeardown {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
        plan: PhysicalPlan,
    },
    /// Build a sorted index's tree on the core that owns its collection.
    SortedIndexRegister {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
        plan: PhysicalPlan,
    },
    /// Drop a sorted index's tree.
    SortedIndexDrop {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
        index_name: String,
    },
    /// Drop a key-value collection's secondary index on `field` from the KV
    /// engine.
    KvIndexDrop {
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: String,
        field: String,
    },
}

/// A secondary index a transaction created in the `Building` state.
#[derive(Debug, Clone)]
pub struct SecondaryIndexBuild {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: String,
    pub index_name: String,
    /// The path the index extracts, without an array suffix.
    pub extraction_path: String,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<String>,
}
