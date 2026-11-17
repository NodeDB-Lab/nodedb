// SPDX-License-Identifier: Apache-2.0

//! The rows of the text history a scan visits and a purge removes.

use nodedb_types::Surrogate;

use crate::scope::IndexScope;

/// One ended version a scan visits, without its tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndedRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub index: IndexScope<'a>,
    pub surrogate: Surrogate,
    pub sys_from: i64,
    pub sys_to: i64,
}

impl EndedRow<'_> {
    /// The owned key of this row.
    pub fn to_key(&self) -> EndedKey {
        EndedKey {
            database_id: self.database_id,
            tid: self.tid,
            collection: self.index.collection().to_string(),
            field_key: self.index.field_key().to_string(),
            surrogate: self.surrogate,
            sys_from: self.sys_from,
            sys_to: self.sys_to,
        }
    }
}

/// One live start a scan visits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRow<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub index: IndexScope<'a>,
    pub surrogate: Surrogate,
    pub sys_from: i64,
}

impl LiveRow<'_> {
    /// The owned key of this row.
    pub fn to_key(&self) -> LiveKey {
        LiveKey {
            database_id: self.database_id,
            tid: self.tid,
            collection: self.index.collection().to_string(),
            field_key: self.index.field_key().to_string(),
            surrogate: self.surrogate,
            sys_from: self.sys_from,
        }
    }
}

/// The owned key of one ended version. `field_key` is empty for the
/// whole-document index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EndedKey {
    pub database_id: u64,
    pub tid: u64,
    pub collection: String,
    pub field_key: String,
    pub surrogate: Surrogate,
    pub sys_from: i64,
    pub sys_to: i64,
}

impl EndedKey {
    /// The index that holds the version.
    pub fn index(&self) -> IndexScope<'_> {
        IndexScope::from_key(&self.collection, &self.field_key)
    }
}

/// The owned key of one live start. `field_key` is empty for the
/// whole-document index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveKey {
    pub database_id: u64,
    pub tid: u64,
    pub collection: String,
    pub field_key: String,
    pub surrogate: Surrogate,
    pub sys_from: i64,
}

impl LiveKey {
    /// The index that holds the start.
    pub fn index(&self) -> IndexScope<'_> {
        IndexScope::from_key(&self.collection, &self.field_key)
    }
}
