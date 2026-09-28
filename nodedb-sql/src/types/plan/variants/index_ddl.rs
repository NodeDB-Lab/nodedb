// SPDX-License-Identifier: Apache-2.0

//! Index DDL plan payloads.

/// Payload of [`SqlPlan::CreateIndex`](crate::types::SqlPlan::CreateIndex).
#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    /// Name of the index. `None` requests an auto-generated name.
    pub index_name: Option<String>,
    /// Target collection.
    pub collection: String,
    /// Indexed field path.
    pub field: String,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// `IF NOT EXISTS` — succeed silently if the index already exists.
    pub if_not_exists: bool,
    /// Case-insensitive string collation (`COLLATE NOCASE`).
    pub case_insensitive: bool,
}

/// Payload of [`SqlPlan::DropIndex`](crate::types::SqlPlan::DropIndex).
#[derive(Debug, Clone)]
pub struct DropIndexPlan {
    /// Name of the index.
    pub index_name: String,
    /// Target collection (may be inferred from the index catalog).
    pub collection: Option<String>,
    /// `IF EXISTS` — succeed silently if the index does not exist.
    pub if_exists: bool,
}
