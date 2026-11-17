// SPDX-License-Identifier: Apache-2.0

//! Stored-column extraction for a write response.

/// A single item in a RETURNING projection list.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReturningItem {
    /// All stored fields at this position in the output list.
    Star,
    /// A stored field and its optional display alias.
    Column { name: String, alias: Option<String> },
    /// A serialized query expression evaluated against the prepared row.
    Computed { expression: Vec<u8>, name: String },
}

impl ReturningItem {
    pub fn output_name(&self) -> &str {
        match self {
            Self::Star => "*",
            Self::Column { name, alias } => alias.as_deref().unwrap_or(name),
            Self::Computed { name, .. } => name,
        }
    }
}

/// The set of columns projected by a RETURNING clause.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReturningColumns {
    /// `RETURNING *` — all fields in the collection.
    Star,
    /// `RETURNING col1, col2 [AS alias], ...`
    Named(Vec<ReturningItem>),
}

/// The columns a write plan reads from stored rows, and optionally the full
/// response list evaluated against prepared rows before source commit.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReturningSpec {
    /// The columns the Data Plane returns from the stored rows.
    pub columns: ReturningColumns,
    /// The complete response list the precommit evaluation runs, in output
    /// order. `None` on a spec that names stored columns only.
    pub precommit: Option<ReturningColumns>,
}

impl ReturningSpec {
    /// Return the output column names (alias when present, else name).
    pub fn output_names(&self) -> Option<Vec<String>> {
        match &self.columns {
            ReturningColumns::Star => None,
            ReturningColumns::Named(items)
                if items.iter().any(|item| matches!(item, ReturningItem::Star)) =>
            {
                None
            }
            ReturningColumns::Named(items) => Some(
                items
                    .iter()
                    .map(|item| item.output_name().to_owned())
                    .collect(),
            ),
        }
    }

    /// Return the source column names (ignoring aliases) for field extraction.
    pub fn source_names(&self) -> Option<Vec<String>> {
        match &self.columns {
            ReturningColumns::Star => None,
            ReturningColumns::Named(items) => items
                .iter()
                .map(|item| match item {
                    ReturningItem::Column { name, .. } => Some(name.clone()),
                    ReturningItem::Computed { .. } | ReturningItem::Star => None,
                })
                .collect(),
        }
    }
}
