// SPDX-License-Identifier: BUSL-1.1

//! The cells of a `RETURNING` list whose items cannot fail, and whether one
//! row of them fits the response limits.

use nodedb_cluster::calvin::types::AnnouncedColumn;
use nodedb_query::expr::SqlExpr;
use nodedb_types::{ReturningColumns, ReturningItem};

use super::payload_bound::{column_cell_bytes, envelope_bytes, literal_cell_bytes, string_bytes};
use super::precommit::ReturningLimits;

/// The fields a `*` expands to on the target collection's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFields {
    /// A strict collection's declared columns: their count and the summed
    /// bytes of their names.
    Declared { count: usize, name_bytes: usize },
    /// A schemaless collection: no cap on a row's fields.
    Unbounded,
}

impl RowFields {
    /// The fields of a strict collection whose columns are `names`.
    pub fn declared<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        let (count, name_bytes) = names
            .into_iter()
            .fold((0usize, 0usize), |(count, bytes), name| {
                (count.saturating_add(1), bytes.saturating_add(name.len()))
            });
        Self::Declared { count, name_bytes }
    }
}

/// The cells of a list whose items cannot fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ListShape {
    /// Each column or literal item: its output name's bytes and the most
    /// bytes its cell renders to.
    fixed: Vec<(usize, usize)>,
    /// How many `*` items the list holds.
    stars: usize,
}

impl ListShape {
    /// The shape of `list`. `None` when an item can fail.
    pub(super) fn of(list: &ReturningColumns) -> Option<Self> {
        let items = match list {
            ReturningColumns::Star => {
                return Some(Self {
                    fixed: Vec::new(),
                    stars: 1,
                });
            }
            ReturningColumns::Named(items) => items,
        };
        let mut shape = Self {
            fixed: Vec::with_capacity(items.len()),
            stars: 0,
        };
        for item in items {
            match item {
                ReturningItem::Star => shape.stars += 1,
                ReturningItem::Column { name, alias } => shape
                    .fixed
                    .push((alias.as_ref().unwrap_or(name).len(), column_cell_bytes())),
                // Any other expression can raise an error. An item that does
                // not decode fails its evaluation.
                ReturningItem::Computed { expression, name } => {
                    match zerompk::from_msgpack::<SqlExpr>(expression) {
                        Ok(SqlExpr::Literal(literal)) => shape
                            .fixed
                            .push((name.len(), literal_cell_bytes(&literal)?)),
                        Ok(_) | Err(_) => return None,
                    }
                }
            }
        }
        Some(shape)
    }

    /// Whether one row of this shape fits `limits`, where a `*` expands to
    /// `fields` and the columns carry the `announced` names when given.
    ///
    /// The payload of one row is at most `envelope + Σ names + Σ literals +
    /// columns × cell` (see `payload_bound`).
    pub(super) fn fits_one_row(
        &self,
        limits: &ReturningLimits,
        fields: RowFields,
        announced: Option<&[AnnouncedColumn]>,
    ) -> bool {
        if limits.rows < 1 {
            return false;
        }
        let (star_fields, star_name_bytes) = match (self.stars, fields) {
            (0, _) => (0, 0),
            (_, RowFields::Declared { count, name_bytes }) => (count, name_bytes),
            (_, RowFields::Unbounded) => {
                return limits.columns == usize::MAX
                    && limits.cells == usize::MAX
                    && limits.payload_bytes == usize::MAX;
            }
        };
        let Some(columns) = self
            .stars
            .checked_mul(star_fields)
            .and_then(|expanded| expanded.checked_add(self.fixed.len()))
        else {
            return false;
        };
        if columns > limits.columns || columns > limits.cells {
            return false;
        }
        let names = match announced {
            Some(announced) if announced.len() != columns => return false,
            Some(announced) => announced
                .iter()
                .map(|column| string_bytes(column.name.len()))
                .fold(0usize, usize::saturating_add),
            None => {
                self.fixed
                    .iter()
                    .map(|(name, _)| string_bytes(*name))
                    .fold(0usize, usize::saturating_add)
                    .saturating_add(self.stars.saturating_mul(
                        star_name_bytes.saturating_add(star_fields.saturating_mul(5)),
                    ))
            }
        };
        let cells = self
            .fixed
            .iter()
            .map(|(_, cell)| *cell)
            .fold(0usize, usize::saturating_add)
            .saturating_add(
                self.stars
                    .saturating_mul(star_fields)
                    .saturating_mul(column_cell_bytes()),
            );
        envelope_bytes(columns)
            .saturating_add(names)
            .saturating_add(cells)
            <= limits.payload_bytes
    }
}
