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
    /// How many of `fixed` are columns.
    column_items: usize,
    /// The summed exact cell bytes of the literals in `fixed`.
    literal_bytes: usize,
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
                    column_items: 0,
                    literal_bytes: 0,
                    stars: 1,
                });
            }
            ReturningColumns::Named(items) => items,
        };
        let mut shape = Self {
            fixed: Vec::with_capacity(items.len()),
            column_items: 0,
            literal_bytes: 0,
            stars: 0,
        };
        for item in items {
            match item {
                ReturningItem::Star => shape.stars += 1,
                ReturningItem::Column { name, alias } => {
                    shape.column_items += 1;
                    shape
                        .fixed
                        .push((alias.as_ref().unwrap_or(name).len(), column_cell_bytes()));
                }
                // Any other expression can raise an error. An item that does
                // not decode fails its evaluation.
                ReturningItem::Computed { expression, name } => {
                    match zerompk::from_msgpack::<SqlExpr>(expression) {
                        Ok(SqlExpr::Literal(literal)) => {
                            let cell = literal_cell_bytes(&literal)?;
                            shape.literal_bytes = shape.literal_bytes.saturating_add(cell);
                            shape.fixed.push((name.len(), cell));
                        }
                        Ok(_) | Err(_) => return None,
                    }
                }
            }
        }
        Some(shape)
    }

    /// The fields each `*` expands to, with their names' bytes. `None` for a
    /// `*` over unbounded fields.
    fn star_expansion(&self, fields: RowFields) -> Option<(usize, usize)> {
        match (self.stars, fields) {
            (0, _) => Some((0, 0)),
            (_, RowFields::Declared { count, name_bytes }) => Some((count, name_bytes)),
            (_, RowFields::Unbounded) => None,
        }
    }

    /// The cells one row renders, where a `*` expands to `fields`. `None`
    /// for a `*` over unbounded fields, or a count past `usize`.
    fn width(&self, fields: RowFields) -> Option<usize> {
        let (star_fields, _) = self.star_expansion(fields)?;
        self.stars
            .checked_mul(star_fields)
            .and_then(|expanded| expanded.checked_add(self.fixed.len()))
    }

    /// The column cells one row renders: every cell but a literal's. `None`
    /// as for [`Self::width`].
    fn column_cells(&self, fields: RowFields) -> Option<usize> {
        let (star_fields, _) = self.star_expansion(fields)?;
        self.stars
            .checked_mul(star_fields)
            .and_then(|expanded| expanded.checked_add(self.column_items))
    }

    /// The bytes of the column names a payload of `width` cells carries:
    /// the `announced` names when given, else the list's own. `None` for a
    /// `*` over unbounded fields, or announced names of another count.
    fn name_bytes(
        &self,
        fields: RowFields,
        width: usize,
        announced: Option<&[AnnouncedColumn]>,
    ) -> Option<usize> {
        let (star_fields, star_name_bytes) = self.star_expansion(fields)?;
        match announced {
            Some(announced) if announced.len() != width => None,
            Some(announced) => Some(
                announced
                    .iter()
                    .map(|column| string_bytes(column.name.len()))
                    .fold(0usize, usize::saturating_add),
            ),
            None => Some(
                self.fixed
                    .iter()
                    .map(|(name, _)| string_bytes(*name))
                    .fold(0usize, usize::saturating_add)
                    .saturating_add(self.stars.saturating_mul(
                        star_name_bytes.saturating_add(star_fields.saturating_mul(5)),
                    )),
            ),
        }
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
        if self.star_expansion(fields).is_none() {
            return limits.columns == usize::MAX
                && limits.cells == usize::MAX
                && limits.payload_bytes == usize::MAX;
        }
        let (Some(columns), Some(column_cells)) = (self.width(fields), self.column_cells(fields))
        else {
            return false;
        };
        if columns > limits.columns || columns > limits.cells {
            return false;
        }
        let Some(names) = self.name_bytes(fields, columns, announced) else {
            return false;
        };
        let cells = self
            .literal_bytes
            .saturating_add(column_cells.saturating_mul(column_cell_bytes()));
        envelope_bytes(columns)
            .saturating_add(names)
            .saturating_add(cells)
            <= limits.payload_bytes
    }
}
