// SPDX-License-Identifier: Apache-2.0

//! Attr specs and their reading metadata.

use super::schema::ArrayAttrType;

/// One attribute spec in `CREATE ARRAY ... ATTRS (...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayAttrAst {
    pub name: String,
    pub dtype: ArrayAttrType,
    pub nullable: bool,
    /// `NODATA`, `SCALE`, `OFFSET` and `UNITS`.
    pub meta: ArrayAttrMetaAst,
}

/// Reading metadata of one attr (mirrors `nodedb_array::schema::AttrMeta`).
///
/// A stored cell holds the raw value. A physical read gives
/// `raw * scale + offset`, and a cell equal to `nodata` holds no data.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArrayAttrMetaAst {
    pub nodata: Option<f64>,
    pub scale: Option<f64>,
    pub offset: Option<f64>,
    pub units: Option<String>,
}

impl ArrayAttrMetaAst {
    /// Whether a physical read changes a numeric cell's value.
    pub fn has_transform(&self) -> bool {
        self.scale.is_some() || self.offset.is_some()
    }
}

/// One metadata field of an attr, as `DROP` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArrayAttrMetaKey {
    Nodata,
    Scale,
    Offset,
    Units,
}

impl ArrayAttrMetaKey {
    /// The DDL keyword.
    pub fn sql_name(self) -> &'static str {
        match self {
            ArrayAttrMetaKey::Nodata => "NODATA",
            ArrayAttrMetaKey::Scale => "SCALE",
            ArrayAttrMetaKey::Offset => "OFFSET",
            ArrayAttrMetaKey::Units => "UNITS",
        }
    }

    /// Whether `meta` holds this field.
    pub fn is_set(self, meta: &ArrayAttrMetaAst) -> bool {
        match self {
            ArrayAttrMetaKey::Nodata => meta.nodata.is_some(),
            ArrayAttrMetaKey::Scale => meta.scale.is_some(),
            ArrayAttrMetaKey::Offset => meta.offset.is_some(),
            ArrayAttrMetaKey::Units => meta.units.is_some(),
        }
    }

    /// Clear this field of `meta`.
    pub fn clear(self, meta: &mut ArrayAttrMetaAst) {
        match self {
            ArrayAttrMetaKey::Nodata => meta.nodata = None,
            ArrayAttrMetaKey::Scale => meta.scale = None,
            ArrayAttrMetaKey::Offset => meta.offset = None,
            ArrayAttrMetaKey::Units => meta.units = None,
        }
    }
}

/// `ALTER ARRAY a ALTER ATTR <attr> SET (...)` or `... DROP <key>, ...`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArrayAttrMetaAlter {
    pub attr: String,
    pub change: ArrayAttrMetaChange,
}

/// The change an attr-metadata `ALTER` makes.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayAttrMetaChange {
    /// Set each given field. A field the clause omits keeps its value.
    Set(ArrayAttrMetaAst),
    /// Remove each named field. Each must be set.
    Drop(Vec<ArrayAttrMetaKey>),
}

impl ArrayAttrMetaChange {
    /// `current` with this change applied.
    pub fn apply(&self, current: &ArrayAttrMetaAst) -> ArrayAttrMetaAst {
        let mut out = current.clone();
        match self {
            ArrayAttrMetaChange::Set(set) => {
                if set.nodata.is_some() {
                    out.nodata = set.nodata;
                }
                if set.scale.is_some() {
                    out.scale = set.scale;
                }
                if set.offset.is_some() {
                    out.offset = set.offset;
                }
                if set.units.is_some() {
                    out.units = set.units.clone();
                }
            }
            ArrayAttrMetaChange::Drop(keys) => {
                for key in keys {
                    key.clear(&mut out);
                }
            }
        }
        out
    }
}
