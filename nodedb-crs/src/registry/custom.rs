// SPDX-License-Identifier: Apache-2.0

//! User-defined CRSs.

use serde::{Deserialize, Serialize};

use crate::def::CrsDef;
use crate::error::CustomCrsError;
use crate::srid::Srid;

/// What a user-defined code holds.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CustomCrsState {
    Live(Box<CrsDef>),
    /// The CRS was dropped. The code stays retired, so stored data that
    /// names it fails to resolve instead of changing meaning.
    Dropped,
}

/// One user-defined code and its state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct CustomCrs {
    pub srid: Srid,
    pub state: CustomCrsState,
}

/// Every user-defined CRS of one database, sorted by SRID with no
/// duplicates.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<CustomCrs>", into = "Vec<CustomCrs>")]
pub struct CustomCrsSet {
    entries: Vec<CustomCrs>,
}

/// The set [`CustomCrsSet::empty`] returns.
static EMPTY: CustomCrsSet = CustomCrsSet::new();

/// A decoded entry list with a repeated SRID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("SRID {srid} appears twice in the custom CRS set")]
pub struct DuplicateCustomSrid {
    pub srid: Srid,
}

impl CustomCrsSet {
    /// An empty set.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// The empty set, borrowed for the program's life.
    pub fn empty() -> &'static CustomCrsSet {
        &EMPTY
    }

    /// Build a set from entries in any order. Fails on a repeated SRID.
    pub fn from_entries(mut entries: Vec<CustomCrs>) -> Result<Self, DuplicateCustomSrid> {
        entries.sort_by_key(|e| e.srid);
        if let Some(pair) = entries.windows(2).find(|w| w[0].srid == w[1].srid) {
            return Err(DuplicateCustomSrid { srid: pair[0].srid });
        }
        Ok(Self { entries })
    }

    fn position(&self, srid: Srid) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&srid, |e| e.srid)
    }

    /// The state of `srid`, if the set has ever held it.
    pub fn get(&self, srid: Srid) -> Option<&CustomCrsState> {
        self.position(srid).ok().map(|i| &self.entries[i].state)
    }

    /// Add a live CRS. Its code must be in the user range and never used.
    pub fn define(&mut self, def: CrsDef) -> Result<(), CustomCrsError> {
        let srid = Srid::new_user(u64::from(def.srid.code()))?;
        match self.position(srid) {
            Ok(i) => Err(match self.entries[i].state {
                CustomCrsState::Live(_) => CustomCrsError::Exists { srid },
                CustomCrsState::Dropped => CustomCrsError::WasDropped { srid },
            }),
            Err(i) => {
                self.entries.insert(
                    i,
                    CustomCrs {
                        srid,
                        state: CustomCrsState::Live(Box::new(def)),
                    },
                );
                Ok(())
            }
        }
    }

    /// Retire a live CRS.
    pub fn drop_crs(&mut self, srid: Srid) -> Result<(), CustomCrsError> {
        let i = self
            .position(srid)
            .map_err(|_| CustomCrsError::NotFound { srid })?;
        match self.entries[i].state {
            CustomCrsState::Live(_) => {
                self.entries[i].state = CustomCrsState::Dropped;
                Ok(())
            }
            CustomCrsState::Dropped => Err(CustomCrsError::NotFound { srid }),
        }
    }

    /// Every entry, ascending by SRID.
    pub fn entries(&self) -> &[CustomCrs] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl TryFrom<Vec<CustomCrs>> for CustomCrsSet {
    type Error = DuplicateCustomSrid;

    fn try_from(entries: Vec<CustomCrs>) -> Result<Self, DuplicateCustomSrid> {
        CustomCrsSet::from_entries(entries)
    }
}

impl From<CustomCrsSet> for Vec<CustomCrs> {
    fn from(set: CustomCrsSet) -> Self {
        set.entries
    }
}

impl zerompk::ToMessagePack for CustomCrsSet {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        self.entries.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for CustomCrsSet {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let entries = Vec::<CustomCrs>::read(reader)?;
        CustomCrsSet::from_entries(entries)
            .map_err(|e| zerompk::Error::KeyDuplicated(e.srid.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_proj_string;

    fn def(code: u32) -> CrsDef {
        let srid = Srid::new(code).unwrap();
        parse_proj_string(srid, "user tm", "+proj=tmerc +lon_0=102 +ellps=GRS80").unwrap()
    }

    #[test]
    fn define_drop_and_refuse_reuse() {
        let mut set = CustomCrsSet::new();
        set.define(def(910_002)).unwrap();
        set.define(def(910_001)).unwrap();
        assert_eq!(set.entries()[0].srid.code(), 910_001);
        let srid = Srid::new(910_001).unwrap();
        assert_eq!(
            set.define(def(910_001)),
            Err(CustomCrsError::Exists { srid })
        );
        set.drop_crs(srid).unwrap();
        assert_eq!(set.get(srid), Some(&CustomCrsState::Dropped));
        assert_eq!(
            set.define(def(910_001)),
            Err(CustomCrsError::WasDropped { srid })
        );
        assert_eq!(set.drop_crs(srid), Err(CustomCrsError::NotFound { srid }));
    }

    #[test]
    fn refuses_codes_outside_user_range() {
        let mut set = CustomCrsSet::new();
        let err = set.define(def(4326));
        assert_eq!(
            err,
            Err(CustomCrsError::NotUserCode(crate::NotUserSrid {
                code: 4326
            }))
        );
    }

    #[test]
    fn msgpack_round_trip_is_exact() {
        let mut set = CustomCrsSet::new();
        set.define(def(910_005)).unwrap();
        set.define(def(910_003)).unwrap();
        set.drop_crs(Srid::new(910_003).unwrap()).unwrap();
        let bytes = zerompk::to_msgpack_vec(&set).unwrap();
        let back: CustomCrsSet = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, set);
    }

    #[test]
    fn decode_refuses_duplicates() {
        let entry = CustomCrs {
            srid: Srid::new(910_001).unwrap(),
            state: CustomCrsState::Dropped,
        };
        let bytes = zerompk::to_msgpack_vec(&vec![entry.clone(), entry]).unwrap();
        assert!(zerompk::from_msgpack::<CustomCrsSet>(&bytes).is_err());
    }
}
