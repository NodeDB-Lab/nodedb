// SPDX-License-Identifier: BUSL-1.1

//! The stored form of one collection's maintained document index paths.
//!
//! Each path holds one entry per owner: an explicit index by name, or the
//! auto-index of a declared field. The entries of a path follow one
//! effective owner. It is the newest explicit owner, else the declared
//! field. A build adds an owner and a retire removes one. The path leaves
//! the set when its last owner goes.
//!
//! A set in top-level mode also keeps every top-level key `$.k` of every
//! row in the [`TOP_LEVEL_OWNER`] shape. An owner stored on a key's path
//! decides that path's entries instead.
//!
//! Every build and retire carries the [`DocIndexFence`] of the descriptor it
//! derives from. The set keeps the incarnation it holds, and the highest
//! fence applied per path and owner, also after the owner leaves. A step
//! below that fence derives from an older descriptor and changes nothing,
//! so a build that a retire overtook on the data log never brings its owner
//! back. A step of a newer incarnation discards the set first.

use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner};
use nodedb_types::Hlc;

/// The spec every top-level key follows in a set in top-level mode, unless
/// an owner stored on its path decides it.
pub static TOP_LEVEL_OWNER: MaintainedOwner = MaintainedOwner {
    owner: DocIndexOwner::DeclaredTopLevel,
    is_array: false,
    unique: false,
    case_insensitive: false,
    predicate: None,
};

/// The extraction path of top-level key `key`. `None` for a key no
/// top-level path names: an empty key, a key with a `.` that extraction
/// reads as nesting, or a key ending in the array suffix `[]`.
pub fn top_level_path(key: &str) -> Option<String> {
    if key.is_empty() || key.contains('.') || key.ends_with("[]") {
        return None;
    }
    Some(format!("$.{key}"))
}

/// Whether `path` is the extraction path of one top-level key.
pub fn is_top_level_path(path: &str) -> bool {
    path.strip_prefix("$.")
        .and_then(top_level_path)
        .is_some_and(|own| own == path)
}

/// One owner of a maintained path, with the spec its build carried.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct MaintainedOwner {
    pub owner: DocIndexOwner,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    /// Raw SQL text of a partial index's `WHERE` body. `None` for a full
    /// index.
    pub predicate: Option<String>,
}

/// What decides the stored entries of a path. Two owners of one shape
/// write the same entries for every row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryShape {
    pub is_array: bool,
    pub case_insensitive: bool,
    pub predicate: Option<String>,
}

impl MaintainedOwner {
    /// The shape of the entries this owner's spec writes.
    pub fn shape(&self) -> EntryShape {
        EntryShape {
            is_array: self.is_array,
            case_insensitive: self.case_insensitive,
            predicate: self.predicate.clone(),
        }
    }
}

/// One maintained path and every owner that keeps it.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct MaintainedPath {
    /// The extraction path, without an array suffix.
    pub path: String,
    /// The owners in build order. Never empty in a stored set.
    pub owners: Vec<MaintainedOwner>,
}

impl MaintainedPath {
    /// The owner whose spec the path's entries follow: the newest explicit
    /// index, else the declared field.
    pub fn effective(&self) -> Option<&MaintainedOwner> {
        self.owners
            .iter()
            .rev()
            .find(|o| o.owner.is_named())
            .or_else(|| self.owners.last())
    }

    /// Whether a UNIQUE probe can judge the path's entries: an owner with
    /// `unique` set has the effective owner's entry shape. A declared field
    /// is never unique, so it never decides this alone.
    pub fn judges_unique(&self) -> bool {
        let Some(effective) = self.effective() else {
            return false;
        };
        let shape = effective.shape();
        self.owners
            .iter()
            .any(|owner| owner.unique && owner.shape() == shape)
    }
}

/// The highest fence applied for one path and owner.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct OwnerFence {
    /// The extraction path. A top-level step names its build path.
    pub path: String,
    pub owner: DocIndexOwner,
    pub fence: DocIndexFence,
}

/// What a fenced step does to the set it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceVerdict {
    /// The step derives from an older descriptor than one already applied
    /// for its path and owner, or from an older incarnation. It changes
    /// nothing.
    Stale,
    /// The step applies to the set as stored.
    Apply,
    /// The step is of a newer incarnation. The set and every index entry of
    /// the collection go before it applies.
    NewIncarnation,
}

/// Every maintained path of one collection on one core.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct MaintainedIndexSet {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    /// Whether every top-level key is kept in the [`TOP_LEVEL_OWNER`] shape.
    pub top_level: bool,
    /// The paths an owner is stored on.
    pub paths: Vec<MaintainedPath>,
    /// The incarnation of the steps the set holds.
    pub incarnation: Hlc,
    /// The highest fence applied per path and owner.
    pub fences: Vec<OwnerFence>,
}

impl MaintainedIndexSet {
    /// A set with no path, not in top-level mode, and no applied step.
    pub fn empty(database_id: u64, tenant_id: u64, collection: &str) -> Self {
        Self {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            top_level: false,
            paths: Vec::new(),
            incarnation: Hlc::default(),
            fences: Vec::new(),
        }
    }

    /// Whether the set keeps nothing. An empty set has no stored row. A
    /// set that holds a fence keeps its row, so the fence outlives the
    /// owner it retired.
    pub fn is_empty(&self) -> bool {
        !self.top_level && self.paths.is_empty() && self.fences.is_empty()
    }

    /// What a step of `owner` on `path` with `fence` does to this set.
    pub fn judge(&self, path: &str, owner: &DocIndexOwner, fence: DocIndexFence) -> FenceVerdict {
        match fence.incarnation.cmp(&self.incarnation) {
            std::cmp::Ordering::Less => FenceVerdict::Stale,
            std::cmp::Ordering::Greater => FenceVerdict::NewIncarnation,
            std::cmp::Ordering::Equal => match self.fence_of(path, owner) {
                Some(applied) if fence < applied => FenceVerdict::Stale,
                Some(_) | None => FenceVerdict::Apply,
            },
        }
    }

    /// The highest fence applied for `owner` on `path`.
    pub fn fence_of(&self, path: &str, owner: &DocIndexOwner) -> Option<DocIndexFence> {
        self.fences
            .iter()
            .find(|f| f.path == path && &f.owner == owner)
            .map(|f| f.fence)
    }

    /// Record `fence` as applied for `owner` on `path`, when it is higher
    /// than the one held.
    pub fn raise_fence(&mut self, path: &str, owner: &DocIndexOwner, fence: DocIndexFence) {
        match self
            .fences
            .iter_mut()
            .find(|f| f.path == path && &f.owner == owner)
        {
            Some(held) => held.fence = held.fence.max(fence),
            None => self.fences.push(OwnerFence {
                path: path.to_string(),
                owner: owner.clone(),
                fence,
            }),
        }
    }

    /// Discard every path, the top-level mode and every fence, and hold
    /// `incarnation`. The caller removes the collection's index entries in
    /// the same transaction.
    pub fn reset(&mut self, incarnation: Hlc) {
        self.top_level = false;
        self.paths.clear();
        self.fences.clear();
        self.incarnation = incarnation;
    }

    /// The effective owner stored on `path`, ignoring top-level mode.
    pub fn stored_effective(&self, path: &str) -> Option<&MaintainedOwner> {
        self.paths
            .iter()
            .find(|p| p.path == path)
            .and_then(MaintainedPath::effective)
    }

    /// Whether `owner` is stored on `path` with exactly `spec`. A build
    /// that finds it changes nothing.
    pub fn holds_owner(&self, path: &str, spec: &MaintainedOwner) -> bool {
        self.paths
            .iter()
            .find(|p| p.path == path)
            .is_some_and(|p| p.owners.iter().any(|o| o == spec))
    }

    /// The table key: `"{database_id}:{tenant_id}:{collection}"`.
    pub fn key(&self) -> String {
        maintained_set_key(self.database_id, self.tenant_id, &self.collection)
    }

    /// The effective owner of `path`: the owner stored on it, else
    /// [`TOP_LEVEL_OWNER`] for a top-level path of a set in top-level mode.
    /// `None` when no owner keeps it.
    pub fn effective(&self, path: &str) -> Option<&MaintainedOwner> {
        self.stored_effective(path)
            .or_else(|| (self.top_level && is_top_level_path(path)).then_some(&TOP_LEVEL_OWNER))
    }

    /// The entry shape of `path`. `None` when no owner keeps it.
    pub fn shape(&self, path: &str) -> Option<EntryShape> {
        self.effective(path).map(MaintainedOwner::shape)
    }

    /// Add `spec` as an owner of `path`. An owner that already keeps the
    /// path takes the new spec in its own place, so a second apply of one
    /// build changes nothing.
    pub fn add_owner(&mut self, path: &str, spec: MaintainedOwner) {
        match self.paths.iter_mut().find(|p| p.path == path) {
            Some(entry) => match entry.owners.iter_mut().find(|o| o.owner == spec.owner) {
                Some(existing) => *existing = spec,
                None => entry.owners.push(spec),
            },
            None => self.paths.push(MaintainedPath {
                path: path.to_string(),
                owners: vec![spec],
            }),
        }
    }

    /// Remove `owner` from `path`. The path leaves the set with its last
    /// owner. An owner that does not keep the path changes nothing.
    pub fn remove_owner(&mut self, path: &str, owner: &DocIndexOwner) {
        for entry in self.paths.iter_mut().filter(|p| p.path == path) {
            entry.owners.retain(|o| &o.owner != owner);
        }
        self.paths.retain(|p| !p.owners.is_empty());
    }

    /// The record's stored bytes.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode maintained index set: {e}"),
        })
    }

    /// A record from its stored bytes. `key` names the row in the error.
    pub fn from_bytes(key: &str, bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("maintained index set '{key}' does not decode: {e}"),
        })
    }
}

/// The table key of one collection's set.
pub fn maintained_set_key(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(owner: DocIndexOwner, case_insensitive: bool) -> MaintainedOwner {
        MaintainedOwner {
            owner,
            is_array: false,
            unique: false,
            case_insensitive,
            predicate: None,
        }
    }

    #[test]
    fn a_named_owner_decides_the_shape_over_the_declared_field() {
        let mut set = MaintainedIndexSet::empty(0, 1, "users");
        set.add_owner("$.email", owner(DocIndexOwner::Declared, false));
        set.add_owner(
            "$.email",
            owner(DocIndexOwner::Named("by_email".into()), true),
        );
        assert!(set.shape("$.email").expect("kept").case_insensitive);

        set.remove_owner("$.email", &DocIndexOwner::Named("by_email".into()));
        let shape = set.shape("$.email").expect("the declared field keeps it");
        assert!(!shape.case_insensitive);

        set.remove_owner("$.email", &DocIndexOwner::Declared);
        assert!(set.shape("$.email").is_none());
        assert!(set.paths.is_empty());
    }

    #[test]
    fn a_path_judges_unique_while_a_unique_owner_has_the_entry_shape() {
        let unique = |name: &str, case_insensitive: bool| MaintainedOwner {
            unique: true,
            ..owner(DocIndexOwner::Named(name.into()), case_insensitive)
        };
        let judges = |set: &MaintainedIndexSet| {
            set.paths
                .iter()
                .find(|p| p.path == "$.code")
                .is_some_and(MaintainedPath::judges_unique)
        };
        let mut set = MaintainedIndexSet::empty(0, 1, "codes");
        set.add_owner("$.code", owner(DocIndexOwner::Declared, false));
        assert!(!judges(&set), "a declared field is not unique");

        set.add_owner("$.code", unique("by_code", false));
        assert!(judges(&set));

        set.add_owner("$.code", owner(DocIndexOwner::Named("plain".into()), false));
        assert!(
            judges(&set),
            "a newer plain index of the same shape keeps the unique entries"
        );

        set.add_owner("$.code", owner(DocIndexOwner::Named("folded".into()), true));
        assert!(
            !judges(&set),
            "the entries follow a shape no unique owner has"
        );
    }

    #[test]
    fn top_level_mode_keeps_each_top_level_path_unless_an_owner_is_stored() {
        let mut set = MaintainedIndexSet::empty(0, 1, "users");
        assert!(set.is_empty());
        set.top_level = true;
        assert!(!set.is_empty());
        assert_eq!(set.effective("$.email"), Some(&TOP_LEVEL_OWNER));
        assert!(set.effective("$.a.b").is_none());
        assert!(set.effective("$.tags[]").is_none());

        set.add_owner(
            "$.email",
            owner(DocIndexOwner::Named("by_email".into()), true),
        );
        assert!(set.shape("$.email").expect("kept").case_insensitive);
        set.remove_owner("$.email", &DocIndexOwner::Named("by_email".into()));
        assert_eq!(set.shape("$.email"), Some(TOP_LEVEL_OWNER.shape()));
    }

    #[test]
    fn a_fence_orders_by_incarnation_then_descriptor_version() {
        let at = |wall: u64, version: u64| DocIndexFence {
            incarnation: Hlc::new(wall, 0),
            descriptor_version: version,
        };
        let named = DocIndexOwner::Named("by_email".into());
        let mut set = MaintainedIndexSet::empty(0, 1, "users");
        assert_eq!(
            set.judge("$.email", &named, at(1, 1)),
            FenceVerdict::NewIncarnation
        );
        set.reset(Hlc::new(1, 0));
        set.raise_fence("$.email", &named, at(1, 5));
        assert!(!set.is_empty(), "a held fence keeps the row");
        assert_eq!(set.judge("$.email", &named, at(1, 4)), FenceVerdict::Stale);
        assert_eq!(set.judge("$.email", &named, at(1, 5)), FenceVerdict::Apply);
        assert_eq!(
            set.judge("$.email", &DocIndexOwner::Declared, at(1, 1)),
            FenceVerdict::Apply,
            "another owner has its own fence"
        );
        assert_eq!(
            set.judge("$.email", &named, at(0, 99)),
            FenceVerdict::Stale,
            "an older incarnation is stale at any version"
        );
        assert_eq!(
            set.judge("$.email", &named, at(2, 1)),
            FenceVerdict::NewIncarnation
        );
        set.raise_fence("$.email", &named, at(1, 3));
        assert_eq!(set.fence_of("$.email", &named), Some(at(1, 5)));
    }

    #[test]
    fn a_top_level_path_names_exactly_one_key() {
        assert_eq!(top_level_path("email").as_deref(), Some("$.email"));
        assert!(top_level_path("").is_none());
        assert!(top_level_path("a.b").is_none());
        assert!(top_level_path("tags[]").is_none());
        assert!(is_top_level_path("$.email"));
        assert!(!is_top_level_path("$.a.b"));
        assert!(!is_top_level_path("email"));
        assert!(!is_top_level_path("$"));
    }

    #[test]
    fn a_second_apply_of_one_build_changes_nothing() {
        let mut set = MaintainedIndexSet::empty(0, 1, "users");
        let spec = owner(DocIndexOwner::Named("by_email".into()), false);
        set.add_owner("$.email", spec.clone());
        let once = set.clone();
        set.add_owner("$.email", spec);
        assert_eq!(set, once);
    }

    #[test]
    fn removing_an_absent_owner_changes_nothing() {
        let mut set = MaintainedIndexSet::empty(0, 1, "users");
        set.add_owner("$.email", owner(DocIndexOwner::Declared, false));
        let before = set.clone();
        set.remove_owner("$.email", &DocIndexOwner::Named("gone".into()));
        assert_eq!(set, before);
    }

    #[test]
    fn a_set_round_trips_through_its_bytes() {
        let mut set = MaintainedIndexSet::empty(3, 7, "a:b");
        set.add_owner("$.tags", owner(DocIndexOwner::Declared, false));
        let bytes = set.to_bytes().expect("encode");
        assert_eq!(
            MaintainedIndexSet::from_bytes(&set.key(), &bytes).expect("decode"),
            set
        );
        assert_eq!(set.key(), "3:7:a:b");
    }
}
