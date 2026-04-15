//! Divergence types — used by both `integrity` (cross-table
//! referential checks) and `registry_verify` (in-memory vs
//! redb).

use std::fmt;

/// What kind of divergence a single check detected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DivergenceKind {
    /// redb has a reference to an object that doesn't exist —
    /// e.g. `StoredOwner.owner_username` points to a user
    /// that isn't in `StoredUser`. Integrity violation.
    DanglingReference {
        from_kind: &'static str,
        from_key: String,
        to_kind: &'static str,
        to_key: String,
    },
    /// An object in redb has no matching parent — e.g. a
    /// `StoredCollection` with no `StoredOwner`. Integrity
    /// violation.
    OrphanRow {
        kind: &'static str,
        key: String,
        expected_parent_kind: &'static str,
    },
    /// A key is present in redb but missing from the in-memory
    /// registry. Registry `load_from` bug — repairable by
    /// re-loading.
    MissingInRegistry { registry: &'static str, key: String },
    /// A key is present in the in-memory registry but missing
    /// from redb. Either a registry bug writing phantom entries
    /// or a half-applied delete. Repairable by swap-in fresh.
    ExtraInRegistry { registry: &'static str, key: String },
    /// A key exists in both but the values differ. Highest-
    /// priority repair target because reads against the
    /// in-memory registry produce wrong results today.
    ValueMismatch {
        registry: &'static str,
        key: String,
        detail: String,
    },
}

impl DivergenceKind {
    /// Short label for metric `kind` dimension and structured
    /// logging.
    pub fn label(&self) -> &'static str {
        match self {
            Self::DanglingReference { .. } => "dangling_reference",
            Self::OrphanRow { .. } => "orphan_row",
            Self::MissingInRegistry { .. } => "missing_in_registry",
            Self::ExtraInRegistry { .. } => "extra_in_registry",
            Self::ValueMismatch { .. } => "value_mismatch",
        }
    }

    /// Whether this divergence is a redb-side integrity bug
    /// (not repairable by re-loading a registry).
    pub fn is_integrity(&self) -> bool {
        matches!(
            self,
            Self::DanglingReference { .. } | Self::OrphanRow { .. }
        )
    }
}

/// Tagged divergence with its location. Produced by every
/// sub-check and aggregated into [`super::report::VerifyReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    pub kind: DivergenceKind,
}

impl Divergence {
    pub fn new(kind: DivergenceKind) -> Self {
        Self { kind }
    }
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DivergenceKind::DanglingReference {
                from_kind,
                from_key,
                to_kind,
                to_key,
            } => write!(
                f,
                "dangling reference {from_kind}({from_key}) → {to_kind}({to_key}) not found"
            ),
            DivergenceKind::OrphanRow {
                kind,
                key,
                expected_parent_kind,
            } => write!(
                f,
                "orphan row {kind}({key}) — no matching {expected_parent_kind}"
            ),
            DivergenceKind::MissingInRegistry { registry, key } => {
                write!(f, "registry {registry}: key {key} missing in memory")
            }
            DivergenceKind::ExtraInRegistry { registry, key } => {
                write!(f, "registry {registry}: key {key} extra in memory")
            }
            DivergenceKind::ValueMismatch {
                registry,
                key,
                detail,
            } => write!(
                f,
                "registry {registry}: value mismatch for key {key} — {detail}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_are_stable() {
        let d = Divergence::new(DivergenceKind::MissingInRegistry {
            registry: "permissions",
            key: "alice".into(),
        });
        assert_eq!(d.kind.label(), "missing_in_registry");
        assert!(!d.kind.is_integrity());
    }

    #[test]
    fn integrity_flag() {
        let d = Divergence::new(DivergenceKind::DanglingReference {
            from_kind: "owner",
            from_key: "collection:1:foo".into(),
            to_kind: "user",
            to_key: "bob".into(),
        });
        assert!(d.kind.is_integrity());
        assert!(d.to_string().contains("dangling reference"));
    }
}
