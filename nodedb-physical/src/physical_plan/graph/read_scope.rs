// SPDX-License-Identifier: Apache-2.0

//! The bitemporal cut a graph read sees.

/// The refusal of `AS OF SYSTEM TIME NULL` on a graph walk: a walk reads one
/// graph, and every system-time version of every edge is no graph.
pub const ALL_VERSIONS_REFUSED: &str =
    "AS OF SYSTEM TIME NULL is not supported on graph walks; name a system time";

/// The system-time cut of a graph read.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GraphSystemCut {
    /// The current edges, through the live CSR.
    #[default]
    Live,
    /// The edges whose newest version at or below this system-time ordinal
    /// is not a tombstone. The Control Plane pins one ordinal per statement,
    /// so every hop of a walk reads the same graph.
    AtOrdinal(i64),
    /// Every system-time version (`AS OF SYSTEM TIME NULL`). Only
    /// `GraphOp::TemporalNeighbors` accepts it.
    AllVersions,
}

/// The cut of a graph read on both temporal axes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GraphReadScope {
    pub system: GraphSystemCut,
    /// An edge is read only when its valid time `[valid_from_ms,
    /// valid_until_ms)` contains this instant. `None` reads every valid time.
    pub valid_at_ms: Option<i64>,
    /// The Calvin cut marker whose cut caps an `AtOrdinal` cut. Every node
    /// waits until the marker applied and every Calvin scheduler it runs
    /// passed it before it reads at the cut. Every edge version at or below
    /// the cut is then installed there. `0` when no marker caps the cut: a
    /// live read, or a cut every reader already installed.
    pub cut_marker: u64,
}

impl GraphReadScope {
    /// A read at `ordinal`, a cut every reader already installed, so no
    /// reader waits for a marker.
    pub fn at_installed_cut(ordinal: i64, valid_at_ms: Option<i64>) -> Self {
        Self {
            system: GraphSystemCut::AtOrdinal(ordinal),
            valid_at_ms,
            cut_marker: 0,
        }
    }

    /// The current edges at every valid time: the live CSR answers.
    pub fn is_live(&self) -> bool {
        matches!(self.system, GraphSystemCut::Live) && self.valid_at_ms.is_none()
    }

    /// The system-time ordinal a versioned read caps at: the pinned ordinal,
    /// or `i64::MAX` for the current versions. `None` for `AllVersions`,
    /// which has no single cap.
    pub fn cutoff_ordinal(&self) -> Option<i64> {
        match self.system {
            GraphSystemCut::Live => Some(i64::MAX),
            GraphSystemCut::AtOrdinal(ordinal) => Some(ordinal),
            GraphSystemCut::AllVersions => None,
        }
    }

    /// The ordinal the Control Plane pinned for a historical read. `None`
    /// for a live read and for `AllVersions`.
    pub fn pinned_ordinal(&self) -> Option<i64> {
        match self.system {
            GraphSystemCut::AtOrdinal(ordinal) => Some(ordinal),
            GraphSystemCut::Live | GraphSystemCut::AllVersions => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_default_scope_is_live() {
        assert!(GraphReadScope::default().is_live());
        assert!(!GraphReadScope::at_installed_cut(5, None).is_live());
        assert!(
            !GraphReadScope {
                system: GraphSystemCut::Live,
                valid_at_ms: Some(5),
                cut_marker: 0,
            }
            .is_live()
        );
    }

    #[test]
    fn cutoff_ordinal_follows_the_system_cut() {
        let at = |system| GraphReadScope {
            system,
            valid_at_ms: None,
            cut_marker: 0,
        };
        assert_eq!(at(GraphSystemCut::Live).cutoff_ordinal(), Some(i64::MAX));
        assert_eq!(at(GraphSystemCut::AtOrdinal(7)).cutoff_ordinal(), Some(7));
        assert_eq!(at(GraphSystemCut::AllVersions).cutoff_ordinal(), None);
    }

    #[test]
    fn only_an_at_ordinal_cut_is_pinned() {
        let at = |system| GraphReadScope {
            system,
            valid_at_ms: None,
            cut_marker: 0,
        };
        assert_eq!(at(GraphSystemCut::Live).pinned_ordinal(), None);
        assert_eq!(at(GraphSystemCut::AtOrdinal(7)).pinned_ordinal(), Some(7));
        assert_eq!(at(GraphSystemCut::AllVersions).pinned_ordinal(), None);
    }

    #[test]
    fn scope_round_trips_through_msgpack() {
        let scope = GraphReadScope {
            system: GraphSystemCut::AtOrdinal(42),
            valid_at_ms: Some(9),
            cut_marker: 77,
        };
        let bytes = zerompk::to_msgpack_vec(&scope).expect("encode");
        let back: GraphReadScope = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, scope);
    }
}
