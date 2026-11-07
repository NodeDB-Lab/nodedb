// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::cut::MirrorGroupPosition;
use super::origin::MirrorMode;

/// Whether a retention pin still holds the source's log.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MirrorPinState {
    /// The pin holds the log at its positions.
    Live,
    /// The pin passed its age or byte bound. It holds nothing, and its link
    /// must bootstrap again.
    Expired,
    /// The source database dropped. The pin holds nothing.
    SourceDropped,
}

/// One mirror link's hold on the source's Raft logs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorPin {
    /// The link's id, unique on the source.
    pub link_id: u64,
    /// The allowlist name of the client that owns the link.
    pub client: String,
    /// The source database the link follows.
    pub database: u64,
    /// The last acked index per group. A group with no position is held whole.
    pub positions: Vec<MirrorGroupPosition>,
    /// Wall-clock milliseconds of the newest ack, or of the hello before one.
    pub acked_ms: u64,
    pub state: MirrorPinState,
    /// Whether a write on the source waits for this link's confirmation.
    pub mode: MirrorMode,
    /// The newest system-time ordinal the mirror confirmed in full. `0` until
    /// its first cut: a sync write waits for a link only from then on.
    pub acked_ordinal: i64,
}

impl MirrorPin {
    /// Whether a committed write on this link's database waits for it. A pin
    /// that stopped holding still counts: the write has no confirmation, and
    /// the answer never downgrades in silence. Only a dropped source ends it.
    pub fn holds_sync(&self) -> bool {
        let sync = match self.mode {
            MirrorMode::Sync => true,
            MirrorMode::Async => false,
        };
        let ended = match self.state {
            MirrorPinState::Live | MirrorPinState::Expired => false,
            MirrorPinState::SourceDropped => true,
        };
        sync && !ended && self.acked_ordinal > 0
    }

    /// The highest index of `group_id` the log can discard for this pin. A
    /// pin that no longer holds places no bound.
    pub fn ceiling(&self, group_id: u64) -> u64 {
        match self.state {
            MirrorPinState::Live => self
                .positions
                .iter()
                .find(|position| position.group_id == group_id)
                .map_or(0, |position| position.log_index),
            MirrorPinState::Expired | MirrorPinState::SourceDropped => u64::MAX,
        }
    }

    /// `self` after `incoming`, the next committed row of the link. Every node
    /// computes it in log order, so each holds the same row.
    ///
    /// Positions and the ack time only rise, so a late ack moves nothing back.
    /// A pin that stopped holding stays stopped: the log it held is gone.
    pub fn merged(&self, incoming: &Self) -> Self {
        let mut next = self.clone();
        next.acked_ms = self.acked_ms.max(incoming.acked_ms);
        next.acked_ordinal = self.acked_ordinal.max(incoming.acked_ordinal);
        for position in &incoming.positions {
            match next
                .positions
                .iter_mut()
                .find(|held| held.group_id == position.group_id)
            {
                Some(held) => held.log_index = held.log_index.max(position.log_index),
                None => next.positions.push(*position),
            }
        }
        next.state = match (self.state, incoming.state) {
            (MirrorPinState::SourceDropped, _) | (_, MirrorPinState::SourceDropped) => {
                MirrorPinState::SourceDropped
            }
            (MirrorPinState::Expired, _) | (_, MirrorPinState::Expired) => MirrorPinState::Expired,
            (MirrorPinState::Live, MirrorPinState::Live) => MirrorPinState::Live,
        };
        next
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::round_trip_msgpack;
    use super::*;

    fn pin(positions: &[(u64, u64)], acked_ms: u64, state: MirrorPinState) -> MirrorPin {
        MirrorPin {
            link_id: 1,
            client: "dr".to_string(),
            database: 1024,
            positions: positions
                .iter()
                .map(|(group_id, log_index)| MirrorGroupPosition {
                    group_id: *group_id,
                    log_index: *log_index,
                })
                .collect(),
            acked_ms,
            state,
            mode: MirrorMode::Async,
            acked_ordinal: 0,
        }
    }

    #[test]
    fn a_live_pin_holds_a_group_it_never_acked_whole() {
        let live = pin(&[(1, 40)], 5, MirrorPinState::Live);
        assert_eq!(live.ceiling(1), 40);
        assert_eq!(live.ceiling(2), 0);
        assert_eq!(
            pin(&[(1, 40)], 5, MirrorPinState::Expired).ceiling(2),
            u64::MAX
        );
    }

    #[test]
    fn a_pin_only_rises_and_an_expiry_sticks() {
        let held = pin(&[(1, 40), (2, 9)], 5, MirrorPinState::Live);
        let late = pin(&[(1, 30), (3, 1)], 4, MirrorPinState::Live);
        let merged = held.merged(&late);
        assert_eq!(merged.ceiling(1), 40);
        assert_eq!(merged.ceiling(2), 9);
        assert_eq!(merged.ceiling(3), 1);
        assert_eq!(merged.acked_ms, 5);

        let expired = held.merged(&pin(&[], 6, MirrorPinState::Expired));
        let revived = expired.merged(&pin(&[(1, 50)], 7, MirrorPinState::Live));
        assert_eq!(revived.state, MirrorPinState::Expired);
        assert_eq!(
            revived
                .merged(&pin(&[], 8, MirrorPinState::SourceDropped))
                .state,
            MirrorPinState::SourceDropped
        );
        assert_eq!(round_trip_msgpack(&revived), revived);
    }
}
