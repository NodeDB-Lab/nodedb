// SPDX-License-Identifier: BUSL-1.1

//! The read cut one historical statement pins, shared by every engine.
//!
//! The statement takes its cap once ([`statement_cap`]). Every graph walk
//! and every versioned document, index, columnar, timeseries, and array
//! read of the statement pins its `AS OF SYSTEM TIME` under that cap
//! ([`StatementCap::pin`]), and every serving node waits for the cap's cut
//! marker before it reads (`super::wait`).

use crate::control::state::SharedState;

/// The cut a historical statement pins: the ordinal every read of the
/// statement reads at, and the Calvin cut marker that caps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoricalCut {
    /// `AS OF SYSTEM TIME as_of_ms`, capped at the statement's read cut.
    pub ordinal: i64,
    /// The cut marker whose cut is the cap, `0` on a node without cluster
    /// routing. Every owner node waits for it before it reads at `ordinal`
    /// (`super::wait`).
    pub marker: u64,
}

/// The read cut of one historical statement: the cap every `AS OF SYSTEM
/// TIME` of the statement is pinned under, and the cut marker that fixes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatementCap {
    /// The newest ordinal any read of the statement reads at.
    pub cap: i64,
    /// The cut marker whose cut is `cap`, `0` on a node without cluster
    /// routing.
    pub marker: u64,
}

impl StatementCap {
    /// The cut a read `AS OF SYSTEM TIME as_of_ms` pins: the last ordinal of
    /// `as_of_ms`, capped at the statement's read cut. A time before the
    /// epoch reads before every write. A time past the cap reads as of the
    /// cap.
    pub(crate) fn pin(self, as_of_ms: i64) -> HistoricalCut {
        HistoricalCut {
            ordinal: nodedb_types::ms_to_ordinal_upper(as_of_ms)
                .max(0)
                .min(self.cap),
            marker: self.marker,
        }
    }
}

/// The read cut of one historical statement, taken once per statement.
/// Every graph walk and every versioned document, index, columnar,
/// timeseries, and array read of the statement pins its `AS OF SYSTEM
/// TIME` under it ([`StatementCap::pin`]), so the statement reads one cut.
///
/// `latest_as_of_ms` is the latest `AS OF SYSTEM TIME` the statement pins.
///
/// Versions carry ordinals from two sources. A write outside the Calvin
/// sequencer stamps a fresh ordinal of the node's ordinal clock, the clock
/// every Data Plane core stamps from. Applying a replicated version folds
/// its stamp into that clock. A Calvin transaction stamps the ordinal of
/// its epoch instant and position. Epoch instants rise strictly in log
/// order, and the sequencer mints each at or after its wall clock.
///
/// A past cut (`latest_as_of_ms` ends below a fresh clock ordinal) needs no
/// cap: the clock stamps every later write above it, and a Calvin epoch
/// minted after the statement starts carries a later instant. An epoch
/// minted before the statement starts can still be applying, so in a
/// cluster the statement places one cut marker, and every serving node
/// waits for it.
///
/// A cut at or past now takes a cap in a cluster in three steps:
///
/// 1. A first marker. Once it resolves, every Calvin version sequenced
///    before it is installed here and folded into the clock.
/// 2. The clock part ([`settled_clock_cap`]), read after the first marker:
///    at or above every version applied here, including one stamped by a
///    node whose clock leads. The wall clock then leaves its millisecond.
/// 3. A second marker, proposed after the clock part settles. A Calvin
///    epoch minted after it carries a later instant, so its versions are
///    above the cap. Its cut covers every Calvin epoch minted before it.
///
/// The cap is the higher of the clock part and the second marker's cut
/// ([`cluster_cap`]), and the plan carries the second marker. On a node
/// without cluster routing the cap is the clock part alone.
pub(crate) async fn statement_cap(
    state: &SharedState,
    latest_as_of_ms: i64,
) -> crate::Result<StatementCap> {
    let clock = state.wal.ordinal_clock();
    let probe = clock_cap(&clock);
    let clustered = state.cluster_routing.is_some();
    if is_past_cut(latest_as_of_ms, probe) {
        let marker = if clustered {
            let marker = super::run_marker::new_cut_marker(state);
            resolve_read_cut(state, marker).await?;
            marker
        } else {
            0
        };
        return Ok(StatementCap { cap: probe, marker });
    }
    if !clustered {
        return Ok(StatementCap {
            cap: settled_clock_cap(&clock).await,
            marker: 0,
        });
    }
    resolve_read_cut(state, super::run_marker::new_cut_marker(state)).await?;
    let local = settled_clock_cap(&clock).await;
    let marker = super::run_marker::new_cut_marker(state);
    let calvin_cut = resolve_read_cut(state, marker).await?;
    let cap = cluster_cap(local, calvin_cut);
    // A Calvin cut above the clock part moves the clock past the cap too.
    clock.update_from_remote(cap);
    Ok(StatementCap { cap, marker })
}

/// True when a cut at `as_of_ms` ends below `clock_now`, a fresh ordinal of
/// the clock: no later clock stamp can fall at or below it.
fn is_past_cut(as_of_ms: i64, clock_now: i64) -> bool {
    nodedb_types::ms_to_ordinal_upper(as_of_ms) < clock_now
}

async fn resolve_read_cut(state: &SharedState, marker: u64) -> crate::Result<i64> {
    crate::control::server::exchange::all_cores::read_cut::resolve_read_cut(state, marker).await
}

/// The cap of a cluster statement: the clock ordinal `local`, raised to the
/// Calvin marker's cut when the sequencer's epoch instants run ahead of the
/// clock. The marker's cut alone misses every clock-stamped version newer
/// than the last Calvin epoch, and is `0` when no epoch applied.
fn cluster_cap(local: i64, calvin_cut: i64) -> i64 {
    local.max(calvin_cut)
}

/// The longest the clock ordinal can lead the wall clock before
/// [`await_wall_past`] stops waiting for it. A clock folded far ahead
/// carries a replayed stamp from a node whose clock ran ahead. Waiting out
/// that lead stalls every historical statement for the whole lead, so the
/// wait stops at this bound.
const MAX_CLOCK_LEAD: std::time::Duration = std::time::Duration::from_millis(2);

/// The clock part of a statement's cap: a fresh ordinal of `clock`, raised
/// to the last ordinal of its millisecond once the wall clock has left that
/// millisecond. A later write then stamps above the cap, and an engine that
/// stamps milliseconds reads the cap's whole millisecond. A clock that leads
/// the wall by more than [`MAX_CLOCK_LEAD`] keeps the fresh ordinal: later
/// writes still stamp from the lead, inside its millisecond.
async fn settled_clock_cap(clock: &nodedb_types::OrdinalClock) -> i64 {
    let local = clock_cap(clock);
    if await_wall_past(local).await {
        nodedb_types::ms_to_ordinal_upper(nodedb_types::ordinal_to_ms(local))
    } else {
        local
    }
}

/// Wait until the wall clock is past the millisecond of `ordinal`. Returns
/// false at once when `ordinal` leads the wall clock by more than
/// [`MAX_CLOCK_LEAD`].
async fn await_wall_past(ordinal: i64) -> bool {
    let target_ns = nodedb_types::ms_to_ordinal_upper(nodedb_types::ordinal_to_ms(ordinal));
    loop {
        let now_ns = nodedb_types::wall_now_ns_i64();
        if now_ns > target_ns {
            return true;
        }
        let wait_ns = u64::try_from(target_ns - now_ns + 1).unwrap_or(0);
        let wait = std::time::Duration::from_nanos(wait_ns);
        if wait > MAX_CLOCK_LEAD {
            return false;
        }
        tokio::time::sleep(wait).await;
    }
}

/// The one system-time ordinal every read of a historical statement reads
/// at: `AS OF SYSTEM TIME as_of_ms`, pinned under the statement's
/// read cut ([`statement_cap`]). Every hop of a walk carries it, so the walk
/// reads one graph.
pub(crate) async fn pin_historical_cut(
    state: &SharedState,
    as_of_ms: i64,
) -> crate::Result<HistoricalCut> {
    Ok(statement_cap(state, as_of_ms).await?.pin(as_of_ms))
}

/// The clock part of a statement's cap: a fresh ordinal of `clock`. Every
/// version stamped from `clock` before the read is at or below it, and
/// every later one is above it.
fn clock_cap(clock: &nodedb_types::OrdinalClock) -> i64 {
    clock.next_ordinal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_node_cut_closes_between_two_writes() {
        let clock = nodedb_types::OrdinalClock::new();
        let before = clock.next_ordinal();
        let cap = StatementCap {
            cap: clock_cap(&clock),
            marker: 0,
        };
        let after = clock.next_ordinal();
        let cut = cap.pin(i64::MAX / nodedb_types::NANOS_PER_MS).ordinal;
        assert!(before <= cut, "a write before the read is under the cut");
        assert!(after > cut, "a write after the read is above the cut");
        assert_eq!(
            cap.pin(5).ordinal,
            nodedb_types::ms_to_ordinal_upper(5),
            "an earlier time is not capped"
        );
        assert_eq!(
            cap.pin(-5).ordinal,
            0,
            "a time before the epoch reads nothing"
        );
    }

    /// A document version stamped from the Data Plane clock, with no Calvin
    /// epoch applied (the marker's cut is `0`), sits under the cluster cap
    /// and under a pin at its own millisecond. The marker's cut alone read
    /// every such version as absent.
    #[test]
    fn a_clock_stamped_version_is_under_the_cluster_cut() {
        let clock = nodedb_types::OrdinalClock::new();
        let version = clock.next_ordinal();
        let no_epoch = 0;
        assert!(
            no_epoch < version,
            "the marker's cut alone hides the version"
        );
        let cap = StatementCap {
            cap: cluster_cap(clock_cap(&clock), no_epoch),
            marker: 9,
        };
        assert!(cap.cap >= version);
        let at_its_ms = cap.pin(nodedb_types::ordinal_to_ms(version));
        assert!(
            at_its_ms.ordinal >= version,
            "a cut at the version's ms reads it"
        );
        let future = cap.pin(nodedb_types::ordinal_to_ms(version) + 86_400_000);
        assert!(future.ordinal >= version, "a future cut reads it");
        let pinned = nodedb_types::SystemTimeScope::Pinned {
            ordinal: future.ordinal,
            cut_marker: future.marker,
        };
        assert!(
            pinned.as_of_ordinal() >= Some(version),
            "a pinned scope reads the version"
        );
    }

    /// A version stamped by a node whose clock leads, folded into this
    /// node's clock when it applies, sits under a clock part read after
    /// the fold.
    #[tokio::test]
    async fn a_folded_version_from_a_leading_clock_is_under_the_cap() {
        let clock = nodedb_types::OrdinalClock::new();
        let ahead = clock.next_ordinal() + 1_500_000;
        clock.update_from_remote(ahead);
        let cap = cluster_cap(settled_clock_cap(&clock).await, 0);
        assert!(cap >= ahead, "a folded version is under the cap");
        assert!(clock.next_ordinal() > cap, "a later write is above the cap");
    }

    /// A cut that ends below a fresh clock ordinal is past: no later stamp
    /// falls at or below it. A cut in the clock's millisecond or later is
    /// not.
    #[test]
    fn only_a_cut_below_the_clock_is_past() {
        let clock = nodedb_types::OrdinalClock::new();
        let now = clock.next_ordinal();
        let now_ms = nodedb_types::ordinal_to_ms(now);
        assert!(is_past_cut(now_ms - 1, now));
        assert!(
            !is_past_cut(now_ms, now),
            "the clock's own millisecond is not past"
        );
        assert!(!is_past_cut(now_ms + 1, now));
        let cap = StatementCap {
            cap: now,
            marker: 0,
        };
        assert_eq!(
            cap.pin(now_ms - 1).ordinal,
            nodedb_types::ms_to_ordinal_upper(now_ms - 1),
            "a past cut is never capped"
        );
    }

    /// Calvin epoch instants that ran ahead of the clock raise the cap.
    #[test]
    fn a_calvin_cut_ahead_of_the_clock_raises_the_cap() {
        let clock = nodedb_types::OrdinalClock::new();
        let local = clock_cap(&clock);
        let ahead = nodedb_types::ms_to_ordinal_upper(nodedb_types::ordinal_to_ms(local) + 5);
        assert_eq!(cluster_cap(local, ahead), ahead);
        assert_eq!(cluster_cap(local, 0), local);
    }

    /// The settled clock cap covers every version stamped before it, the
    /// whole of its millisecond, and no version stamped after it.
    #[tokio::test]
    async fn a_settled_clock_cap_closes_its_millisecond() {
        let clock = nodedb_types::OrdinalClock::new();
        let before = clock.next_ordinal();
        let cap = settled_clock_cap(&clock).await;
        let after = clock.next_ordinal();
        assert!(
            before <= cap,
            "a write before the statement is under the cap"
        );
        assert!(after > cap, "a write after the statement is above the cap");
        assert_eq!(
            cap,
            nodedb_types::ms_to_ordinal_upper(nodedb_types::ordinal_to_ms(cap)),
            "the cap is the last ordinal of its millisecond"
        );
    }

    #[tokio::test]
    async fn a_clock_far_ahead_of_the_wall_is_not_waited_out() {
        let clock = nodedb_types::OrdinalClock::new();
        let lead = nodedb_types::wall_now_ns_i64() + 10_000_000_000;
        clock.update_from_remote(lead);
        let started = std::time::Instant::now();
        let cap = settled_clock_cap(&clock).await;
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(cap, lead + 1, "a leading clock keeps its fresh ordinal");
    }

    #[test]
    fn every_pin_of_one_statement_shares_its_marker() {
        let cap = StatementCap {
            cap: 10_000_000_000,
            marker: 7,
        };
        let early = cap.pin(3);
        let late = cap.pin(1_000_000);
        assert_eq!(early.marker, 7);
        assert_eq!(late.marker, 7);
        assert_eq!(
            late.ordinal, 10_000_000_000,
            "a future time reads at the cap"
        );
    }
}
