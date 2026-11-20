// SPDX-License-Identifier: BUSL-1.1

//! The WAL truncation point of one checkpoint cycle.

/// Decide the WAL truncation LSN for a checkpoint cycle.
///
/// Returns `None` (defer truncation) unless EVERY core reported a fresh
/// flush LSN. A core that failed to dispatch or missed its response
/// deadline can still hold acknowledged-but-unflushed records below the
/// reporting cores' minimum LSN; truncating there deletes them and
/// loses the writes on restart. Also returns `None` when the minimum is 0
/// (no writes yet, nothing to truncate).
///
/// ## Why the Event Plane is a floor too
///
/// The engines are not the only consumer of the WAL. Each Event Plane consumer
/// recovers from exactly one place — the WAL suffix above its persisted
/// watermark — and that watermark is flushed lazily, so it always trails the
/// engines. Once a segment below a consumer's watermark is unlinked, that
/// suffix is gone: every CDC row, trigger fire, and streaming-MV update for the
/// acknowledged writes in it is unrecoverable. Replay refuses a request
/// below the retained floor rather than returning the shorter suffix that
/// survives, so the loss is loud instead of silent. Refusing is only an
/// alarm. This floor keeps the loss from happening.
///
/// `event_watermarks` is therefore folded in exactly as each core's engine LSN
/// is, with the same conservatism: one entry per core, and a core that has
/// persisted nothing contributes 0, which defers the whole cycle. Deferring
/// costs WAL growth until the next cycle; not deferring costs the events.
pub(super) fn checkpoint_truncation_lsn(
    reported_lsns: &[u64],
    event_watermarks: &[u64],
    num_cores: usize,
) -> Option<u64> {
    if reported_lsns.len() != num_cores || event_watermarks.len() != num_cores {
        return None;
    }
    let engine_min = *reported_lsns.iter().min()?;
    let event_min = *event_watermarks.iter().min()?;
    let min = engine_min.min(event_min);
    if min == 0 { None } else { Some(min) }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Event Plane watermarks that never bind, so a case can isolate the
    /// engine-side rule it is actually about.
    fn ahead(num_cores: usize) -> Vec<u64> {
        vec![u64::MAX; num_cores]
    }

    #[test]
    fn all_cores_reported_distinct_lsns_returns_min() {
        assert_eq!(
            checkpoint_truncation_lsn(&[10, 5, 8], &ahead(3), 3),
            Some(5)
        );
    }

    #[test]
    fn one_core_missing_defers_even_though_responders_have_a_min() {
        // Truncating at 5 deletes the missing core's unflushed records.
        assert_eq!(checkpoint_truncation_lsn(&[5, 10], &ahead(3), 3), None);
    }

    #[test]
    fn dispatch_gap_only_one_core_responded_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[7], &ahead(2), 2), None);
    }

    #[test]
    fn all_reported_but_min_is_zero_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[0, 5], &ahead(2), 2), None);
    }

    #[test]
    fn single_core_reported_returns_its_lsn() {
        assert_eq!(checkpoint_truncation_lsn(&[42], &ahead(1), 1), Some(42));
    }

    #[test]
    fn degenerate_empty_input_does_not_panic() {
        assert_eq!(checkpoint_truncation_lsn(&[], &[], 0), None);
    }

    /// The Event Plane binds truncation when it trails the engines. Its
    /// consumers recover ONLY from the WAL above their persisted watermark, so
    /// truncating at the engine minimum drops every CDC row, trigger fire
    /// and MV update in between — unrecoverably, whether or not replay notices.
    #[test]
    fn event_plane_behind_the_engines_clamps_truncation_to_it() {
        assert_eq!(
            checkpoint_truncation_lsn(&[900, 900], &[400, 950], 2),
            Some(400),
            "the lagging consumer's watermark is the floor, not the engines' minimum"
        );
    }

    /// A consumer that has persisted nothing has processed nothing it can prove,
    /// so the whole cycle defers rather than truncating to the engine minimum.
    #[test]
    fn consumer_with_no_persisted_watermark_defers_the_cycle() {
        assert_eq!(checkpoint_truncation_lsn(&[900, 900], &[900, 0], 2), None);
    }

    /// An Event Plane ahead of the engines changes nothing: the engines are
    /// still the binding floor, exactly as before.
    #[test]
    fn event_plane_ahead_of_the_engines_does_not_raise_the_floor() {
        assert_eq!(
            checkpoint_truncation_lsn(&[500, 800], &[900, 900], 2),
            Some(500)
        );
    }

    /// A watermark set shorter than the core count means a core's progress is
    /// unknown — unknown is never permissive.
    #[test]
    fn missing_event_watermark_for_a_core_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[900, 900], &[900], 2), None);
    }
}
