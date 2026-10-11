// SPDX-License-Identifier: BUSL-1.1

//! Bounded inbox for Calvin sequencer submissions.
//!
//! The [`Inbox`] is the admission point for cross-shard transactions. Only the
//! sequencer leader admits transactions; followers reject with
//! [`SequencerError::NotLeader`].
//!
//! [`InboxReceiver`] is held by [`super::service::SequencerService`]; it drains
//! the inbox each epoch tick.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::warn;

use crate::calvin::completion::{AssignmentReceiver, CalvinCompletionRegistry};
use crate::calvin::sequencer::config::SequencerConfig;
use crate::calvin::sequencer::error::SequencerError;
use crate::calvin::sequencer::parts_intake::{PartsIntake, PartsOffer};
use crate::calvin::types::{PartStreamId, StreamedPart, TxClass};

use nodedb_types::TenantId;

/// A transaction that has been admitted to the inbox and assigned a
/// monotonic `inbox_seq` by the leader.
///
/// `inbox_seq` is the primary conflict-ordering key within an epoch. It is
/// assigned at admit time on the leader, used by the validator to break ties
/// deterministically, and is NOT serialized into the Raft log (it is only
/// relevant within a leader's tenure as a tiebreaker).
#[derive(Debug, Clone)]
pub struct AdmittedTx {
    /// Monotonically increasing counter assigned at admit time. Used for
    /// deterministic tie-breaking in the validator.
    pub inbox_seq: u64,
    /// The fully-declared transaction class submitted by the client.
    pub tx_class: TxClass,
}

/// A transaction that was rejected by the pre-validation pass.
#[derive(Debug)]
pub struct RejectedTx {
    /// The original admitted transaction.
    pub admitted: AdmittedTx,
    /// Why the transaction was rejected.
    pub reason: SequencerError,
    /// When `reason` is [`SequencerError::Conflict`], this carries the
    /// `(tenant, engine, collection)` context of the conflicting key so that
    /// the metrics layer can record per-context hotness.
    pub conflict_context: Option<crate::calvin::sequencer::metrics::ConflictKey>,
}

/// The sending half of the sequencer inbox.
///
/// Held by API submitters (one per connection, cloned freely). Submission is
/// lock-free via the underlying bounded mpsc channel. The `inbox_seq` counter
/// is shared across all senders via an `Arc<AtomicU64>`.
#[derive(Clone)]
pub struct Inbox {
    tx: mpsc::Sender<AdmittedTx>,
    next_seq: Arc<AtomicU64>,
    /// Shared depth counter: incremented on successful submit, decremented on
    /// drain. Used by `InboxReceiver::depth()` for the Prometheus gauge.
    depth_counter: Arc<AtomicU64>,
    /// Per-tenant in-flight count shared with the receiver. Uses `BTreeMap`
    /// for deterministic iteration order (determinism contract).
    tenant_in_flight: Arc<Mutex<BTreeMap<u64, u64>>>,
    max_plans_bytes: usize,
    max_participating_vshards: usize,
    /// The leader's queue of streamed parts, shared with the receiver.
    parts: Arc<PartsIntake>,
    tenant_quota: usize,
    max_dependent_read_bytes: usize,
    max_dependent_read_passives: usize,
}

/// The receiving half of the sequencer inbox.
///
/// Held exclusively by [`super::service::SequencerService`]. Not `Clone`.
pub struct InboxReceiver {
    rx: mpsc::Receiver<AdmittedTx>,
    /// Snapshot of how many items are currently queued (capacity - permits).
    capacity: usize,
    depth_counter: Arc<AtomicU64>,
    /// Per-tenant in-flight count shared with all [`Inbox`] senders.
    tenant_in_flight: Arc<Mutex<BTreeMap<u64, u64>>>,
    /// Single-slot lookahead: when `drain_into_capped` stops because the next
    /// item would push the epoch's byte total over the cap, the item is held
    /// here and drained first on the next call. The tenant counter and
    /// `depth_counter` are NOT decremented while an item sits here — they are
    /// decremented when the item is finally moved into the output vector.
    pending: Option<AdmittedTx>,
    /// The leader's queue of streamed parts, shared with every sender.
    parts: Arc<PartsIntake>,
}

impl Inbox {
    /// Submit a transaction to the sequencer inbox.
    ///
    /// Checks are performed in fail-fast order; no state mutation occurs on
    /// rejection:
    ///
    /// 1. A single entry above the per-entry byte cap → `TxnTooLarge`. A
    ///    multi-part header with plans or a malformed manifest →
    ///    `MalformedParts`.
    /// 2. A single entry above the per-entry vShard cap → `FanoutTooWide`.
    /// 3. Dependent-read payload too large → `DependentReadTooLarge`.
    /// 4. Dependent-read fan-in too wide → `DependentReadFanoutTooWide`.
    /// 5. Tenant in-flight quota exceeded → `TenantQuotaExceeded`.
    /// 6. Channel full → `Overloaded` (tenant counter rolled back).
    ///
    /// Returns `Ok(inbox_seq)` on success.
    ///
    /// This call is **non-blocking**: it never waits for the epoch ticker.
    pub fn submit(&self, tx_class: TxClass) -> Result<u64, SequencerError> {
        self.admit(tx_class, |_| (), |_| ()).map(|(seq, ())| seq)
    }

    /// [`Self::submit`], registering for the submission's assignment in
    /// `registry` before the submission reaches the channel.
    ///
    /// The epoch tick can drain the submission the moment it is sent. Its
    /// `note_assigned` then always finds the sender, so no assignment is lost.
    /// A send that fails drops the registration again.
    pub fn submit_with(
        &self,
        tx_class: TxClass,
        registry: &CalvinCompletionRegistry,
    ) -> Result<(u64, AssignmentReceiver), SequencerError> {
        self.admit(
            tx_class,
            |seq| registry.register_submission(seq),
            |seq| registry.drop_assignment(seq),
        )
    }

    /// The submit body. `register` runs with the assigned seq before the
    /// send. `unregister` runs with it when the send fails.
    fn admit<R>(
        &self,
        tx_class: TxClass,
        register: impl FnOnce(u64) -> R,
        unregister: impl FnOnce(u64),
    ) -> Result<(u64, R), SequencerError> {
        // Checks 1 & 2: a single entry fits one entry, and a multi-part
        // header is well formed. Its parts are checked as they stream in.
        crate::calvin::sequencer::entry_limits::check_entry_shape(
            &tx_class,
            crate::calvin::sequencer::entry_limits::EntryLimits {
                max_plans_bytes: self.max_plans_bytes,
                max_participating_vshards: self.max_participating_vshards,
            },
        )?;

        // Checks 3 & 4: dependent-read caps.
        if let Some(spec) = &tx_class.dependent_reads {
            let total_bytes = spec.total_bytes();
            if total_bytes > self.max_dependent_read_bytes {
                return Err(SequencerError::DependentReadTooLarge {
                    bytes: total_bytes,
                    limit: self.max_dependent_read_bytes,
                });
            }
            let passives = spec.passive_reads.len();
            if passives > self.max_dependent_read_passives {
                return Err(SequencerError::DependentReadFanoutTooWide {
                    passives,
                    limit: self.max_dependent_read_passives,
                });
            }
        }

        // Check 3: per-tenant quota. Increment under lock; roll back on send
        // failure.
        let tenant = tx_class.tenant_id;
        let tenant_key = tenant.as_u64();
        {
            let mut map = self
                .tenant_in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let in_flight = map.entry(tenant_key).or_insert(0);
            if *in_flight >= self.tenant_quota as u64 {
                return Err(SequencerError::TenantQuotaExceeded {
                    tenant: tenant.as_u64(),
                    quota: self.tenant_quota,
                    in_flight: *in_flight as usize,
                });
            }
            *in_flight += 1;
        }

        // Assign sequence number and try to send.
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let admitted = AdmittedTx {
            inbox_seq: seq,
            tx_class,
        };

        // Add the `sequencer_admit` span here; trace_id threading requires
        // the caller to propagate a trace ID through the TxClass envelope
        // (not yet available in v1).
        let _admit_span = tracing::info_span!(
            "sequencer_admit",
            tenant_id = admitted.tx_class.tenant_id.as_u64(),
            inbox_seq = seq,
        );

        let registered = register(seq);
        match self.tx.try_send(admitted) {
            Ok(()) => {
                self.depth_counter.fetch_add(1, Ordering::Relaxed);
                Ok((seq, registered))
            }
            Err(e) => {
                unregister(seq);
                // Roll back the tenant counter because the message was never
                // enqueued.
                {
                    let mut map = self
                        .tenant_in_flight
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if let Some(count) = map.get_mut(&tenant_key) {
                        *count = count.saturating_sub(1);
                    }
                }
                match e {
                    mpsc::error::TrySendError::Full(_) => Err(SequencerError::Overloaded),
                    mpsc::error::TrySendError::Closed(_) => {
                        warn!("sequencer inbox channel closed; service may have exited");
                        Err(SequencerError::Unavailable)
                    }
                }
            }
        }
    }

    /// Current number of items queued in the inbox (approximate).
    pub fn depth(&self) -> usize {
        self.depth_counter.load(Ordering::Relaxed) as usize
    }

    /// Offer streamed parts of a multi-part transaction to this node's
    /// sequencer leader queue.
    pub fn offer_parts(&self, stream: PartStreamId, parts: Vec<StreamedPart>) -> PartsOffer {
        self.parts.offer(stream, parts)
    }
}

impl InboxReceiver {
    /// Drain up to `max_count` transactions or until the cumulative `plans`
    /// bytes would exceed `max_bytes`.
    ///
    /// The `pending` slot is drained first. When the next candidate would push
    /// the running byte total over `max_bytes`, it is stored in `pending` and
    /// the drain stops. Items left in `pending` are **not** decremented from
    /// `tenant_in_flight` or `depth_counter` — they remain attributed to the
    /// tenant until they are actually emitted in a subsequent call.
    ///
    /// Returns the number of transactions added to `out`.
    pub fn drain_into_capped(
        &mut self,
        out: &mut Vec<AdmittedTx>,
        max_count: usize,
        max_bytes: usize,
    ) -> usize {
        let mut count = 0;
        let mut bytes_so_far: usize = 0;

        // Drain the pending slot first.
        if let Some(pending) = self.pending.take() {
            let tx_bytes = pending.tx_class.plans.len();
            if count < max_count && bytes_so_far.saturating_add(tx_bytes) <= max_bytes {
                bytes_so_far += tx_bytes;
                self.decrement_tenant(&pending.tx_class.tenant_id);
                self.depth_counter.fetch_sub(1, Ordering::Relaxed);
                out.push(pending);
                count += 1;
            } else {
                // Can't fit it — put it back and stop.
                self.pending = Some(pending);
                return 0;
            }
        }

        // Drain from the channel.
        while count < max_count {
            match self.rx.try_recv() {
                Ok(tx) => {
                    let tx_bytes = tx.tx_class.plans.len();
                    let new_total = bytes_so_far.saturating_add(tx_bytes);
                    if new_total > max_bytes {
                        // Defer to next epoch.
                        self.pending = Some(tx);
                        break;
                    }
                    bytes_so_far = new_total;
                    self.decrement_tenant(&tx.tx_class.tenant_id);
                    self.depth_counter.fetch_sub(1, Ordering::Relaxed);
                    out.push(tx);
                    count += 1;
                }
                Err(_) => break,
            }
        }

        count
    }

    /// Drain and discard all items including the `pending` slot.
    ///
    /// Used by the non-leader and halted discard paths. Decrements
    /// `tenant_in_flight` and `depth_counter` per item. Returns the
    /// `inbox_seq` of every discarded item, so the caller can drop each
    /// one's assignment.
    pub fn drain_all_discard(&mut self) -> Vec<u64> {
        let mut discarded = Vec::new();

        // Discard the pending slot.
        if let Some(pending) = self.pending.take() {
            self.decrement_tenant(&pending.tx_class.tenant_id);
            self.depth_counter.fetch_sub(1, Ordering::Relaxed);
            discarded.push(pending.inbox_seq);
        }

        // Drain the channel.
        while let Ok(tx) = self.rx.try_recv() {
            self.decrement_tenant(&tx.tx_class.tenant_id);
            self.depth_counter.fetch_sub(1, Ordering::Relaxed);
            discarded.push(tx.inbox_seq);
        }

        discarded
    }

    /// The inbox's configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The leader's queue of streamed parts.
    pub fn parts_intake(&self) -> Arc<PartsIntake> {
        Arc::clone(&self.parts)
    }

    /// Approximate current depth (items queued).
    pub fn depth(&self) -> u64 {
        self.depth_counter.load(Ordering::Relaxed)
    }

    fn decrement_tenant(&self, tenant: &TenantId) {
        let key = tenant.as_u64();
        let mut map = self
            .tenant_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(count) = map.get_mut(&key) {
            *count = count.saturating_sub(1);
        }
    }
}

/// Build a linked (`Inbox`, `InboxReceiver`) pair configured from `config`.
///
/// Both halves share the `tenant_in_flight` `Arc` so per-tenant counts remain
/// consistent across submit and drain.
pub fn new_inbox(capacity: usize, config: &SequencerConfig) -> (Inbox, InboxReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    let next_seq = Arc::new(AtomicU64::new(0));
    let depth_counter = Arc::new(AtomicU64::new(0));
    let tenant_in_flight: Arc<Mutex<BTreeMap<u64, u64>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let parts = Arc::new(PartsIntake::new(config));

    let inbox = Inbox {
        tx,
        next_seq,
        depth_counter: Arc::clone(&depth_counter),
        tenant_in_flight: Arc::clone(&tenant_in_flight),
        max_plans_bytes: config.max_plans_bytes_per_txn,
        max_participating_vshards: config.max_participating_vshards_per_txn,
        parts: Arc::clone(&parts),
        tenant_quota: config.tenant_inbox_quota,
        max_dependent_read_bytes: config.max_dependent_read_bytes_per_txn,
        max_dependent_read_passives: config.max_dependent_read_passives_per_txn,
    };
    let receiver = InboxReceiver {
        rx,
        capacity,
        depth_counter,
        tenant_in_flight,
        pending: None,
        parts,
    };
    (inbox, receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calvin::types::{EngineKeySet, ReadWriteSet, SortedVec, TxClass};
    use nodedb_types::id::{CollectionKey, DatabaseId};

    fn default_config() -> SequencerConfig {
        SequencerConfig::default()
    }

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    fn make_tx_class_with_tenant(tenant: u64) -> TxClass {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![1]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![2]),
            },
        ]);
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![],
            TenantId::new(tenant),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    fn make_tx_class() -> TxClass {
        make_tx_class_with_tenant(1)
    }

    #[test]
    fn submit_succeeds_when_capacity_available() {
        let config = default_config();
        let (inbox, mut rx) = new_inbox(10, &config);
        let seq = inbox
            .submit(make_tx_class())
            .expect("submit should succeed");
        assert_eq!(seq, 0);
        let mut out = Vec::new();
        let drained = rx.drain_into_capped(&mut out, 100, usize::MAX);
        assert_eq!(drained, 1);
        assert_eq!(out[0].inbox_seq, 0);
    }

    #[test]
    fn submit_returns_overloaded_when_full() {
        let config = SequencerConfig {
            inbox_capacity: 1,
            tenant_inbox_quota: 10,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(1, &config);
        let _ = inbox.submit(make_tx_class()).expect("first submit");
        let err = inbox
            .submit(make_tx_class())
            .expect_err("should be overloaded");
        assert_eq!(err, SequencerError::Overloaded);
    }

    #[test]
    fn inbox_seq_is_monotonically_increasing() {
        let config = default_config();
        let (inbox, mut rx) = new_inbox(10, &config);
        for _ in 0..5 {
            inbox.submit(make_tx_class()).expect("submit");
        }
        let mut out = Vec::new();
        rx.drain_into_capped(&mut out, 100, usize::MAX);
        let seqs: Vec<u64> = out.iter().map(|t| t.inbox_seq).collect();
        let mut sorted = seqs.clone();
        sorted.sort();
        assert_eq!(seqs, sorted, "inbox_seq must be monotonically increasing");
    }

    #[test]
    fn drain_into_returns_count_and_clears_buffer() {
        let config = default_config();
        let (inbox, mut rx) = new_inbox(10, &config);
        inbox.submit(make_tx_class()).expect("s1");
        inbox.submit(make_tx_class()).expect("s2");
        inbox.submit(make_tx_class()).expect("s3");
        let mut out = Vec::new();
        let n = rx.drain_into_capped(&mut out, 100, usize::MAX);
        assert_eq!(n, 3);
        assert_eq!(out.len(), 3);
        // Second drain should find nothing.
        let n2 = rx.drain_into_capped(&mut out, 100, usize::MAX);
        assert_eq!(n2, 0);
    }

    /// The tick can drain a submission before the submitter's next line runs.
    /// Registering inside `submit_with` means the assignment still reaches the
    /// submitter.
    #[test]
    fn submit_with_delivers_the_assignment_to_a_drain_right_after_submit() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (inbox, mut rx) = new_inbox(10, &default_config());
        let (seq, mut assignment) = inbox
            .submit_with(make_tx_class(), &registry)
            .expect("submit");

        let mut out = Vec::new();
        assert_eq!(rx.drain_into_capped(&mut out, 100, usize::MAX), 1);
        registry.note_assigned(out[0].inbox_seq, crate::calvin::TxnId::new(3, 0), 2);

        assert_eq!(out[0].inbox_seq, seq);
        assert_eq!(assignment.try_recv(), Ok((3, 0, 2)));
    }

    /// A submission the channel refuses holds no assignment sender.
    #[test]
    fn submit_with_drops_the_registration_when_the_send_fails() {
        let registry = CalvinCompletionRegistry::new_detached();
        let config = SequencerConfig {
            inbox_capacity: 1,
            tenant_inbox_quota: 10,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(1, &config);
        inbox
            .submit_with(make_tx_class(), &registry)
            .expect("first submit fills the channel");
        let err = inbox
            .submit_with(make_tx_class(), &registry)
            .expect_err("channel full");
        assert_eq!(err, SequencerError::Overloaded);
        assert_eq!(registry.pending_assignments_len(), 1);
    }

    #[test]
    fn drain_all_discard_returns_every_discarded_seq() {
        let (inbox, mut rx) = new_inbox(10, &default_config());
        for _ in 0..3 {
            inbox.submit(make_tx_class()).expect("submit");
        }
        // Park one item in the pending slot: a zero-byte cap defers it.
        let mut tx = make_tx_class();
        tx.plans = vec![0u8; 4];
        inbox.submit(tx).expect("submit");
        let mut out = Vec::new();
        rx.drain_into_capped(&mut out, 100, 0);
        assert_eq!(out.len(), 3);

        assert_eq!(rx.drain_all_discard(), vec![3]);
        assert_eq!(rx.depth(), 0);
    }

    #[test]
    fn capacity_is_reported_correctly() {
        let config = default_config();
        let (_inbox, rx) = new_inbox(42, &config);
        assert_eq!(rx.capacity(), 42);
    }

    // ── New admission-cap tests ───────────────────────────────────────────────

    #[test]
    fn submit_rejects_oversized_plans() {
        let config = SequencerConfig {
            max_plans_bytes_per_txn: 4,
            tenant_inbox_quota: 100,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(10, &config);
        let mut tx = make_tx_class();
        tx.plans = vec![0u8; 5]; // 5 bytes > limit of 4
        let err = inbox.submit(tx).expect_err("should be rejected");
        assert!(
            matches!(err, SequencerError::TxnTooLarge { bytes: 5, limit: 4 }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn submit_rejects_wide_fanout() {
        // Build a TxClass whose write set spans > 1 distinct vshard.
        // We use a per-txn cap of 1 vshard so even 2 is too many.
        let config = SequencerConfig {
            max_participating_vshards_per_txn: 1,
            tenant_inbox_quota: 100,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(10, &config);
        // make_tx_class() always produces 2 distinct vshards.
        let tx = make_tx_class();
        let err = inbox.submit(tx).expect_err("should be rejected");
        assert!(
            matches!(err, SequencerError::FanoutTooWide { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn tenant_quota_isolation() {
        // Tenant 1 fills its quota; tenant 2 must still be accepted.
        let config = SequencerConfig {
            inbox_capacity: 100,
            tenant_inbox_quota: 2,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(100, &config);

        inbox
            .submit(make_tx_class_with_tenant(1))
            .expect("t1 first");
        inbox
            .submit(make_tx_class_with_tenant(1))
            .expect("t1 second");
        // Third submission from tenant 1 must fail.
        let err = inbox
            .submit(make_tx_class_with_tenant(1))
            .expect_err("t1 quota exceeded");
        assert!(
            matches!(err, SequencerError::TenantQuotaExceeded { tenant: 1, .. }),
            "unexpected error: {err:?}"
        );
        // Tenant 2 must still be accepted.
        inbox
            .submit(make_tx_class_with_tenant(2))
            .expect("t2 unaffected by t1 quota");
    }

    #[test]
    fn tenant_quota_decrements_on_drain() {
        let config = SequencerConfig {
            inbox_capacity: 10,
            tenant_inbox_quota: 1,
            ..default_config()
        };
        let (inbox, mut rx) = new_inbox(10, &config);

        // Fill tenant's quota.
        inbox
            .submit(make_tx_class_with_tenant(7))
            .expect("first submit");
        // Quota exhausted.
        assert!(inbox.submit(make_tx_class_with_tenant(7)).is_err());

        // Drain the item; quota should free up.
        let mut out = Vec::new();
        let n = rx.drain_into_capped(&mut out, 10, usize::MAX);
        assert_eq!(n, 1);

        // Now tenant 7 should be accepted again.
        inbox
            .submit(make_tx_class_with_tenant(7))
            .expect("submit after drain");
    }

    #[test]
    fn tenant_counter_rolls_back_on_send_failure() {
        // Channel capacity = 1, quota = 10. Fill the channel, then try to
        // submit again. The channel is full before the quota is reached, so
        // the Overloaded path must roll back the tenant counter increment.
        let config = SequencerConfig {
            inbox_capacity: 1,
            tenant_inbox_quota: 10,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(1, &config);

        // Fill the channel.
        inbox
            .submit(make_tx_class_with_tenant(3))
            .expect("first submit fills channel");

        // This submit fails because the channel is full (Overloaded), NOT
        // because of the quota.
        let err = inbox
            .submit(make_tx_class_with_tenant(3))
            .expect_err("channel full");
        assert_eq!(err, SequencerError::Overloaded);

        // The tenant counter must NOT remain at 2 — it must have been rolled
        // back to 1. Verify by checking the in-flight map directly.
        let map = inbox
            .tenant_in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let in_flight = *map.get(&TenantId::new(3).as_u64()).unwrap_or(&0);
        assert_eq!(
            in_flight, 1,
            "tenant counter must be 1 (rolled back from 2 on Overloaded), got {in_flight}"
        );
    }

    #[test]
    fn submit_rejects_dependent_read_too_large() {
        use crate::calvin::types::{DependentReadSpec, PassiveReadKey};
        use std::collections::BTreeMap;

        let config = SequencerConfig {
            // Force a very tight byte limit: 4 bytes = one surrogate.
            max_dependent_read_bytes_per_txn: 4,
            tenant_inbox_quota: 100,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(10, &config);

        // Build a tx with a dependent-read spec that has 2 surrogates (8 bytes).
        let mut tx = make_tx_class();
        tx.dependent_reads = Some(DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                // 2 surrogates × 4 bytes = 8 bytes > limit of 4.
                m.insert(
                    999u32,
                    vec![PassiveReadKey {
                        engine_key: crate::calvin::types::EngineKeySet::Document {
                            collection: "passive_col".to_owned(),
                            surrogates: crate::calvin::types::SortedVec::new(vec![1u32, 2u32]),
                        },
                    }],
                );
                m
            },
            expected: BTreeMap::new(),
        });

        let err = inbox.submit(tx).expect_err("should be rejected");
        assert!(
            matches!(
                err,
                SequencerError::DependentReadTooLarge { bytes: 8, limit: 4 }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn submit_rejects_dependent_read_fanout_too_wide() {
        use crate::calvin::types::{DependentReadSpec, EngineKeySet, PassiveReadKey, SortedVec};
        use std::collections::BTreeMap;

        let config = SequencerConfig {
            // Only 1 passive vshard allowed.
            max_dependent_read_passives_per_txn: 1,
            tenant_inbox_quota: 100,
            ..default_config()
        };
        let (inbox, _rx) = new_inbox(10, &config);

        let mut tx = make_tx_class();
        tx.dependent_reads = Some(DependentReadSpec {
            passive_reads: {
                let mut m = BTreeMap::new();
                for vshard in [101u32, 102u32] {
                    m.insert(
                        vshard,
                        vec![PassiveReadKey {
                            engine_key: EngineKeySet::Document {
                                collection: format!("col_{vshard}"),
                                surrogates: SortedVec::new(vec![vshard]),
                            },
                        }],
                    );
                }
                m
            },
            expected: BTreeMap::new(),
        });

        let err = inbox.submit(tx).expect_err("should be rejected");
        assert!(
            matches!(
                err,
                SequencerError::DependentReadFanoutTooWide {
                    passives: 2,
                    limit: 1
                }
            ),
            "unexpected error: {err:?}"
        );
    }
}
