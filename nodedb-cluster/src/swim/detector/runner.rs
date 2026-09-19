// SPDX-License-Identifier: BUSL-1.1

//! `FailureDetector` — the SWIM runtime task.
//!
//! One instance per node. Owns the membership list (shared via `Arc`),
//! the probe scheduler, the suspicion timer, the inflight-probe registry,
//! and the async transport. Drives a `tokio::select!` loop over four
//! arms: probe tick, inbound datagram, suspicion expiry, shutdown.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::time::{Instant, interval};

use crate::swim::config::SwimConfig;
use crate::swim::dissemination::{DisseminationQueue, apply_and_disseminate};
use crate::swim::error::SwimError;
use crate::swim::incarnation::Incarnation;
use crate::swim::incarnation_store::IncarnationStore;
use crate::swim::member::MemberState;
use crate::swim::member::record::MemberUpdate;
use crate::swim::membership::{MembershipList, MergeOutcome};
use crate::swim::subscriber::MembershipSubscriber;
use crate::swim::wire::{Ack, Ping, PingReq, ProbeId, SwimMessage};

use super::probe_round::{InflightProbes, ProbeOutcome, ProbeRound};
use super::scheduler::ProbeScheduler;
use super::suspicion::SuspicionTimer;
use super::transport::Transport;

/// Minimum interval between self-alive advertisements on the ping-ack
/// path. A heavily-pinged node would otherwise flood its dissemination
/// queue with copies of its own record, starving real gossip.
const SELF_ADVERTISE_INTERVAL: Duration = Duration::from_millis(500);

/// Top-level failure detector handle.
///
/// Construct with [`FailureDetector::new`], then call
/// [`FailureDetector::run`] on a dedicated tokio task. The run loop
/// returns when `shutdown` flips to `true`.
pub struct FailureDetector {
    cfg: SwimConfig,
    membership: Arc<MembershipList>,
    transport: Arc<dyn Transport>,
    scheduler: Mutex<ProbeScheduler>,
    suspicion: Mutex<SuspicionTimer>,
    inflight: Arc<InflightProbes>,
    dissemination: Arc<DisseminationQueue>,
    probe_counter: AtomicU64,
    local_incarnation: Mutex<Incarnation>,
    subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    /// Persistence backend for the local incarnation. `None` in bare
    /// tests; production installs a catalog-backed store so a restart
    /// can resume above its last advertised value.
    incarnation_store: Option<Arc<dyn IncarnationStore>>,
    /// When we last enqueued our own `Alive` record on the ping-ack
    /// path; rate-limits self-advertisement (see
    /// [`SELF_ADVERTISE_INTERVAL`]).
    last_self_advertise: Mutex<Option<Instant>>,
}

impl FailureDetector {
    /// Construct. Does not spawn anything — the caller is responsible
    /// for driving [`Self::run`] on a tokio task.
    pub fn new(
        cfg: SwimConfig,
        membership: Arc<MembershipList>,
        transport: Arc<dyn Transport>,
        scheduler: ProbeScheduler,
    ) -> Self {
        Self::with_subscribers(cfg, membership, transport, scheduler, Vec::new())
    }

    /// Construct with a list of [`MembershipSubscriber`]s that will be
    /// notified on every member state transition.
    pub fn with_subscribers(
        cfg: SwimConfig,
        membership: Arc<MembershipList>,
        transport: Arc<dyn Transport>,
        scheduler: ProbeScheduler,
        subscribers: Vec<Arc<dyn MembershipSubscriber>>,
    ) -> Self {
        let initial_inc = cfg.initial_incarnation;
        Self {
            cfg,
            membership,
            transport,
            scheduler: Mutex::new(scheduler),
            suspicion: Mutex::new(SuspicionTimer::new()),
            inflight: Arc::new(InflightProbes::new()),
            dissemination: Arc::new(DisseminationQueue::new()),
            probe_counter: AtomicU64::new(0),
            local_incarnation: Mutex::new(initial_inc),
            subscribers,
            incarnation_store: None,
            last_self_advertise: Mutex::new(None),
        }
    }

    /// Install the persistence backend for the local incarnation.
    /// Production wires a catalog-backed store so self-refutation bumps
    /// survive a restart; bare tests leave it `None`.
    pub fn set_incarnation_store(&mut self, store: Arc<dyn IncarnationStore>) {
        self.incarnation_store = Some(store);
    }

    /// Apply an update via [`apply_and_disseminate`] while notifying
    /// every subscriber of any resulting state transition. Returns the
    /// raw [`MergeOutcome`] so callers can still react to
    /// `SelfRefute` etc.
    fn apply_and_notify(&self, update: &MemberUpdate) -> MergeOutcome {
        let old_state = self.membership.get(&update.node_id).map(|m| m.state);
        let outcome = apply_and_disseminate(&self.membership, &self.dissemination, update);
        if self.subscribers.is_empty() {
            return outcome;
        }
        let new_state = match self.membership.get(&update.node_id) {
            Some(m) => m.state,
            None => return outcome,
        };
        if old_state != Some(new_state) {
            let incarnation = self
                .membership
                .get(&update.node_id)
                .map(|member| member.incarnation.get())
                .unwrap_or(0);
            for sub in &self.subscribers {
                sub.on_state_change(&update.node_id, old_state, new_state);
                sub.on_state_change_with_incarnation(&update.node_id, new_state, incarnation);
            }
        }
        outcome
    }

    /// Shared reference to the dissemination queue. Tests use it to
    /// enqueue synthetic rumours without constructing a full message.
    pub fn dissemination(&self) -> &Arc<DisseminationQueue> {
        &self.dissemination
    }

    /// Ingest every piggyback entry attached to an inbound datagram.
    /// Applies each update to the membership list via
    /// [`apply_and_disseminate`] and, on a self-refutation, bumps the
    /// local incarnation so subsequent probes advertise the new value.
    ///
    /// Returns the refutations to echo deterministically back to the
    /// sender: whenever we `Refute` an update (the sender gossiped a
    /// stale view), we include our stored record in the reply piggyback.
    /// Without this, the refutation only reaches the sender
    /// probabilistically via the regular gossip fanout.
    async fn ingest_piggyback(&self, piggyback: &[MemberUpdate]) -> Vec<MemberUpdate> {
        let mut refutations = Vec::new();
        for update in piggyback {
            let outcome = self.apply_and_notify(update);
            match outcome {
                MergeOutcome::SelfRefute { new_incarnation } => {
                    let mut guard = self.local_incarnation.lock().await;
                    if new_incarnation > *guard {
                        *guard = new_incarnation;
                        // Persist the bump so a fast restart resumes above
                        // the rumoured incarnation. Off the probe path: the
                        // write must not stall detection, and the store is a
                        // synchronous catalog commit, so it goes to a
                        // blocking thread rather than a reactor worker.
                        //
                        // The write is allowed to land out of order with
                        // respect to another bump — `save` is monotonic and
                        // discards anything at or below what is already
                        // stored, so the higher incarnation always survives.
                        if let Some(store) = &self.incarnation_store {
                            let store = Arc::clone(store);
                            let v = new_incarnation.get();
                            tokio::task::spawn_blocking(move || {
                                if let Err(e) = store.save(v) {
                                    tracing::warn!(
                                        incarnation = v,
                                        error = %e,
                                        "failed to persist swim incarnation"
                                    );
                                }
                            });
                        }
                    }
                }
                MergeOutcome::Refute => {
                    // Echo our stored (newer) view back to the sender.
                    if let Some(stored) = self.membership.get(&update.node_id) {
                        refutations.push(MemberUpdate::from(&stored));
                    }
                }
                MergeOutcome::Insert | MergeOutcome::Apply => {
                    // A fresh Alive clears any pending suspicion timer for
                    // that node — the timer would otherwise promote it to
                    // Dead at expiry even though we just saw it live.
                    if update.state == MemberState::Alive {
                        self.suspicion.lock().await.cancel(&update.node_id);
                    }
                }
                MergeOutcome::Ignore | MergeOutcome::TerminalLeft => {}
            }
        }
        refutations
    }

    /// Exposed for tests that need to route a synthetic message into the
    /// inflight table without going through the transport.
    #[cfg(test)]
    pub fn inflight(&self) -> &Arc<InflightProbes> {
        &self.inflight
    }

    fn next_probe_id(&self) -> ProbeId {
        ProbeId::new(self.probe_counter.fetch_add(1, Ordering::Relaxed))
    }

    /// Main loop. Returns when `shutdown` receives `true`.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = interval(self.cfg.probe_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the first immediate tick so the first probe aligns
        // with a full interval from start.
        tick.tick().await;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
                _ = tick.tick() => {
                    self.on_tick().await;
                }
                recv = self.transport.recv() => {
                    match recv {
                        Ok((from_addr, msg)) => self.on_incoming(from_addr, msg).await,
                        Err(SwimError::TransportClosed) => break,
                        Err(_) => {}
                    }
                }
            }
        }
    }

    async fn on_tick(&self) {
        // Expire suspect members that have waited out their timeout.
        let now = Instant::now();
        let expired = self.suspicion.lock().await.drain_expired(now);
        for node_id in expired {
            if let Some(member) = self.membership.get(&node_id) {
                let dead_update = MemberUpdate {
                    node_id: node_id.clone(),
                    addr: member.addr.to_string(),
                    state: MemberState::Dead,
                    incarnation: member.incarnation,
                };
                self.apply_and_notify(&dead_update);
            }
        }

        // Execute one probe round against the next target.
        let local_inc = *self.local_incarnation.lock().await;
        let mut sched = self.scheduler.lock().await;
        let outcome = ProbeRound {
            scheduler: &mut sched,
            membership: &self.membership,
            transport: &self.transport,
            inflight: &self.inflight,
            dissemination: &self.dissemination,
            probe_timeout: self.cfg.probe_timeout,
            k_indirect: self.cfg.indirect_probes as usize,
            max_piggyback: self.cfg.max_piggyback,
            fanout_lambda: self.cfg.fanout_lambda,
            next_probe_id: || self.next_probe_id(),
            local_incarnation: local_inc,
        }
        .execute()
        .await;
        drop(sched);

        match outcome {
            Ok(ProbeOutcome::Idle) | Ok(ProbeOutcome::Acked { .. }) => {}
            Ok(ProbeOutcome::Suspect { target }) => {
                if let Some(member) = self.membership.get(&target) {
                    let suspect_update = MemberUpdate {
                        node_id: target.clone(),
                        addr: member.addr.to_string(),
                        state: MemberState::Suspect,
                        incarnation: member.incarnation,
                    };
                    self.apply_and_notify(&suspect_update);
                    let cluster_size = self.membership.len();
                    self.suspicion.lock().await.arm(
                        target,
                        Instant::now(),
                        &self.cfg,
                        cluster_size,
                    );
                }
            }
            Err(_) => {}
        }
    }

    async fn on_incoming(&self, from_addr: SocketAddr, msg: SwimMessage) {
        // Every datagram carries piggyback; ingest before dispatching so
        // a self-refutation bump is reflected in the outgoing Ack below.
        // Refutations (our stored view of records the sender got wrong)
        // ride back deterministically on the reply.
        let refutations = self.ingest_piggyback(msg.piggyback()).await;
        match msg {
            SwimMessage::Ping(ping) => self.handle_ping(from_addr, ping, refutations).await,
            SwimMessage::PingReq(req) => self.handle_ping_req(from_addr, req, refutations).await,
            SwimMessage::Ack(ack) => {
                self.inflight
                    .resolve(ack.probe_id, SwimMessage::Ack(ack))
                    .await
            }
            SwimMessage::Nack(nack) => {
                self.inflight
                    .resolve(nack.probe_id, SwimMessage::Nack(nack))
                    .await
            }
        }
    }

    async fn handle_ping(
        &self,
        from_addr: SocketAddr,
        ping: Ping,
        mut refutations: Vec<MemberUpdate>,
    ) {
        // A ping IS liveness evidence (SWIM paper §3): apply the sender's
        // claim before acking so an up-but-Dead node gets credit for
        // probing. Stale claims fall out naturally via the merge rules.
        let sender_claim = MemberUpdate {
            node_id: ping.from.clone(),
            addr: from_addr.to_string(),
            state: MemberState::Alive,
            incarnation: ping.incarnation,
        };
        let outcome = self.apply_and_notify(&sender_claim);
        if matches!(outcome, MergeOutcome::Insert | MergeOutcome::Apply) {
            self.suspicion.lock().await.cancel(&ping.from);
        }
        if matches!(outcome, MergeOutcome::Refute)
            && let Some(stored) = self.membership.get(&ping.from)
        {
            refutations.push(MemberUpdate::from(&stored));
        }

        // Rate-limited self-advertisement: a heavily-pinged node should
        // not flood the queue with copies of its own record.
        let advertise = {
            let mut last = self.last_self_advertise.lock().await;
            let now = Instant::now();
            match *last {
                Some(prev) if now.duration_since(prev) < SELF_ADVERTISE_INTERVAL => false,
                _ => {
                    *last = Some(now);
                    true
                }
            }
        };
        if advertise {
            let local_inc = *self.local_incarnation.lock().await;
            if let Some(me) = self.membership.get(self.membership.local_node_id()) {
                self.dissemination.enqueue(MemberUpdate {
                    node_id: me.node_id.clone(),
                    addr: me.addr.to_string(),
                    state: MemberState::Alive,
                    incarnation: local_inc,
                });
            }
        }

        let local_inc = *self.local_incarnation.lock().await;
        let fanout =
            DisseminationQueue::fanout_threshold(self.membership.len(), self.cfg.fanout_lambda);
        let mut piggyback = self
            .dissemination
            .take_for_message(self.cfg.max_piggyback, fanout);

        // Echo refutations back to the sender (deduped, bounded).
        for r in refutations {
            if !piggyback.iter().any(|p| p.node_id == r.node_id) {
                piggyback.push(r);
            }
        }
        piggyback.truncate(self.cfg.max_piggyback);

        let ack = SwimMessage::Ack(Ack {
            probe_id: ping.probe_id,
            from: self.membership.local_node_id().clone(),
            incarnation: local_inc,
            piggyback,
        });
        let _ = self.transport.send(from_addr, ack).await;
    }

    async fn handle_ping_req(
        &self,
        requester_addr: SocketAddr,
        req: PingReq,
        refutations: Vec<MemberUpdate>,
    ) {
        let Ok(target_sock) = req.target_addr.parse::<SocketAddr>() else {
            return;
        };

        // Register a nested probe id; when the forwarded ack arrives
        // we rewrap it with the original probe id and relay to the
        // requester. The relay runs on a dedicated task so the detector
        // run-loop stays responsive.
        let forward_id = self.next_probe_id();
        let Ok(forward_rx) = self.inflight.register(forward_id).await else {
            return;
        };

        let local_node = self.membership.local_node_id().clone();
        let local_inc = *self.local_incarnation.lock().await;
        let transport = Arc::clone(&self.transport);
        let inflight = Arc::clone(&self.inflight);
        let dissemination = Arc::clone(&self.dissemination);
        let timeout_dur = self.cfg.probe_timeout;
        let max_piggyback = self.cfg.max_piggyback;
        let fanout =
            DisseminationQueue::fanout_threshold(self.membership.len(), self.cfg.fanout_lambda);
        let original_probe_id = req.probe_id;

        tokio::spawn(async move {
            // Merge echoed refutations into the forwarded ping piggyback.
            let mut fwd_piggyback = dissemination.take_for_message(max_piggyback, fanout);
            for r in refutations {
                if !fwd_piggyback.iter().any(|p| p.node_id == r.node_id) {
                    fwd_piggyback.push(r);
                }
            }
            fwd_piggyback.truncate(max_piggyback);

            let send_res = transport
                .send(
                    target_sock,
                    SwimMessage::Ping(Ping {
                        probe_id: forward_id,
                        from: local_node.clone(),
                        incarnation: local_inc,
                        piggyback: fwd_piggyback,
                    }),
                )
                .await;
            if send_res.is_err() {
                inflight.forget(forward_id).await;
                return;
            }
            match tokio::time::timeout(timeout_dur, forward_rx).await {
                Ok(Ok(SwimMessage::Ack(ack))) => {
                    let relay = SwimMessage::Ack(Ack {
                        probe_id: original_probe_id,
                        from: ack.from,
                        incarnation: ack.incarnation,
                        piggyback: dissemination.take_for_message(max_piggyback, fanout),
                    });
                    let _ = transport.send(requester_addr, relay).await;
                }
                _ => {
                    inflight.forget(forward_id).await;
                }
            }
        });
    }

    /// Refute a self-suspect rumour by bumping local incarnation and
    /// rebroadcasting `Alive`. Exposed for tests that assert the
    /// refutation machinery directly; the piggyback ingestor calls
    /// the same underlying path automatically in production.
    #[cfg(test)]
    pub async fn bump_local_incarnation(&self, past: Incarnation) -> Incarnation {
        let mut guard = self.local_incarnation.lock().await;
        *guard = guard.refute(past);
        *guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::detector::transport::TransportFabric;
    use crate::swim::incarnation_store::{IncarnationStore, MemIncarnationStore};
    use crate::swim::member::MemberState;
    use crate::swim::wire::ProbeId;
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn cfg() -> SwimConfig {
        SwimConfig {
            probe_interval: Duration::from_millis(100),
            probe_timeout: Duration::from_millis(40),
            indirect_probes: 2,
            suspicion_mult: 4,
            min_suspicion: Duration::from_millis(500),
            initial_incarnation: Incarnation::ZERO,
            max_piggyback: 6,
            fanout_lambda: 3,
        }
    }

    async fn spawn_node(
        fab: &Arc<TransportFabric>,
        id: &str,
        port: u16,
        peers: &[(String, u16)],
    ) -> (
        Arc<FailureDetector>,
        watch::Sender<bool>,
        tokio::task::JoinHandle<()>,
    ) {
        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(port)).await);
        let list = Arc::new(MembershipList::new_local(
            NodeId::try_new(id).expect("test fixture"),
            addr(port),
            Incarnation::ZERO,
        ));
        for (peer_id, peer_port) in peers {
            list.apply(&MemberUpdate {
                node_id: NodeId::try_new(peer_id.as_str()).expect("test fixture"),
                addr: addr(*peer_port).to_string(),
                state: MemberState::Alive,
                incarnation: Incarnation::new(1),
            });
        }
        let detector = Arc::new(FailureDetector::new(
            cfg(),
            list,
            transport,
            ProbeScheduler::with_seed(port as u64),
        ));
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn({
            let det = Arc::clone(&detector);
            async move { det.run(rx).await }
        });
        (detector, tx, handle)
    }

    #[tokio::test(start_paused = true)]
    async fn three_node_mesh_converges_when_target_partitioned() {
        let fab = TransportFabric::new();
        let peers_of = |me: &str| {
            ["a", "b", "c"]
                .iter()
                .filter(|p| **p != me)
                .map(|p| {
                    let port = match *p {
                        "a" => 7010,
                        "b" => 7011,
                        "c" => 7012,
                        _ => unreachable!(),
                    };
                    (p.to_string(), port)
                })
                .collect::<Vec<_>>()
        };
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7010, &peers_of("a")).await;
        let (_det_b, sd_b, h_b) = spawn_node(&fab, "b", 7011, &peers_of("b")).await;
        let (_det_c, sd_c, h_c) = spawn_node(&fab, "c", 7012, &peers_of("c")).await;

        // Partition b from everything (both directions).
        fab.drop_edge(addr(7010), addr(7011)).await;
        fab.drop_edge(addr(7011), addr(7010)).await;
        fab.drop_edge(addr(7012), addr(7011)).await;
        fab.drop_edge(addr(7011), addr(7012)).await;

        // Give the detector a few probe intervals to converge. Use
        // advance() in a loop so timers, inflight probes, and suspicion
        // expiry all get a chance to fire.
        for _ in 0..30 {
            tokio::time::advance(cfg().probe_interval).await;
            tokio::task::yield_now().await;
        }

        // A's membership view must have marked b as Dead (Suspect →
        // Dead after suspicion timeout).
        let m = det_a
            .membership
            .get(&NodeId::try_new("b").expect("test fixture"))
            .expect("b in list");
        assert!(
            matches!(m.state, MemberState::Suspect | MemberState::Dead),
            "expected Suspect or Dead, got {:?}",
            m.state
        );

        // Shutdown.
        let _ = sd_a.send(true);
        let _ = sd_b.send(true);
        let _ = sd_c.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(200), h_a).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_b).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_c).await;
    }

    #[tokio::test(start_paused = true)]
    async fn ping_triggers_ack_reply() {
        let fab = TransportFabric::new();
        let (_det_a, sd_a, h_a) = spawn_node(&fab, "a", 7020, &[]).await;
        let probe_addr = addr(7021);
        let probe_transport = Arc::new(fab.bind(probe_addr).await);

        // Send a raw Ping from probe → a and wait for the Ack.
        probe_transport
            .send(
                addr(7020),
                SwimMessage::Ping(Ping {
                    probe_id: ProbeId::new(42),
                    from: NodeId::try_new("probe").expect("test fixture"),
                    incarnation: Incarnation::ZERO,
                    piggyback: vec![],
                }),
            )
            .await
            .unwrap();

        // Let the detector's recv arm fire.
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        let (from, msg) = tokio::time::timeout(Duration::from_millis(50), probe_transport.recv())
            .await
            .expect("recv did not time out")
            .expect("recv");
        assert_eq!(from, addr(7020));
        match msg {
            SwimMessage::Ack(ack) => assert_eq!(ack.probe_id, ProbeId::new(42)),
            other => panic!("expected Ack, got {other:?}"),
        }

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_terminates_loop_promptly() {
        let fab = TransportFabric::new();
        let (_det_a, sd_a, h_a) = spawn_node(&fab, "a", 7030, &[]).await;
        let _ = sd_a.send(true);
        let joined = tokio::time::timeout(Duration::from_millis(100), h_a).await;
        assert!(joined.is_ok(), "detector did not shut down in time");
    }

    #[tokio::test(start_paused = true)]
    async fn bump_local_incarnation_is_monotonic() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7040, &[]).await;
        let bumped = det_a.bump_local_incarnation(Incarnation::new(5)).await;
        assert!(bumped > Incarnation::new(5));
        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    /// Enqueue a synthetic rumour about a never-probed peer on node A's
    /// dissemination queue, then let the 3-node mesh run a few probe
    /// rounds. Nodes B and C must observe the delta via piggyback.
    #[tokio::test(start_paused = true)]
    async fn piggyback_propagates_delta_to_peers() {
        let fab = TransportFabric::new();
        let peers_of = |me: &str| {
            ["a", "b", "c"]
                .iter()
                .filter(|p| **p != me)
                .map(|p| {
                    let port = match *p {
                        "a" => 7050,
                        "b" => 7051,
                        "c" => 7052,
                        _ => unreachable!(),
                    };
                    (p.to_string(), port)
                })
                .collect::<Vec<_>>()
        };
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7050, &peers_of("a")).await;
        let (det_b, sd_b, h_b) = spawn_node(&fab, "b", 7051, &peers_of("b")).await;
        let (det_c, sd_c, h_c) = spawn_node(&fab, "c", 7052, &peers_of("c")).await;

        // Synthetic rumour: "ghost" is an Alive peer A learned about
        // out of band. It is NOT in B or C's membership initially.
        det_a.dissemination().enqueue(MemberUpdate {
            node_id: NodeId::try_new("ghost").expect("test fixture"),
            addr: "127.0.0.1:9999".to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(1),
        });
        // A's list has to know about ghost too, otherwise the outgoing
        // piggyback is still correct but there's nothing asserting the
        // local state. Apply it now.
        det_a.membership.apply(&MemberUpdate {
            node_id: NodeId::try_new("ghost").expect("test fixture"),
            addr: "127.0.0.1:9999".to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(1),
        });

        // Run enough probe rounds for gossip to reach B and C.
        for _ in 0..20 {
            tokio::time::advance(cfg().probe_interval).await;
            tokio::task::yield_now().await;
        }

        assert!(
            det_b
                .membership
                .get(&NodeId::try_new("ghost").expect("test fixture"))
                .is_some(),
            "B must learn about ghost via piggyback"
        );
        assert!(
            det_c
                .membership
                .get(&NodeId::try_new("ghost").expect("test fixture"))
                .is_some(),
            "C must learn about ghost via piggyback"
        );

        let _ = sd_a.send(true);
        let _ = sd_b.send(true);
        let _ = sd_c.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(200), h_a).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_b).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_c).await;
    }

    /// A receives a Ping whose piggyback claims A is Suspect. A must
    /// bump its own incarnation and enqueue an Alive refutation.
    #[tokio::test(start_paused = true)]
    async fn self_refute_bumps_incarnation_via_piggyback() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7060, &[]).await;
        let probe = Arc::new(fab.bind(addr(7061)).await);

        // Send a ping whose piggyback suspects "a" at inc 7.
        probe
            .send(
                addr(7060),
                SwimMessage::Ping(Ping {
                    probe_id: ProbeId::new(1),
                    from: NodeId::try_new("probe").expect("test fixture"),
                    incarnation: Incarnation::ZERO,
                    piggyback: vec![MemberUpdate {
                        node_id: NodeId::try_new("a").expect("test fixture"),
                        addr: addr(7060).to_string(),
                        state: MemberState::Suspect,
                        incarnation: Incarnation::new(7),
                    }],
                }),
            )
            .await
            .unwrap();

        // Drain the Ack so the detector actually processes recv.
        let (_from, _ack) = tokio::time::timeout(Duration::from_millis(50), probe.recv())
            .await
            .expect("did not time out")
            .expect("recv");

        // A's local incarnation must now be > 7.
        let bumped = *det_a.local_incarnation.lock().await;
        assert!(
            bumped > Incarnation::new(7),
            "local incarnation {bumped:?} did not refute rumoured Suspect(7)"
        );
        // A's membership view for itself is Alive at the bumped value.
        let me = det_a
            .membership
            .get(&NodeId::try_new("a").expect("test fixture"))
            .expect("self");
        assert_eq!(me.state, MemberState::Alive);
        assert!(me.incarnation > Incarnation::new(7));

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    /// F4 (restart side): a non-Alive rumour about the local node always
    /// triggers a self-refutation bump (merge rule), and a node restarted
    /// at `persisted + 1` therefore refutes a lingering `Dead(A, persisted)`
    /// to `persisted + 2` — strictly above the rumour, so its next Alive
    /// announcement dominates.
    #[test]
    fn persisted_incarnation_dominates_lingering_dead() {
        let list = Arc::new(MembershipList::new_local(
            NodeId::try_new("a").expect("test fixture"),
            addr(7100),
            // Simulated restart: persisted 5, resume at 6.
            Incarnation::new(5).bump(),
        ));
        let stale = MemberUpdate {
            node_id: NodeId::try_new("a").expect("test fixture"),
            addr: addr(7100).to_string(),
            state: MemberState::Dead,
            incarnation: Incarnation::new(5),
        };
        let outcome = list.apply(&stale);
        match outcome {
            MergeOutcome::SelfRefute { new_incarnation } => {
                assert!(
                    new_incarnation > Incarnation::new(5),
                    "restart at 6 must refute Dead(5) above it, got {new_incarnation:?}"
                );
            }
            other => panic!("expected SelfRefute, got {other:?}"),
        }
        let me = list
            .get(&NodeId::try_new("a").expect("test fixture"))
            .expect("self");
        assert!(
            me.incarnation > Incarnation::new(5),
            "restart at 6 must end up strictly above Dead(5), got {:?}",
            me.incarnation
        );
        assert_eq!(me.state, MemberState::Alive);
    }

    /// F1: a stale update about a peer yields the STORED view as an echo,
    /// so the sender learns the newer truth deterministically on the reply
    /// path instead of relying on probabilistic gossip.
    #[tokio::test]
    async fn stale_update_echoes_refutation() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7110, &[]).await;

        // A holds Alive(c, 9).
        det_a.apply_and_notify(&MemberUpdate {
            node_id: NodeId::try_new("c").expect("test fixture"),
            addr: addr(7112).to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(9),
        });

        // B gossips a stale Dead(c, 3) → ingest must echo Alive(c, 9).
        let refutations = det_a
            .ingest_piggyback(&[MemberUpdate {
                node_id: NodeId::try_new("c").expect("test fixture"),
                addr: addr(7112).to_string(),
                state: MemberState::Dead,
                incarnation: Incarnation::new(3),
            }])
            .await;

        assert_eq!(refutations.len(), 1);
        assert_eq!(refutations[0].node_id.as_str(), "c");
        assert_eq!(refutations[0].state, MemberState::Alive);
        assert_eq!(refutations[0].incarnation, Incarnation::new(9));

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    /// F2: a ping IS liveness evidence — the sender's Alive claim is
    /// applied, so an up-but-Dead node gets credit for probing.
    #[tokio::test(start_paused = true)]
    async fn ping_applies_sender_liveness() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7120, &[]).await;

        // A believes B is Dead(5).
        det_a.apply_and_notify(&MemberUpdate {
            node_id: NodeId::try_new("b").expect("test fixture"),
            addr: addr(7121).to_string(),
            state: MemberState::Dead,
            incarnation: Incarnation::new(5),
        });

        // B pings with incarnation 6 → A must apply Alive(B, 6).
        det_a
            .on_incoming(
                addr(7121),
                SwimMessage::Ping(Ping {
                    probe_id: ProbeId::new(9),
                    from: NodeId::try_new("b").expect("test fixture"),
                    incarnation: Incarnation::new(6),
                    piggyback: vec![],
                }),
            )
            .await;

        let b = det_a
            .membership
            .get(&NodeId::try_new("b").expect("test fixture"))
            .expect("b in list");
        assert_eq!(b.state, MemberState::Alive, "ping must count as liveness");
        assert_eq!(b.incarnation, Incarnation::new(6));

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    /// F3: applying a fresh Alive clears the pending suspicion timer, so
    /// a stale expiry cannot promote the node to Dead afterwards.
    #[tokio::test(start_paused = true)]
    async fn alive_apply_cancels_suspicion_timer() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7130, &[]).await;

        // Arm a suspicion timer for c.
        det_a.suspicion.lock().await.arm(
            NodeId::try_new("c").expect("test fixture"),
            Instant::now(),
            &cfg(),
            3,
        );
        assert_eq!(det_a.suspicion.lock().await.len(), 1);

        // Alive(c) arrives via piggyback → timer must be cancelled.
        det_a
            .ingest_piggyback(&[MemberUpdate {
                node_id: NodeId::try_new("c").expect("test fixture"),
                addr: addr(7132).to_string(),
                state: MemberState::Alive,
                incarnation: Incarnation::new(2),
            }])
            .await;

        assert_eq!(
            det_a.suspicion.lock().await.len(),
            0,
            "Alive apply must cancel the suspicion timer"
        );

        let _ = sd_a.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), h_a).await;
    }

    /// F4 (bump side): a self-refutation bump is persisted through the
    /// store so the next restart resumes above the rumoured incarnation.
    #[tokio::test]
    async fn self_refute_persists_incarnation() {
        let fab = TransportFabric::new();
        let (_det_a, sd_a, h_a) = spawn_node(&fab, "a", 7140, &[]).await;
        let store = Arc::new(MemIncarnationStore::new());
        // spawn_node builds the detector already; install the store via
        // the public setter equivalent — rebuild through the same path
        // the bootstrap uses.
        drop((sd_a, h_a));

        let transport: Arc<dyn Transport> = Arc::new(fab.bind(addr(7141)).await);
        let list = Arc::new(MembershipList::new_local(
            NodeId::try_new("a").expect("test fixture"),
            addr(7141),
            Incarnation::ZERO,
        ));
        let mut detector =
            FailureDetector::new(cfg(), list, transport, ProbeScheduler::with_seed(1));
        detector.set_incarnation_store(Arc::clone(&store) as Arc<dyn IncarnationStore>);
        let det = Arc::new(detector);
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn({
            let d = Arc::clone(&det);
            async move { d.run(rx).await }
        });

        // Rumour: Suspect(a, 7) → SelfRefute → bump to 8 + persist.
        det.ingest_piggyback(&[MemberUpdate {
            node_id: NodeId::try_new("a").expect("test fixture"),
            addr: addr(7141).to_string(),
            state: MemberState::Suspect,
            incarnation: Incarnation::new(7),
        }])
        .await;

        let bumped = *det.local_incarnation.lock().await;
        assert!(bumped > Incarnation::new(7));

        // The persist runs on a spawned task — give it a moment.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            store.load().unwrap(),
            Some(bumped.get()),
            "bump must be persisted through the store"
        );

        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(100), handle).await;
    }

    /// F1 + F4 combined: the full deterministic recovery loop, driven
    /// manually at the ingest layer (the transport/run-loop interleaving
    /// is exercised by the fabric tests; this pins the protocol logic).
    /// A holds Dead(B, 5); B restarts at incarnation 0. B's stale Alive(0)
    /// reaches A → A echoes its stored Dead(B, 5) (F1) → B self-refutes to
    /// 6 (F4 bump) → B's next Alive(6) dominates and A converges.
    #[tokio::test]
    async fn fast_restart_rejoins_via_refutation_echo() {
        let fab = TransportFabric::new();
        let (det_a, sd_a, h_a) = spawn_node(&fab, "a", 7150, &[]).await;
        let (det_b, sd_b, h_b) = spawn_node(&fab, "b", 7151, &[]).await;

        // A believes B is Dead at incarnation 5 (lingering rumour).
        det_a.apply_and_notify(&MemberUpdate {
            node_id: NodeId::try_new("b").expect("test fixture"),
            addr: addr(7151).to_string(),
            state: MemberState::Dead,
            incarnation: Incarnation::new(5),
        });

        // Step 1: B's restart announcement Alive(b, 0) reaches A via
        // piggyback → A must echo its stored Dead(b, 5) back (F1).
        let echoed = det_a
            .ingest_piggyback(&[MemberUpdate {
                node_id: NodeId::try_new("b").expect("test fixture"),
                addr: addr(7151).to_string(),
                state: MemberState::Alive,
                incarnation: Incarnation::ZERO,
            }])
            .await;
        assert_eq!(echoed.len(), 1, "A must echo exactly one refutation");
        assert_eq!(echoed[0].state, MemberState::Dead);
        assert_eq!(echoed[0].incarnation, Incarnation::new(5));

        // Step 2: the echo reaches B → SelfRefute bump past 5 (F4).
        det_b.ingest_piggyback(&echoed).await;
        let bumped = *det_b.local_incarnation.lock().await;
        assert!(
            bumped > Incarnation::new(5),
            "B must self-refute above Dead(5), got {bumped:?}"
        );

        // Step 3: B's next announcement Alive(b, bumped) dominates → A
        // converges to Alive.
        det_a
            .ingest_piggyback(&[MemberUpdate {
                node_id: NodeId::try_new("b").expect("test fixture"),
                addr: addr(7151).to_string(),
                state: MemberState::Alive,
                incarnation: bumped,
            }])
            .await;
        let b_view = det_a
            .membership
            .get(&NodeId::try_new("b").expect("test fixture"))
            .expect("b in list");
        assert_eq!(
            b_view.state,
            MemberState::Alive,
            "A must converge to Alive after the refutation echo"
        );
        assert!(b_view.incarnation > Incarnation::new(5));

        let _ = sd_a.send(true);
        let _ = sd_b.send(true);
        let _ = tokio::time::timeout(Duration::from_millis(200), h_a).await;
        let _ = tokio::time::timeout(Duration::from_millis(200), h_b).await;
    }
}
