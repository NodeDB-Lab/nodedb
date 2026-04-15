//! Single cluster node: SharedState + cluster + Data Plane + pgwire.
//!
//! A `TestClusterNode` owns one full NodeDB server instance configured
//! for cluster mode. Spawn via [`TestClusterNode::spawn`], passing the
//! node id and the seed list (empty for the bootstrap node; otherwise
//! a list of already-running peer addresses). On return the node has:
//!
//! - Pre-bound QUIC transport (so the listen address is known before
//!   peers need it).
//! - `SharedState` wired with credentials, metadata_cache, and the
//!   cluster handles (topology / routing / transport / applied-index
//!   watcher).
//! - Data Plane core running on a spawn_blocking task.
//! - Response poller running on a tokio task.
//! - Event Plane spawned.
//! - Raft loop + QUIC RPC server started via `start_raft` (installs
//!   the production `MetadataCommitApplier` with `Weak<SharedState>`
//!   so committed `CollectionDdl::Create` entries trigger Data Plane
//!   registers on every node).
//! - pgwire listener bound on an ephemeral port.
//! - `tokio_postgres::Client` connected to that listener.
//!
//! Shutdown flips every shutdown channel, aborts background tasks,
//! and drops the TempDir.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nodedb::bridge::dispatch::Dispatcher;
use nodedb::config::auth::AuthMode;
use nodedb::config::server::ClusterSettings;
use nodedb::control::server::pgwire::listener::PgListener;
use nodedb::control::state::SharedState;
use nodedb::data::executor::core_loop::CoreLoop;
use nodedb::event::{EventPlane, create_event_bus};
use nodedb::wal::WalManager;
use nodedb_types::config::tuning::ClusterTransportTuning;

/// Running cluster node.
pub struct TestClusterNode {
    pub node_id: u64,
    pub listen_addr: SocketAddr,
    pub pg_addr: SocketAddr,
    pub client: tokio_postgres::Client,
    pub shared: Arc<SharedState>,
    _data_dir: tempfile::TempDir,
    _conn_handle: tokio::task::JoinHandle<()>,
    pg_shutdown_bus: nodedb::control::shutdown::ShutdownBus,
    poller_shutdown_tx: tokio::sync::watch::Sender<bool>,
    cluster_shutdown_tx: tokio::sync::watch::Sender<bool>,
    core_stop_tx: std::sync::mpsc::Sender<()>,
    _pg_handle: tokio::task::JoinHandle<()>,
    _poller_handle: tokio::task::JoinHandle<()>,
    _core_handle: tokio::task::JoinHandle<()>,
    _event_plane: EventPlane,
}

impl TestClusterNode {
    /// Spawn a cluster node.
    ///
    /// - `node_id` — non-zero unique id within the cluster.
    /// - `seed_nodes` — empty for the bootstrap node; otherwise the
    ///   pre-bound listen address of at least one already-running
    ///   peer (typically node 1).
    pub async fn spawn(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::spawn_with_tuning(node_id, seed_nodes, ClusterTransportTuning::default()).await
    }

    /// Spawn a cluster node with a custom `ClusterTransportTuning`.
    /// Used by tests that need to override the descriptor lease
    /// duration or renewal cadence to drive renewal within a
    /// short test budget.
    pub async fn spawn_with_tuning(
        node_id: u64,
        seed_nodes: Vec<SocketAddr>,
        tuning: ClusterTransportTuning,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_dir = tempfile::tempdir()?;
        let data_dir_path: PathBuf = data_dir.path().to_path_buf();

        // Pre-bind the QUIC transport on a random port so we know the
        // listen address before wiring seeds / cluster settings.
        let transport = Arc::new(nodedb_cluster::NexarTransport::new(
            node_id,
            "127.0.0.1:0".parse()?,
        )?);
        let listen_addr = transport.local_addr();

        // Build cluster settings. Empty `seed_nodes` → single-node
        // bootstrap by listing only our own address.
        let seeds = if seed_nodes.is_empty() {
            vec![listen_addr]
        } else {
            seed_nodes
        };
        let cluster_settings = ClusterSettings {
            node_id,
            listen: listen_addr,
            seed_nodes: seeds,
            num_groups: 2,
            replication_factor: 3,
            force_bootstrap: false,
        };

        // Open WAL + dispatcher + event bus.
        let wal = Arc::new(WalManager::open_for_testing(
            &data_dir_path.join("test.wal"),
        )?);
        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let (event_producers, event_consumers) = create_event_bus(1);

        // Credential store backed by the system catalog — required for
        // CREATE COLLECTION to exercise the full persistence path.
        let credentials = Arc::new(
            nodedb::control::security::credential::store::CredentialStore::open(
                &data_dir_path.join("system.redb"),
            )?,
        );
        let mut shared =
            SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials);

        // Initialise the cluster using the pre-bound transport.
        let handle = nodedb::control::cluster::init_cluster_with_transport(
            &cluster_settings,
            transport.clone(),
            &data_dir_path,
        )
        .await?;

        // Wire cluster handles into SharedState (mirrors main.rs).
        // `Arc::get_mut` is valid here: `shared` has not been cloned.
        if let Some(state) = Arc::get_mut(&mut shared) {
            state.node_id = handle.node_id;
            state.cluster_topology = Some(Arc::clone(&handle.topology));
            state.cluster_routing = Some(Arc::clone(&handle.routing));
            state.cluster_transport = Some(Arc::clone(&handle.transport));
            state.metadata_cache = Arc::clone(&handle.metadata_cache);
            state.metadata_applied_index_watcher = Arc::clone(&handle.applied_index_watcher);
        } else {
            return Err("SharedState already cloned before cluster wire-up".into());
        }

        // Start Data Plane core.
        let data_side = data_sides.into_iter().next().expect("data side");
        let event_producer = event_producers.into_iter().next().expect("event producer");
        let core_dir = data_dir_path.clone();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let core_handle = tokio::task::spawn_blocking(move || {
            let mut core =
                CoreLoop::open(0, data_side.request_rx, data_side.response_tx, &core_dir)
                    .expect("core open");
            core.set_event_producer(event_producer);
            // Continue ticking only while the channel is Empty.
            // `Ok(())` means we got an explicit stop signal;
            // `Disconnected` means the sender was dropped (e.g. the
            // owning `TestClusterNode` was dropped mid-panic). In
            // both cases we must exit — `spawn_blocking` threads
            // cannot be aborted, so a loop that continued on
            // `Disconnected` would block tokio runtime shutdown
            // indefinitely and force nextest to kill the test
            // process at `slow-timeout` (~2 minutes of wasted CI
            // time per flaky cluster test).
            while matches!(
                core_stop_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ) {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        // Response poller (Data Plane → control plane routing).
        let shared_poller = Arc::clone(&shared);
        let (poller_shutdown_tx, mut poller_shutdown_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_shutdown_rx.changed() => break,
                }
            }
        });

        // Event Plane (triggers, CDC, scheduler).
        let watermark_store = Arc::new(nodedb::event::watermark::WatermarkStore::open(
            &data_dir_path,
        )?);
        let trigger_dlq = Arc::new(std::sync::Mutex::new(
            nodedb::event::trigger::TriggerDlq::open(&data_dir_path)?,
        ));
        let event_plane = EventPlane::spawn(
            event_consumers,
            Arc::clone(&wal),
            watermark_store,
            Arc::clone(&shared),
            trigger_dlq,
            Arc::clone(&shared.cdc_router),
            Arc::clone(&shared.shutdown),
        );

        // Start Raft + install MetadataCommitApplier.
        let (cluster_shutdown_tx, cluster_shutdown_rx) = tokio::sync::watch::channel(false);
        nodedb::control::cluster::start_raft(
            &handle,
            Arc::clone(&shared),
            &data_dir_path,
            cluster_shutdown_rx.clone(),
            &tuning,
        )?;

        // Spawn the descriptor lease renewal loop on the same
        // shutdown channel as raft so cluster shutdown stops it
        // cleanly. Returns None on single-node clusters that
        // never wired metadata_raft (the harness always wires it,
        // so this returns Some in practice for cluster tests).
        let _lease_renewal = nodedb::control::lease::LeaseRenewalLoop::spawn(
            Arc::clone(&shared),
            &tuning,
            cluster_shutdown_rx,
        );

        // Construct the gateway and install it (plus its DDL invalidator) on
        // SharedState, mirroring what main.rs does before listeners bind.
        //
        // We use a raw-pointer write because `shared` has already been cloned
        // by the response poller task, making `Arc::get_mut` return None.
        // This is sound at this point in setup because:
        //   1. The response poller only calls `poll_and_route_responses()`,
        //      which never touches the `gateway` or `gateway_invalidator` fields.
        //   2. No other concurrent task reads those fields before the pgwire
        //      listener binds (a few lines below).
        //   3. The write completes before the pgwire listener spawns, so the
        //      happens-before relationship is guaranteed.
        {
            let shared_for_gw = Arc::clone(&shared);
            let gateway = Arc::new(nodedb::control::gateway::Gateway::new(shared_for_gw));
            let invalidator = Arc::new(nodedb::control::gateway::PlanCacheInvalidator::new(
                &gateway.plan_cache,
            ));
            // SAFETY: no concurrent reads of `gateway` / `gateway_invalidator`
            // at this point (see comment above). Fields start as `None` and
            // are written once here before any listener starts.
            unsafe {
                let state = Arc::as_ptr(&shared) as *mut nodedb::control::state::SharedState;
                (*state).gateway = Some(Arc::clone(&gateway));
                (*state).gateway_invalidator = Some(invalidator);
            }
        }

        // pgwire listener.
        // In the test harness, use the startup gate already on SharedState
        // (a pre-fired placeholder from `new_inner`). This means the listener
        // accepts immediately without a startup-phase delay.
        let pg_listener = PgListener::bind("127.0.0.1:0".parse()?).await?;
        let pg_addr = pg_listener.local_addr();
        let (pg_shutdown_bus, _) =
            nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let shared_pg = Arc::clone(&shared);
        let test_startup_gate = Arc::clone(&shared.startup);
        let bus_pg = pg_shutdown_bus.clone();
        let pg_handle = tokio::spawn(async move {
            let _ = pg_listener
                .run(
                    shared_pg,
                    AuthMode::Trust,
                    None,
                    Arc::new(tokio::sync::Semaphore::new(128)),
                    test_startup_gate,
                    bus_pg,
                )
                .await;
        });

        // Give the listener a moment to start accepting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect tokio_postgres client.
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
            pg_addr.port()
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("pgwire connect failed: {e}"))?;
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(Self {
            node_id,
            listen_addr,
            pg_addr,
            client,
            shared,
            _data_dir: data_dir,
            _conn_handle: conn_handle,
            pg_shutdown_bus,
            poller_shutdown_tx,
            cluster_shutdown_tx,
            core_stop_tx,
            _pg_handle: pg_handle,
            _poller_handle: poller_handle,
            _core_handle: core_handle,
            _event_plane: event_plane,
        })
    }

    /// Number of nodes currently visible in this node's topology view.
    pub fn topology_size(&self) -> usize {
        self.shared
            .cluster_topology
            .as_ref()
            .map(|t| t.read().unwrap_or_else(|p| p.into_inner()).node_count())
            .unwrap_or(0)
    }

    /// Observed metadata-group leader id from this node's local Raft
    /// state, or `0` if no leader is known yet (election in progress).
    /// Polled by the cluster harness `spawn_three()` to gate test
    /// execution on a stable leader — otherwise tests racing the first
    /// election see `not leader (leader hint: None)` errors when CPU
    /// pressure delays the initial heartbeats past topology convergence.
    pub fn metadata_group_leader(&self) -> u64 {
        let Some(observer) = self.shared.cluster_observer.get() else {
            return 0;
        };
        observer
            .group_status
            .group_statuses()
            .into_iter()
            .find(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID)
            .map(|g| g.leader_id)
            .unwrap_or(0)
    }

    /// Number of active collections visible on this node (read through
    /// the local `SystemCatalog` redb — populated by the
    /// `MetadataCommitApplier` on every node via
    /// `CatalogEntry::apply_to`).
    pub fn cached_collection_count(&self) -> usize {
        let Some(catalog) = self.shared.credentials.catalog() else {
            return 0;
        };
        // `load_collections_for_tenant` filters out `is_active = false`
        // records, so a deactivated collection drops out of the count.
        catalog
            .load_collections_for_tenant(1)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Number of sequences visible in this node's in-memory
    /// `sequence_registry`. After the applier spawns its
    /// post-apply side effect for a `PutSequence`, the registry
    /// should see the new record on every node.
    pub fn sequence_count(&self, tenant_id: u32) -> usize {
        self.shared.sequence_registry.list(tenant_id).len()
    }

    /// Check whether a sequence with the given name exists in this
    /// node's in-memory registry.
    pub fn has_sequence(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.sequence_registry.exists(tenant_id, name)
    }

    /// Read the current counter of a sequence from this node's
    /// in-memory registry, if present.
    pub fn sequence_current_value(&self, tenant_id: u32, name: &str) -> Option<i64> {
        self.shared
            .sequence_registry
            .list(tenant_id)
            .into_iter()
            .find(|(n, _, _)| n == name)
            .map(|(_, current, _)| current)
    }

    /// Check whether a trigger with the given name exists in this
    /// node's in-memory trigger registry.
    pub fn has_trigger(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .trigger_registry
            .list_for_tenant(tenant_id)
            .iter()
            .any(|t| t.name == name)
    }

    /// Read a function record from this node's local `SystemCatalog`
    /// redb (which the applier writes through on every node).
    pub fn has_function(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_function(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Read a procedure record from this node's local `SystemCatalog`.
    pub fn has_procedure(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_procedure(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Check whether a scheduled job with the given name exists in
    /// this node's in-memory `schedule_registry`.
    pub fn has_schedule(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.schedule_registry.get(tenant_id, name).is_some()
    }

    /// Check whether a change stream with the given name exists in
    /// this node's in-memory `stream_registry`.
    pub fn has_change_stream(&self, tenant_id: u32, name: &str) -> bool {
        self.shared.stream_registry.get(tenant_id, name).is_some()
    }

    /// Check whether a user exists and is active in this node's
    /// in-memory `credentials` cache (which the applier writes via
    /// `install_replicated_user`).
    pub fn has_active_user(&self, username: &str) -> bool {
        self.shared.credentials.get_user(username).is_some()
    }

    /// Check whether a permission grant exists in this node's
    /// in-memory `PermissionStore`. `permission` is the lowercase
    /// canonical name (`read|write|create|drop|alter|admin|monitor|execute`).
    pub fn has_grant(&self, target: &str, grantee: &str, permission: &str) -> bool {
        let Some(perm) = nodedb::control::security::permission::parse_permission(permission) else {
            return false;
        };
        self.shared
            .permissions
            .grants_on(target)
            .iter()
            .any(|g| g.grantee == grantee && g.permission == perm)
    }

    /// Read the recorded owner of an object on this node.
    pub fn owner_of(&self, object_type: &str, tenant_id: u32, object_name: &str) -> Option<String> {
        self.shared.permissions.get_owner(
            object_type,
            nodedb_types::TenantId::new(tenant_id),
            object_name,
        )
    }

    /// Check whether a tenant identity exists in this node's local
    /// `SystemCatalog` redb (written by the `PutTenant` applier).
    pub fn has_tenant(&self, tenant_id: u32) -> bool {
        let Some(catalog) = self.shared.credentials.catalog() else {
            return false;
        };
        match catalog.load_all_tenants() {
            Ok(list) => list.iter().any(|t| t.tenant_id == tenant_id),
            Err(_) => false,
        }
    }

    /// Check whether an RLS policy exists in this node's local
    /// `SystemCatalog` redb (written by the `PutRlsPolicy` applier).
    pub fn has_rls_policy(&self, tenant_id: u32, collection: &str, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_rls_policy(tenant_id, collection, name).ok().flatten())
            .is_some()
    }

    /// Check whether a materialized view exists in this node's local
    /// `SystemCatalog` redb (written by the applier on every node).
    pub fn has_materialized_view(&self, tenant_id: u32, name: &str) -> bool {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_materialized_view(tenant_id, name).ok().flatten())
            .is_some()
    }

    /// Whether this node's `lease_drain` tracker currently holds
    /// an ACTIVE drain entry (not expired) for the given
    /// `(descriptor_id, min_version)`. Used by the drain tests
    /// to assert replicated drain state.
    pub fn has_drain_for(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
        min_version: u64,
    ) -> bool {
        let now_wall_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        self.shared
            .lease_drain
            .is_draining(descriptor_id, min_version, now_wall_ns)
    }

    /// Total number of leases (across all descriptors and node_ids)
    /// in this node's `MetadataCache.leases` map. Includes expired
    /// records — for filtered counts use [`active_lease_count`].
    pub fn lease_count(&self) -> usize {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.len()
    }

    /// Number of leases whose `expires_at` is strictly greater
    /// than this node's current HLC peek.
    pub fn active_lease_count(&self) -> usize {
        let now = self.shared.hlc_clock.peek();
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.leases.values().filter(|l| l.expires_at > now).count()
    }

    /// Whether this node's `MetadataCache` holds a non-expired
    /// lease at the given version (or higher) on
    /// `(kind, tenant_id, name)` granted to `holder_node_id`.
    pub fn has_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
        holder_node_id: u64,
        min_version: u64,
    ) -> bool {
        let now = self.shared.hlc_clock.peek();
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name);
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(id, holder_node_id))
            .map(|l| l.expires_at > now && l.version >= min_version)
            .unwrap_or(false)
    }

    /// Snapshot of every lease on this node for the given descriptor.
    pub fn leases_for_descriptor(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
    ) -> Vec<nodedb_cluster::DescriptorLease> {
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name);
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .iter()
            .filter(|((did, _), _)| did == &id)
            .map(|(_, l)| l.clone())
            .collect()
    }

    /// Direct accessor for the `applied_index` watermark — used by
    /// the lease tests to assert that the fast-path acquire did NOT
    /// advance raft.
    pub fn metadata_applied_index(&self) -> u64 {
        let cache = self
            .shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache.applied_index
    }

    /// Acquire a lease on this node via the SharedState facade.
    /// Called directly from the test's tokio runtime worker so the
    /// `block_in_place` inside `acquire_descriptor_lease` lands on
    /// a real runtime thread (which is what `block_in_place`
    /// requires — it cannot be called from a `spawn_blocking`
    /// worker).
    pub async fn acquire_lease(
        &self,
        kind: nodedb_cluster::DescriptorKind,
        tenant_id: u32,
        name: &str,
        version: u64,
        duration: std::time::Duration,
    ) -> Result<nodedb_cluster::DescriptorLease, String> {
        let id = nodedb_cluster::DescriptorId::new(tenant_id, kind, name.to_string());
        self.shared
            .acquire_descriptor_lease(id, version, duration)
            .map_err(|e| format!("acquire failed: {e}"))
    }

    /// Release a batch of leases on this node via the SharedState facade.
    pub async fn release_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> Result<(), String> {
        self.shared
            .release_descriptor_leases(descriptor_ids)
            .map_err(|e| format!("release failed: {e}"))
    }

    /// Read the `(descriptor_version, modification_hlc)` stamp of a
    /// collection on this node's local `SystemCatalog`. The applier
    /// is the only writer, so this is what every other node should
    /// agree on after the apply has propagated.
    pub fn collection_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_collection(tenant_id, name).ok().flatten())
            .map(|coll| (coll.descriptor_version, coll.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored functions.
    pub fn function_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_function(tenant_id, name).ok().flatten())
            .map(|f| (f.descriptor_version, f.modification_hlc))
    }

    /// Same as [`collection_descriptor`] for stored procedures.
    pub fn procedure_descriptor(
        &self,
        tenant_id: u32,
        name: &str,
    ) -> Option<(u64, nodedb_types::Hlc)> {
        self.shared
            .credentials
            .catalog()
            .as_ref()
            .and_then(|c| c.get_procedure(tenant_id, name).ok().flatten())
            .map(|p| (p.descriptor_version, p.modification_hlc))
    }

    /// Check whether a custom role exists in this node's in-memory
    /// `roles` cache.
    pub fn has_role(&self, name: &str) -> bool {
        self.shared.roles.get_role(name).is_some()
    }

    /// Check whether an API key exists and is active in this node's
    /// in-memory `api_keys` cache.
    pub fn has_active_api_key(&self, key_id: &str) -> bool {
        self.shared
            .api_keys
            .get_key(key_id)
            .map(|k| !k.is_revoked)
            .unwrap_or(false)
    }

    /// Check whether a given user's role set contains a specific
    /// role. Used to assert `ALTER USER ... SET ROLE` replication.
    pub fn user_has_role(&self, username: &str, role: &str) -> bool {
        self.shared
            .credentials
            .get_user(username)
            .map(|u| u.roles.iter().any(|r| r.to_string() == role))
            .unwrap_or(false)
    }

    /// Force the routing table on this node to point `group_id` at `fake_leader`,
    /// creating a stale route.
    ///
    /// When the gateway on this node next dispatches to `group_id`, it will send
    /// the request to `fake_leader` instead of the real leader. The remote node
    /// (which is NOT the leader for that group) will return `TypedClusterError::NotLeader`,
    /// causing `retry_not_leader` to update the routing table and retry against
    /// the real leader. This is the canonical way to exercise the NotLeader retry
    /// path in tests without needing a real leadership change (which is slow and
    /// flaky).
    pub fn force_stale_route_for_test(&self, group_id: u64, fake_leader: u64) {
        if let Some(ref routing) = self.shared.cluster_routing {
            let mut table = routing.write().unwrap_or_else(|p| p.into_inner());
            table.set_leader(group_id, fake_leader);
        }
    }

    /// Read the current `not_leader_retry_count` from this node's shared gateway.
    ///
    /// Returns 0 if the gateway has not been constructed yet (shouldn't happen
    /// in tests since the harness wires the gateway during spawn).
    pub fn not_leader_retry_count(&self) -> u64 {
        self.shared
            .gateway
            .as_ref()
            .map(|gw| gw.not_leader_retry_count())
            .unwrap_or(0)
    }

    /// Execute a simple query; returns an error message on SQL error.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self.client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Cooperatively shut down every background task this node owns.
    pub async fn shutdown(self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        let _ = self.core_stop_tx.send(());
        // Give tokio a chance to drop the task futures before TempDir
        // is dropped — otherwise redb file locks can linger.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
}

/// Panic-safe teardown. Without this, a test that panics (e.g. a
/// `wait_for` tripping its budget) would drop `TestClusterNode`
/// without ever calling the async `shutdown()`, leaving every
/// background task still running:
///
/// - `watch::Sender`s close on drop but DO NOT transmit their last
///   value, so the raft / pgwire / poller loops block on
///   `select { shutdown.changed() }` forever.
/// - `JoinHandle`s on drop DETACH the task instead of cancelling it.
/// - Those detached tasks keep the tempdir's redb files open, so
///   `TempDir::drop` either hangs or the whole test process sticks
///   around until nextest kills it at `slow-timeout` (previously
///   ~2 minutes of wasted CI time per flaky cluster test).
///
/// The Drop here fires the watch senders synchronously and aborts
/// every JoinHandle we own. `abort()` is non-blocking: the next time
/// the task hits an `.await` it gets cancelled and releases its
/// resources, including the redb handles. Combined with the
/// already-present `core_stop_tx` drop (which disconnects the
/// blocking Data Plane loop), this guarantees the node tears down
/// in milliseconds instead of minutes.
impl Drop for TestClusterNode {
    fn drop(&mut self) {
        self.pg_shutdown_bus.initiate();
        let _ = self.cluster_shutdown_tx.send(true);
        let _ = self.poller_shutdown_tx.send(true);
        // `core_stop_tx` is a std mpsc Sender; dropping it disconnects
        // the receiver the spawn_blocking data-plane loop polls, so
        // no explicit signal needed here.
        self._conn_handle.abort();
        self._pg_handle.abort();
        self._poller_handle.abort();
        self._core_handle.abort();
    }
}

fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}
