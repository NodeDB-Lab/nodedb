// SPDX-License-Identifier: BUSL-1.1

//! `SharedState::open` — production constructor loading from disk.

use std::sync::Arc;

use nodedb_types::config::TuningConfig;

use crate::wal::WalManager;

use super::handles::DataPlaneHandles;
use crate::control::state::SharedState;
use crate::control::state::init_assemble_parts::{
    AssembleParts, AuthInputs, BusParts, CoreParts, RegistryParts, StoreParts,
};

impl SharedState {
    /// Create shared state with persistent credential store (for production).
    ///
    /// `is_cluster` is the static, deployment-time choice (from
    /// `config.cluster.is_some()` — presence of a `[cluster]` section,
    /// not seed-list length) between `Local` and `Cluster`
    /// surrogate-registry mode — see `SurrogateRegistryMode`.
    pub fn open(
        handles: DataPlaneHandles,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
        config: super::open_config::OpenConfig<'_>,
        tuning: TuningConfig,
        is_cluster: bool,
        governor: Arc<nodedb_mem::MemoryGovernor>,
    ) -> crate::Result<Arc<Self>> {
        let DataPlaneHandles {
            dispatcher,
            quiesce,
            array_catalog,
            system_metrics,
        } = handles;
        let super::open_config::OpenConfig {
            auth: auth_config,
            egress,
        } = config;
        let remote_sources =
            crate::control::security::egress::RemoteSources::new(egress).map_err(|e| {
                crate::Error::Config {
                    detail: format!("outbound HTTP client: {e}"),
                }
            })?;
        let super::bootstrap::ProdBootstrap {
            credentials,
            producer_registry,
            api_keys,
            roles,
            permissions,
            blacklist,
            trigger_registry,
            stream_registry,
            group_registry,
            schedule_registry,
            synonym_registry,
            custom_type_registry,
            retention_policy_registry,
            alert_registry,
            alert_hysteresis,
            ep_topic_registry,
            mv_registry,
            sequence_registry,
            rls_store,
            redaction_store,
            shared_audit,
            database_registry,
            surrogate_registry_handle,
            surrogate_assigner,
            permission_cache,
            shutdown,
            loop_registry,
            startup_gate,
            prod_session_registry,
            si_bus,
            uc_bus,
            bus_consumer_handle,
        } = super::bootstrap::run(&wal, catalog_path, auth_config, is_cluster)?;

        let super::auth_parts::AuthParts {
            metering_config,
            rate_limit_config,
            siem,
            risk_scorer,
            escalation,
            tls_policy,
            auth_users,
            scope_grants,
            quota_manager,
        } = super::auth_parts::build(auth_config, &remote_sources, &credentials, &shared_audit)?;

        let super::stores::DiskStores {
            data_dir,
            array_sync_op_log,
            array_ack_registry,
            array_snapshot_store,
            array_sync_schemas,
            array_subscriber_cursors,
            offset_store,
            job_history,
            mv_persistence,
        } = super::stores::open_disk_stores(catalog_path)?;
        let super::stores::SecurityStores {
            orgs,
            scope_defs,
            usage_counter,
            usage_store,
            auth_api_keys,
            impersonation,
            emergency,
            auth_metrics,
            ceilings,
        } = super::stores::security_stores(&metering_config);
        let super::stores::SessionControls {
            rate_limiter,
            session_handles,
        } = super::stores::session_controls(auth_config, &rate_limit_config);

        let state = Self::assemble(AssembleParts {
            core: CoreParts {
                dispatcher,
                wal,
                quiesce,
                array_catalog,
                system_metrics,
                governor,
                tuning,
                credentials,
                shutdown,
                loop_registry,
                startup_gate,
                shuffle_dir: data_dir.clone(),
                data_dir,
                test_state_dir: None,
            },
            registries: RegistryParts {
                api_keys,
                roles,
                permissions,
                blacklist,
                rls: rls_store,
                redaction: redaction_store,
                trigger_registry,
                stream_registry,
                group_registry,
                schedule_registry,
                synonym_registry,
                custom_type_registry,
                retention_policy_registry,
                alert_registry,
                alert_hysteresis,
                ep_topic_registry,
                mv_registry,
                sequence_registry,
                database_registry,
                surrogate_registry: surrogate_registry_handle,
                surrogate_assigner,
                producer_registry,
            },
            buses: BusParts {
                audit: shared_audit,
                session_registry: prod_session_registry,
                si_bus,
                uc_bus,
                bus_consumer_handle,
                permission_cache,
            },
            auth: AuthInputs {
                metering_config,
                siem,
                risk_scorer,
                escalation,
                tls_policy,
                auth_users,
                scope_grants,
                quota_manager,
                rate_limiter,
                session_handles,
                remote_sources,
                audit_retention_days: auth_config.audit_retention_days,
                audit_max_entries: auth_config.audit_max_entries,
                idle_timeout_secs: auth_config.idle_timeout_secs,
                session_absolute_timeout_secs: auth_config.session_absolute_timeout_secs,
            },
            stores: StoreParts {
                orgs,
                scope_defs,
                usage_counter,
                usage_store,
                auth_api_keys,
                impersonation,
                emergency,
                auth_metrics,
                ceilings,
                array_sync_op_log,
                array_ack_registry,
                array_snapshot_store,
                array_sync_schemas,
                array_subscriber_cursors,
                offset_store,
                job_history,
                mv_persistence,
            },
        })?;

        crate::event::topic::hydrate_topic_buffers(&state)?;
        super::post_init::hydrate_caches(&state)?;
        crate::control::cluster::metadata_applier::seed_host_tables(&state)?;
        super::post_init::spawn_array_gc(&state);

        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::ratelimit::config::RateLimitConfig;
    use crate::control::security::ratelimit::limiter::LoginRateLimitOutcome;

    /// Build a `SharedState` via the production `open()` path with a
    /// caller-supplied `AuthConfig`, so tests can observe how operator
    /// config actually threads through construction.
    fn open_with_auth_config(
        dir: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
    ) -> Arc<SharedState> {
        let wal_dir = dir.join("wal");
        std::fs::create_dir_all(&wal_dir).expect("create wal dir");
        let wal = Arc::new(WalManager::open_for_testing(&wal_dir).expect("open wal"));
        let (dispatcher, _) = crate::bridge::dispatch::Dispatcher::new(1, 16);
        let catalog_path = dir.join("catalog.redb");
        SharedState::open(
            DataPlaneHandles {
                dispatcher,
                quiesce: crate::bridge::quiesce::CollectionQuiesce::new(),
                array_catalog: crate::control::array_catalog::ArrayCatalog::handle(),
                system_metrics: Arc::new(crate::control::metrics::SystemMetrics::new()),
            },
            wal,
            &catalog_path,
            super::super::open_config::OpenConfig::strict(auth_config),
            TuningConfig::default(),
            false,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open shared state")
    }

    #[test]
    fn configured_rate_limit_is_applied_not_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        // `RateLimitConfig::default()` is `enabled: false` with `default_burst:
        // 200` — a distinctive small, enabled burst here can only show up if
        // this exact config was threaded into the constructed `RateLimiter`.
        let auth_config = crate::config::auth::AuthConfig {
            rate_limit: Some(RateLimitConfig {
                enabled: true,
                default_qps: 3,
                default_burst: 3,
                ..Default::default()
            }),
            ..Default::default()
        };

        let state = open_with_auth_config(dir.path(), &auth_config);

        for i in 0..3 {
            let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
            assert!(
                r.allowed,
                "request {i} should be allowed under configured burst=3"
            );
        }
        let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
        assert!(
            !r.allowed,
            "4th request must be denied by the operator-configured burst=3, \
             not the hardcoded RateLimitConfig::default() burst=200"
        );
    }

    #[test]
    fn unconfigured_rate_limit_falls_back_to_disabled_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let auth_config = crate::config::auth::AuthConfig::default();
        assert!(auth_config.rate_limit.is_none());

        let state = open_with_auth_config(dir.path(), &auth_config);

        // `RateLimitConfig::default()` has `enabled: false`, so every
        // request is admitted regardless of volume.
        for _ in 0..500 {
            let r = state.rate_limiter.check("u1", &[], None, "point_get", None);
            assert!(r.allowed);
        }
    }

    #[test]
    fn login_capacities_still_apply_after_configured_rate_limit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let auth_config = crate::config::auth::AuthConfig {
            rate_limit: Some(RateLimitConfig {
                enabled: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let state = open_with_auth_config(dir.path(), &auth_config);

        // Mirrors `main_boot::shared_state`'s post-construction call.
        state.rate_limiter.set_login_capacities(5, 100);

        for _ in 0..5 {
            assert!(matches!(
                state.rate_limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::Allowed
            ));
            state
                .rate_limiter
                .record_login_failure("10.0.0.9", "victim");
        }
        assert!(
            matches!(
                state.rate_limiter.check_login("10.0.0.9", "victim"),
                LoginRateLimitOutcome::IpExceeded { .. }
            ),
            "login brute-force capacities set via set_login_capacities must \
             still apply after threading the configured RateLimitConfig"
        );
    }
}
