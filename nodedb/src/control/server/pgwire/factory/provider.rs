// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::auth::DefaultServerParameterProvider;

/// Server parameter provider used by BOTH the trust and SCRAM startup paths.
///
/// Wraps pgwire's `DefaultServerParameterProvider` (which carries a fixed set
/// of parameters and has no `server_version_num`) and augments it with
/// `server_version_num` so PostgreSQL clients that inspect the numeric server
/// version at connect time (e.g. drivers gating feature use on it) receive it
/// in the startup `ParameterStatus` burst. `server_version` is overridden to
/// NodeDB's own version string.
#[derive(Debug)]
pub(super) struct NodeDbParameterProvider {
    inner: DefaultServerParameterProvider,
}

impl NodeDbParameterProvider {
    fn new() -> Self {
        let mut inner = DefaultServerParameterProvider::default();
        inner.server_version = format!("NodeDB {}", crate::version::VERSION);
        Self { inner }
    }
}

impl pgwire::api::auth::ServerParameterProvider for NodeDbParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<std::collections::HashMap<String, String>>
    where
        C: pgwire::api::ClientInfo,
    {
        let mut params = self.inner.server_parameters(client)?;
        params.insert(
            "server_version_num".to_owned(),
            nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM.to_owned(),
        );
        Some(params)
    }
}

pub(super) fn nodedb_parameter_provider() -> NodeDbParameterProvider {
    NodeDbParameterProvider::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::api::DefaultClient;
    use pgwire::api::auth::ServerParameterProvider;

    /// The custom provider used by BOTH startup paths must emit NodeDB's own
    /// `server_version` and the PG-compat `server_version_num` in the startup
    /// parameter set, on top of pgwire's default fixed parameters.
    #[test]
    fn parameter_provider_advertises_server_version_num_and_nodedb_version() {
        let addr = "127.0.0.1:5432"
            .parse::<std::net::SocketAddr>()
            .expect("valid socket addr");
        let client: DefaultClient<()> = DefaultClient::new(addr, false);
        let provider = NodeDbParameterProvider::new();

        let params = provider
            .server_parameters(&client)
            .expect("provider must yield parameters");

        assert_eq!(
            params.get("server_version_num").map(String::as_str),
            Some(nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM),
            "startup params must advertise server_version_num, got {params:?}"
        );
        assert_eq!(
            params.get("server_version").cloned(),
            Some(format!("NodeDB {}", crate::version::VERSION)),
            "startup params must advertise NodeDB server_version, got {params:?}"
        );
    }
}
