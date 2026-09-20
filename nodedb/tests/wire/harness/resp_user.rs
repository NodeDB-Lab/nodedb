// SPDX-License-Identifier: BUSL-1.1

//! Authenticated RESP sessions for cases under `tests/wire/cases/`.
//!
//! Trust-mode `TestServer::start()` accepts pgwire as the superuser but
//! refuses RESP `AUTH` as one; a case needs a real `readwrite` user instead.

use super::resp_client::{self, RespClient};
use super::types::TestServer;

impl TestServer {
    /// Create `user` with a runtime-generated password, grant it
    /// `readwrite`, and return an authenticated RESP session scoped to
    /// `collection`.
    pub async fn resp_session(&self, user: &str, collection: &str) -> RespClient {
        let password = format!("{user}-{}-{}", std::process::id(), self.resp_port);
        self.exec(&format!("CREATE USER {user} PASSWORD '{password}'"))
            .await
            .unwrap_or_else(|e| panic!("CREATE USER {user} failed: {e}"));
        self.exec(&format!("GRANT ROLE readwrite TO {user}"))
            .await
            .unwrap_or_else(|e| panic!("GRANT ROLE readwrite TO {user} failed: {e}"));

        let addr = format!("127.0.0.1:{}", self.resp_port)
            .parse()
            .expect("loopback RESP address must parse");
        resp_client::session(addr, user, &password, collection).await
    }
}
