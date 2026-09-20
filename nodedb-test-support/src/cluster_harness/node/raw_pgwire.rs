// SPDX-License-Identifier: BUSL-1.1

//! Raw pgwire connection construction for [`TestClusterNode`].
//!
//! `tokio_postgres` decodes `CommandComplete` down to a row count, so a test
//! that asserts the tag verb (`INSERT 0 1` vs `OK`) reads the wire directly.

use crate::pgwire_harness::raw_pgwire::RawPgConn;

use super::lifecycle::{HARNESS_SUPERUSER, TestClusterNode};

/// Database the pre-wired `client` field connects to.
const HARNESS_DATABASE: &str = "default";

impl TestClusterNode {
    /// A raw simple-query pgwire connection to this node, authenticated as
    /// the harness's bootstrapped trust superuser on the same database the
    /// `client` field uses.
    pub async fn raw_pgwire(&self) -> RawPgConn {
        RawPgConn::connect(self.pg_addr.port(), HARNESS_SUPERUSER, HARNESS_DATABASE).await
    }
}
