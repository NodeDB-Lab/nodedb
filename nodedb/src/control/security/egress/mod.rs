// SPDX-License-Identifier: BUSL-1.1

pub mod client;
pub mod global_unicast;
pub mod policy;
pub mod remote;
pub mod resolver;

pub use client::{
    EgressClientOptions, EgressFault, OUTBOUND_OPTIONS, RedirectError, classify, egress_client,
    outbound_client,
};
pub use global_unicast::is_global_unicast;
pub use policy::{EgressError, EgressPolicy};
pub use remote::RemoteSources;
pub use resolver::{EgressResolver, HostLookup, SystemLookup};
