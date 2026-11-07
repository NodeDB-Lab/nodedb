// SPDX-License-Identifier: BUSL-1.1

//! Helper for wiring up the security bus pair during SharedState construction.

use std::sync::{Arc, Mutex};

use crate::control::security::audit::AuditLog;
use crate::control::security::buses::{SessionInvalidationBus, UserChangeBus};
use crate::control::security::credential::CredentialStore;
use crate::control::security::sessions::SessionRegistry;

/// Connect a credential store to the buses and the session registry the state
/// holds. A store with no registry ends no session on a hard revoke, so every
/// store a state adopts passes through here.
pub(super) fn connect_credential_store(
    credentials: &CredentialStore,
    si_bus: &SessionInvalidationBus,
    uc_bus: &UserChangeBus,
    session_registry: &Arc<SessionRegistry>,
) {
    credentials.set_buses(
        Arc::new(SessionInvalidationBus::from_existing(si_bus.sender())),
        Arc::new(UserChangeBus::from_existing(uc_bus.sender())),
    );
    credentials.set_session_registry(Arc::clone(session_registry));
}

/// Construct both security buses, subscribe the consumer receivers, spawn the
/// audit-log consumer task, and return the three values that `SharedState`
/// fields require.
pub(super) fn init_security_buses(
    audit: Arc<Mutex<AuditLog>>,
) -> (
    SessionInvalidationBus,
    UserChangeBus,
    Option<tokio::task::JoinHandle<()>>,
) {
    let (si_bus, si_rx) = SessionInvalidationBus::new();
    let (uc_bus, uc_rx) = UserChangeBus::new();
    let handle = crate::control::security::buses::spawn_bus_consumer(si_rx, uc_rx, audit);
    (si_bus, uc_bus, handle)
}
