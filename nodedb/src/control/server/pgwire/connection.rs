// SPDX-License-Identifier: BUSL-1.1

//! Panic-isolated pgwire connection processing.
//!
//! The pgwire crate owns message decoding and protocol handlers. This module
//! mirrors its public socket orchestration so a panic in one client connection
//! cannot unwind the listener task. Normal protocol errors retain pgwire's
//! `process_error` state transitions; only panics get the fixed fatal reply.

use std::future::Future;
use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::{FutureExt, SinkExt, StreamExt};
use pgwire::api::{ClientInfo, ErrorHandler, PgWireConnectionState};
use pgwire::error::ErrorInfo;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::server::{MaybeTls, negotiate_tls, process_error, process_message};
use tokio::net::TcpStream;
use tracing::warn;

use crate::control::security::tls_policy::TransportSecurity;

use super::connection_identity::PgConnectionContext;
use super::factory::NodeDbPgHandlerFactory;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const INTERNAL_ERROR_MESSAGE: &str = "internal server error";

/// The observable outcome of a single connection loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionOutcome {
    Closed,
    Panicked,
}

/// Read the negotiated transport out of the socket pgwire produced.
///
/// `MaybeTls` is `#[non_exhaustive]`, so the catch-all arm is required; every
/// non-TLS arm (plain TCP, Unix socket) is cleartext as far as the policy is
/// concerned.
fn transport_security_of(socket: &MaybeTls) -> TransportSecurity {
    match socket {
        MaybeTls::Tls(stream) => {
            let (_, session) = stream.get_ref();
            TransportSecurity::from_rustls(session)
        }
        _ => TransportSecurity::Cleartext,
    }
}

fn recovery_is_safe(pending_output_len: usize) -> bool {
    pending_output_len == 0
}

fn materialize_handlers<T>(build: impl FnOnce() -> T) -> Result<T, ()> {
    std::panic::catch_unwind(AssertUnwindSafe(build)).map_err(|_| ())
}

fn fixed_panic_response() -> PgWireBackendMessage {
    let error = ErrorInfo::new(
        "FATAL".to_owned(),
        nodedb_types::error::sqlstate::INTERNAL_ERROR.to_owned(),
        INTERNAL_ERROR_MESSAGE.to_owned(),
    );
    PgWireBackendMessage::ErrorResponse(error.into())
}

macro_rules! recover_from_panic {
    ($socket:expr, $response:expr) => {{
        // A queued response can already be partially visible to the peer. Do
        // not append another frame in that case; dropping the connection is
        // the only conservative recovery. The response was allocated before
        // panic-prone dispatch and is consumed at most once.
        if recovery_is_safe(($socket).write_buffer().len()) {
            if let Some(response) = ($response).take() {
                let recovery = AssertUnwindSafe(async {
                    ($socket).send(response).await?;
                    ($socket).close().await
                })
                .catch_unwind()
                .await;
                if !matches!(recovery, Ok(Ok(()))) {
                    warn!("pgwire panic recovery could not send fixed fatal response");
                }
            }
        } else {
            warn!("pgwire panic recovery closed connection with pending output");
        }
    }};
}

/// Process one pgwire TCP connection without allowing an application panic to
/// escape its connection task.
///
/// Panics before TLS negotiation yields a framed socket cannot be replied to,
/// so the TCP stream is dropped. Once a socket exists, every panic-prone
/// stage is caught locally and gets one fixed fatal response only if pgwire has
/// no buffered output that can make the wire stream ambiguous.
///
/// The connection future is boxed, once per connection. Every statement path
/// nests inside it, and unboxed it can overflow the compiler's layout depth
/// limit in the listener's connection task.
pub(crate) fn run(
    stream: TcpStream,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    factory: Arc<NodeDbPgHandlerFactory>,
    context: PgConnectionContext,
) -> BoxFuture<'static, ConnectionOutcome> {
    // Session slots are installed for the whole connection, not per statement:
    // a transaction's statements are polled on whichever worker tokio picks,
    // so the DDL buffer must follow the task across every await.
    Box::pin(crate::control::server::shared::session::conn_scope::scoped(
        isolate_connection_future(run_inner(stream, tls_acceptor, factory, context)),
    ))
}

async fn isolate_connection_future<F>(future: F) -> ConnectionOutcome
where
    F: Future<Output = io::Result<()>>,
{
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => ConnectionOutcome::Closed,
        Ok(Err(error)) => {
            warn!(error = %error, "pgwire connection I/O error");
            ConnectionOutcome::Closed
        }
        Err(_) => {
            // Never inspect or format a panic payload: it can contain SQL,
            // credentials, or application internals.
            warn!("pgwire connection panicked before a safe recovery point");
            ConnectionOutcome::Panicked
        }
    }
}

async fn run_inner(
    stream: TcpStream,
    tls_acceptor: Option<pgwire::tokio::TlsAcceptor>,
    factory: Arc<NodeDbPgHandlerFactory>,
    context: PgConnectionContext,
) -> io::Result<()> {
    let startup_timeout = tokio::time::sleep(STARTUP_TIMEOUT);
    tokio::pin!(startup_timeout);

    let negotiated = tokio::select! {
        _ = &mut startup_timeout => return Ok(()),
        result = negotiate_tls(stream, tls_acceptor) => result?,
    };
    let Some(mut socket) = negotiated else {
        return Ok(());
    };
    // pgwire owns the SSLRequest negotiation, so the handshake facts are only
    // reachable here, between `negotiate_tls` returning and the framed socket
    // being handed to the message loop. They are stashed in the connection's
    // typed session-extension store — not `metadata`, which the startup
    // handler fills from client-supplied startup parameters and a client can
    // therefore forge — and read back at identity resolution.
    socket
        .session_extensions()
        .insert(transport_security_of(socket.get_ref()));
    // Allocate the fixed recovery frame before entering any application-owned
    // handler construction or message dispatch. Panic recovery only takes this
    // prebuilt value and never formats a panic payload.
    let mut panic_response = Some(fixed_panic_response());

    // Handler construction can touch application state. It is synchronous, so
    // isolate it before any protocol message is processed while the framed
    // socket is still safe for the fixed fatal response.
    let Ok(handlers) = materialize_handlers(|| factory.connection_handlers(context)) else {
        recover_from_panic!(&mut socket, &mut panic_response);
        return Ok(());
    };

    loop {
        let next = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => None,
                result = AssertUnwindSafe(socket.next()).catch_unwind() => {
                    match result {
                        Ok(message) => message,
                        Err(_) => {
                            recover_from_panic!(&mut socket, &mut panic_response);
                            break;
                        }
                    }
                }
            }
        } else {
            match AssertUnwindSafe(socket.next()).catch_unwind().await {
                Ok(message) => message,
                Err(_) => {
                    recover_from_panic!(&mut socket, &mut panic_response);
                    break;
                }
            }
        };

        let Some(Ok(message)) = next else {
            break;
        };
        let is_extended_query = match socket.state() {
            PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
            _ => message.is_extended_query(),
        };

        let dispatch = AssertUnwindSafe(process_message(
            message,
            &mut socket,
            Arc::clone(&handlers.startup),
            Arc::clone(&handlers.query),
            Arc::clone(&handlers.query),
            Arc::clone(&handlers.copy),
            Arc::clone(&handlers.cancel),
        ))
        .catch_unwind()
        .await;
        let error = match dispatch {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(_) => {
                recover_from_panic!(&mut socket, &mut panic_response);
                break;
            }
        };

        // An error in any extended-query message (Parse, Bind, Describe,
        // Execute) inside a transaction block aborts the block, the same
        // rule a failed simple-query statement applies.
        if is_extended_query {
            handlers.query.abort_transaction_block();
        }

        let mut error = error;
        if std::panic::catch_unwind(AssertUnwindSafe(|| {
            handlers.error.on_error(&socket, &mut error);
        }))
        .is_err()
        {
            recover_from_panic!(&mut socket, &mut panic_response);
            break;
        }

        match AssertUnwindSafe(process_error(&mut socket, error, is_extended_query))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                recover_from_panic!(&mut socket, &mut panic_response);
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgwire::messages::response::ErrorResponse;

    fn panic_error_fields() -> Vec<(u8, String)> {
        let PgWireBackendMessage::ErrorResponse(ErrorResponse { fields, .. }) =
            fixed_panic_response()
        else {
            panic!("fixed panic response must be an ErrorResponse");
        };
        fields
    }

    #[test]
    fn fixed_panic_response_is_fatal_and_does_not_disclose_payload() {
        let fields = panic_error_fields();
        assert!(
            fields
                .iter()
                .any(|(tag, value)| *tag == b'S' && value == "FATAL")
        );
        assert!(
            fields
                .iter()
                .any(|(tag, value)| *tag == b'C' && value == "XX000")
        );
        assert!(
            fields
                .iter()
                .any(|(tag, value)| *tag == b'M' && value == "internal server error")
        );
        assert!(
            !fields
                .iter()
                .any(|(_, value)| value.contains("secret panic payload"))
        );
    }

    #[test]
    fn recovery_is_permitted_only_without_pending_output() {
        assert!(recovery_is_safe(0));
        assert!(!recovery_is_safe(1));
    }

    #[test]
    fn handler_materialization_panic_is_caught_before_dispatch() {
        let result = materialize_handlers(|| -> () {
            panic!("secret panic payload must not reach pgwire dispatch");
        });
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn isolated_panic_leaves_the_listener_task_able_to_reap_connections() {
        let mut connections = tokio::task::JoinSet::new();
        connections.spawn(isolate_connection_future(async {
            panic!("secret panic payload must not be sent to the client");
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        }));
        connections.spawn(isolate_connection_future(async { Ok(()) }));

        let first = connections
            .join_next()
            .await
            .expect("first task")
            .expect("join");
        let second = connections
            .join_next()
            .await
            .expect("second task")
            .expect("join");
        assert!(
            matches!(first, ConnectionOutcome::Panicked)
                || matches!(second, ConnectionOutcome::Panicked)
        );
        assert!(
            matches!(first, ConnectionOutcome::Closed)
                || matches!(second, ConnectionOutcome::Closed)
        );
    }
}
