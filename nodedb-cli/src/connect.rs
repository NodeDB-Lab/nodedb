//! Connection helper: build a NativeClient from CLI args.

use nodedb_client::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use nodedb_types::protocol::AuthMethod;

use crate::args::CliArgs;
use crate::error::CliResult;

/// Connect to the server using CLI arguments.
pub fn build_client(args: &CliArgs) -> CliResult<NativeClient> {
    let auth = if let Some(ref pw) = args.password {
        AuthMethod::Password {
            username: args.user.clone(),
            password: pw.clone(),
        }
    } else {
        AuthMethod::Trust {
            username: args.user.clone(),
        }
    };

    let config = PoolConfig {
        addr: args.addr(),
        max_size: 2, // CLI only needs 1-2 connections
        auth,
        ..Default::default()
    };

    Ok(NativeClient::new(config))
}
