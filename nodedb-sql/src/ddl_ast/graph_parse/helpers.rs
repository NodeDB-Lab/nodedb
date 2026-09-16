// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the graph DSL parsers.

use crate::error::SqlError;

/// A required clause was absent.
pub(super) fn missing_clause(statement: &str, clause: &str) -> SqlError {
    SqlError::Parse {
        detail: format!("{statement} requires {clause}"),
    }
}
