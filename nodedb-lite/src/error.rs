//! Error types for NodeDB-Lite.

/// Errors specific to the Lite embedded engine.
#[derive(Debug, thiserror::Error)]
pub enum LiteError {
    #[error("storage error: {detail}")]
    Storage { detail: String },

    #[error("storage backend returned poison lock")]
    LockPoisoned,

    #[error("async task join failed: {detail}")]
    JoinError { detail: String },

    #[error("serialization error: {detail}")]
    Serialization { detail: String },

    #[error("namespace {ns} not recognized")]
    InvalidNamespace { ns: u8 },
}

impl From<redb::Error> for LiteError {
    fn from(e: redb::Error) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<redb::DatabaseError> for LiteError {
    fn from(e: redb::DatabaseError) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<redb::TransactionError> for LiteError {
    fn from(e: redb::TransactionError) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<redb::StorageError> for LiteError {
    fn from(e: redb::StorageError) -> Self {
        Self::Storage {
            detail: e.to_string(),
        }
    }
}

impl From<LiteError> for nodedb_types::error::NodeDbError {
    fn from(e: LiteError) -> Self {
        nodedb_types::error::NodeDbError::Storage {
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lite_error_display() {
        let e = LiteError::Storage {
            detail: "disk full".into(),
        };
        assert!(e.to_string().contains("disk full"));
    }

    #[test]
    fn lite_error_converts_to_nodedb_error() {
        let e = LiteError::Storage {
            detail: "test".into(),
        };
        let ndb: nodedb_types::error::NodeDbError = e.into();
        assert!(ndb.to_string().contains("test"));
    }
}
