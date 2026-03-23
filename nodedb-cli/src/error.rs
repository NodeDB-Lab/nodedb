//! CLI error type.

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Client(#[from] nodedb_types::error::NodeDbError),
}

pub type CliResult<T> = std::result::Result<T, CliError>;
