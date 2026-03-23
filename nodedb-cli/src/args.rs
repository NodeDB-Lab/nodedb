//! CLI argument parsing.

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(name = "ndb", about = "NodeDB terminal client", version)]
pub struct CliArgs {
    /// Server host.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Server port (native protocol).
    #[arg(short, long, default_value_t = 6433)]
    pub port: u16,

    /// Username.
    #[arg(short = 'U', long, default_value = "admin")]
    pub user: String,

    /// Password (omit for trust mode).
    #[arg(short = 'W', long)]
    pub password: Option<String>,

    /// Execute a single SQL command and exit.
    #[arg(short, long)]
    pub execute: Option<String>,

    /// Output format.
    #[arg(long, default_value = "table", value_enum)]
    pub format: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl CliArgs {
    /// Build the server address string.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
