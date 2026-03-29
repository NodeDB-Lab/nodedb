//! CLI argument parsing with config file and env var resolution.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

use crate::config::CliConfig;

#[derive(Parser)]
#[command(
    name = "ndb",
    about = "NodeDB terminal client",
    version,
    disable_help_flag = true
)]
pub struct CliArgs {
    /// Server host.
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    pub host: String,

    /// Print help information.
    #[arg(long, action = clap::ArgAction::Help)]
    pub help: Option<bool>,

    /// Server port (native protocol).
    #[arg(short, long, default_value_t = 6433)]
    pub port: u16,

    /// Username.
    #[arg(short = 'u', long, default_value = "admin")]
    pub user: String,

    /// Password. If flag given without value, prompts interactively.
    #[arg(short = 'w', long, num_args = 0..=1, default_missing_value = "")]
    pub password: Option<String>,

    /// Execute a single SQL command and exit.
    #[arg(short, long)]
    pub execute: Option<String>,

    /// Execute SQL from file and exit.
    #[arg(short, long)]
    pub file: Option<PathBuf>,

    /// Write output to file.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Enable TLS encryption.
    #[arg(long)]
    pub tls: bool,

    /// Path to CA certificate file (PEM) for TLS verification.
    #[arg(long)]
    pub tls_ca_cert: Option<PathBuf>,

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

    /// Resolve effective settings: CLI flags > env vars > config file > defaults.
    pub fn resolve(mut self, config: &CliConfig) -> Self {
        // Host: env > config > CLI default
        if self.host == "127.0.0.1" {
            if let Ok(h) = std::env::var("NODEDB_HOST") {
                self.host = h;
            } else if let Some(ref h) = config.host {
                self.host = h.clone();
            }
        }

        // Port: env > config > CLI default
        if self.port == 6433 {
            if let Ok(p) = std::env::var("NODEDB_PORT") {
                if let Ok(p) = p.parse() {
                    self.port = p;
                }
            } else if let Some(p) = config.port {
                self.port = p;
            }
        }

        // User: env > config > CLI default
        if self.user == "admin" {
            if let Ok(u) = std::env::var("NODEDB_USER") {
                self.user = u;
            } else if let Some(ref u) = config.user {
                self.user = u.clone();
            }
        }

        // Password: env > config > CLI flag
        if self.password.is_none() {
            if let Ok(pw) = std::env::var("NODEDB_PASSWORD") {
                self.password = Some(pw);
            } else if let Some(ref pw) = config.password {
                self.password = Some(pw.clone());
            }
        }

        // Format: config override (CLI flag takes precedence via clap)
        if let Some(ref f) = config.format {
            // Only override if user didn't explicitly pass --format
            match f.as_str() {
                "json" => self.format = OutputFormat::Json,
                "csv" => self.format = OutputFormat::Csv,
                _ => {}
            }
        }

        self
    }
}
