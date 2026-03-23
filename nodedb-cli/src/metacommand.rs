//! Metacommand parsing and expansion.
//!
//! Backslash commands like `\d`, `\nodes`, `\cluster` are expanded
//! into SQL statements for execution.

/// Result of parsing a metacommand.
pub enum MetaAction {
    /// Execute this SQL statement.
    Sql(String),
    /// Change the output format.
    SetFormat(String),
    /// Toggle timing display.
    ToggleTiming,
    /// Show help.
    Help,
    /// Quit the TUI.
    Quit,
    /// Unknown metacommand.
    Unknown(String),
}

/// Parse a metacommand string (starts with `\`).
pub fn parse(input: &str) -> MetaAction {
    let trimmed = input.trim();
    let (cmd, arg) = trimmed
        .split_once(char::is_whitespace)
        .map(|(c, a)| (c, a.trim()))
        .unwrap_or((trimmed, ""));

    match cmd {
        // Schema introspection
        "\\d" | "\\collections" => MetaAction::Sql("SHOW COLLECTIONS".into()),
        "\\di" | "\\indexes" => {
            if arg.is_empty() {
                MetaAction::Sql("SHOW INDEXES".into())
            } else {
                MetaAction::Sql(format!("SHOW INDEXES ON {arg}"))
            }
        }
        "\\du" | "\\users" => MetaAction::Sql("SHOW USERS".into()),

        // Cluster management (kubectl-style)
        "\\nodes" => MetaAction::Sql("SHOW NODES".into()),
        "\\node" => {
            if arg.is_empty() {
                MetaAction::Sql("SHOW NODES".into())
            } else {
                MetaAction::Sql(format!("SHOW NODE {arg}"))
            }
        }
        "\\cluster" => MetaAction::Sql("SHOW CLUSTER".into()),
        "\\raft" => {
            if arg.is_empty() {
                MetaAction::Sql("SHOW RAFT GROUPS".into())
            } else {
                MetaAction::Sql(format!("SHOW RAFT GROUP {arg}"))
            }
        }
        "\\migrations" => MetaAction::Sql("SHOW MIGRATIONS".into()),
        "\\health" => MetaAction::Sql("SHOW PEER HEALTH".into()),
        "\\rebalance" => MetaAction::Sql("REBALANCE".into()),

        // Server status
        "\\s" | "\\status" => MetaAction::Sql("SHOW SESSION".into()),
        "\\connections" => MetaAction::Sql("SHOW CONNECTIONS".into()),

        // Session
        "\\format" => {
            if arg.is_empty() {
                MetaAction::Help
            } else {
                MetaAction::SetFormat(arg.to_string())
            }
        }
        "\\timing" => MetaAction::ToggleTiming,

        // Help / quit
        "\\?" | "\\help" => MetaAction::Help,
        "\\q" | "\\quit" | "\\exit" => MetaAction::Quit,

        _ => MetaAction::Unknown(cmd.to_string()),
    }
}

/// Return help text for metacommands.
pub fn help_text() -> &'static str {
    r#"Metacommands:
  \d                 Show collections
  \di [collection]   Show indexes
  \du                Show users
  \s                 Show session info

Cluster:
  \nodes             Show cluster nodes
  \node <id>         Show node details
  \cluster           Show cluster topology
  \raft [group]      Show raft groups
  \migrations        Show active migrations
  \health            Show peer health
  \rebalance         Trigger vShard rebalance

Session:
  \format <t|j|c>    Set output format (table/json/csv)
  \timing            Toggle query timing
  \connections       Show active connections

  \?                 Show this help
  \q                 Quit"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quit() {
        assert!(matches!(parse("\\q"), MetaAction::Quit));
        assert!(matches!(parse("\\quit"), MetaAction::Quit));
        assert!(matches!(parse("\\exit"), MetaAction::Quit));
    }

    #[test]
    fn parse_collections() {
        match parse("\\d") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW COLLECTIONS"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_nodes() {
        match parse("\\nodes") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW NODES"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_node_with_id() {
        match parse("\\node 3") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW NODE 3"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_format() {
        match parse("\\format json") {
            MetaAction::SetFormat(f) => assert_eq!(f, "json"),
            _ => panic!("expected SetFormat"),
        }
    }

    #[test]
    fn parse_unknown() {
        assert!(matches!(parse("\\xyz"), MetaAction::Unknown(_)));
    }
}
