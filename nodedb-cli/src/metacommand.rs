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
    /// Toggle expanded (vertical) display mode.
    ToggleExpanded,
    /// Show connection info.
    ConnInfo,
    /// Open external editor.
    ExternalEditor,
    /// Execute SQL from file.
    ExecuteFile(String),
    /// Save last results to file.
    SaveResults(String),
    /// Repeat last query every N seconds.
    Watch(u64),
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

        // Functions, triggers, procedures
        "\\df" | "\\functions" => MetaAction::Sql("SHOW FUNCTIONS".into()),
        "\\dtr" | "\\triggers" => {
            if arg.is_empty() {
                MetaAction::Sql("SHOW TRIGGERS".into())
            } else {
                MetaAction::Sql(format!("SHOW TRIGGERS ON {arg}"))
            }
        }
        "\\dpr" | "\\procedures" => MetaAction::Sql("SHOW PROCEDURES".into()),

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
        "\\x" => MetaAction::ToggleExpanded,
        "\\conninfo" => MetaAction::ConnInfo,
        "\\e" | "\\edit" => MetaAction::ExternalEditor,
        "\\i" => {
            if arg.is_empty() {
                MetaAction::Unknown("\\i requires a filename".into())
            } else {
                MetaAction::ExecuteFile(arg.to_string())
            }
        }
        "\\g" => {
            if arg.is_empty() {
                MetaAction::Unknown("\\g requires a filename".into())
            } else {
                MetaAction::SaveResults(arg.to_string())
            }
        }
        "\\watch" => {
            let n = arg.parse::<u64>().unwrap_or(2);
            MetaAction::Watch(n)
        }

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
  \df                Show functions
  \dtr [collection]  Show triggers
  \dpr               Show procedures
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
  \x                 Toggle expanded (vertical) display
  \conninfo          Show connection details
  \connections       Show active connections

Editor & Files:
  \e                 Edit query in $EDITOR
  \i <file>          Execute SQL from file
  \g <file>          Save last results to file
  \watch <N>         Repeat last query every N seconds

  \?                 Show this help
  \q                 Quit

Keyboard:
  Tab                Auto-complete keywords/collections
  Ctrl+R             Reverse search history
  Ctrl+C             Clear input (or quit if empty)
  PageUp/PageDown    Scroll results

SQL Examples:
  -- Collections & Documents
  CREATE COLLECTION users;
  INSERT INTO users (id, name, age) VALUES ('u1', 'Alice', 30);
  SELECT * FROM users WHERE age > 25;
  UPDATE users SET age = 31 WHERE id = 'u1';
  DELETE FROM users WHERE id = 'u1';

  -- Vector Search
  SEARCH embeddings USING VECTOR(ARRAY[0.1, 0.2, 0.3], 5);
  CREATE VECTOR INDEX idx ON embeddings METRIC cosine M 16 DIM 3;

  -- Graph
  GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'KNOWS';
  GRAPH TRAVERSE FROM 'alice' DEPTH 3 DIRECTION both;
  GRAPH NEIGHBORS OF 'alice' LABEL 'KNOWS';
  GRAPH PATH FROM 'alice' TO 'charlie' MAX_DEPTH 5;
  GRAPH DELETE EDGE FROM 'alice' TO 'bob' TYPE 'KNOWS';

  -- CRDT
  SELECT CRDT_STATE('collection', 'doc_id');
  CRDT MERGE INTO collection FROM 'src_id' TO 'dst_id';

  -- GraphRAG Fusion (vector + graph + RRF)
  SEARCH docs USING FUSION(VECTOR_TOP_K 20, DEPTH 2, TOP 10);

  -- Functions
  CREATE FUNCTION normalize(email TEXT) RETURNS TEXT AS SELECT LOWER(TRIM(email));
  SELECT normalize(email) FROM users;
  DROP FUNCTION normalize;

  -- Triggers
  CREATE TRIGGER audit AFTER INSERT ON orders FOR EACH ROW
    BEGIN INSERT INTO audit_log (id) VALUES (NEW.id); END;
  ALTER TRIGGER audit DISABLE;

  -- Procedures
  CREATE PROCEDURE cleanup(days INT) AS BEGIN DELETE FROM temp WHERE age > days; END;
  CALL cleanup(30);"#
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
    fn parse_functions() {
        match parse("\\df") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW FUNCTIONS"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_triggers() {
        match parse("\\dtr") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW TRIGGERS"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_triggers_on_collection() {
        match parse("\\dtr orders") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW TRIGGERS ON orders"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_procedures() {
        match parse("\\dpr") {
            MetaAction::Sql(s) => assert_eq!(s, "SHOW PROCEDURES"),
            _ => panic!("expected Sql"),
        }
    }

    #[test]
    fn parse_unknown() {
        assert!(matches!(parse("\\xyz"), MetaAction::Unknown(_)));
    }
}
