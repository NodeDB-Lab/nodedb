// SPDX-License-Identifier: BUSL-1.1

//! Minimal flag parser shared by all subcommands.
//!
//! The operator CLI needs two kinds of long-form flags:
//!
//! - Key-value: `--data-dir /var/nodedb`, `--node-id 3`, `--ttl 10m`
//! - Boolean (presence-only): `--stage`, `--finalize`, `--create`
//!
//! A flag is treated as boolean when its next token is absent or starts
//! with `--`. Boolean flags are stored with an empty-string value so
//! callers can use `flags.contains_key("stage")` for presence checks.
//! Pulling in clap would be heavy for this surface; a small parser
//! is enough and keeps the binary's compile-time cost unchanged.

use std::collections::HashMap;

/// Parse a list of `--key [value]` arguments into a map.
///
/// Each `--flag` is consumed as:
/// - **key-value** when the next token exists and does not start with `--`
/// - **boolean** (stored with value `""`) when the next token is absent
///   or starts with `--`
///
/// Repeated flags overwrite. Returns an error for unrecognised tokens
/// (positional args, single-dash shortflags) so operators get a clear
/// message instead of silent ignore.
pub fn parse_flags(args: &[String]) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if let Some(name) = a.strip_prefix("--") {
            if let Some(value) = args.get(i + 1).filter(|n| !n.starts_with("--")) {
                map.insert(name.to_string(), value.clone());
                i += 2;
            } else {
                // Boolean flag — no value token follows.
                map.insert(name.to_string(), String::new());
                i += 1;
            }
        } else {
            return Err(format!("unexpected argument: {a}"));
        }
    }
    Ok(map)
}

/// Return `true` if a boolean (value-less) flag is present in `args`.
///
/// Boolean flags have no value argument — they are present or absent.
/// Example: `rotate-ca --stage --data-dir /var/nodedb`
pub fn has_bool_flag(args: &[String], name: &str) -> bool {
    let needle = format!("--{name}");
    args.iter().any(|a| a == &needle)
}

/// Require a flag and return its value, or produce a uniform error.
pub fn required<'a>(flags: &'a HashMap<String, String>, name: &str) -> Result<&'a String, String> {
    flags
        .get(name)
        .ok_or_else(|| format!("missing required flag --{name}"))
}

/// Parse a human-friendly duration string (`10s`, `5m`, `2h`). No
/// fractional units. Used by `--ttl`.
pub fn parse_duration(s: &str) -> Result<std::time::Duration, String> {
    let (num_str, unit) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| format!("duration needs a unit suffix (s/m/h): {s}"))?,
    );
    let n: u64 = num_str
        .parse()
        .map_err(|_| format!("bad duration number: {s}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        other => return Err(format!("unknown duration unit {other:?} (expected s/m/h)")),
    };
    Ok(std::time::Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kv_flags() {
        let args = vec![
            "--data-dir".into(),
            "/var/nodedb".into(),
            "--node-id".into(),
            "3".into(),
        ];
        let m = parse_flags(&args).unwrap();
        assert_eq!(m.get("data-dir").unwrap(), "/var/nodedb");
        assert_eq!(m.get("node-id").unwrap(), "3");
    }

    #[test]
    fn lone_flag_is_boolean() {
        // A flag with no following token is treated as boolean (empty value),
        // not an error — `--stage` in `rotate-ca --stage --data-dir /d` is
        // the canonical example.
        let m = parse_flags(&["--stage".into()]).unwrap();
        assert_eq!(m.get("stage").map(|s| s.as_str()), Some(""));
    }

    #[test]
    fn flag_followed_by_flag_is_boolean() {
        let m = parse_flags(&["--stage".into(), "--data-dir".into(), "/d".into()]).unwrap();
        assert_eq!(m.get("stage").map(|s| s.as_str()), Some(""));
        assert_eq!(m.get("data-dir").map(|s| s.as_str()), Some("/d"));
    }

    #[test]
    fn rejects_positional() {
        assert!(parse_flags(&["positional".into()]).is_err());
    }

    #[test]
    fn bool_flag_detection() {
        let args: Vec<String> = vec!["--stage".into(), "/etc/nodedb.toml".into()];
        assert!(has_bool_flag(&args, "stage"));
        assert!(!has_bool_flag(&args, "other-flag"));
        assert!(!has_bool_flag(&[], "stage"));
    }

    #[test]
    fn duration_units() {
        use std::time::Duration;
        assert_eq!(parse_duration("10s").unwrap(), Duration::from_secs(10));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert!(parse_duration("10x").is_err());
        assert!(parse_duration("10").is_err());
    }
}
