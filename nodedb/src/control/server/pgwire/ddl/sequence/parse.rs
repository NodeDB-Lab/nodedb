//! Parsing helpers shared by `create_sequence` and `drop_sequence`.

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::catalog::sequence_types::StoredSequence;

/// Parse
/// `CREATE SEQUENCE name [START n] [INCREMENT n] [MINVALUE n]
/// [MAXVALUE n] [CYCLE | NO CYCLE] [CACHE n] [FORMAT 'template']
/// [RESET YEARLY|MONTHLY|QUARTERLY|DAILY] [GAP_FREE] [SCOPE TENANT]`.
pub fn parse_create_sequence(
    sql: &str,
    tenant_id: u32,
    owner: &str,
) -> PgWireResult<StoredSequence> {
    let parts: Vec<&str> = sql.split_whitespace().collect();

    let name = parts
        .get(2)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "CREATE SEQUENCE requires a name".to_owned(),
            )))
        })?
        .to_lowercase();

    let upper: Vec<String> = parts.iter().map(|p| p.to_uppercase()).collect();

    let mut def = StoredSequence::new(tenant_id, name, owner.to_string());

    // Parse options by scanning for keywords.
    let mut i = 3; // skip "CREATE SEQUENCE name"
    while i < parts.len() {
        match upper[i].as_str() {
            "START" => {
                i += 1;
                if i < parts.len() && upper[i] == "WITH" {
                    i += 1;
                }
                if i < parts.len() {
                    def.start_value = parse_i64(parts[i], "START")?;
                }
            }
            "INCREMENT" => {
                i += 1;
                if i < parts.len() && upper[i] == "BY" {
                    i += 1;
                }
                if i < parts.len() {
                    def.increment = parse_i64(parts[i], "INCREMENT")?;
                }
            }
            "MINVALUE" => {
                i += 1;
                if i < parts.len() {
                    def.min_value = parse_i64(parts[i], "MINVALUE")?;
                }
            }
            "MAXVALUE" => {
                i += 1;
                if i < parts.len() {
                    def.max_value = parse_i64(parts[i], "MAXVALUE")?;
                }
            }
            "CYCLE" => {
                def.cycle = true;
            }
            "NO" => {
                i += 1;
                if i < parts.len() && upper[i] == "CYCLE" {
                    def.cycle = false;
                }
            }
            "CACHE" => {
                i += 1;
                if i < parts.len() {
                    def.cache_size = parse_i64(parts[i], "CACHE")?;
                }
            }
            "FORMAT" => {
                i += 1;
                if i < parts.len() {
                    let raw = parts[i].trim_matches('\'').trim_matches('"');
                    let tokens = crate::control::sequence::format::parse_format_template(raw)
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                format!("invalid FORMAT: {e}"),
                            )))
                        })?;
                    def.format_template = Some(tokens);
                }
            }
            "RESET" => {
                i += 1;
                if i < parts.len() {
                    def.reset_scope = crate::control::sequence::format::ResetScope::parse(parts[i])
                        .map_err(|e| {
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                "ERROR".to_owned(),
                                "42601".to_owned(),
                                e.to_string(),
                            )))
                        })?;
                }
            }
            "GAP_FREE" => {
                def.gap_free = true;
            }
            "SCOPE" => {
                // SCOPE TENANT — informational; affects `{TENANT}`
                // token resolution in FORMAT templates.
                i += 1;
            }
            _ => {
                // Ignore unknown tokens (e.g., "IF NOT EXISTS").
            }
        }
        i += 1;
    }

    // Apply defaults for descending sequences.
    if def.increment < 0 && def.min_value == 1 && def.max_value == i64::MAX {
        def.max_value = -1;
        def.min_value = i64::MIN;
        if def.start_value == 1 {
            def.start_value = -1;
        }
    }

    Ok(def)
}

fn parse_i64(s: &str, ctx: &str) -> PgWireResult<i64> {
    s.parse::<i64>().map_err(|_| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "22023".to_owned(),
            format!("invalid value for {ctx}: '{s}'"),
        )))
    })
}

/// Parse DROP target: extract name and `IF EXISTS` flag.
pub fn parse_drop_target(parts: &[&str], skip: usize) -> (String, bool) {
    let rest = &parts[skip..];
    if rest.len() >= 3
        && rest[0].eq_ignore_ascii_case("IF")
        && rest[1].eq_ignore_ascii_case("EXISTS")
    {
        (rest[2].to_lowercase(), true)
    } else if let Some(name) = rest.first() {
        (name.to_lowercase(), false)
    } else {
        (String::new(), false)
    }
}
