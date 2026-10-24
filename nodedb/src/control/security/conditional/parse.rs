// SPDX-License-Identifier: BUSL-1.1

//! The single SQL → [`GrantCondition`] parser.
//!
//! `GRANT SCOPE` has no typed AST: `nodedb-sql`'s grant parser returns `None`
//! for it on purpose, and the whole family (`GRANT` / `REVOKE` / `RENEW` /
//! `SHOW SCOPE GRANTS`, with their `EXPIRES` / `GRACE PERIOD` / `ON EXPIRE`
//! clauses) is parsed from the raw token slice by the neutral scope DDL
//! handlers. The condition clauses are part of that same statement and
//! produce a `GrantCondition`, a type this crate owns — so they are parsed
//! here, beside the rest of the statement, and nowhere else.
//!
//! Grammar (clauses may appear in any order, interleaved with the expiry
//! clauses this parser skips over):
//!
//! ```sql
//! WHEN BETWEEN '09:00' AND '17:00' [ON WEEKDAYS | ON WEEKENDS | ON ALL]
//! REQUIRE MFA
//! REQUIRE IP IN ('10.0.0.0/8', '192.168.0.0/16')
//! REQUIRE STEP_UP [<seconds>]
//! REQUIRE DEVICE_TRUST
//! ```
//!
//! Malformed clauses are rejected at DDL time rather than silently dropped:
//! a condition that failed to parse would leave a grant looking conditional
//! while applying unconditionally.

use crate::control::security::net::Cidr;

use super::condition::{DEFAULT_STEP_UP_SECS, GrantCondition, WEEKDAYS, WEEKENDS};

/// Hours in a day; the exclusive end of a full-day window.
const HOURS_PER_DAY: u8 = 24;

/// Keywords that open a clause of the `GRANT SCOPE` statement. A token from
/// this set terminates an optional-argument lookahead (so `REQUIRE STEP_UP
/// EXPIRES 99` reads as a defaulted step-up followed by an expiry clause,
/// not as a step-up with a garbage interval).
const CLAUSE_KEYWORDS: [&str; 5] = ["WHEN", "REQUIRE", "EXPIRES", "GRACE", "ON"];

/// Parse every condition clause out of a `GRANT SCOPE` token slice.
///
/// Tokens that belong to other clauses (`EXPIRES`, `GRACE PERIOD`, `ON
/// EXPIRE`, the grantee, ...) are skipped; only `WHEN` and `REQUIRE` clauses
/// are consumed, and those are parsed strictly.
pub fn parse_conditions(tokens: &[&str]) -> crate::Result<Vec<GrantCondition>> {
    let mut conditions = Vec::new();
    let mut i = 0;

    while i < tokens.len() {
        if keyword_is(tokens[i], "WHEN") {
            let (condition, next) = parse_temporal(tokens, i)?;
            conditions.push(condition);
            i = next;
        } else if keyword_is(tokens[i], "REQUIRE") {
            let (condition, next) = parse_requirement(tokens, i)?;
            conditions.push(condition);
            i = next;
        } else {
            i += 1;
        }
    }

    Ok(conditions)
}

/// `WHEN BETWEEN '<start>' AND '<end>' [ON <day selector>]`, starting at the
/// `WHEN` token. Returns the condition and the index just past the clause.
fn parse_temporal(tokens: &[&str], start: usize) -> crate::Result<(GrantCondition, usize)> {
    expect_keyword(tokens, start + 1, "BETWEEN")?;
    let start_hour = parse_hour(token_at(tokens, start + 2, "WHEN BETWEEN <start time>")?)?;
    expect_keyword(tokens, start + 3, "AND")?;
    let end_hour = parse_hour(token_at(
        tokens,
        start + 4,
        "WHEN BETWEEN ... AND <end time>",
    )?)?;

    if start_hour >= HOURS_PER_DAY {
        return Err(condition_error(
            "WHEN BETWEEN: start hour must be between 00 and 23",
        ));
    }
    if end_hour == 0 || end_hour > HOURS_PER_DAY {
        return Err(condition_error(
            "WHEN BETWEEN: end hour must be between 01 and 24",
        ));
    }
    if start_hour == end_hour {
        return Err(condition_error(
            "WHEN BETWEEN: start and end hour are equal, so the window is never open",
        ));
    }

    let mut next = start + 5;
    let mut days = Vec::new();
    // `ON` here is ambiguous with the statement's own `ON EXPIRE` clause, so
    // only consume it when a day selector follows.
    if tokens
        .get(next)
        .is_some_and(|token| keyword_is(token, "ON"))
        && let Some(selector) = tokens.get(next + 1)
        && !keyword_is(selector, "EXPIRE")
    {
        days = parse_day_selector(selector)?;
        next += 2;
    }

    Ok((
        GrantCondition::Temporal {
            start_hour,
            end_hour,
            days,
        },
        next,
    ))
}

/// `REQUIRE <MFA | IP IN (...) | STEP_UP [secs] | DEVICE_TRUST>`, starting at
/// the `REQUIRE` token.
fn parse_requirement(tokens: &[&str], start: usize) -> crate::Result<(GrantCondition, usize)> {
    let kind = token_at(
        tokens,
        start + 1,
        "REQUIRE <MFA | IP | STEP_UP | DEVICE_TRUST>",
    )?;

    if keyword_is(kind, "MFA") {
        return Ok((GrantCondition::RequireMfa, start + 2));
    }
    if keyword_is(kind, "DEVICE_TRUST") {
        return Ok((GrantCondition::RequireDeviceTrust, start + 2));
    }
    if keyword_is(kind, "STEP_UP") {
        return Ok(parse_step_up(tokens, start));
    }
    if keyword_is(kind, "IP") {
        return parse_require_ip(tokens, start);
    }

    Err(condition_error(format!(
        "REQUIRE {kind}: expected MFA, IP, STEP_UP, or DEVICE_TRUST"
    )))
}

/// `REQUIRE STEP_UP [<seconds>]`. An omitted interval defaults to
/// [`DEFAULT_STEP_UP_SECS`].
fn parse_step_up(tokens: &[&str], start: usize) -> (GrantCondition, usize) {
    match tokens.get(start + 2) {
        Some(token) if !is_clause_keyword(token) => match token.parse::<u64>() {
            Ok(max_age_secs) => (GrantCondition::StepUpAuth { max_age_secs }, start + 3),
            // Not an interval and not a clause opener — leave it for the
            // outer scan, which skips tokens belonging to other clauses.
            Err(_) => (
                GrantCondition::StepUpAuth {
                    max_age_secs: DEFAULT_STEP_UP_SECS,
                },
                start + 2,
            ),
        },
        _ => (
            GrantCondition::StepUpAuth {
                max_age_secs: DEFAULT_STEP_UP_SECS,
            },
            start + 2,
        ),
    }
}

/// `REQUIRE IP IN ('<cidr>'[, '<cidr>']...)`. The CIDR list is validated
/// here so a typo is a DDL error instead of a grant that silently never
/// applies.
fn parse_require_ip(tokens: &[&str], start: usize) -> crate::Result<(GrantCondition, usize)> {
    expect_keyword(tokens, start + 2, "IN")?;

    let mut list = String::new();
    let mut cursor = start + 3;
    let mut closed = false;
    while let Some(token) = tokens.get(cursor) {
        if !list.is_empty() {
            list.push(' ');
        }
        list.push_str(token);
        cursor += 1;
        if token.ends_with(')') {
            closed = true;
            break;
        }
    }
    if !closed {
        return Err(condition_error(
            "REQUIRE IP IN: expected a parenthesized CIDR list, e.g. ('10.0.0.0/8')",
        ));
    }

    let inner = list
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(|| {
            condition_error(
                "REQUIRE IP IN: expected a parenthesized CIDR list, e.g. ('10.0.0.0/8')",
            )
        })?;

    let mut allowed_cidrs = Vec::new();
    for entry in inner.split(',') {
        let cidr = entry.trim().trim_matches('\'').trim();
        if cidr.is_empty() {
            continue;
        }
        if Cidr::parse(cidr).is_none() {
            return Err(condition_error(format!(
                "REQUIRE IP IN: '{cidr}' is not a valid IP address or CIDR range"
            )));
        }
        allowed_cidrs.push(cidr.to_string());
    }
    if allowed_cidrs.is_empty() {
        return Err(condition_error(
            "REQUIRE IP IN: the CIDR list is empty, so the grant could never apply",
        ));
    }

    Ok((GrantCondition::RequireIp { allowed_cidrs }, cursor))
}

/// `WEEKDAYS` / `WEEKENDS` / `ALL` → the days a temporal window covers.
fn parse_day_selector(selector: &str) -> crate::Result<Vec<u8>> {
    let bare = selector.trim_matches('\'');
    if bare.eq_ignore_ascii_case("WEEKDAYS") {
        Ok(WEEKDAYS.to_vec())
    } else if bare.eq_ignore_ascii_case("WEEKENDS") {
        Ok(WEEKENDS.to_vec())
    } else if bare.eq_ignore_ascii_case("ALL") {
        Ok(Vec::new())
    } else {
        Err(condition_error(format!(
            "WHEN ... ON {bare}: expected WEEKDAYS, WEEKENDS, or ALL"
        )))
    }
}

/// Parse `'HH'` or `'HH:MM'` into its hour. Minutes are validated but not
/// retained — the window granularity is one hour.
fn parse_hour(token: &str) -> crate::Result<u8> {
    let bare = token.trim_matches('\'');
    let mut segments = bare.split(':');
    let hour = segments
        .next()
        .and_then(|h| h.parse::<u8>().ok())
        .ok_or_else(|| condition_error("WHEN BETWEEN: times must be written 'HH' or 'HH:MM'"))?;
    if let Some(minutes) = segments.next() {
        let parsed = minutes
            .parse::<u8>()
            .map_err(|_| condition_error("WHEN BETWEEN: times must be written 'HH' or 'HH:MM'"))?;
        if parsed >= 60 {
            return Err(condition_error("WHEN BETWEEN: minutes must be 00-59"));
        }
    }
    if segments.next().is_some() {
        return Err(condition_error(
            "WHEN BETWEEN: times must be written 'HH' or 'HH:MM'",
        ));
    }
    Ok(hour)
}

/// Fetch the token at `index`, erroring with the expected shape when the
/// statement ends first.
fn token_at<'a>(tokens: &[&'a str], index: usize, expected: &str) -> crate::Result<&'a str> {
    tokens
        .get(index)
        .copied()
        .ok_or_else(|| condition_error(format!("expected {expected}")))
}

/// Require the token at `index` to be `keyword`.
fn expect_keyword(tokens: &[&str], index: usize, keyword: &str) -> crate::Result<()> {
    let token = token_at(tokens, index, keyword)?;
    if keyword_is(token, keyword) {
        Ok(())
    } else {
        Err(condition_error(format!(
            "expected {keyword}, found {token}"
        )))
    }
}

/// Case-insensitive keyword comparison.
fn keyword_is(token: &str, keyword: &str) -> bool {
    token.eq_ignore_ascii_case(keyword)
}

/// True for tokens that open one of the statement's clauses.
fn is_clause_keyword(token: &str) -> bool {
    CLAUSE_KEYWORDS
        .iter()
        .any(|keyword| keyword_is(token, keyword))
}

/// Build the typed error the scope DDL handler reports as SQLSTATE 42601.
fn condition_error(detail: impl Into<String>) -> crate::Error {
    crate::Error::BadRequest {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(sql: &str) -> crate::Result<Vec<GrantCondition>> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        parse_conditions(&tokens)
    }

    #[test]
    fn temporal_window_with_weekdays() {
        let conditions = parse("WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS").expect("parses");
        assert_eq!(
            conditions,
            vec![GrantCondition::Temporal {
                start_hour: 9,
                end_hour: 17,
                days: WEEKDAYS.to_vec(),
            }]
        );
    }

    #[test]
    fn temporal_window_without_day_selector_covers_every_day() {
        let conditions = parse("WHEN BETWEEN '09:00' AND '17:00'").expect("parses");
        assert_eq!(
            conditions,
            vec![GrantCondition::Temporal {
                start_hour: 9,
                end_hour: 17,
                days: Vec::new(),
            }]
        );
    }

    #[test]
    fn temporal_on_expire_is_not_mistaken_for_a_day_selector() {
        let conditions =
            parse("WHEN BETWEEN '09:00' AND '17:00' ON EXPIRE REVOKE ALL").expect("parses");
        assert_eq!(conditions.len(), 1);
        assert!(matches!(
            conditions[0],
            GrantCondition::Temporal { days: ref d, .. } if d.is_empty()
        ));
    }

    #[test]
    fn equal_start_and_end_hours_are_rejected() {
        assert!(parse("WHEN BETWEEN '09:00' AND '09:00'").is_err());
    }

    #[test]
    fn out_of_range_hours_are_rejected() {
        assert!(parse("WHEN BETWEEN '09:00' AND '25:00'").is_err());
        assert!(parse("WHEN BETWEEN '24:00' AND '17:00'").is_err());
        assert!(parse("WHEN BETWEEN '09:70' AND '17:00'").is_err());
    }

    #[test]
    fn malformed_temporal_clause_is_an_error_not_a_silent_skip() {
        assert!(parse("WHEN BETWEEN '09:00' TO '17:00'").is_err());
        assert!(parse("WHEN AT '09:00'").is_err());
        assert!(parse("WHEN BETWEEN '09:00' AND '17:00' ON MONDAYS").is_err());
    }

    #[test]
    fn require_mfa_and_device_trust() {
        assert_eq!(
            parse("REQUIRE MFA REQUIRE DEVICE_TRUST").expect("parses"),
            vec![
                GrantCondition::RequireMfa,
                GrantCondition::RequireDeviceTrust
            ]
        );
    }

    #[test]
    fn require_ip_accepts_a_cidr_list() {
        let conditions = parse("REQUIRE IP IN ('10.0.0.0/8', '192.168.0.0/16')").expect("parses");
        assert_eq!(
            conditions,
            vec![GrantCondition::RequireIp {
                allowed_cidrs: vec!["10.0.0.0/8".into(), "192.168.0.0/16".into()],
            }]
        );
    }

    #[test]
    fn require_ip_rejects_an_invalid_cidr() {
        assert!(parse("REQUIRE IP IN ('10.0.0.0/64')").is_err());
        assert!(parse("REQUIRE IP IN ('not-an-address')").is_err());
        assert!(parse("REQUIRE IP IN ()").is_err());
        assert!(parse("REQUIRE IP IN ('10.0.0.0/8'").is_err());
    }

    #[test]
    fn step_up_takes_an_optional_interval() {
        assert_eq!(
            parse("REQUIRE STEP_UP 300").expect("parses"),
            vec![GrantCondition::StepUpAuth { max_age_secs: 300 }]
        );
        assert_eq!(
            parse("REQUIRE STEP_UP EXPIRES 99").expect("parses"),
            vec![GrantCondition::StepUpAuth {
                max_age_secs: DEFAULT_STEP_UP_SECS
            }]
        );
        assert_eq!(
            parse("REQUIRE STEP_UP").expect("parses"),
            vec![GrantCondition::StepUpAuth {
                max_age_secs: DEFAULT_STEP_UP_SECS
            }]
        );
    }

    #[test]
    fn unknown_requirement_is_rejected() {
        assert!(parse("REQUIRE TELEPATHY").is_err());
        assert!(parse("REQUIRE").is_err());
    }

    #[test]
    fn expiry_clauses_are_skipped_without_producing_conditions() {
        let conditions =
            parse("EXPIRES 1900000000 GRACE PERIOD 7d ON EXPIRE REVOKE ALL").expect("parses");
        assert!(conditions.is_empty());
    }

    #[test]
    fn conditions_and_expiry_clauses_interleave() {
        let conditions = parse(
            "EXPIRES 1900000000 REQUIRE MFA GRACE PERIOD 7d \
             WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS",
        )
        .expect("parses");
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], GrantCondition::RequireMfa);
    }
}
