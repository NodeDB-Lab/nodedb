// SPDX-License-Identifier: BUSL-1.1

//! The splitter against PostgreSQL string and comment syntax. Unit cells for
//! `split_sql_statements`.

use super::sql_split::split_sql_statements;

#[test]
fn an_escape_string_hides_an_escaped_quote_and_its_semicolon() {
    assert_eq!(
        split_sql_statements("SELECT E'a\\';b'; SELECT 2"),
        vec!["SELECT E'a\\';b'", "SELECT 2"]
    );
}

#[test]
fn a_lowercase_escape_prefix_opens_an_escape_string() {
    assert_eq!(
        split_sql_statements("SELECT e'a\\';b'; SELECT 2"),
        vec!["SELECT e'a\\';b'", "SELECT 2"]
    );
}

#[test]
fn an_escape_prefix_after_a_parenthesis_opens_an_escape_string() {
    assert_eq!(
        split_sql_statements("SELECT (E'a\\';b'); SELECT 2"),
        vec!["SELECT (E'a\\';b')", "SELECT 2"]
    );
}

#[test]
fn an_escaped_backslash_before_the_closing_quote_closes_the_string() {
    assert_eq!(
        split_sql_statements("SELECT E'a\\\\'; SELECT 2"),
        vec!["SELECT E'a\\\\'", "SELECT 2"]
    );
}

#[test]
fn a_doubled_quote_inside_an_escape_string_stays_inside() {
    assert_eq!(
        split_sql_statements("SELECT E'a''b;c'; SELECT 2"),
        vec!["SELECT E'a''b;c'", "SELECT 2"]
    );
}

#[test]
fn a_plain_string_has_no_backslash_escape() {
    assert_eq!(
        split_sql_statements("SELECT 'a\\'; SELECT 2"),
        vec!["SELECT 'a\\'", "SELECT 2"]
    );
}

#[test]
fn an_identifier_ending_in_e_opens_no_escape_string() {
    assert_eq!(
        split_sql_statements("SELECT ae'a\\'; SELECT 2"),
        vec!["SELECT ae'a\\'", "SELECT 2"]
    );
}

#[test]
fn a_tagged_dollar_quote_hides_its_semicolon() {
    assert_eq!(
        split_sql_statements("SELECT $t$a;b$t$; SELECT 2"),
        vec!["SELECT $t$a;b$t$", "SELECT 2"]
    );
}

#[test]
fn a_nested_block_comment_hides_its_semicolon() {
    assert_eq!(
        split_sql_statements("SELECT /* a /* b */ ; */ 1; SELECT 2"),
        vec!["SELECT /* a /* b */ ; */ 1", "SELECT 2"]
    );
}

#[test]
fn a_comment_open_followed_by_a_slash_stays_open() {
    assert_eq!(
        split_sql_statements("SELECT /*/ ; */ 1; SELECT 2"),
        vec!["SELECT /*/ ; */ 1", "SELECT 2"]
    );
}

#[test]
fn a_closed_comment_does_not_hide_the_next_semicolon() {
    assert_eq!(
        split_sql_statements("SELECT 1 /* a */; SELECT 2"),
        vec!["SELECT 1 /* a */", "SELECT 2"]
    );
}

#[test]
fn a_line_comment_marker_inside_a_dollar_quote_is_text() {
    assert_eq!(
        split_sql_statements("SELECT $$-- x$$; SELECT 2"),
        vec!["SELECT $$-- x$$", "SELECT 2"]
    );
}

#[test]
fn a_block_comment_marker_inside_a_dollar_quote_is_text() {
    assert_eq!(
        split_sql_statements("SELECT $$/* x$$; SELECT 2"),
        vec!["SELECT $$/* x$$", "SELECT 2"]
    );
}

#[test]
fn a_dollar_quote_closes_only_on_its_own_tag() {
    assert_eq!(
        split_sql_statements("SELECT $a$ $b$; $b$ $a$; SELECT 2"),
        vec!["SELECT $a$ $b$; $b$ $a$", "SELECT 2"]
    );
}

#[test]
fn a_parameter_and_an_embedded_dollar_open_no_dollar_quote() {
    assert_eq!(
        split_sql_statements("SELECT $1; SELECT a$b$c; SELECT 3"),
        vec!["SELECT $1", "SELECT a$b$c", "SELECT 3"]
    );
}

#[test]
fn a_second_backslash_pair_inside_an_escape_string_stays_inside() {
    assert_eq!(
        split_sql_statements("SELECT E'a\\\\\\';b'; SELECT 2"),
        vec!["SELECT E'a\\\\\\';b'", "SELECT 2"]
    );
}
