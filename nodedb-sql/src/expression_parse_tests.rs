// SPDX-License-Identifier: Apache-2.0

//! Standalone expression parsing consumes the complete input.

#[test]
fn standalone_expressions_accept_complete_input() {
    for text in ["7", "7 + 8", "uuid()", "LOWER('X')", "NULL", "7 /* end */"] {
        assert!(crate::parse_expr_string(text).is_ok(), "{text}");
    }
}

#[test]
fn standalone_expressions_reject_trailing_tokens() {
    for text in ["7 garbage", "7;", "uuid() uuid()", "7, 8", "7; SELECT 8"] {
        assert!(
            matches!(
                crate::parse_expr_string(text),
                Err(crate::SqlError::Parse { .. })
            ),
            "{text}"
        );
    }
}
