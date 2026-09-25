// SPDX-License-Identifier: BUSL-1.1

//! Read facts back from the server log a crash test captured.
//!
//! The harness appends every boot's output to one file and marks the start of
//! each boot. A crash test proves what a boot did from the lines it logged.

/// The server output of boot `n`, from its harness marker to the next one.
pub fn boot_section(log: &str, n: u32) -> String {
    let marker = format!("=== crash harness boot {n} (pid");
    let Some(start) = log.find(&marker) else {
        return String::new();
    };
    let rest = &log[start..];
    let next = format!("=== crash harness boot {} (pid", n + 1);
    match rest.find(&next) {
        Some(end) => rest[..end].to_string(),
        None => rest.to_string(),
    }
}

/// Whether two read values are equal: by value when both are numbers, by
/// text otherwise.
pub fn same_value(read: &str, expected: &str) -> bool {
    match (read.parse::<f64>(), expected.parse::<f64>()) {
        (Ok(a), Ok(b)) => a == b,
        _ => read == expected,
    }
}

/// The numeric `field` of every log line carrying `message`.
pub fn log_field(log: &str, message: &str, field: &str) -> Vec<u64> {
    let key = format!("{field}=");
    strip_ansi(log)
        .lines()
        .filter(|line| line.contains(message))
        .filter_map(|line| {
            let rest = line.split_once(key.as_str())?.1;
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .collect()
}

/// The number of log lines carrying any of `messages`.
pub fn count_lines(log: &str, messages: &[&str]) -> usize {
    strip_ansi(log)
        .lines()
        .filter(|line| messages.iter().any(|message| line.contains(message)))
        .count()
}

/// `text` without terminal colour escape sequences.
pub fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_fields_are_read_through_colour_codes() {
        let log = "INFO KV checkpoint published \u{1b}[3mapplied_ranges\u{1b}[0m\u{1b}[2m=\u{1b}[0m2\n\
                   INFO KV checkpoint published applied_ranges=0\n";
        assert_eq!(
            log_field(log, "KV checkpoint published", "applied_ranges"),
            vec![2, 0]
        );
        assert_eq!(count_lines(log, &["published", "absent"]), 2);
        assert_eq!(strip_ansi("\u{1b}[3ma\u{1b}[0m"), "a");
        assert!(same_value("8.0", "8"));
        assert!(!same_value("8.5", "8"));
        assert!(same_value("a", "a"));
        let booted = "=== crash harness boot 1 (pid 1) ===\nfirst-line\n\
                      === crash harness boot 2 (pid 2) ===\nsecond-line\n";
        assert!(boot_section(booted, 2).contains("second-line"));
        assert!(!boot_section(booted, 2).contains("first-line"));
        assert!(boot_section(booted, 1).contains("first-line"));
        assert!(!boot_section(booted, 1).contains("second-line"));
    }
}
