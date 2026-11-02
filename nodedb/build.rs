// SPDX-License-Identifier: BUSL-1.1

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Invalidate when the current commit changes: when HEAD moves to
    // another branch, and when the branch HEAD names moves.
    for watched in git_head_paths() {
        println!("cargo:rerun-if-changed={watched}");
    }

    // --- git commit ---
    let git_commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=NODEDB_GIT_COMMIT={git_commit}");

    // --- build date ---
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs();

    let date = civil_from_days(secs / 86400);
    println!("cargo:rustc-env=NODEDB_BUILD_DATE={date}");

    // --- build profile ---
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    println!("cargo:rustc-env=NODEDB_BUILD_PROFILE={profile}");

    // --- rust version ---
    let rust_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=NODEDB_RUST_VERSION={rust_version}");
}

/// The files whose change moves the current commit: the HEAD file, and the
/// ref file and packed refs of the branch it names, each one that exists.
/// Empty outside a git checkout.
fn git_head_paths() -> Vec<String> {
    let git = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
    };
    let mut paths = Vec::new();
    let Some(head) = git(&["rev-parse", "--path-format=absolute", "--git-path", "HEAD"]) else {
        return paths;
    };
    paths.push(head);
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        for name in [branch.as_str(), "packed-refs"] {
            if let Some(path) = git(&["rev-parse", "--path-format=absolute", "--git-path", name]) {
                paths.push(path);
            }
        }
    }
    // Cargo reruns on every build for a watched path that does not exist.
    paths.retain(|path| std::path::Path::new(path).exists());
    paths
}

/// Howard Hinnant's civil_from_days algorithm.
/// Converts a day count since the Unix epoch (1970-01-01) to a `YYYY-MM-DD` string.
fn civil_from_days(z: u64) -> String {
    // Shift epoch from 1970-01-01 to 0000-03-01 for the algorithm.
    let z = z as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // year of era [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month prime [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if m <= 2 { y + 1 } else { y };

    format!("{:04}-{:02}-{:02}", y, m, d)
}
