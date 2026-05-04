//! Build script — inject version string as a compile-time constant so
//! `tunasync --version` and `tunasynctl --version` can display it.

use std::process::Command;

fn main() {
    let pkg_version = env!("CARGO_PKG_VERSION");
    let build_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // Git short SHA — only available when building from a git clone.
    let git_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() && !o.stdout.is_empty() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        });

    let version = match git_sha {
        Some(sha) => format!("{pkg_version}, build {build_date}, git {sha}"),
        None => format!("{pkg_version}, build {build_date}"),
    };

    println!("cargo:rustc-env=TUNASYNC_VERSION={version}");
}
