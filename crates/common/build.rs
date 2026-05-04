//! Build script — inject git commit hash into the binary as a compile-time
//! constant so `tunasync --version` and `tunasynctl --version` can display it.

use std::process::Command;

fn main() {
    let git_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=TUNASYNC_GIT_SHA={git_sha}");
}
