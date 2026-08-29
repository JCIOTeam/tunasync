use std::process::Command;

#[test]
fn manager_check_rejects_missing_config() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing-manager.conf");
    let output = Command::new(env!("CARGO_BIN_EXE_tunasync"))
        .args(["manager", "--check", "-c"])
        .arg(&missing)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("does not exist"), "{stderr}");
}

#[test]
fn manager_check_accepts_valid_config_without_opening_database() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("manager.conf");
    std::fs::write(
        &config,
        r#"
[server]
addr = "127.0.0.1"
port = 14242

[files]
db_type = "sqlite"
db_file = "/definitely/not/opened/by/check.sqlite"
status_file = ""
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_tunasync"))
        .args(["manager", "--check", "-c"])
        .arg(&config)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!std::path::Path::new("/definitely/not/opened/by/check.sqlite").exists());
}

#[test]
fn manager_check_rejects_omitted_database_type() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("manager.conf");
    std::fs::write(
        &config,
        r#"
[files]
db_file = "/var/lib/tunasync/tunasync.db"
"#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_tunasync"))
        .args(["manager", "--check", "-c"])
        .arg(&config)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("db_type must be set explicitly"),
        "{stderr}"
    );
}
