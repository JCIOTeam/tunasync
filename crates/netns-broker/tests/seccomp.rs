#[cfg(target_os = "linux")]
#[test]
fn inherited_seccomp_denies_setsid_and_setpgid_in_unprivileged_subprocess() {
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_tunasync-netns-broker"))
        .arg("--seccomp-test-helper")
        .status()
        .expect("run broker seccomp test helper");
    assert!(status.success(), "seccomp test helper failed: {status}");
}
