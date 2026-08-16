#![cfg(target_os = "linux")]

/// Full namespace entry and privilege-drop smoke test. Build and install the
/// assertion helper as a root-owned ELF before running this ignored test.
#[test]
#[ignore = "requires root, ip netns, and CAP_SYS_ADMIN"]
fn privileged_setns_execution_smoke_test() {
    let uid = std::env::var("SUDO_UID").unwrap_or_else(|_| "65534".into());
    let gid = std::env::var("SUDO_GID").unwrap_or_else(|_| "65534".into());
    assert_ne!(uid, "0", "smoke target uid must be non-root");
    assert_ne!(gid, "0", "smoke target gid must be non-root");
    let helper = std::env::var("TUNASYNC_NETNS_SMOKE_HELPER")
        .expect("set TUNASYNC_NETNS_SMOKE_HELPER to the root-owned helper ELF");
    let broker = std::env::var("TUNASYNC_NETNS_BROKER")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_tunasync-netns-broker").into());
    let name = format!("tunasync-test-{}", std::process::id());
    let add = std::process::Command::new("ip")
        .args(["netns", "add", &name])
        .status()
        .expect("run ip netns add");
    assert!(add.success(), "ip netns add failed");
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::process::Command::new("ip")
                .args(["netns", "del", &self.0])
                .status();
        }
    }
    let _cleanup = Cleanup(name.clone());

    let status = std::process::Command::new(broker)
        .args([
            "--privileged-smoke-helper",
            "--smoke-namespace",
            &name,
            "--smoke-target",
            &helper,
            "--uid",
            &uid,
            "--gid",
            &gid,
        ])
        .status()
        .expect("run privileged broker smoke helper");
    assert!(
        status.success(),
        "isolated helper failed or was not reaped: {status}"
    );
}
