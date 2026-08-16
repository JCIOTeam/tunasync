#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("tunasync-netns-smoke-helper requires Linux");
}

#[cfg(target_os = "linux")]
fn main() {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::process::ExitStatusExt;

    if std::env::args().nth(1).as_deref() == Some("--descendant") {
        run_descendant_mode();
        return;
    }

    let expected_uid: u32 = required_env("EXPECTED_UID").parse().unwrap();
    let expected_gid: u32 = required_env("EXPECTED_GID").parse().unwrap();
    let expected_dev: u64 = required_env("EXPECTED_NETNS_DEV").parse().unwrap();
    let expected_ino: u64 = required_env("EXPECTED_NETNS_INO").parse().unwrap();

    // SAFETY: credential getters have no pointer arguments or preconditions.
    assert_eq!(unsafe { libc::geteuid() }, expected_uid);
    // SAFETY: credential getters have no pointer arguments or preconditions.
    assert_eq!(unsafe { libc::getegid() }, expected_gid);
    let namespace = std::fs::metadata("/proc/self/ns/net").unwrap();
    assert_eq!(
        (namespace.dev(), namespace.ino()),
        (expected_dev, expected_ino)
    );

    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    assert_status_value(&status, "NoNewPrivs", "1");
    for capability in ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"] {
        assert_status_value(&status, capability, "0000000000000000");
    }
    let listed_fds = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .parse::<i32>()
                .unwrap()
        })
        .filter(|fd| *fd >= 3)
        .collect::<Vec<_>>();
    for fd in listed_fds {
        // SAFETY: fcntl(F_GETFD) only inspects the integer descriptor and does
        // not dereference a variadic argument for this command.
        let result = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if result != -1 {
            panic!("unexpected inherited fd {fd}");
        }
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::EBADF)
        );
    }

    let child = std::process::Command::new("/proc/self/exe")
        .arg("--descendant")
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(0), "descendant failed: {child:?}");
    assert_eq!(child.signal(), None);
    println!("isolated-smoke-ok");
}

#[cfg(target_os = "linux")]
fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"))
}

#[cfg(target_os = "linux")]
fn assert_status_value(status: &str, key: &str, expected: &str) {
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{key}:")))
        .map(str::trim)
        .unwrap_or_else(|| panic!("missing {key} in /proc/self/status"));
    assert_eq!(value, expected, "unexpected {key}");
}

#[cfg(target_os = "linux")]
fn run_descendant_mode() {
    // This process is not a process-group leader. Both calls would normally
    // succeed, so EPERM proves the inherited seccomp filter blocked escape.
    // SAFETY: setsid has no pointer arguments or preconditions.
    assert_eq!(unsafe { libc::setsid() }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    );
    // SAFETY: pid and pgid zero are documented self/new-group sentinels.
    assert_eq!(unsafe { libc::setpgid(0, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    );
}
