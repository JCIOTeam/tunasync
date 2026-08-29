use ratatui::backend::TestBackend;
use ratatui::Terminal;
use tunasync_protocol::{CmdVerb, MirrorStatus, SyncStatus};

use super::*;

fn job(name: &str, worker: &str, status: SyncStatus) -> MirrorStatus {
    MirrorStatus {
        name: name.to_owned(),
        worker: worker.to_owned(),
        status,
        ..MirrorStatus::default()
    }
}

fn client(base_url: &str, api_token: &str) -> Client {
    Client {
        base_url: base_url.to_owned(),
        http: reqwest::Client::new(),
        api_token: api_token.to_owned(),
    }
}

#[test]
fn combines_worker_and_status_filters() {
    let mut app = App::default();
    app.replace_jobs(vec![
        job("debian", "edge-a", SyncStatus::Success),
        job("ubuntu", "edge-a", SyncStatus::Failed),
        job("alpine", "edge-b", SyncStatus::Failed),
    ]);
    app.worker_filter = Some("edge-a".to_owned());
    app.status_filter = Some(SyncStatus::Failed);
    let visible = app.visible_jobs();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].name, "ubuntu");
}

#[test]
fn refresh_keeps_selection_by_worker_and_mirror() {
    let mut app = App::default();
    app.replace_jobs(vec![
        job("debian", "edge-a", SyncStatus::Success),
        job("ubuntu", "edge-b", SyncStatus::Syncing),
    ]);
    app.select_next();
    app.replace_jobs(vec![
        job("ubuntu", "edge-b", SyncStatus::Success),
        job("debian", "edge-a", SyncStatus::Success),
    ]);
    let selected = app.selected_job().expect("selected job");
    assert_eq!(
        (selected.worker.as_str(), selected.name.as_str()),
        ("edge-b", "ubuntu")
    );
}

#[test]
fn removing_selected_job_falls_back_without_panicking() {
    let mut app = App::default();
    app.replace_jobs(vec![
        job("debian", "edge-a", SyncStatus::Success),
        job("ubuntu", "edge-b", SyncStatus::Syncing),
    ]);
    app.select_next();
    app.replace_jobs(vec![job("debian", "edge-a", SyncStatus::Success)]);
    assert_eq!(
        app.selected_job().expect("fallback selection").name,
        "debian"
    );
    app.replace_jobs(Vec::new());
    assert!(app.selected_job().is_none());
}

#[test]
fn selected_action_targets_exact_worker_and_mirror() {
    let mut app = App::default();
    app.replace_jobs(vec![job("debian", "edge-a", SyncStatus::Failed)]);
    let cmd = app
        .command_for_selected(CmdVerb::Restart)
        .expect("selected command");
    assert_eq!(cmd.cmd, CmdVerb::Restart);
    assert_eq!(cmd.mirror_id, "debian");
    assert_eq!(cmd.worker_id, "edge-a");
    assert!(cmd.args.is_empty());
    assert!(cmd.options.is_empty());
}

#[test]
fn missing_worker_resets_filter_to_all() {
    let mut app = App::default();
    app.replace_jobs(vec![job("debian", "edge-a", SyncStatus::Success)]);
    app.worker_filter = Some("edge-a".to_owned());
    app.replace_jobs(vec![job("ubuntu", "edge-b", SyncStatus::Success)]);
    assert_eq!(app.worker_filter, None);
    assert_eq!(app.visible_jobs().len(), 1);
}

#[test]
fn partial_snapshot_retains_previous_rows_for_unavailable_worker() {
    let mut app = App::default();
    app.replace_jobs(vec![
        job("debian", "edge-a", SyncStatus::Success),
        job("ubuntu", "edge-b", SyncStatus::Success),
    ]);
    app.apply_snapshot(Snapshot {
        workers: vec!["edge-a".to_owned(), "edge-b".to_owned()],
        jobs: vec![job("debian", "edge-a", SyncStatus::Syncing)],
        unavailable_workers: vec!["edge-b".to_owned()],
    });
    assert_eq!(app.jobs.len(), 2);
    assert_eq!(
        app.jobs
            .iter()
            .find(|mirror| mirror.worker == "edge-b")
            .expect("retained worker row")
            .name,
        "ubuntu"
    );
}

#[test]
fn api_token_requires_https_for_remote_manager() {
    assert!(
        validate_transport(&client("http://manager.example.org:14242", "secret"), false).is_err()
    );
    assert!(validate_transport(&client("http://127.0.0.1:14242", "secret"), false).is_ok());
    assert!(validate_transport(&client("http://[::1]:14242", "secret"), false).is_ok());
    assert!(validate_transport(&client("https://manager.example.org", "secret"), false).is_ok());
    assert!(validate_transport(&client("http://manager.example.org:14242", ""), false).is_ok());
}

#[test]
fn error_summary_redacts_token_manager_credentials_and_control_characters() {
    let client = client(
        "https://alice:secret@manager.example.org?token=x",
        "bearer-secret",
    );
    let error = anyhow::anyhow!(
        "request to {} failed\nAuthorization: Bearer {}",
        client.base_url,
        client.api_token
    );
    let summary = sanitized_error(&client, &error);
    assert!(!summary.contains("alice"));
    assert!(!summary.contains("secret"));
    assert!(!summary.contains("token=x"));
    assert!(!summary.contains('\n'));
    assert!(summary.contains("[redacted]"));
}

#[test]
fn error_summary_truncates_unicode_on_grapheme_boundaries() {
    let client = client("https://manager.example.org", "");
    let error = anyhow::anyhow!("{}", "👩‍💻".repeat(300));
    let summary = sanitized_error(&client, &error);
    assert_eq!(summary.graphemes(true).count(), 241);
    assert!(summary.ends_with('…'));
}

#[test]
fn redacts_credentials_query_and_fragment_from_urls() {
    assert_eq!(
        redact_url("rsync://alice:secret@example.org/module?token=x#part"),
        "rsync://example.org/module"
    );
    assert_eq!(
        redact_url("broken://alice:secret@example.org/path?token=x"),
        "broken://example.org/path"
    );
}

#[test]
fn narrow_dashboard_render_does_not_expose_upstream_credentials() {
    let mut app = App::default();
    let mut mirror = job("debian", "edge-a", SyncStatus::Failed);
    mirror.upstream = "rsync://alice:secret@example.org/module?token=x".to_owned();
    mirror.error_msg =
        "\x1b]52;c;payload\x07 rsync://bob:secret@example.org/private?key=x".to_owned();
    mirror.name = "debian\x1b[2J https://carol:secret@example.org?token=name".to_owned();
    mirror.worker = "https://dave:secret@example.org?token=worker".to_owned();
    mirror.size = "https://erin:secret@example.org?token=size".to_owned();
    app.replace_jobs(vec![mirror]);
    let backend = TestBackend::new(48, 18);
    let mut terminal = Terminal::new(backend).expect("test terminal");
    terminal
        .draw(|frame| render(frame, &mut app, "https://manager.example.org"))
        .expect("render dashboard");
    let rendered =
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .fold(String::new(), |mut output, cell| {
                output.push_str(cell.symbol());
                output
            });
    assert!(!rendered.contains("alice"));
    assert!(!rendered.contains("secret"));
    assert!(!rendered.contains("token=x"));
    assert!(!rendered.contains('\x1b'));
    assert!(!rendered.contains('\x07'));
    assert!(!rendered.contains("bob"));
    assert!(!rendered.contains("carol"));
    assert!(!rendered.contains("dave"));
    assert!(!rendered.contains("erin"));
    assert!(!rendered.contains("key=x"));
}
