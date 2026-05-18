//! Integration tests for the manager server and DB adapters.
//!
//! Each test suite is parameterised over `redb`, `sqlite`, and `redis` backends
//! to ensure they behave identically. Redis tests auto-skip when
//! `TUNASYNC_TEST_REDIS_URL` is not set.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use serde_json::Value;
use tower::ServiceExt;
use tunasync_protocol::{zero_time, MirrorStatus, SyncStatus, WorkerStatus};

use tunasync_manager::db::{open as open_db, DbAdapter};
use tunasync_manager::server::{build_router, AppState};

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

fn tmp_path(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("tunasync-rs-tests");
    std::fs::create_dir_all(&dir).unwrap();
    // Include thread ID to avoid collisions when tests run in parallel.
    let tid = format!("{:?}", std::thread::current().id())
        .replace("ThreadId(", "")
        .replace(')', "");
    dir.join(format!("test-{suffix}-{}-{tid}.db", std::process::id()))
}

fn sample_worker(id: &str) -> WorkerStatus {
    WorkerStatus {
        id: id.into(),
        url: "http://localhost:6000".to_string(),
        token: "tok".into(),
        last_online: zero_time(),
        last_register: zero_time(),
    }
}

fn sample_status(mirror: &str, worker: &str) -> MirrorStatus {
    MirrorStatus {
        name: mirror.into(),
        worker: worker.into(),
        is_master: true,
        status: SyncStatus::Success,
        last_update: Utc::now(),
        last_started: Utc::now(),
        last_ended: Utc::now(),
        upstream: "rsync://example.com/".into(),
        size: "1.2T".into(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// DB adapter tests (redb + sqlite)
// ---------------------------------------------------------------------------

macro_rules! db_tests {
    ($name:ident, $db_type:expr) => {
        mod $name {
            use super::*;

            fn open() -> Box<dyn DbAdapter> {
                let path = tmp_path(concat!(stringify!($name)));
                open_db($db_type, &path).expect("open db")
            }

            #[test]
            fn worker_crud() {
                let db = open();
                let w = sample_worker("worker-1");

                // create
                let created = db.create_worker(w.clone()).unwrap();
                assert_eq!(created.id, "worker-1");

                // get
                let got = db.get_worker("worker-1").unwrap();
                assert_eq!(got.id, "worker-1");

                // list
                let list = db.list_workers().unwrap();
                assert_eq!(list.len(), 1);

                // refresh
                let refreshed = db.refresh_worker("worker-1").unwrap();
                assert!(refreshed.last_online > zero_time());

                // delete
                db.delete_worker("worker-1").unwrap();
                assert!(db.get_worker("worker-1").is_err());

                // double-delete should error
                assert!(db.delete_worker("worker-1").is_err());
            }

            #[test]
            fn mirror_status_crud() {
                let db = open();
                db.create_worker(sample_worker("w1")).unwrap();
                let s = sample_status("ubuntu", "w1");

                let stored = db.update_mirror_status("w1", "ubuntu", s.clone()).unwrap();
                assert_eq!(stored.name, "ubuntu");
                assert_eq!(stored.size, "1.2T");

                let got = db.get_mirror_status("w1", "ubuntu").unwrap();
                assert_eq!(got.status, SyncStatus::Success);

                // list by worker
                db.update_mirror_status("w1", "debian", sample_status("debian", "w1"))
                    .unwrap();
                let list = db.list_mirror_status("w1").unwrap();
                assert_eq!(list.len(), 2);

                // list all
                db.create_worker(sample_worker("w2")).unwrap();
                db.update_mirror_status("w2", "fedora", sample_status("fedora", "w2"))
                    .unwrap();
                let all = db.list_all_mirror_status().unwrap();
                assert_eq!(all.len(), 3);
            }

            #[test]
            fn flush_disabled() {
                let db = open();
                db.create_worker(sample_worker("w1")).unwrap();

                let mut s = sample_status("ubuntu", "w1");
                s.status = SyncStatus::Disabled;
                db.update_mirror_status("w1", "ubuntu", s).unwrap();

                let s2 = sample_status("debian", "w1");
                db.update_mirror_status("w1", "debian", s2).unwrap();

                db.flush_disabled_jobs().unwrap();

                let all = db.list_all_mirror_status().unwrap();
                assert_eq!(all.len(), 1);
                assert_eq!(all[0].name, "debian");
            }
        }
    };
}

db_tests!(redb_db, "redb");
db_tests!(sqlite_db, "sqlite");

// ---------------------------------------------------------------------------
// Redis DB tests — auto-skip when TUNASYNC_TEST_REDIS_URL is not set
// ---------------------------------------------------------------------------

mod redis_db {
    use super::*;

    fn redis_url() -> Option<String> {
        std::env::var("TUNASYNC_TEST_REDIS_URL").ok()
    }

    fn open() -> Option<Box<dyn DbAdapter>> {
        let url = redis_url()?;
        let path = std::path::PathBuf::from(&url);
        open_db("redis", &path).ok()
    }

    /// Flush test data before each test.
    fn flush_test_db() {
        if let Some(url) = redis_url() {
            let client = redis::Client::open(url.as_str()).unwrap();
            let mut conn = client.get_connection().unwrap();
            redis::cmd("FLUSHDB").exec(&mut conn).unwrap();
        }
    }

    #[test]
    fn worker_crud() {
        let Some(db) = open() else {
            eprintln!("SKIP: TUNASYNC_TEST_REDIS_URL not set");
            return;
        };
        flush_test_db();

        let w = sample_worker("worker-1");
        let created = db.create_worker(w.clone()).unwrap();
        assert_eq!(created.id, "worker-1");

        let got = db.get_worker("worker-1").unwrap();
        assert_eq!(got.id, "worker-1");

        let list = db.list_workers().unwrap();
        assert_eq!(list.len(), 1);

        let refreshed = db.refresh_worker("worker-1").unwrap();
        assert!(refreshed.last_online > zero_time());

        db.delete_worker("worker-1").unwrap();
        assert!(db.get_worker("worker-1").is_err());
        assert!(db.delete_worker("worker-1").is_err());
    }

    #[test]
    fn mirror_status_crud() {
        let Some(db) = open() else {
            eprintln!("SKIP: TUNASYNC_TEST_REDIS_URL not set");
            return;
        };
        flush_test_db();

        db.create_worker(sample_worker("w1")).unwrap();
        let s = sample_status("ubuntu", "w1");
        db.update_mirror_status("w1", "ubuntu", s).unwrap();

        let got = db.get_mirror_status("w1", "ubuntu").unwrap();
        assert_eq!(got.status, SyncStatus::Success);

        db.update_mirror_status("w1", "debian", sample_status("debian", "w1"))
            .unwrap();
        assert_eq!(db.list_mirror_status("w1").unwrap().len(), 2);

        db.create_worker(sample_worker("w2")).unwrap();
        db.update_mirror_status("w2", "fedora", sample_status("fedora", "w2"))
            .unwrap();
        assert_eq!(db.list_all_mirror_status().unwrap().len(), 3);
    }

    #[test]
    fn flush_disabled() {
        let Some(db) = open() else {
            eprintln!("SKIP: TUNASYNC_TEST_REDIS_URL not set");
            return;
        };
        flush_test_db();

        db.create_worker(sample_worker("w1")).unwrap();
        let mut s = sample_status("ubuntu", "w1");
        s.status = SyncStatus::Disabled;
        db.update_mirror_status("w1", "ubuntu", s).unwrap();

        let s2 = sample_status("debian", "w1");
        db.update_mirror_status("w1", "debian", s2).unwrap();

        db.flush_disabled_jobs().unwrap();

        let all = db.list_all_mirror_status().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "debian");
    }
}

// ---------------------------------------------------------------------------
// HTTP server route tests
// ---------------------------------------------------------------------------

fn make_app() -> axum::Router {
    let db = open_db("sqlite", &tmp_path("server")).unwrap();
    let http_client = reqwest::Client::new();
    let state = std::sync::Arc::new(AppState {
        db,
        http_client,
        maintenance: std::sync::atomic::AtomicBool::new(false),
        notify: Default::default(),
    });
    build_router(state)
}

async fn get_json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn post_json(app: &axum::Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn delete_req(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

#[tokio::test]
async fn ping() {
    let app = make_app();
    let (status, json) = get_json(&app, "/ping").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["message"], "pong");
}

#[tokio::test]
async fn register_and_list_workers() {
    let app = make_app();

    let worker = serde_json::json!({
        "id": "worker-1",
        "url": "http://worker:6000",
        "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });

    let (status, resp) = post_json(&app, "/workers", &worker).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["id"], "worker-1");

    let (status, list) = get_json(&app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1);
    // Token must be redacted.
    assert_eq!(list[0]["token"], "REDACTED");
}

#[tokio::test]
async fn delete_worker() {
    let app = make_app();
    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });
    post_json(&app, "/workers", &worker).await;

    let (status, _) = delete_req(&app, "/workers/w1").await;
    assert_eq!(status, StatusCode::OK);

    // Second delete → 400.
    let (status, _) = delete_req(&app, "/workers/w1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn update_job_and_list() {
    let app = make_app();

    // Register worker first.
    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });
    post_json(&app, "/workers", &worker).await;

    // Post job status.
    let status_body = serde_json::json!({
        "name": "ubuntu",
        "worker": "w1",
        "is_master": true,
        "status": "syncing",
        "last_update": "0001-01-01T00:00:00Z",
        "last_started": "0001-01-01T00:00:00Z",
        "last_ended": "0001-01-01T00:00:00Z",
        "next_schedule": "0001-01-01T00:00:00Z",
        "upstream": "rsync://archive.ubuntu.com/ubuntu/",
        "size": "",
        "error_msg": ""
    });
    let (status, resp) = post_json(&app, "/workers/w1/jobs/ubuntu", &status_body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["name"], "ubuntu");

    // List all jobs.
    let (status, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(jobs.as_array().unwrap().len(), 1);

    // List worker jobs.
    let (status, jobs) = get_json(&app, "/workers/w1/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(jobs.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn job_status_timestamp_merge() {
    let app = make_app();

    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });
    post_json(&app, "/workers", &worker).await;

    let zero = "0001-01-01T00:00:00Z";

    // First: pre-syncing — last_started should be set.
    let pre_sync = serde_json::json!({
        "name": "ubuntu", "worker": "w1", "is_master": true,
        "status": "pre-syncing",
        "last_update": zero, "last_started": zero,
        "last_ended": zero, "next_schedule": zero,
        "upstream": "", "size": "", "error_msg": ""
    });
    let (_, r) = post_json(&app, "/workers/w1/jobs/ubuntu", &pre_sync).await;
    assert_ne!(
        r["last_started"].as_str().unwrap(),
        zero,
        "last_started should be set on pre-syncing"
    );

    // Second: success — last_update and last_ended should be set.
    let success = serde_json::json!({
        "name": "ubuntu", "worker": "w1", "is_master": true,
        "status": "success",
        "last_update": zero, "last_started": zero,
        "last_ended": zero, "next_schedule": zero,
        "upstream": "", "size": "1.2T", "error_msg": ""
    });
    let (_, r) = post_json(&app, "/workers/w1/jobs/ubuntu", &success).await;
    assert_ne!(
        r["last_update"].as_str().unwrap(),
        zero,
        "last_update on success"
    );
    assert_ne!(
        r["last_ended"].as_str().unwrap(),
        zero,
        "last_ended on success"
    );
}

#[tokio::test]
async fn flush_disabled() {
    let app = make_app();

    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });
    post_json(&app, "/workers", &worker).await;

    let zero = "0001-01-01T00:00:00Z";
    let disabled = serde_json::json!({
        "name": "ubuntu", "worker": "w1", "is_master": true,
        "status": "disabled",
        "last_update": zero, "last_started": zero,
        "last_ended": zero, "next_schedule": zero,
        "upstream": "", "size": "", "error_msg": ""
    });
    post_json(&app, "/workers/w1/jobs/ubuntu", &disabled).await;

    let (status, _) = delete_req(&app, "/jobs/disabled").await;
    assert_eq!(status, StatusCode::OK);

    let (_, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(jobs.as_array().unwrap().len(), 0);
}

// ── helpers ────────────────────────────────────────────────────────────────

async fn get_text(app: &axum::Router, path: &str) -> (StatusCode, String) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

async fn setup_worker_and_mirror(app: &axum::Router, worker_id: &str, mirror_name: &str) {
    let zero = "0001-01-01T00:00:00Z";
    let worker = serde_json::json!({
        "id": worker_id,
        "url": format!("http://{worker_id}:6000"),
        "token": "",
        "last_online": zero,
        "last_register": zero,
    });
    post_json(app, "/workers", &worker).await;

    let job = serde_json::json!({
        "name": mirror_name, "worker": worker_id, "is_master": true,
        "status": "success",
        "last_update": zero, "last_started": zero,
        "last_ended": zero, "next_schedule": zero,
        "upstream": "rsync://example.com/", "size": "1.2T", "error_msg": ""
    });
    post_json(
        app,
        &format!("/workers/{worker_id}/jobs/{mirror_name}"),
        &job,
    )
    .await;
}

// ── heartbeat ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn heartbeat_worker() {
    let app = make_app();
    let zero = "0001-01-01T00:00:00Z";
    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": zero, "last_register": zero,
    });
    post_json(&app, "/workers", &worker).await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/w1/heartbeat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Unknown worker → 400.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/nobody/heartbeat")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── update_mirror_size ─────────────────────────────────────────────────────

#[tokio::test]
async fn update_mirror_size() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    // Normal size update.
    let (status, resp) = post_json(
        &app,
        "/workers/w1/jobs/ubuntu/size",
        &serde_json::json!({ "name": "ubuntu", "size": "2T" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["size"], "2T");

    // GET /jobs should reflect new size.
    let (_, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(jobs[0]["size"], "2T");
}

#[tokio::test]
async fn update_mirror_size_unknown_does_not_overwrite() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    // Size update with "unknown" should not overwrite "1.2T".
    let (status, resp) = post_json(
        &app,
        "/workers/w1/jobs/ubuntu/size",
        &serde_json::json!({ "name": "ubuntu", "size": "unknown" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["size"], "1.2T",
        "unknown should not overwrite existing size"
    );

    // Empty size should not overwrite either.
    let (_, resp) = post_json(
        &app,
        "/workers/w1/jobs/ubuntu/size",
        &serde_json::json!({ "name": "ubuntu", "size": "" }),
    )
    .await;
    assert_eq!(
        resp["size"], "1.2T",
        "empty should not overwrite existing size"
    );
}

// ── size preservation on job status update ─────────────────────────────────

#[tokio::test]
async fn job_update_preserves_size_when_incoming_is_blank() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    let zero = "0001-01-01T00:00:00Z";

    // Post a new status with blank size — existing "1.2T" must be kept.
    let (_, resp) = post_json(
        &app,
        "/workers/w1/jobs/ubuntu",
        &serde_json::json!({
            "name": "ubuntu", "worker": "w1", "is_master": true,
            "status": "success",
            "last_update": zero, "last_started": zero,
            "last_ended": zero, "next_schedule": zero,
            "upstream": "", "size": "", "error_msg": ""
        }),
    )
    .await;
    assert_eq!(
        resp["size"], "1.2T",
        "blank incoming size should not erase existing value"
    );

    // Same for "unknown".
    let (_, resp) = post_json(
        &app,
        "/workers/w1/jobs/ubuntu",
        &serde_json::json!({
            "name": "ubuntu", "worker": "w1", "is_master": true,
            "status": "success",
            "last_update": zero, "last_started": zero,
            "last_ended": zero, "next_schedule": zero,
            "upstream": "", "size": "unknown", "error_msg": ""
        }),
    )
    .await;
    assert_eq!(
        resp["size"], "1.2T",
        "'unknown' incoming size should not erase existing value"
    );
}

// ── update_schedules_of_worker ─────────────────────────────────────────────

#[tokio::test]
async fn update_schedules() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    let next = "2099-01-01T00:00:00Z";
    let (status, resp) = post_json(
        &app,
        "/workers/w1/schedules",
        &serde_json::json!({
            "schedules": [{ "name": "ubuntu", "next_schedule": next }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Go returns `{}` on success.
    assert!(resp.is_object());

    // GET /jobs should reflect updated next_schedule.
    let (_, jobs) = get_json(&app, "/jobs").await;
    let job = &jobs[0];
    // WebMirrorStatus exposes next_schedule as text_time string.
    assert!(
        job["next_schedule"].as_str().unwrap().starts_with("2099"),
        "next_schedule should be updated: got {}",
        job["next_schedule"]
    );
}

// ── handle_client_cmd ─────────────────────────────────────────────────────
// The manager pre-updates the DB (disable → Disabled, stop → Paused) before
// forwarding to the worker. Since no real worker is listening in these tests
// the forward will fail with 500, but the DB update has already happened and
// is visible via GET /jobs.

#[tokio::test]
async fn cmd_disable_pre_updates_status() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    post_json(
        &app,
        "/cmd",
        &serde_json::json!({
            "cmd": "disable",
            "mirror_id": "ubuntu",
            "worker_id": "w1",
            "args": [],
            "options": {}
        }),
    )
    .await;
    // Forward to worker fails (no real worker), but DB was pre-updated.
    let (_, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(jobs[0]["status"], "disabled");
}

#[tokio::test]
async fn cmd_stop_pre_updates_status() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;

    post_json(
        &app,
        "/cmd",
        &serde_json::json!({
            "cmd": "stop",
            "mirror_id": "ubuntu",
            "worker_id": "w1",
            "args": [],
            "options": {}
        }),
    )
    .await;
    let (_, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(jobs[0]["status"], "paused");
}

#[tokio::test]
async fn cmd_unknown_worker_returns_400() {
    let app = make_app();

    let (status, _) = post_json(
        &app,
        "/cmd",
        &serde_json::json!({
            "cmd": "start",
            "mirror_id": "ubuntu",
            "worker_id": "nobody",
            "args": [],
            "options": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ── /metrics endpoint ──────────────────────────────────────────────────────

#[tokio::test]
async fn metrics_endpoint() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;
    // Add a second failed mirror.
    let zero = "0001-01-01T00:00:00Z";
    post_json(
        &app,
        "/workers/w1/jobs/debian",
        &serde_json::json!({
            "name": "debian", "worker": "w1", "is_master": true,
            "status": "failed",
            "last_update": zero, "last_started": zero,
            "last_ended": zero, "next_schedule": zero,
            "upstream": "rsync://ftp.debian.org/debian/",
            "size": "500G", "error_msg": "timeout"
        }),
    )
    .await;

    let (status, body) = get_text(&app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);

    // Content-type should be Prometheus text format.
    // (We check the body directly since get_text strips the header.)

    // Workers total.
    assert!(
        body.contains("tunasync_workers_total 1"),
        "workers_total: {body}"
    );

    // Mirror status codes: success=3, failed=4.
    assert!(
        body.contains("tunasync_mirror_status{mirror=\"ubuntu\",worker=\"w1\"} 3"),
        "ubuntu status: {body}"
    );
    assert!(
        body.contains("tunasync_mirror_status{mirror=\"debian\",worker=\"w1\"} 4"),
        "debian status: {body}"
    );

    // Aggregate counts.
    assert!(
        body.contains("tunasync_mirrors_total{status=\"success\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("tunasync_mirrors_total{status=\"failed\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("tunasync_mirrors_total{status=\"syncing\"} 0"),
        "{body}"
    );

    // Size bytes: 1.2T and 500G.
    assert!(
        body.contains("tunasync_mirror_size_bytes{mirror=\"ubuntu\",worker=\"w1\"}"),
        "{body}"
    );
    assert!(
        body.contains("tunasync_mirror_size_bytes{mirror=\"debian\",worker=\"w1\"}"),
        "{body}"
    );
}

// ── multi-worker same mirror ───────────────────────────────────────────────

#[tokio::test]
async fn multi_worker_same_mirror_name() {
    let app = make_app();
    setup_worker_and_mirror(&app, "w1", "ubuntu").await;
    setup_worker_and_mirror(&app, "w2", "ubuntu").await;

    // GET /jobs should show both (as WebMirrorStatus — no worker field).
    let (_, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(jobs.as_array().unwrap().len(), 2);

    // GET /jobs/ubuntu should return both MirrorStatus (with worker field).
    let (_, detail) = get_json(&app, "/jobs/ubuntu").await;
    let arr = detail.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let workers: Vec<&str> = arr.iter().map(|j| j["worker"].as_str().unwrap()).collect();
    assert!(workers.contains(&"w1"));
    assert!(workers.contains(&"w2"));
}

#[tokio::test]
async fn get_mirror_by_name() {
    let app = make_app();

    let worker = serde_json::json!({
        "id": "w1", "url": "http://w1:6000", "token": "",
        "last_online": "0001-01-01T00:00:00Z",
        "last_register": "0001-01-01T00:00:00Z"
    });
    post_json(&app, "/workers", &worker).await;

    let zero = "0001-01-01T00:00:00Z";

    // Post a failed job with error_msg.
    let failed = serde_json::json!({
        "name": "ubuntu", "worker": "w1", "is_master": true,
        "status": "failed",
        "last_update": zero, "last_started": zero,
        "last_ended": zero, "next_schedule": zero,
        "upstream": "rsync://archive.ubuntu.com/ubuntu/",
        "size": "", "error_msg": "rsync error: timeout (30) waiting for data"
    });
    let (status, _) = post_json(&app, "/workers/w1/jobs/ubuntu", &failed).await;
    assert_eq!(status, StatusCode::OK);

    // GET /jobs/ubuntu should return the mirror with error_msg.
    let (status, jobs) = get_json(&app, "/jobs/ubuntu").await;
    assert_eq!(status, StatusCode::OK);
    let arr = jobs.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "ubuntu");
    assert_eq!(arr[0]["status"], "failed");
    assert_eq!(
        arr[0]["error_msg"],
        "rsync error: timeout (30) waiting for data"
    );

    // GET /jobs/nonexistent should return empty array.
    let (status, jobs) = get_json(&app, "/jobs/nonexistent").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(jobs.as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// End-to-end mirror lifecycle test
// ---------------------------------------------------------------------------

/// Full lifecycle of one mirror: register → sync-success with traffic →
/// sync-failure × N triggering webhook threshold → recovery → stale → recovered.
///
/// The webhook is not actually sent in this test (no real HTTP server), but
/// we exercise the full state transitions and verify the stored status at each
/// step.
#[tokio::test]
async fn mirror_lifecycle_e2e() {
    let app = make_app();
    let zero = "0001-01-01T00:00:00Z";

    // ── 1. Register worker ────────────────────────────────────────────────

    let worker = serde_json::json!({
        "id": "worker-e2e",
        "url": "http://127.0.0.1:14243",
        "token": "",
        "last_online": zero,
        "last_register": zero
    });
    let (status, _) = post_json(&app, "/workers", &worker).await;
    assert_eq!(status, StatusCode::OK, "register worker");

    // ── 2. First status update — PreSyncing ───────────────────────────────

    let pre_sync = serde_json::json!({
        "name": "fedora",
        "worker": "worker-e2e",
        "is_master": true,
        "status": "pre-syncing",
        "last_update": zero,
        "last_started": zero,
        "last_ended": zero,
        "next_schedule": zero,
        "upstream": "rsync://dl.fedoraproject.org/fedora-linux-releases/",
        "size": "",
        "error_msg": ""
    });
    let (status, _) = post_json(&app, "/workers/worker-e2e/jobs/fedora", &pre_sync).await;
    assert_eq!(status, StatusCode::OK, "pre-syncing update");

    // ── 3. Successful sync with transferred bytes ─────────────────────────

    let now = Utc::now().to_rfc3339();
    let success = serde_json::json!({
        "name": "fedora",
        "worker": "worker-e2e",
        "is_master": true,
        "status": "success",
        "last_update": now,
        "last_started": now,
        "last_ended": now,
        "next_schedule": now,
        "upstream": "rsync://dl.fedoraproject.org/fedora-linux-releases/",
        "size": "2.1T",
        "error_msg": "",
        "last_transferred_bytes": 1_073_741_824u64
    });
    let (status, body) = post_json(&app, "/workers/worker-e2e/jobs/fedora", &success).await;
    assert_eq!(status, StatusCode::OK, "success update");
    assert_eq!(body["status"], "success");
    assert_eq!(body["size"], "2.1T");
    // last_transferred_bytes is stored; verify it's reflected on subsequent GET.
    let (status, arr) = get_json(&app, "/workers/worker-e2e/jobs").await;
    assert_eq!(status, StatusCode::OK, "list worker jobs after success");
    let fedora = arr
        .as_array()
        .unwrap()
        .iter()
        .find(|j| j["name"] == "fedora");
    assert!(
        fedora.is_some(),
        "fedora must appear in worker jobs after success"
    );

    // ── 4. Three consecutive failures → consecutive_failures increments ───

    for i in 1u32..=3 {
        let fail = serde_json::json!({
            "name": "fedora",
            "worker": "worker-e2e",
            "is_master": true,
            "status": "failed",
            "last_update": now,
            "last_started": now,
            "last_ended": now,
            "next_schedule": now,
            "upstream": "rsync://dl.fedoraproject.org/fedora-linux-releases/",
            "size": "",
            "error_msg": format!("simulated failure {i}")
        });
        let (status, body) = post_json(&app, "/workers/worker-e2e/jobs/fedora", &fail).await;
        assert_eq!(status, StatusCode::OK, "failure update {i}");
        assert_eq!(
            body["consecutive_failures"].as_u64().unwrap_or(0),
            i as u64,
            "consecutive_failures should be {i} after failure {i}"
        );
    }

    // ── 5. Recovery — consecutive_failures resets to 0 ───────────────────

    let recovery = serde_json::json!({
        "name": "fedora",
        "worker": "worker-e2e",
        "is_master": true,
        "status": "success",
        "last_update": now,
        "last_started": now,
        "last_ended": now,
        "next_schedule": now,
        "upstream": "rsync://dl.fedoraproject.org/fedora-linux-releases/",
        "size": "2.1T",
        "error_msg": ""
    });
    let (status, body) = post_json(&app, "/workers/worker-e2e/jobs/fedora", &recovery).await;
    assert_eq!(status, StatusCode::OK, "recovery update");
    // consecutive_failures resets to 0 on success. The field is omitted from
    // JSON when 0 (skip_serializing_if), so absence also means 0.
    let cf = body["consecutive_failures"].as_u64().unwrap_or(0);
    assert_eq!(
        cf, 0,
        "consecutive_failures should reset to 0 on success, got {cf}"
    );

    // ── 6. List mirror via GET /jobs ──────────────────────────────────────

    let (status, jobs) = get_json(&app, "/jobs").await;
    assert_eq!(status, StatusCode::OK);
    let arr = jobs.as_array().unwrap();
    assert!(
        arr.iter().any(|j| j["name"] == "fedora"),
        "fedora should appear in /jobs"
    );

    // ── 7. GET /jobs/fedora returns the mirror ────────────────────────────

    let (status, arr) = get_json(&app, "/jobs/fedora").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(arr.as_array().unwrap().len(), 1);
    assert_eq!(arr[0]["name"], "fedora");
    assert_eq!(arr[0]["status"], "success");

    // ── 8. Maintenance mode: enable → status → disable ───────────────────

    let (status, _) = post_json(&app, "/maintenance", &serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK, "enable maintenance");

    let (status, _) = get_json(&app, "/maintenance").await;
    assert_eq!(status, StatusCode::OK, "get maintenance status");

    let (status, _) = delete_req(&app, "/maintenance").await;
    assert_eq!(status, StatusCode::OK, "disable maintenance");
}

/// Maintenance mode is for stopping destructive *operator* actions, not for
/// freezing telemetry. Workers must continue to report status, schedules,
/// size, and heartbeats while operators are doing maintenance — otherwise
/// the UI freezes and worker state diverges from the manager's view.
///
/// Conversely, mutating operator actions (`/cmd`, `/jobs/disabled` flush,
/// worker deletion) must be blocked.
#[tokio::test]
async fn maintenance_blocks_operator_actions_not_worker_telemetry() {
    let app = make_app();
    let zero = "0001-01-01T00:00:00Z";

    // Register a worker + mirror so the endpoints have something to act on.
    let worker = serde_json::json!({
        "id": "w-maint",
        "url": "http://127.0.0.1:65500",
        "token": "",
        "last_online": zero,
        "last_register": zero
    });
    let (status, _) = post_json(&app, "/workers", &worker).await;
    assert_eq!(status, StatusCode::OK);

    let first_update = serde_json::json!({
        "name": "m1",
        "worker": "w-maint",
        "is_master": true,
        "status": "pre-syncing",
        "last_update": zero,
        "last_started": zero,
        "last_ended": zero,
        "next_schedule": zero,
        "upstream": "rsync://u/",
        "size": "",
        "error_msg": ""
    });
    let (status, _) = post_json(&app, "/workers/w-maint/jobs/m1", &first_update).await;
    assert_eq!(status, StatusCode::OK);

    // Enable maintenance.
    let (status, _) = post_json(&app, "/maintenance", &serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK, "enable maintenance");

    // ─── Worker telemetry must STILL succeed ─────────────────────────────

    // Status update.
    let now_update = serde_json::json!({
        "name": "m1",
        "worker": "w-maint",
        "is_master": true,
        "status": "success",
        "last_update": zero,
        "last_started": zero,
        "last_ended": zero,
        "next_schedule": zero,
        "upstream": "rsync://u/",
        "size": "1.2T",
        "error_msg": ""
    });
    let (status, _) = post_json(&app, "/workers/w-maint/jobs/m1", &now_update).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "worker status update must succeed during maintenance"
    );

    // Heartbeat.
    let (status, _) = post_json(&app, "/workers/w-maint/heartbeat", &serde_json::json!({})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "worker heartbeat must succeed during maintenance"
    );

    // Size update.
    let size_msg = serde_json::json!({"name": "m1", "size": "1.3T"});
    let (status, _) = post_json(&app, "/workers/w-maint/jobs/m1/size", &size_msg).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "worker size update must succeed during maintenance"
    );

    // ─── Destructive operator actions must FAIL ──────────────────────────

    // flush_disabled_jobs — newly gated.
    let (status, _) = delete_req(&app, "/jobs/disabled").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "/jobs/disabled flush must be blocked during maintenance"
    );

    // delete_worker — already gated.
    let (status, _) = delete_req(&app, "/workers/w-maint").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "DELETE /workers/:id must be blocked during maintenance"
    );

    // Disable maintenance and verify operator actions work again.
    let (status, _) = delete_req(&app, "/maintenance").await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = delete_req(&app, "/jobs/disabled").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/jobs/disabled flush must work again after maintenance disable"
    );
}
