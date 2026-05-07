//! Integration tests for the manager server and DB adapters.
//!
//! Each test suite is parameterised over `redb`, `sqlite`, and `redis` backends
//! to ensure they behave identically. Redis tests auto-skip when
//! `TUNASYNC_TEST_REDIS_URL` is not set.

use std::sync::Arc;

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
        url: format!("http://localhost:6000"),
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
        scheduled: zero_time(),
        upstream: "rsync://example.com/".into(),
        size: "1.2T".into(),
        error_msg: String::new(),
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
            redis::cmd("FLUSHDB").execute(&mut conn);
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
    let state = std::sync::Arc::new(AppState { db, http_client });
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
