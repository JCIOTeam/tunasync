use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tunasync_protocol::{zero_time, SyncStatus};

fn status(name: &str, state: SyncStatus) -> MirrorStatus {
    MirrorStatus {
        name: name.into(),
        worker: "w1".into(),
        status: state,
        ..Default::default()
    }
}

fn worker_status() -> WorkerStatus {
    WorkerStatus {
        id: "w1".into(),
        url: "http://127.0.0.1:6000".into(),
        token: String::new(),
        last_online: zero_time(),
        last_register: zero_time(),
    }
}

fn schedules(names: &[&str]) -> MirrorSchedules {
    MirrorSchedules {
        schedules: names
            .iter()
            .map(|name| tunasync_protocol::MirrorSchedule {
                mirror_name: (*name).into(),
                next_schedule: zero_time(),
            })
            .collect(),
    }
}

fn retained_report_count(handle: &ReportHandle) -> usize {
    handle.mailbox.lock().retained_report_count()
}

#[test]
fn one_entry_mailbox_stays_bounded_without_actor() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 1);

    for index in 0..500 {
        if index % 2 == 0 {
            assert!(handle.report_status(status(&format!("status-{index}"), SyncStatus::Success)));
        } else {
            assert!(handle.report_size(format!("size-{index}"), index.to_string()));
        }
        assert!(retained_report_count(&handle) <= 1);
    }
}

#[test]
fn retained_reports_and_schedule_rows_have_independent_caps() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    for index in 0..32 {
        assert!(handle.report_status(status(&format!("fifo-{index}"), SyncStatus::Syncing)));
    }
    assert_eq!(handle.mailbox_counts().0, 32);
    for index in 0..10_000 {
        assert!(handle.report_status(status(&format!("status-{index}"), SyncStatus::Success)));
        assert!(handle.report_size(format!("size-{index}"), index.to_string()));
    }
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: Vec::new()
    }));
    let (_, _, _, schedules) = handle.mailbox_counts();
    assert_eq!(retained_report_count(&handle), 32);
    assert!(
        schedules,
        "the separate complete schedule snapshot is retained"
    );
}

#[test]
fn requeued_retry_evicts_oldest_other_report_and_keeps_sequence_order() {
    let mut mailbox = MailboxState::new(3);
    for name in ["first", "second", "third"] {
        mailbox.enqueue_new(Report::Status(status(name, SyncStatus::Syncing)));
    }

    let retry = mailbox
        .take_next_report_for_delivery()
        .expect("first report missing");
    assert_eq!(retry.sequence, 1);
    mailbox.enqueue_new(Report::Status(status("fourth", SyncStatus::Success)));
    assert_eq!(mailbox.retained_report_count(), 3);

    mailbox.requeue_in_flight(retry);
    assert_eq!(mailbox.retained_report_count(), 3);
    assert_eq!(
        mailbox.retry.as_ref().map(|report| report.sequence),
        Some(1)
    );

    let sequences: Vec<u64> = std::iter::from_fn(|| mailbox.take_next_report())
        .map(|report| report.sequence)
        .collect();
    assert_eq!(sequences, [1, 3, 4]);
}

#[test]
fn schedule_snapshots_are_latest_wins_and_never_enter_fifo() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 128);
    assert!(handle.report_status(status("status", SyncStatus::Syncing)));
    assert!(handle.report_size("size".into(), "1T".into()));

    for index in 0..10_000 {
        assert!(handle.report_schedules(MirrorSchedules {
            schedules: vec![
                tunasync_protocol::MirrorSchedule {
                    mirror_name: format!("schedule-{index}"),
                    next_schedule: zero_time(),
                };
                128
            ],
        }));
    }

    let mailbox = handle.mailbox.lock();
    assert_eq!(mailbox.fifo.len(), 2);
    assert!(mailbox
        .fifo
        .iter()
        .all(|report| !matches!(report.report, Report::Schedules(_))));
    let queued = match &mailbox.schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("queued schedule snapshot missing"),
    };
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("latest schedule snapshot missing"),
    };
    assert!(Arc::ptr_eq(queued, latest));
    assert_eq!(Arc::strong_count(queued), 2);
    assert_eq!(queued.schedules[0].mirror_name, "schedule-9999");
}

#[test]
fn oversized_schedule_snapshot_is_rejected_without_replacing_retained_arc() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 2);
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "retained".into(),
            next_schedule: zero_time(),
        }],
    }));

    let before = {
        let mailbox = handle.mailbox.lock();
        let retained = match &mailbox.schedules {
            Some(SequencedReport {
                report: Report::Schedules(schedules),
                ..
            }) => schedules,
            _ => panic!("retained schedule snapshot missing"),
        };
        (
            mailbox.next_sequence,
            mailbox.fifo.len(),
            mailbox.statuses.len(),
            mailbox.sizes.len(),
            Arc::as_ptr(retained),
            retained.schedules[0].mirror_name.clone(),
        )
    };

    assert!(!handle.report_schedules(MirrorSchedules {
        schedules: (0..3)
            .map(|index| tunasync_protocol::MirrorSchedule {
                mirror_name: format!("oversized-{index}"),
                next_schedule: zero_time(),
            })
            .collect(),
    }));

    let mailbox = handle.mailbox.lock();
    let retained = match &mailbox.schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("retained schedule snapshot was removed"),
    };
    assert_eq!(
        before,
        (
            mailbox.next_sequence,
            mailbox.fifo.len(),
            mailbox.statuses.len(),
            mailbox.sizes.len(),
            Arc::as_ptr(retained),
            retained.schedules[0].mirror_name.clone(),
        )
    );
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("latest schedule snapshot was removed"),
    };
    assert!(Arc::ptr_eq(retained, latest));
}

#[test]
fn active_full_schedule_rejects_without_invoking_builder_or_replacing_latest() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 2);
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: (0..2)
            .map(|index| tunasync_protocol::MirrorSchedule {
                mirror_name: format!("active-{index}"),
                next_schedule: zero_time(),
            })
            .collect(),
    }));
    let active = handle
        .mailbox
        .lock()
        .take_next_report_for_delivery()
        .expect("active schedule missing");
    assert_eq!(handle.schedule_allocation_rows(), 2);

    let builds = AtomicUsize::new(0);
    assert!(!handle.report_schedules_with(2, || {
        builds.fetch_add(1, Ordering::SeqCst);
        schedules(&["rejected-0", "rejected-1"])
    }));
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    let mailbox = handle.mailbox.lock();
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("latest schedule missing"),
    };
    assert_eq!(latest.schedules[0].mirror_name, "active-0");
    drop(mailbox);
    handle.mailbox.lock().release_in_flight(&active);
}

#[test]
fn admitted_schedule_builder_runs_once_and_stores_complete_latest_snapshot() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 4);
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: (0..2)
            .map(|index| tunasync_protocol::MirrorSchedule {
                mirror_name: format!("active-{index}"),
                next_schedule: zero_time(),
            })
            .collect(),
    }));
    let active = handle
        .mailbox
        .lock()
        .take_next_report_for_delivery()
        .expect("active schedule missing");

    let builds = AtomicUsize::new(0);
    assert!(handle.report_schedules_with(2, || {
        builds.fetch_add(1, Ordering::SeqCst);
        schedules(&["latest-0", "latest-1"])
    }));
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(handle.schedule_allocation_rows(), 4);
    let mailbox = handle.mailbox.lock();
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("latest schedule missing"),
    };
    assert_eq!(
        latest
            .schedules
            .iter()
            .map(|schedule| schedule.mirror_name.as_str())
            .collect::<Vec<_>>(),
        ["latest-0", "latest-1"]
    );
    drop(mailbox);
    handle.mailbox.lock().release_in_flight(&active);
}

#[test]
fn mismatched_schedule_builder_length_does_not_replace_prior_snapshot() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 4);
    assert!(handle.report_schedules(schedules(&["retained"])));

    let before = {
        let mailbox = handle.mailbox.lock();
        let retained = match &mailbox.latest_schedules {
            Some(SequencedReport {
                report: Report::Schedules(schedules),
                ..
            }) => schedules,
            _ => panic!("latest schedule missing"),
        };
        (mailbox.next_sequence, Arc::as_ptr(retained))
    };

    assert!(!handle.report_schedules_with(2, || schedules(&["mismatch"])));

    let mailbox = handle.mailbox.lock();
    let retained = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("latest schedule was removed"),
    };
    assert_eq!(before, (mailbox.next_sequence, Arc::as_ptr(retained)));
    assert_eq!(retained.schedules[0].mirror_name, "retained");
}

#[test]
fn dequeue_merges_retry_fifo_and_latest_schedule_by_sequence() {
    let mut mailbox = MailboxState::new(32);
    mailbox.enqueue_new(Report::Status(status("first", SyncStatus::Syncing)));
    mailbox.enqueue_new(Report::Schedules(Arc::new(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "old".into(),
            next_schedule: zero_time(),
        }],
    })));
    mailbox.enqueue_new(Report::Size {
        mirror: "size".into(),
        size: "1T".into(),
    });
    mailbox.enqueue_new(Report::Schedules(Arc::new(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "new".into(),
            next_schedule: zero_time(),
        }],
    })));

    let sequences: Vec<u64> = std::iter::from_fn(|| mailbox.take_next_report())
        .map(|report| report.sequence)
        .collect();
    assert_eq!(sequences, [1, 3, 4]);
}

#[test]
fn coalesced_dequeue_is_globally_monotonic_during_overflow() {
    let mut mailbox = MailboxState::new(64);
    for index in 0..ORDINARY_CAPACITY {
        mailbox.enqueue_new(Report::Status(status(
            &format!("fifo-{index}"),
            SyncStatus::Syncing,
        )));
    }
    mailbox.enqueue_new(Report::Status(status("same", SyncStatus::Failed)));
    mailbox.enqueue_new(Report::Status(status("other", SyncStatus::Success)));
    mailbox.enqueue_new(Report::Status(status("same", SyncStatus::Success)));

    let mut previous = 0;
    while let Some(report) = mailbox.take_fifo() {
        assert!(report.sequence > previous);
        previous = report.sequence;
    }
    while let Some(report) = mailbox.take_coalesced() {
        assert!(report.sequence > previous);
        previous = report.sequence;
    }
}

#[test]
fn forget_mirror_purges_fifo_coalesced_and_schedule_rows() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, _, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    assert!(handle.report_status(status("deleted", SyncStatus::Failed)));
    for index in 0..ORDINARY_CAPACITY {
        assert!(handle.report_status(status(&format!("fill-{index}"), SyncStatus::Syncing,)));
    }
    assert!(handle.report_size("deleted".into(), "1T".into()));
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "deleted".into(),
            next_schedule: zero_time(),
        }],
    }));

    assert!(handle.forget_mirror("deleted".into()));
    let mailbox = handle.mailbox.lock();
    assert!(mailbox
        .fifo
        .iter()
        .all(|report| !report.report.belongs_to("deleted")));
    assert!(!mailbox.statuses.contains_key("deleted"));
    assert!(!mailbox.sizes.contains_key("deleted"));
    let schedules = match &mailbox.schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => schedules,
        _ => panic!("schedule report missing"),
    };
    assert!(schedules.schedules.is_empty());
}

#[test]
fn forget_filters_max_queued_schedule_in_place_without_old_payload_owner() {
    let mut mailbox = MailboxState::new(4);
    mailbox.enqueue_new(Report::Schedules(Arc::new(schedules(&[
        "deleted", "one", "two", "three",
    ]))));
    let original = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => {
            assert_eq!(Arc::strong_count(schedules), 2);
            Arc::as_ptr(schedules)
        }
        _ => panic!("latest schedule snapshot missing"),
    };

    mailbox.remove_mirror("deleted");

    let queued = match &mailbox.schedules {
        Some(SequencedReport {
            sequence: 2,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered queued schedule snapshot missing"),
    };
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            sequence: 2,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered latest schedule snapshot missing"),
    };
    assert_eq!(Arc::as_ptr(queued), original);
    assert!(Arc::ptr_eq(queued, latest));
    assert_eq!(Arc::strong_count(queued), 2);
    assert_eq!(
        queued
            .schedules
            .iter()
            .map(|schedule| schedule.mirror_name.as_str())
            .collect::<Vec<_>>(),
        ["one", "two", "three"]
    );
    assert_eq!(mailbox.schedule_allocation_rows(), 3);
}

#[test]
fn forget_filters_distinct_queued_schedule_in_place_within_combined_budget() {
    let mut mailbox = MailboxState::new(5);
    mailbox.enqueue_new(Report::Schedules(Arc::new(schedules(&[
        "active-0", "active-1",
    ]))));
    let active_report = mailbox
        .take_next_report_for_delivery()
        .expect("active schedule missing");
    mailbox.enqueue_new(Report::Schedules(Arc::new(schedules(&[
        "deleted", "latest-0", "latest-1",
    ]))));
    assert_eq!(mailbox.schedule_allocation_rows(), 5);
    let queued_ptr = match &mailbox.latest_schedules {
        Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) => Arc::as_ptr(schedules),
        _ => panic!("latest schedule snapshot missing"),
    };

    mailbox.remove_mirror("deleted");

    let active = mailbox
        .active_schedules
        .as_ref()
        .expect("active schedule reservation missing");
    let queued = match &mailbox.schedules {
        Some(SequencedReport {
            sequence: 3,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered queued schedule snapshot missing"),
    };
    let latest = match &mailbox.latest_schedules {
        Some(SequencedReport {
            sequence: 3,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered latest schedule snapshot missing"),
    };
    assert!(!Arc::ptr_eq(active, queued));
    assert_eq!(Arc::as_ptr(queued), queued_ptr);
    assert!(Arc::ptr_eq(queued, latest));
    assert_eq!(Arc::strong_count(queued), 2);
    assert_eq!(mailbox.schedule_allocation_rows(), 4);
    mailbox.release_in_flight(&active_report);
}

#[test]
fn forget_shared_active_schedule_preserves_or_drops_filtered_snapshot_by_budget() {
    let mut preserving = MailboxState::new(3);
    preserving.enqueue_new(Report::Schedules(Arc::new(schedules(&["deleted", "kept"]))));
    let active_report = preserving
        .take_next_report_for_delivery()
        .expect("active schedule missing");
    let active = preserving
        .active_schedules
        .as_ref()
        .expect("active schedule reservation missing");
    let original = Arc::as_ptr(active);
    assert!(matches!(
        &preserving.latest_schedules,
        Some(SequencedReport {
            report: Report::Schedules(latest),
            ..
        }) if Arc::ptr_eq(active, latest)
    ));

    preserving.remove_mirror("deleted");

    let active = preserving
        .active_schedules
        .as_ref()
        .expect("active schedule reservation missing");
    let queued = match &preserving.schedules {
        Some(SequencedReport {
            sequence: 2,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered queued schedule snapshot missing"),
    };
    let latest = match &preserving.latest_schedules {
        Some(SequencedReport {
            sequence: 2,
            report: Report::Schedules(schedules),
        }) => schedules,
        _ => panic!("filtered latest schedule snapshot missing"),
    };
    assert_eq!(Arc::as_ptr(active), original);
    assert!(!Arc::ptr_eq(active, queued));
    assert!(Arc::ptr_eq(queued, latest));
    assert_eq!(Arc::strong_count(active), 2);
    assert_eq!(Arc::strong_count(queued), 2);
    assert_eq!(queued.schedules.len(), 1);
    assert_eq!(queued.schedules[0].mirror_name, "kept");
    assert_eq!(preserving.schedule_allocation_rows(), 3);
    preserving.release_in_flight(&active_report);

    let mut dropping = MailboxState::new(2);
    dropping.enqueue_new(Report::Schedules(Arc::new(schedules(&["deleted", "kept"]))));
    let active_report = dropping
        .take_next_report_for_delivery()
        .expect("active schedule missing");
    dropping.remove_mirror("deleted");
    assert!(dropping.schedules.is_none());
    assert!(dropping.latest_schedules.is_none());
    assert_eq!(dropping.next_sequence, 1);
    assert_eq!(dropping.schedule_allocation_rows(), 2);
    dropping.release_in_flight(&active_report);
}

#[tokio::test]
async fn delayed_forget_preserves_same_name_readd_and_cleans_old_pending_first() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct Capture {
        old_started: mpsc::Sender<()>,
        old_gate: Semaphore,
        new_started: mpsc::Sender<()>,
        new_gate: Semaphore,
        statuses: mpsc::Sender<MirrorStatus>,
        schedules: mpsc::Sender<MirrorSchedules>,
    }

    async fn report_status(
        State(state): State<Arc<Capture>>,
        Json(status): Json<MirrorStatus>,
    ) -> StatusCode {
        if status.error_msg == "active-old" {
            let _ = state.old_started.try_send(());
            state.old_gate.acquire().await.unwrap().forget();
        } else if status.error_msg == "new" {
            let _ = state.new_started.try_send(());
            state.new_gate.acquire().await.unwrap().forget();
        }
        state.statuses.send(status).await.unwrap();
        StatusCode::OK
    }

    async fn report_schedules(
        State(state): State<Arc<Capture>>,
        Json(schedules): Json<MirrorSchedules>,
    ) -> StatusCode {
        state.schedules.send(schedules).await.unwrap();
        StatusCode::OK
    }

    let (old_started_tx, mut old_started_rx) = mpsc::channel(1);
    let (new_started_tx, mut new_started_rx) = mpsc::channel(1);
    let (status_tx, mut status_rx) = mpsc::channel(4);
    let (schedule_tx, mut schedule_rx) = mpsc::channel(4);
    let capture = Arc::new(Capture {
        old_started: old_started_tx,
        old_gate: Semaphore::new(0),
        new_started: new_started_tx,
        new_gate: Semaphore::new(0),
        statuses: status_tx,
        schedules: schedule_tx,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn({
        let capture = Arc::clone(&capture);
        async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/workers/{worker}/jobs/{mirror}", post(report_status))
                    .route("/workers/{worker}/schedules", post(report_schedules))
                    .with_state(capture),
            )
            .await
            .unwrap();
        }
    });

    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let mut old_pending = status("mirror", SyncStatus::Failed);
    old_pending.error_msg = "pending-old".into();
    assert!(manager.report_status("w1", &old_pending).await.is_err());
    assert!(manager
        .report_schedules(
            "w1",
            &MirrorSchedules {
                schedules: vec![tunasync_protocol::MirrorSchedule {
                    mirror_name: "mirror".into(),
                    next_schedule: zero_time(),
                }],
            },
        )
        .await
        .is_err());
    manager.reconfigure(
        vec![format!("http://{addr}"), "http://127.0.0.1:9".into()],
        Client::new(),
        String::new(),
    );

    let (handle, actor, _) = ReportActor::new(
        Arc::clone(&manager),
        "w1".into(),
        Duration::from_secs(60),
        8,
    );
    let actor_task = tokio::spawn(actor.run());
    let mut active_old = status("mirror", SyncStatus::Syncing);
    active_old.error_msg = "active-old".into();
    assert!(handle.report_status(active_old));
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "mirror".into(),
            next_schedule: zero_time(),
        }],
    }));
    tokio::time::timeout(Duration::from_secs(1), old_started_rx.recv())
        .await
        .expect("old delivery did not start")
        .expect("old-start channel closed");

    assert!(handle.forget_mirror("mirror".into()));
    let mut new_status = status("mirror", SyncStatus::Success);
    new_status.error_msg = "new".into();
    assert!(handle.report_status(new_status));
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "mirror".into(),
            next_schedule: zero_time(),
        }],
    }));

    tokio::time::timeout(Duration::from_secs(1), new_started_rx.recv())
        .await
        .expect("new delivery did not start after delayed forget")
        .expect("new-start channel closed");
    assert!(manager.pending_status("mirror").await.is_none());
    assert_eq!(
        manager.pending_schedule_mirrors().await,
        Some(Vec::new()),
        "old pending schedule row must be removed before the new delivery can stash"
    );
    capture.new_gate.add_permits(1);

    let delivered_status = tokio::time::timeout(Duration::from_secs(2), status_rx.recv())
        .await
        .expect("new status was not delivered")
        .expect("status channel closed");
    assert_eq!(delivered_status.error_msg, "new");
    let delivered_schedules = tokio::time::timeout(Duration::from_secs(2), schedule_rx.recv())
        .await
        .expect("new schedules were not delivered")
        .expect("schedule channel closed");
    assert_eq!(delivered_schedules.schedules[0].mirror_name, "mirror");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if manager
                .pending_status("mirror")
                .await
                .is_some_and(|status| status.error_msg == "new")
                && manager.pending_schedule_mirrors().await.as_deref()
                    == Some(["mirror".to_string()].as_slice())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("new pending state was not stashed after old pending cleanup");

    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    server.abort();
}

#[tokio::test]
async fn one_entry_in_flight_reservation_stays_bounded_and_shutdown_releases_it() {
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct Hanging {
        started: mpsc::Sender<()>,
        gate: Semaphore,
    }

    async fn hang(State(state): State<Arc<Hanging>>) -> StatusCode {
        let _ = state.started.try_send(());
        state.gate.acquire().await.unwrap().forget();
        StatusCode::OK
    }

    let (started_tx, mut started_rx) = mpsc::channel(1);
    let state = Arc::new(Hanging {
        started: started_tx,
        gate: Semaphore::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn({
        let state = Arc::clone(&state);
        async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/workers/{worker}/jobs/{mirror}", post(hang))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });
    let manager = Arc::new(ManagerClient::new(
        vec![format!("http://{addr}")],
        Client::new(),
        String::new(),
    ));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 1);
    let actor = actor.with_shutdown_drain_timeout(Duration::from_millis(60));
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.report_status(status("active", SyncStatus::Syncing)));
    tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .expect("live delivery did not start")
        .expect("start channel closed");

    for index in 0..500 {
        assert!(handle.report_status(status(&format!("queued-{index}"), SyncStatus::Success,)));
        assert!(handle.actor_report_allocation_count() <= 1);
    }

    let mut done = handle.shutdown();
    assert!(tokio::time::timeout(Duration::from_millis(10), &mut done)
        .await
        .is_err());
    tokio::time::timeout(Duration::from_millis(500), done)
        .await
        .expect("bounded drain did not acknowledge")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    assert_eq!(handle.actor_report_allocation_count(), 0);
    let mailbox = handle.mailbox.lock();
    assert!(!mailbox.ordinary_in_flight);
    assert!(mailbox.active_schedules.is_none());
    drop(mailbox);
    server.abort();
}

#[tokio::test]
async fn shutdown_drains_status_and_schedule_and_rejects_new_reports() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::mpsc;

    struct Capture {
        statuses: mpsc::Sender<MirrorStatus>,
        schedules: mpsc::Sender<MirrorSchedules>,
    }

    async fn report_status(
        State(state): State<Arc<Capture>>,
        Json(status): Json<MirrorStatus>,
    ) -> StatusCode {
        state.statuses.send(status).await.unwrap();
        StatusCode::OK
    }

    async fn report_schedules(
        State(state): State<Arc<Capture>>,
        Json(schedules): Json<MirrorSchedules>,
    ) -> StatusCode {
        state.schedules.send(schedules).await.unwrap();
        StatusCode::OK
    }

    let (status_tx, mut status_rx) = mpsc::channel(1);
    let (schedule_tx, mut schedule_rx) = mpsc::channel(1);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/workers/{worker}/jobs/{mirror}", post(report_status))
                .route("/workers/{worker}/schedules", post(report_schedules))
                .with_state(Arc::new(Capture {
                    statuses: status_tx,
                    schedules: schedule_tx,
                })),
        )
        .await
        .unwrap();
    });
    let manager = Arc::new(ManagerClient::new(
        vec![format!("http://{addr}")],
        Client::new(),
        String::new(),
    ));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 4);
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.report_status(status("final", SyncStatus::Failed)));
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "final".into(),
            next_schedule: zero_time(),
        }],
    }));
    let done = handle.shutdown();
    assert!(!handle.report_status(status("rejected", SyncStatus::Success)));
    assert!(!handle.report_schedules(MirrorSchedules {
        schedules: Vec::new(),
    }));

    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown drain timed out")
        .expect("actor dropped acknowledgement");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), status_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .name,
        "final"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), schedule_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .schedules[0]
            .mirror_name,
        "final"
    );
    actor_task.await.unwrap();
    assert_eq!(handle.actor_report_allocation_count(), 0);
    assert_eq!(handle.schedule_allocation_rows(), 1);
    server.abort();
}

#[test]
fn restore_and_reconfigure_results_keep_independent_slots() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (_, actor, results) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    actor.publish_restore(RestoreResult {
        captured_versions: HashMap::new(),
        outcome: RestoreOutcome::Success(Vec::new()),
    });
    actor.publish_reconfigure(ReconfigureResult {
        generation: 7,
        outcome: ReconfigureOutcome::Success,
    });

    let state = results.borrow().clone();
    assert_eq!(state.restore.as_ref().unwrap().revision, 1);
    assert_eq!(state.reconfigure.as_ref().unwrap().revision, 1);
    assert_eq!(state.reconfigure.unwrap().value.generation, 7);
}

#[tokio::test]
async fn shutdown_aborts_hanging_candidate_registration() {
    use axum::{routing::post, Router};

    async fn hang() -> std::convert::Infallible {
        std::future::pending().await
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/workers", post(hang)))
            .await
            .unwrap();
    });
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    let task = tokio::spawn(actor.run());
    handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    });
    tokio::task::yield_now().await;
    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out")
        .expect("actor dropped acknowledgement");
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("actor task did not stop")
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn hanging_candidate_does_not_block_live_reports_to_current_manager() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::mpsc;

    struct CurrentManager {
        statuses: mpsc::Sender<MirrorStatus>,
        sizes: mpsc::Sender<serde_json::Value>,
        schedules: mpsc::Sender<MirrorSchedules>,
    }

    async fn capture_status(
        State(state): State<Arc<CurrentManager>>,
        Json(status): Json<MirrorStatus>,
    ) -> StatusCode {
        state.statuses.send(status).await.unwrap();
        StatusCode::OK
    }

    async fn capture_size(
        State(state): State<Arc<CurrentManager>>,
        Json(size): Json<serde_json::Value>,
    ) -> StatusCode {
        state.sizes.send(size).await.unwrap();
        StatusCode::OK
    }

    async fn capture_schedules(
        State(state): State<Arc<CurrentManager>>,
        Json(schedules): Json<MirrorSchedules>,
    ) -> StatusCode {
        state.schedules.send(schedules).await.unwrap();
        StatusCode::OK
    }

    async fn hang_registration(
        State(started): State<mpsc::Sender<()>>,
    ) -> std::convert::Infallible {
        let _ = started.try_send(());
        std::future::pending().await
    }

    let (status_tx, mut status_rx) = mpsc::channel(1);
    let (size_tx, mut size_rx) = mpsc::channel(1);
    let (schedule_tx, mut schedule_rx) = mpsc::channel(1);
    let current_state = Arc::new(CurrentManager {
        statuses: status_tx,
        sizes: size_tx,
        schedules: schedule_tx,
    });
    let current_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let current_addr = current_listener.local_addr().unwrap();
    let current_server = tokio::spawn({
        let state = Arc::clone(&current_state);
        async move {
            axum::serve(
                current_listener,
                Router::new()
                    .route("/workers/{worker}/jobs/{mirror}", post(capture_status))
                    .route("/workers/{worker}/jobs/{mirror}/size", post(capture_size))
                    .route("/workers/{worker}/schedules", post(capture_schedules))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });

    let (candidate_tx, mut candidate_rx) = mpsc::channel(1);
    let candidate_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate_addr = candidate_listener.local_addr().unwrap();
    let candidate_server = tokio::spawn(async move {
        axum::serve(
            candidate_listener,
            Router::new()
                .route("/workers", post(hang_registration))
                .with_state(candidate_tx),
        )
        .await
        .unwrap();
    });

    let manager = Arc::new(ManagerClient::new(
        vec![format!("http://{current_addr}")],
        Client::new(),
        String::new(),
    ));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 8);
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{candidate_addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));
    tokio::time::timeout(Duration::from_secs(1), candidate_rx.recv())
        .await
        .expect("candidate registration did not start")
        .expect("candidate start channel closed");

    assert!(handle.report_status(status("live", SyncStatus::Success)));
    assert!(handle.report_size("live".into(), "1T".into()));
    assert!(handle.report_schedules(schedules(&["live"])));

    let delivered_status = tokio::time::timeout(Duration::from_secs(1), status_rx.recv())
        .await
        .expect("status was blocked by candidate registration")
        .expect("status channel closed");
    assert_eq!(delivered_status.name, "live");
    let delivered_size = tokio::time::timeout(Duration::from_secs(1), size_rx.recv())
        .await
        .expect("size was blocked by candidate registration")
        .expect("size channel closed");
    assert_eq!(delivered_size["size"], "1T");
    let delivered_schedules = tokio::time::timeout(Duration::from_secs(1), schedule_rx.recv())
        .await
        .expect("schedules were blocked by candidate registration")
        .expect("schedule channel closed");
    assert_eq!(delivered_schedules.schedules[0].mirror_name, "live");

    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 2,
        bases: vec![format!("http://{candidate_addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));
    tokio::time::timeout(Duration::from_secs(1), candidate_rx.recv())
        .await
        .expect("replacement candidate registration was blocked")
        .expect("candidate start channel closed");

    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown was blocked by candidate registration")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    current_server.abort();
    candidate_server.abort();
}

#[tokio::test]
async fn shutdown_pending_at_final_launch_gate_prevents_due_heartbeat() {
    use axum::{
        extract::State,
        http::StatusCode,
        routing::{get, post},
        Json, Router,
    };
    use tokio::sync::mpsc;

    async fn register(
        State(started): State<mpsc::Sender<()>>,
        Json(status): Json<WorkerStatus>,
    ) -> Json<WorkerStatus> {
        let _ = started.try_send(());
        Json(status)
    }

    async fn restore() -> Json<Vec<MirrorStatus>> {
        Json(Vec::new())
    }

    async fn heartbeat(State(started): State<mpsc::Sender<()>>) -> StatusCode {
        let _ = started.try_send(());
        StatusCode::OK
    }

    let (request_tx, mut request_rx) = mpsc::channel(4);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/workers", post(register))
                .route("/workers/{worker}/jobs", get(restore))
                .route("/workers/{worker}/heartbeat", post(heartbeat))
                .with_state(request_tx),
        )
        .await
        .unwrap();
    });

    let manager = Arc::new(ManagerClient::new(
        vec![format!("http://{addr}")],
        Client::new(),
        String::new(),
    ));
    let hook = Arc::new(TestStartReadyHook::new());
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_millis(30), 8);
    let actor_task = tokio::spawn(actor.with_start_ready_hook(Arc::clone(&hook)).run());
    assert!(handle.bootstrap(BootstrapCommand {
        registration: worker_status(),
        restore_versions: HashMap::new(),
    }));
    tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
        .await
        .expect("bootstrap registration did not start")
        .expect("request channel closed");

    hook.arm();
    tokio::time::timeout(Duration::from_secs(1), hook.wait_until_reached())
        .await
        .expect("actor did not reach final launch gate");
    tokio::time::sleep(Duration::from_millis(40)).await;
    let done = handle.shutdown();
    hook.release();

    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out at final launch gate")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), request_rx.recv())
            .await
            .is_err(),
        "heartbeat started after shutdown became pending"
    );
    server.abort();
}

#[tokio::test]
async fn notify_wins_when_heartbeat_deadline_and_completion_are_ready() {
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 8);
    let mut runtime = ActorRuntime::new(Duration::from_secs(60));
    runtime.next_heartbeat = tokio::time::Instant::now();
    runtime.heartbeat_task = Some(tokio::spawn(async { Ok(()) }));
    tokio::task::yield_now().await;

    let done = handle.shutdown();
    assert!(matches!(
        actor.wait_for_active_event(&mut runtime).await,
        ActorEvent::Notified
    ));
    let control = actor
        .take_control(&runtime)
        .expect("shutdown control was not visible after notify wake");
    actor.handle_control(control, &mut runtime).await;
    assert!(actor.finish_or_expire_drain(&mut runtime).await);
    done.await.expect("actor dropped shutdown acknowledgement");
}

#[tokio::test]
async fn hanging_live_delivery_does_not_block_candidate_and_is_requeued() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct OldManager {
        report_started: mpsc::Sender<()>,
        gate: Semaphore,
    }

    async fn hang_report(State(state): State<Arc<OldManager>>) -> StatusCode {
        let _ = state.report_started.try_send(());
        let permit = state.gate.acquire().await.unwrap();
        permit.forget();
        StatusCode::OK
    }

    struct NewManager {
        registration_started: mpsc::Sender<()>,
        reports: mpsc::Sender<MirrorStatus>,
    }

    async fn register(
        State(state): State<Arc<NewManager>>,
        Json(status): Json<WorkerStatus>,
    ) -> Json<WorkerStatus> {
        let _ = state.registration_started.try_send(());
        Json(status)
    }

    async fn capture_report(
        State(state): State<Arc<NewManager>>,
        Json(status): Json<MirrorStatus>,
    ) -> StatusCode {
        state.reports.send(status).await.unwrap();
        StatusCode::OK
    }

    let (old_started_tx, mut old_started_rx) = mpsc::channel(1);
    let old_state = Arc::new(OldManager {
        report_started: old_started_tx,
        gate: Semaphore::new(0),
    });
    let old_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_addr = old_listener.local_addr().unwrap();
    let old_server = tokio::spawn({
        let state = Arc::clone(&old_state);
        async move {
            axum::serve(
                old_listener,
                Router::new()
                    .route("/workers/{worker}/jobs/{mirror}", post(hang_report))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });

    let (registration_tx, mut registration_rx) = mpsc::channel(1);
    let (report_tx, mut report_rx) = mpsc::channel(1);
    let new_state = Arc::new(NewManager {
        registration_started: registration_tx,
        reports: report_tx,
    });
    let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_addr = new_listener.local_addr().unwrap();
    let new_server = tokio::spawn({
        let state = Arc::clone(&new_state);
        async move {
            axum::serve(
                new_listener,
                Router::new()
                    .route("/workers", post(register))
                    .route("/workers/{worker}/jobs/{mirror}", post(capture_report))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });

    let manager = Arc::new(ManagerClient::new(
        vec![format!("http://{old_addr}")],
        Client::new(),
        String::new(),
    ));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.report_status(status("live", SyncStatus::Success)));
    tokio::time::timeout(Duration::from_secs(1), old_started_rx.recv())
        .await
        .expect("live delivery did not start")
        .expect("live start channel closed");
    assert!(handle.report_status(status("later", SyncStatus::Failed)));

    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{new_addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));
    tokio::time::timeout(Duration::from_secs(1), registration_rx.recv())
        .await
        .expect("candidate registration was blocked by live delivery")
        .expect("registration channel closed");
    let delivered = tokio::time::timeout(Duration::from_secs(1), report_rx.recv())
        .await
        .expect("aborted live report was not retried")
        .expect("report channel closed");
    assert_eq!(delivered.name, "live");
    let later = tokio::time::timeout(Duration::from_secs(1), report_rx.recv())
        .await
        .expect("later live report was not delivered")
        .expect("report channel closed");
    assert_eq!(later.name, "later");

    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown was blocked by live delivery")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    old_server.abort();
    new_server.abort();
}

#[tokio::test]
async fn hanging_replay_does_not_block_candidate_or_shutdown() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct RecoveryManager {
        replay_started: mpsc::Sender<()>,
        gate: Semaphore,
    }

    async fn hang_replay(State(state): State<Arc<RecoveryManager>>) -> StatusCode {
        let _ = state.replay_started.try_send(());
        let permit = state.gate.acquire().await.unwrap();
        permit.forget();
        StatusCode::OK
    }

    async fn bootstrap_register(Json(status): Json<WorkerStatus>) -> Json<WorkerStatus> {
        Json(status)
    }

    let (replay_tx, mut replay_rx) = mpsc::channel(1);
    let state = Arc::new(RecoveryManager {
        replay_started: replay_tx,
        gate: Semaphore::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn({
        let state = Arc::clone(&state);
        async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/workers", post(bootstrap_register))
                    .route(
                        "/workers/{worker}/heartbeat",
                        post(|| async { StatusCode::OK }),
                    )
                    .route("/workers/{worker}/jobs/{mirror}", post(hang_replay))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });

    let (candidate_tx, mut candidate_rx) = mpsc::channel(1);
    async fn candidate_register(
        State(started): State<mpsc::Sender<()>>,
        Json(status): Json<WorkerStatus>,
    ) -> Json<WorkerStatus> {
        let _ = started.try_send(());
        Json(status)
    }
    let candidate_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate_addr = candidate_listener.local_addr().unwrap();
    let candidate_server = tokio::spawn(async move {
        axum::serve(
            candidate_listener,
            Router::new()
                .route("/workers", post(candidate_register))
                .with_state(candidate_tx),
        )
        .await
        .unwrap();
    });

    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    assert!(manager
        .report_status("w1", &status("pending", SyncStatus::Failed))
        .await
        .is_err());
    manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
    let (handle, actor, _) = ReportActor::new(
        Arc::clone(&manager),
        "w1".into(),
        Duration::from_millis(10),
        32,
    );
    let actor_task = tokio::spawn(actor.run());
    handle.bootstrap(BootstrapCommand {
        registration: worker_status(),
        restore_versions: HashMap::new(),
    });
    tokio::time::timeout(Duration::from_secs(1), replay_rx.recv())
        .await
        .expect("replay did not start")
        .expect("replay channel closed");

    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{candidate_addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));
    tokio::time::timeout(Duration::from_secs(1), candidate_rx.recv())
        .await
        .expect("candidate registration was blocked by replay")
        .expect("registration channel closed");

    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown was blocked by replay")
        .expect("actor dropped acknowledgement");
    tokio::time::timeout(Duration::from_secs(1), actor_task)
        .await
        .expect("actor did not stop")
        .unwrap();
    assert_eq!(manager.pending_counts().await, (1, 0, false));
    server.abort();
    candidate_server.abort();
}

#[tokio::test]
async fn replay_batch_yields_to_live_report_and_reconfigure() {
    use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct Recovery {
        replay_gate: Semaphore,
        replay_started: mpsc::Sender<()>,
        events: Mutex<Vec<String>>,
        reports: AtomicUsize,
    }

    async fn register(Json(status): Json<WorkerStatus>) -> Json<WorkerStatus> {
        Json(status)
    }

    async fn heartbeat() -> StatusCode {
        StatusCode::OK
    }

    async fn restore() -> Json<Vec<MirrorStatus>> {
        Json(Vec::new())
    }

    async fn report(
        State(state): State<Arc<Recovery>>,
        Json(status): Json<MirrorStatus>,
    ) -> StatusCode {
        state.events.lock().push(status.name.clone());
        state.reports.fetch_add(1, Ordering::SeqCst);
        if status.name.starts_with("pending-") {
            let _ = state.replay_started.try_send(());
            let permit = state.replay_gate.acquire().await.unwrap();
            permit.forget();
        }
        StatusCode::OK
    }

    let (replay_started_tx, mut replay_started_rx) = mpsc::channel(32);
    let recovery = Arc::new(Recovery {
        replay_gate: Semaphore::new(0),
        replay_started: replay_started_tx,
        events: Mutex::new(Vec::new()),
        reports: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn({
        let recovery = Arc::clone(&recovery);
        async move {
            let app = Router::new()
                .route("/workers", post(register))
                .route("/workers/{worker}/heartbeat", post(heartbeat))
                .route("/workers/{worker}/jobs", get(restore))
                .route("/workers/{worker}/jobs/{mirror}", post(report))
                .with_state(recovery);
            axum::serve(listener, app).await.unwrap();
        }
    });

    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    for index in 0..24 {
        assert!(manager
            .report_status(
                "w1",
                &status(&format!("pending-{index}"), SyncStatus::Failed)
            )
            .await
            .is_err());
    }
    manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
    let (handle, actor, mut results) = ReportActor::new(
        Arc::clone(&manager),
        "w1".into(),
        Duration::from_millis(20),
        64,
    );
    let actor_task = tokio::spawn(actor.run());
    handle.bootstrap(BootstrapCommand {
        registration: worker_status(),
        restore_versions: HashMap::new(),
    });

    tokio::time::timeout(Duration::from_secs(1), replay_started_rx.recv())
        .await
        .expect("replay did not start")
        .expect("replay signal channel closed");
    assert!(handle.report_status(status("live", SyncStatus::Success)));
    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));

    recovery.replay_gate.add_permits(MAX_REPLAY_BATCH);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            results.changed().await.unwrap();
            if matches!(
                results
                    .borrow_and_update()
                    .reconfigure
                    .as_ref()
                    .map(|result| &result.value),
                Some(ReconfigureResult {
                    generation: 1,
                    outcome: ReconfigureOutcome::Success,
                })
            ) {
                break;
            }
        }
    })
    .await
    .expect("reconfigure did not finish between replay batches");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if recovery.events.lock().iter().any(|event| event == "live") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("live report did not overtake remaining replay backlog");
    assert!(recovery.reports.load(Ordering::SeqCst) < 24);

    let done = handle.shutdown();
    recovery.replay_gate.add_permits(MAX_REPLAY_BATCH);
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown did not preempt remaining replay")
        .expect("actor dropped shutdown acknowledgement");
    tokio::time::timeout(Duration::from_secs(1), actor_task)
        .await
        .expect("actor did not stop")
        .unwrap();
    assert!(recovery.reports.load(Ordering::SeqCst) < 24);
    server.abort();
}

#[tokio::test]
async fn sustained_live_reports_bound_replay_and_do_not_starve_heartbeat() {
    use axum::{extract::Path, extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::{mpsc, Semaphore};

    struct FairnessState {
        heartbeat_started: Semaphore,
        heartbeat_gate: Semaphore,
        live_gate: Semaphore,
        heartbeat_live_baseline: AtomicUsize,
        live_before_replay: AtomicUsize,
        live_total: AtomicUsize,
        replay_seen: AtomicUsize,
    }

    async fn register(Json(status): Json<WorkerStatus>) -> Json<WorkerStatus> {
        Json(status)
    }

    async fn heartbeat(State(state): State<Arc<FairnessState>>) -> StatusCode {
        state.heartbeat_started.add_permits(1);
        state.heartbeat_gate.acquire().await.unwrap().forget();
        StatusCode::OK
    }

    async fn report(
        Path((_worker, mirror)): Path<(String, String)>,
        State(state): State<Arc<FairnessState>>,
    ) -> StatusCode {
        if mirror == "pending" {
            let baseline = state.heartbeat_live_baseline.load(Ordering::SeqCst);
            state.live_before_replay.store(
                state.live_total.load(Ordering::SeqCst) - baseline,
                Ordering::SeqCst,
            );
            state.replay_seen.fetch_add(1, Ordering::SeqCst);
        } else {
            state.live_total.fetch_add(1, Ordering::SeqCst);
            state.live_gate.acquire().await.unwrap().forget();
        }
        StatusCode::OK
    }

    let state = Arc::new(FairnessState {
        heartbeat_started: Semaphore::new(0),
        heartbeat_gate: Semaphore::new(0),
        live_gate: Semaphore::new(0),
        heartbeat_live_baseline: AtomicUsize::new(usize::MAX),
        live_before_replay: AtomicUsize::new(0),
        live_total: AtomicUsize::new(0),
        replay_seen: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn({
        let state = Arc::clone(&state);
        async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/workers", post(register))
                    .route("/workers/{worker}/heartbeat", post(heartbeat))
                    .route("/workers/{worker}/jobs/{mirror}", post(report))
                    .with_state(state),
            )
            .await
            .unwrap();
        }
    });

    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    assert!(manager
        .report_status("w1", &status("pending", SyncStatus::Failed))
        .await
        .is_err());
    manager.reconfigure(vec![format!("http://{addr}")], Client::new(), String::new());
    let (handle, actor, _) = ReportActor::new(
        Arc::clone(&manager),
        "w1".into(),
        Duration::from_millis(10),
        512,
    );
    let actor = actor.with_shutdown_drain_timeout(Duration::from_millis(100));
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.bootstrap(BootstrapCommand {
        registration: worker_status(),
        restore_versions: HashMap::new(),
    }));

    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let producer = tokio::spawn({
        let handle = handle.clone();
        async move {
            let mut index = 0;
            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }
                assert!(
                    handle.report_status(status(&format!("live-{index}"), SyncStatus::Success,))
                );
                index += 1;
                tokio::task::yield_now().await;
            }
        }
    });

    tokio::time::timeout(Duration::from_secs(1), state.heartbeat_started.acquire())
        .await
        .expect("heartbeat endpoint was starved by live notifications")
        .unwrap()
        .forget();
    state.heartbeat_gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(20)).await;
    state
        .heartbeat_live_baseline
        .store(state.live_total.load(Ordering::SeqCst), Ordering::SeqCst);
    state.live_gate.add_permits(LIVE_REPORTS_PER_REPLAY * 3);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state.replay_seen.load(Ordering::SeqCst) > 0
                && state.live_total.load(Ordering::SeqCst) > LIVE_REPORTS_PER_REPLAY
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("heartbeat, replay, or live delivery was starved");
    assert!(
        state.live_before_replay.load(Ordering::SeqCst) <= LIVE_REPORTS_PER_REPLAY,
        "pending replay exceeded the live delivery quota"
    );

    stop_tx.send(()).await.unwrap();
    producer.await.unwrap();
    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    server.abort();
}

#[tokio::test]
async fn startup_schedule_snapshot_reaches_recovered_manager() {
    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
    use tokio::sync::mpsc;

    async fn register(Json(status): Json<WorkerStatus>) -> Json<WorkerStatus> {
        Json(status)
    }

    async fn schedules(
        State(tx): State<mpsc::Sender<MirrorSchedules>>,
        Json(schedules): Json<MirrorSchedules>,
    ) -> StatusCode {
        tx.send(schedules).await.unwrap();
        StatusCode::OK
    }

    let (schedule_tx, mut schedule_rx) = mpsc::channel(1);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/workers", post(register))
                .route("/workers/{worker}/schedules", post(schedules))
                .with_state(schedule_tx),
        )
        .await
        .unwrap();
    });

    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, actor, _) = ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "old".into(),
            next_schedule: zero_time(),
        }],
    }));
    assert!(handle.report_schedules(MirrorSchedules {
        schedules: vec![tunasync_protocol::MirrorSchedule {
            mirror_name: "newest".into(),
            next_schedule: zero_time(),
        }],
    }));
    let actor_task = tokio::spawn(actor.run());
    assert!(handle.reconfigure(ReconfigureCommand {
        generation: 1,
        bases: vec![format!("http://{addr}")],
        client: Client::new(),
        token: String::new(),
        registration: worker_status(),
    }));

    let delivered = tokio::time::timeout(Duration::from_secs(1), schedule_rx.recv())
        .await
        .expect("startup schedule snapshot was not recovered")
        .expect("schedule capture channel closed");
    assert_eq!(delivered.schedules[0].mirror_name, "newest");

    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    server.abort();
}

#[tokio::test]
async fn latest_wins_result_slot_does_not_grow_with_reconfigures() {
    use axum::{routing::post, Json, Router};

    async fn register(Json(status): Json<WorkerStatus>) -> Json<WorkerStatus> {
        Json(status)
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/workers", post(register)))
            .await
            .unwrap();
    });
    let manager = Arc::new(ManagerClient::new(Vec::new(), Client::new(), String::new()));
    let (handle, actor, results) =
        ReportActor::new(manager, "w1".into(), Duration::from_secs(60), 32);
    let actor_task = tokio::spawn(actor.run());
    for generation in 1..=64 {
        assert!(handle.reconfigure(ReconfigureCommand {
            generation,
            bases: vec![format!("http://{addr}")],
            client: Client::new(),
            token: String::new(),
            registration: worker_status(),
        }));
    }

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                results
                    .borrow()
                    .reconfigure
                    .as_ref()
                    .map(|result| &result.value),
                Some(ReconfigureResult {
                    generation: 64,
                    outcome: ReconfigureOutcome::Success,
                })
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("latest reconfigure result was not published");
    assert!(matches!(
        results
            .borrow()
            .reconfigure
            .as_ref()
            .map(|result| &result.value),
        Some(ReconfigureResult {
            generation: 64,
            outcome: ReconfigureOutcome::Success,
        })
    ));
    let done = handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), done)
        .await
        .expect("shutdown timed out")
        .expect("actor dropped acknowledgement");
    actor_task.await.unwrap();
    server.abort();
}
