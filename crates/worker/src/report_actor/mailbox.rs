use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify};
use tunasync_protocol::{MirrorSchedules, MirrorStatus};

use super::{BootstrapCommand, ReconfigureCommand};

pub(super) const ORDINARY_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct ReportHandle {
    pub(super) mailbox: Arc<Mutex<MailboxState>>,
    notify: Arc<Notify>,
}

#[derive(Clone)]
pub(super) enum Report {
    Status(MirrorStatus),
    Size { mirror: String, size: String },
    Schedules(Arc<MirrorSchedules>),
}

impl Report {
    fn resource_name(&self) -> String {
        match self {
            Self::Status(status) => format!("status:{}", status.name),
            Self::Size { mirror, .. } => format!("size:{mirror}"),
            Self::Schedules(_) => "schedules".into(),
        }
    }

    pub(super) fn belongs_to(&self, mirror: &str) -> bool {
        match self {
            Self::Status(status) => status.name == mirror,
            Self::Size {
                mirror: report_mirror,
                ..
            } => report_mirror == mirror,
            Self::Schedules(schedules) => schedules
                .schedules
                .iter()
                .any(|schedule| schedule.mirror_name == mirror),
        }
    }
}

#[derive(Clone)]
pub(super) struct SequencedReport {
    pub(super) sequence: u64,
    pub(super) report: Report,
}

pub(super) struct MailboxState {
    pub(super) next_sequence: u64,
    pub(super) retry: Option<SequencedReport>,
    pub(super) fifo: VecDeque<SequencedReport>,
    overflowed: bool,
    pub(super) statuses: HashMap<String, SequencedReport>,
    pub(super) sizes: HashMap<String, SequencedReport>,
    pub(super) schedules: Option<SequencedReport>,
    pub(super) latest_schedules: Option<SequencedReport>,
    pub(super) ordinary_in_flight: bool,
    pub(super) active_schedules: Option<Arc<MirrorSchedules>>,
    max_resources: usize,
    max_schedule_entries: usize,
    bootstrap: Option<BootstrapCommand>,
    reconfigure: Option<ReconfigureCommand>,
    pub(super) latest_reconfigure_generation: u64,
    forget_mirrors: HashSet<String>,
    shutdown: Option<oneshot::Sender<()>>,
    accepting: bool,
}

pub(super) enum ControlItem {
    Bootstrap(BootstrapCommand),
    Reconfigure(ReconfigureCommand),
    ForgetMirror(String),
    Shutdown(oneshot::Sender<()>),
}

enum ReportSource {
    Retry,
    Fifo,
    Coalesced,
}

impl ReportHandle {
    pub(super) fn new(mailbox: Arc<Mutex<MailboxState>>, notify: Arc<Notify>) -> Self {
        Self { mailbox, notify }
    }

    pub fn bootstrap(&self, command: BootstrapCommand) -> bool {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting {
            return false;
        }
        mailbox.bootstrap = Some(command);
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    pub fn report_status(&self, status: MirrorStatus) -> bool {
        self.enqueue(Report::Status(status))
    }

    pub fn report_size(&self, mirror: String, size: String) -> bool {
        self.enqueue(Report::Size { mirror, size })
    }

    /// Validate and enqueue an already-constructed external schedule snapshot.
    /// Production callers that own the source rows should use
    /// [`Self::report_schedules_with`] to avoid allocating rejected snapshots.
    pub fn report_schedules(&self, schedules: MirrorSchedules) -> bool {
        let row_count = schedules.schedules.len();
        self.report_schedules_with(row_count, || schedules)
    }

    /// Admit a schedule snapshot by row count before constructing its rows.
    pub fn report_schedules_with<F>(&self, row_count: usize, build: F) -> bool
    where
        F: FnOnce() -> MirrorSchedules,
    {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting {
            tracing::warn!(
                schedule_rows = row_count,
                "rejecting schedule snapshot after shutdown"
            );
            return false;
        }
        if row_count > mailbox.max_schedule_entries {
            let limit = mailbox.max_schedule_entries;
            drop(mailbox);
            tracing::warn!(
                schedule_rows = row_count,
                max_schedule_entries = limit,
                "rejecting oversized complete schedule snapshot"
            );
            return false;
        }
        if !mailbox.can_replace_schedule(row_count) {
            let active_rows = mailbox.active_schedule_rows();
            let limit = mailbox.max_schedule_entries;
            drop(mailbox);
            tracing::warn!(
                schedule_rows = row_count,
                active_schedule_rows = active_rows,
                max_schedule_entries = limit,
                "rejecting schedule snapshot that would exceed active plus queued row limit"
            );
            return false;
        }

        let schedules = build();
        let built_row_count = schedules.schedules.len();
        if built_row_count != row_count {
            tracing::warn!(
                declared_schedule_rows = row_count,
                built_schedule_rows = built_row_count,
                "rejecting schedule snapshot whose built row count differs from its declaration"
            );
            return false;
        }
        mailbox.enqueue_new(Report::Schedules(Arc::new(schedules)));
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    pub fn reconfigure(&self, command: ReconfigureCommand) -> bool {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting || command.generation < mailbox.latest_reconfigure_generation {
            return false;
        }
        mailbox.latest_reconfigure_generation = command.generation;
        mailbox.reconfigure = Some(command);
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    pub fn supersede_reconfigure(&self, generation: u64) -> bool {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting || generation < mailbox.latest_reconfigure_generation {
            return false;
        }
        mailbox.latest_reconfigure_generation = generation;
        mailbox.reconfigure = None;
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    pub fn forget_mirror(&self, mirror: String) -> bool {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting {
            return false;
        }
        mailbox.remove_mirror(&mirror);
        mailbox.forget_mirrors.insert(mirror);
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    pub fn shutdown(&self) -> oneshot::Receiver<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let mut mailbox = self.mailbox.lock();
        mailbox.accepting = false;
        mailbox.shutdown = Some(done_tx);
        drop(mailbox);
        self.notify.notify_one();
        done_rx
    }

    pub fn control_pending(&self) -> bool {
        self.mailbox.lock().control_pending()
    }

    fn enqueue(&self, report: Report) -> bool {
        let mut mailbox = self.mailbox.lock();
        if !mailbox.accepting {
            return false;
        }
        mailbox.enqueue_new(report);
        drop(mailbox);
        self.notify.notify_one();
        true
    }

    #[cfg(test)]
    pub(crate) fn mailbox_counts(&self) -> (usize, usize, usize, bool) {
        let mailbox = self.mailbox.lock();
        (
            mailbox.fifo.len(),
            mailbox.statuses.len(),
            mailbox.sizes.len(),
            mailbox.schedules.is_some(),
        )
    }

    #[cfg(test)]
    pub(crate) fn actor_report_allocation_count(&self) -> usize {
        self.mailbox.lock().retained_report_count()
    }

    #[cfg(test)]
    pub(crate) fn schedule_allocation_rows(&self) -> usize {
        self.mailbox.lock().schedule_allocation_rows()
    }
}

impl MailboxState {
    pub(super) fn new(max_resources: usize) -> Self {
        let max_resources = crate::config::effective_report_max_resources(max_resources);
        Self {
            next_sequence: 0,
            retry: None,
            fifo: VecDeque::new(),
            overflowed: false,
            statuses: HashMap::new(),
            sizes: HashMap::new(),
            schedules: None,
            latest_schedules: None,
            ordinary_in_flight: false,
            active_schedules: None,
            max_resources,
            max_schedule_entries: max_resources,
            bootstrap: None,
            reconfigure: None,
            latest_reconfigure_generation: 0,
            forget_mirrors: HashSet::new(),
            shutdown: None,
            accepting: true,
        }
    }

    fn next_report(&mut self, report: Report) -> SequencedReport {
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .expect("report sequence exhausted");
        SequencedReport {
            sequence: self.next_sequence,
            report,
        }
    }

    pub(super) fn enqueue_new(&mut self, report: Report) {
        let report = self.next_report(report);
        if matches!(&report.report, Report::Schedules(_)) {
            self.latest_schedules = Some(report.clone());
            self.schedules = Some(report);
            return;
        }
        self.enqueue(report);
    }

    fn enqueue(&mut self, report: SequencedReport) {
        let ordinary_capacity = ORDINARY_CAPACITY.min(self.max_resources);
        if !self.overflowed
            && self.fifo.len() < ordinary_capacity
            && self.retained_report_count() < self.max_resources
        {
            self.fifo.push_back(report);
            return;
        }
        self.overflowed = true;
        match &report.report {
            Report::Status(status) => {
                let grows = !self.statuses.contains_key(&status.name);
                if grows && !self.make_report_room(false) {
                    tracing::warn!(resource = %report.report.resource_name(), max_resources = self.max_resources, "dropping report while all retained capacity is in flight");
                    return;
                }
                self.statuses.insert(status.name.clone(), report);
            }
            Report::Size { mirror, .. } => {
                let grows = !self.sizes.contains_key(mirror);
                if grows && !self.make_report_room(false) {
                    tracing::warn!(resource = %report.report.resource_name(), max_resources = self.max_resources, "dropping report while all retained capacity is in flight");
                    return;
                }
                self.sizes.insert(mirror.clone(), report);
            }
            Report::Schedules(_) => unreachable!("schedules bypass the ordinary FIFO"),
        }
    }

    pub(super) fn retained_report_count(&self) -> usize {
        usize::from(self.ordinary_in_flight)
            + usize::from(self.retry.is_some())
            + self.fifo.len()
            + self.statuses.len()
            + self.sizes.len()
    }

    fn make_report_room(&mut self, retain_retry: bool) -> bool {
        if self.retained_report_count() < self.max_resources {
            return true;
        }
        enum ReportKey {
            Retry,
            Fifo,
            Status(String),
            Size(String),
        }

        let mut oldest = if retain_retry {
            None
        } else {
            self.retry
                .as_ref()
                .map(|report| (report.sequence, ReportKey::Retry))
        };
        if let Some(report) = self.fifo.front() {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, ReportKey::Fifo));
            }
        }
        for (name, report) in &self.statuses {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, ReportKey::Status(name.clone())));
            }
        }
        for (name, report) in &self.sizes {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, ReportKey::Size(name.clone())));
            }
        }

        if let Some((sequence, key)) = oldest {
            let (source, resource) = match key {
                ReportKey::Retry => {
                    let report = self.retry.take().expect("selected retry report");
                    ("retry", report.report.resource_name())
                }
                ReportKey::Fifo => {
                    let report = self.fifo.pop_front().expect("selected FIFO report");
                    ("fifo", report.report.resource_name())
                }
                ReportKey::Status(name) => {
                    self.statuses.remove(&name).expect("selected status report");
                    ("coalesced", format!("status:{name}"))
                }
                ReportKey::Size(name) => {
                    self.sizes.remove(&name).expect("selected size report");
                    ("coalesced", format!("size:{name}"))
                }
            };
            tracing::warn!(sequence, source, %resource, max_resources = self.max_resources, "evicting oldest retained status/size report");
        }
        self.retained_report_count() < self.max_resources
    }

    pub(super) fn take_control(&mut self) -> Option<ControlItem> {
        if let Some(done) = self.shutdown.take() {
            return Some(ControlItem::Shutdown(done));
        }
        if let Some(command) = self.reconfigure.take() {
            return Some(ControlItem::Reconfigure(command));
        }
        if let Some(mirror) = self.forget_mirrors.iter().next().cloned() {
            self.forget_mirrors.remove(&mirror);
            return Some(ControlItem::ForgetMirror(mirror));
        }
        self.bootstrap.take().map(ControlItem::Bootstrap)
    }

    pub(super) fn control_pending(&self) -> bool {
        self.shutdown.is_some()
            || self.reconfigure.is_some()
            || !self.forget_mirrors.is_empty()
            || self.bootstrap.is_some()
    }

    pub(super) fn discard_control_after_shutdown(&mut self) {
        self.bootstrap = None;
        self.reconfigure = None;
        self.forget_mirrors.clear();
    }

    pub(super) fn queued_report_count(&self) -> usize {
        usize::from(self.retry.is_some())
            + self.fifo.len()
            + self.statuses.len()
            + self.sizes.len()
            + usize::from(self.schedules.is_some())
    }

    pub(super) fn drop_queued_reports(&mut self) {
        self.retry = None;
        self.fifo.clear();
        self.statuses.clear();
        self.sizes.clear();
        self.schedules = None;
        self.latest_schedules = None;
    }

    pub(super) fn take_next_report_for_delivery(&mut self) -> Option<SequencedReport> {
        let report = self.take_next_report()?;
        match &report.report {
            Report::Status(_) | Report::Size { .. } => {
                assert!(
                    !self.ordinary_in_flight,
                    "only one live report is supported"
                );
                self.ordinary_in_flight = true;
            }
            Report::Schedules(schedules) => {
                assert!(
                    self.active_schedules.is_none(),
                    "only one live schedule is supported"
                );
                self.active_schedules = Some(Arc::clone(schedules));
            }
        }
        Some(report)
    }

    pub(super) fn release_in_flight(&mut self, report: &SequencedReport) {
        match &report.report {
            Report::Status(_) | Report::Size { .. } => {
                assert!(
                    self.ordinary_in_flight,
                    "ordinary in-flight reservation missing"
                );
                self.ordinary_in_flight = false;
            }
            Report::Schedules(schedules) => {
                let active = self
                    .active_schedules
                    .take()
                    .expect("schedule in-flight reservation missing");
                assert!(
                    Arc::ptr_eq(&active, schedules),
                    "active schedule Arc changed"
                );
            }
        }
    }

    pub(super) fn requeue_in_flight(&mut self, report: SequencedReport) {
        // Reservation release and retry insertion share this lock, so the
        // retained total never grows during an abort/reconfigure transition.
        self.release_in_flight(&report);
        self.requeue_retry(report);
    }

    fn active_schedule_rows(&self) -> usize {
        self.active_schedules
            .as_ref()
            .map_or(0, |schedules| schedules.schedules.len())
    }

    fn can_replace_schedule(&self, rows: usize) -> bool {
        self.active_schedule_rows().saturating_add(rows) <= self.max_schedule_entries
    }

    #[cfg(test)]
    pub(super) fn schedule_allocation_rows(&self) -> usize {
        let active_rows = self.active_schedule_rows();
        let latest_rows = match (&self.active_schedules, &self.latest_schedules) {
            (
                Some(active),
                Some(SequencedReport {
                    report: Report::Schedules(latest),
                    ..
                }),
            ) if Arc::ptr_eq(active, latest) => 0,
            (
                _,
                Some(SequencedReport {
                    report: Report::Schedules(latest),
                    ..
                }),
            ) => latest.schedules.len(),
            _ => 0,
        };
        active_rows + latest_rows
    }

    pub(super) fn take_next_report(&mut self) -> Option<SequencedReport> {
        let mut oldest: Option<(u64, ReportSource)> = self
            .retry
            .as_ref()
            .map(|report| (report.sequence, ReportSource::Retry));
        if let Some(report) = self.fifo.front() {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, ReportSource::Fifo));
            }
        }
        if let Some(sequence) = self.oldest_coalesced_sequence() {
            if oldest
                .as_ref()
                .map_or(true, |(oldest_sequence, _)| sequence < *oldest_sequence)
            {
                oldest = Some((sequence, ReportSource::Coalesced));
            }
        }
        match oldest.map(|(_, source)| source) {
            Some(ReportSource::Retry) => self.retry.take(),
            Some(ReportSource::Fifo) => self.fifo.pop_front(),
            Some(ReportSource::Coalesced) => self.take_coalesced(),
            None => None,
        }
    }

    pub(super) fn has_reports(&self) -> bool {
        self.retry.is_some()
            || !self.fifo.is_empty()
            || !self.statuses.is_empty()
            || !self.sizes.is_empty()
            || self.schedules.is_some()
    }

    #[cfg(test)]
    pub(super) fn take_fifo(&mut self) -> Option<SequencedReport> {
        self.fifo.pop_front()
    }

    fn requeue_retry(&mut self, report: SequencedReport) {
        if matches!(&report.report, Report::Schedules(_)) {
            if self
                .schedules
                .as_ref()
                .map_or(true, |queued| report.sequence > queued.sequence)
            {
                self.schedules = Some(report.clone());
                self.latest_schedules = Some(report);
            }
            return;
        }
        assert!(self.retry.is_none(), "only one live report may be retried");
        if self.retained_report_count() >= self.max_resources {
            self.overflowed = true;
            assert!(
                self.make_report_room(true),
                "retry reservation could not be converted back into retained storage"
            );
        }
        self.retry = Some(report);
    }

    pub(super) fn remove_mirror(&mut self, mirror: &str) {
        if self
            .retry
            .as_ref()
            .is_some_and(|report| report.report.belongs_to(mirror))
        {
            self.retry = None;
        }
        self.fifo.retain(|report| !report.report.belongs_to(mirror));
        self.statuses.remove(mirror);
        self.sizes.remove(mirror);
        let Some(mut schedules) = self.take_latest_schedule_payload() else {
            return;
        };
        let filtered_rows = schedules
            .schedules
            .iter()
            .filter(|schedule| schedule.mirror_name != mirror)
            .count();
        let shared_with_active = self
            .active_schedules
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &schedules));

        if !self.can_replace_schedule(filtered_rows) {
            tracing::warn!(
                mirror,
                active_schedule_rows = self.active_schedule_rows(),
                filtered_schedule_rows = filtered_rows,
                max_schedule_entries = self.max_schedule_entries,
                "dropping queued schedule snapshot after mirror removal to preserve row limit"
            );
            return;
        }
        schedules =
            self.filter_schedule_payload(schedules, mirror, filtered_rows, shared_with_active);
        let report = self.next_report(Report::Schedules(schedules));
        self.latest_schedules = Some(report.clone());
        self.schedules = Some(report);
    }

    fn take_latest_schedule_payload(&mut self) -> Option<Arc<MirrorSchedules>> {
        let latest = self.latest_schedules.take();
        let queued = self.schedules.take();
        let source = match latest {
            Some(latest) => {
                drop(queued);
                latest
            }
            None => queued?,
        };
        let SequencedReport {
            report: Report::Schedules(schedules),
            ..
        } = source
        else {
            unreachable!("schedule slots only contain schedule reports");
        };
        Some(schedules)
    }

    fn filter_schedule_payload(
        &self,
        mut schedules: Arc<MirrorSchedules>,
        mirror: &str,
        filtered_rows: usize,
        shared_with_active: bool,
    ) -> Arc<MirrorSchedules> {
        if shared_with_active {
            drop(schedules);
            let active = self
                .active_schedules
                .as_ref()
                .expect("shared active schedule disappeared");
            let mut filtered = Vec::with_capacity(filtered_rows);
            filtered.extend(
                active
                    .schedules
                    .iter()
                    .filter(|schedule| schedule.mirror_name != mirror)
                    .cloned(),
            );
            Arc::new(MirrorSchedules {
                schedules: filtered,
            })
        } else {
            debug_assert_eq!(
                Arc::strong_count(&schedules),
                1,
                "non-active latest schedule should be uniquely owned"
            );
            Arc::make_mut(&mut schedules)
                .schedules
                .retain(|schedule| schedule.mirror_name != mirror);
            schedules
        }
    }

    fn oldest_coalesced_sequence(&self) -> Option<u64> {
        self.statuses
            .values()
            .chain(self.sizes.values())
            .map(|report| report.sequence)
            .chain(self.schedules.iter().map(|report| report.sequence))
            .min()
    }

    pub(super) fn take_coalesced(&mut self) -> Option<SequencedReport> {
        enum Key {
            Status(String),
            Size(String),
            Schedules,
        }
        let mut oldest: Option<(u64, Key)> = None;
        for (name, report) in &self.statuses {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, Key::Status(name.clone())));
            }
        }
        for (name, report) in &self.sizes {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, Key::Size(name.clone())));
            }
        }
        if let Some(report) = &self.schedules {
            if oldest
                .as_ref()
                .map_or(true, |(sequence, _)| report.sequence < *sequence)
            {
                oldest = Some((report.sequence, Key::Schedules));
            }
        }
        match oldest.map(|(_, key)| key) {
            Some(Key::Status(name)) => self.statuses.remove(&name),
            Some(Key::Size(name)) => self.sizes.remove(&name),
            Some(Key::Schedules) => self.schedules.take(),
            None => None,
        }
    }

    pub(super) fn requeue_latest_schedules(&mut self) {
        let Some(SequencedReport {
            report: Report::Schedules(schedules),
            ..
        }) = self.latest_schedules.clone()
        else {
            return;
        };
        let report = self.next_report(Report::Schedules(schedules));
        self.latest_schedules = Some(report.clone());
        self.schedules = Some(report);
    }
}
