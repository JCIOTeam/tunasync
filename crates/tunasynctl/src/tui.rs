use std::collections::HashMap;
use std::io::{self, IsTerminal, Stdout};
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use futures::{stream, StreamExt};
use ratatui::backend::CrosstermBackend;
use ratatui::widgets::TableState;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tunasync_protocol::{ClientCmd, CmdVerb, MirrorStatus, SyncStatus};
use unicode_segmentation::UnicodeSegmentation;

use super::Client;

mod render;

use render::render;

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_WORKER_FETCHES: usize = 12;
const MAX_WORKERS: usize = 4096;
const STATUS_FILTERS: [Option<SyncStatus>; 8] = [
    None,
    Some(SyncStatus::Syncing),
    Some(SyncStatus::PreSyncing),
    Some(SyncStatus::Failed),
    Some(SyncStatus::Success),
    Some(SyncStatus::Paused),
    Some(SyncStatus::Disabled),
    Some(SyncStatus::None),
];

type DashboardTerminal = Terminal<CrosstermBackend<Stdout>>;

#[derive(Default)]
struct App {
    jobs: Vec<MirrorStatus>,
    workers: Vec<String>,
    worker_filter: Option<String>,
    status_filter: Option<SyncStatus>,
    selected: usize,
    table_state: TableState,
    pending: Option<ClientCmd>,
    refreshing: bool,
    command_in_flight: bool,
    status_message: String,
    last_refresh: Option<chrono::DateTime<chrono::Local>>,
    should_quit: bool,
    zh: bool,
}

impl App {
    fn new(zh: bool) -> Self {
        Self {
            zh,
            status_message: localized(zh, "Loading manager state...", "正在读取 manager 状态...")
                .to_owned(),
            ..Self::default()
        }
    }

    fn visible_job_indices(&self) -> Vec<usize> {
        self.jobs
            .iter()
            .enumerate()
            .filter(|(_, job)| {
                self.worker_filter
                    .as_ref()
                    .map_or(true, |worker| &job.worker == worker)
                    && self
                        .status_filter
                        .map_or(true, |status| job.status == status)
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn visible_jobs(&self) -> Vec<&MirrorStatus> {
        self.visible_job_indices()
            .into_iter()
            .map(|index| &self.jobs[index])
            .collect()
    }

    fn selected_job(&self) -> Option<&MirrorStatus> {
        let index = *self.visible_job_indices().get(self.selected)?;
        self.jobs.get(index)
    }

    fn selected_identity(&self) -> Option<(String, String)> {
        self.selected_job()
            .map(|job| (job.worker.clone(), job.name.clone()))
    }

    #[cfg(test)]
    fn replace_jobs(&mut self, jobs: Vec<MirrorStatus>) {
        let workers = jobs.iter().map(|job| job.worker.clone()).collect();
        self.replace_snapshot(workers, jobs);
    }

    fn replace_snapshot(&mut self, mut workers: Vec<String>, mut jobs: Vec<MirrorStatus>) {
        let selected = self.selected_identity();
        workers.sort();
        workers.dedup();
        if self
            .worker_filter
            .as_ref()
            .is_some_and(|worker| !workers.contains(worker))
        {
            self.worker_filter = None;
        }
        jobs.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.worker.cmp(&right.worker))
        });
        self.workers = workers;
        self.jobs = jobs;
        self.restore_selection(selected.as_ref());
    }

    fn apply_snapshot(&mut self, snapshot: Snapshot) {
        let mut jobs = snapshot.jobs;
        jobs.extend(
            self.jobs
                .iter()
                .filter(|job| snapshot.unavailable_workers.contains(&job.worker))
                .cloned(),
        );
        self.replace_snapshot(snapshot.workers, jobs);
    }

    fn restore_selection(&mut self, identity: Option<&(String, String)>) {
        let visible = self.visible_job_indices();
        self.selected = identity
            .and_then(|(worker, name)| {
                visible.iter().position(|index| {
                    let job = &self.jobs[*index];
                    &job.worker == worker && &job.name == name
                })
            })
            .unwrap_or_else(|| self.selected.min(visible.len().saturating_sub(1)));
        self.sync_table_selection(visible.len());
    }

    fn sync_table_selection(&mut self, visible_len: usize) {
        self.table_state
            .select((visible_len > 0).then_some(self.selected));
    }

    fn select_next(&mut self) {
        let len = self.visible_job_indices().len();
        if len > 0 {
            self.selected = (self.selected + 1).min(len - 1);
        }
        self.sync_table_selection(len);
    }

    fn select_previous(&mut self) {
        let len = self.visible_job_indices().len();
        self.selected = self.selected.saturating_sub(1);
        self.sync_table_selection(len);
    }

    fn cycle_worker_filter(&mut self) {
        let selected = self.selected_identity();
        self.worker_filter = match &self.worker_filter {
            None => self.workers.first().cloned(),
            Some(current) => self
                .workers
                .iter()
                .position(|worker| worker == current)
                .and_then(|index| self.workers.get(index + 1).cloned()),
        };
        self.restore_selection(selected.as_ref());
    }

    fn cycle_status_filter(&mut self) {
        let selected = self.selected_identity();
        let current = STATUS_FILTERS
            .iter()
            .position(|status| *status == self.status_filter)
            .unwrap_or(0);
        self.status_filter = STATUS_FILTERS[(current + 1) % STATUS_FILTERS.len()];
        self.restore_selection(selected.as_ref());
    }

    fn command_for_selected(&self, verb: CmdVerb) -> Option<ClientCmd> {
        let job = self.selected_job()?;
        Some(ClientCmd {
            cmd: verb,
            mirror_id: job.name.clone(),
            worker_id: job.worker.clone(),
            args: Vec::new(),
            options: HashMap::new(),
        })
    }

    fn begin_action(&mut self, verb: CmdVerb) {
        if self.command_in_flight {
            return;
        }
        self.pending = self.command_for_selected(verb);
    }

    fn worker_filter_label(&self) -> &str {
        self.worker_filter.as_deref().unwrap_or("all")
    }

    fn status_filter_label(&self) -> &str {
        self.status_filter.map(SyncStatus::as_str).unwrap_or("all")
    }
}

struct Snapshot {
    workers: Vec<String>,
    jobs: Vec<MirrorStatus>,
    unavailable_workers: Vec<String>,
}

enum BackgroundEvent {
    Snapshot(Result<Snapshot, String>),
    Command {
        verb: CmdVerb,
        mirror: String,
        result: Result<(), String>,
    },
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
    }
}

pub(super) async fn run(client: Client) -> Result<()> {
    let zh = super::is_zh();
    anyhow::ensure!(
        io::stdin().is_terminal() && io::stdout().is_terminal(),
        "{}",
        localized(
            zh,
            "TUI requires an interactive terminal",
            "TUI 需要交互式终端"
        )
    );
    validate_transport(&client, zh)?;
    let manager = redact_url(&client.base_url);
    let mut terminal = setup_terminal().context(localized(
        zh,
        "initialize terminal dashboard",
        "初始化终端仪表盘",
    ))?;
    let _guard = TerminalGuard;
    let mut app = App::new(zh);
    let mut events = EventStream::new();
    let mut refresh_interval = tokio::time::interval(REFRESH_INTERVAL);
    refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let (background_tx, mut background_rx) = mpsc::channel(4);

    spawn_refresh(&client, &background_tx, &mut app);

    while !app.should_quit {
        terminal
            .draw(|frame| render(frame, &mut app, &manager))
            .context(localized(zh, "render terminal dashboard", "绘制终端仪表盘"))?;

        tokio::select! {
            _ = refresh_interval.tick() => {
                spawn_refresh(&client, &background_tx, &mut app);
            }
            event = events.next() => {
                match event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        handle_key(key, &client, &background_tx, &mut app);
                    }
                    Some(Ok(Event::Resize(_, _))) | Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error).context(localized(
                        zh,
                        "read terminal input",
                        "读取终端输入",
                    )),
                    None => app.should_quit = true,
                }
            }
            background = background_rx.recv() => {
                match background {
                    Some(event) => handle_background(event, &client, &background_tx, &mut app),
                    None => app.should_quit = true,
                }
            }
        }
    }

    terminal.show_cursor().ok();
    Ok(())
}

fn setup_terminal() -> io::Result<DashboardTerminal> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
        return Err(error);
    }
    match Terminal::new(CrosstermBackend::new(stdout)) {
        Ok(terminal) => Ok(terminal),
        Err(error) => {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
            Err(error)
        }
    }
}

fn spawn_refresh(client: &Client, background_tx: &mpsc::Sender<BackgroundEvent>, app: &mut App) {
    if app.refreshing {
        return;
    }
    app.refreshing = true;
    let client = client.clone();
    let background_tx = background_tx.clone();
    tokio::spawn(async move {
        let result = fetch_snapshot(&client)
            .await
            .map_err(|error| sanitized_error(&client, &error));
        let _ = background_tx.send(BackgroundEvent::Snapshot(result)).await;
    });
}

async fn fetch_snapshot(client: &Client) -> Result<Snapshot> {
    let workers = client.list_workers().await?;
    anyhow::ensure!(
        workers.len() <= MAX_WORKERS,
        "manager returned too many workers ({})",
        workers.len()
    );
    let worker_ids: Vec<String> = workers.into_iter().map(|worker| worker.id).collect();
    let requests = stream::iter(worker_ids.clone().into_iter().map(|worker_id| async move {
        let result = client.list_jobs_of_worker(&worker_id).await;
        (worker_id, result)
    }))
    .buffer_unordered(MAX_CONCURRENT_WORKER_FETCHES);

    let mut jobs = Vec::new();
    let mut unavailable_workers = Vec::new();
    futures::pin_mut!(requests);
    while let Some((worker_id, result)) = requests.next().await {
        match result {
            Ok(mut worker_jobs) => {
                for job in &mut worker_jobs {
                    if job.worker.is_empty() {
                        job.worker.clone_from(&worker_id);
                    }
                }
                jobs.extend(worker_jobs);
            }
            Err(_) => unavailable_workers.push(worker_id),
        }
    }

    Ok(Snapshot {
        workers: worker_ids,
        jobs,
        unavailable_workers,
    })
}

fn handle_background(
    event: BackgroundEvent,
    client: &Client,
    background_tx: &mpsc::Sender<BackgroundEvent>,
    app: &mut App,
) {
    match event {
        BackgroundEvent::Snapshot(Ok(snapshot)) => {
            app.refreshing = false;
            let unavailable_workers = snapshot.unavailable_workers.len();
            app.apply_snapshot(snapshot);
            app.last_refresh = Some(chrono::Local::now());
            app.status_message = if unavailable_workers == 0 {
                localized(app.zh, "Manager state refreshed.", "Manager 状态已刷新。").to_owned()
            } else {
                localized(
                    app.zh,
                    "Some workers were unavailable; retaining their previous rows.",
                    "部分 Worker 不可用，继续显示其上次数据。",
                )
                .to_owned()
            };
        }
        BackgroundEvent::Snapshot(Err(error)) => {
            app.refreshing = false;
            app.status_message = if app.zh {
                format!("刷新失败，继续显示上次数据：{error}")
            } else {
                format!("Refresh failed; keeping the last snapshot: {error}")
            };
        }
        BackgroundEvent::Command {
            verb,
            mirror,
            result,
        } => {
            app.command_in_flight = false;
            match result {
                Ok(()) => {
                    let action = format_command(verb, app.zh);
                    app.status_message = if app.zh {
                        format!("已向镜像 {mirror:?} 发送{action}命令。")
                    } else {
                        format!("Sent {action} command for mirror {mirror:?}.")
                    };
                    spawn_refresh(client, background_tx, app);
                }
                Err(error) => {
                    let action = format_command(verb, app.zh);
                    app.status_message = if app.zh {
                        format!("镜像 {mirror:?} 的{action}命令失败：{error}")
                    } else {
                        format!("{action} command failed for mirror {mirror:?}: {error}")
                    };
                }
            }
        }
    }
}

fn handle_key(
    key: KeyEvent,
    client: &Client,
    background_tx: &mpsc::Sender<BackgroundEvent>,
    app: &mut App,
) {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.should_quit = true;
        return;
    }

    if app.pending.is_some() {
        match key.code {
            KeyCode::Enter | KeyCode::Char('y') => send_pending(client, background_tx, app),
            KeyCode::Esc | KeyCode::Char('n') => app.pending = None,
            _ => {}
        }
        return;
    }

    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Down | KeyCode::Char('j') => app.select_next(),
        KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
        KeyCode::Char('w') => app.cycle_worker_filter(),
        KeyCode::Char('s') => app.cycle_status_filter(),
        KeyCode::Char('r') => spawn_refresh(client, background_tx, app),
        KeyCode::Char('a') => app.begin_action(CmdVerb::Start),
        KeyCode::Char('x') => app.begin_action(CmdVerb::Stop),
        KeyCode::Char('R') => app.begin_action(CmdVerb::Restart),
        KeyCode::Char('d') => app.begin_action(CmdVerb::Disable),
        _ => {}
    }
}

fn send_pending(client: &Client, background_tx: &mpsc::Sender<BackgroundEvent>, app: &mut App) {
    let Some(command) = app.pending.take() else {
        return;
    };
    if app.command_in_flight {
        return;
    }
    app.command_in_flight = true;
    app.status_message = localized(app.zh, "Sending command...", "正在发送命令...").to_owned();
    let client = client.clone();
    let background_tx = background_tx.clone();
    tokio::spawn(async move {
        let verb = command.cmd;
        let mirror = command.mirror_id.clone();
        let result = client
            .send_cmd(command)
            .await
            .map(|_| ())
            .map_err(|error| sanitized_error(&client, &error));
        let _ = background_tx
            .send(BackgroundEvent::Command {
                verb,
                mirror,
                result,
            })
            .await;
    });
}

fn redact_url(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    if let Ok(mut url) = reqwest::Url::parse(value) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return url.to_string();
    }

    let value = value.split(['?', '#']).next().unwrap_or_default();
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}://{authority}{path}")
}

fn validate_transport(client: &Client, zh: bool) -> Result<()> {
    if client.api_token.is_empty() {
        return Ok(());
    }
    let url = reqwest::Url::parse(&client.base_url).context("parse manager URL")?;
    if url.scheme() != "http" {
        return Ok(());
    }
    let is_loopback = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    anyhow::ensure!(
        is_loopback,
        "{}",
        localized(
            zh,
            "refusing to send the API token over remote cleartext HTTP; use HTTPS or a loopback manager address",
            "拒绝通过远程明文 HTTP 发送 API token；请使用 HTTPS 或 loopback manager 地址"
        )
    );
    Ok(())
}

fn sanitized_error(client: &Client, error: &anyhow::Error) -> String {
    let mut message = format!("{error:#}");
    message = message.replace(&client.base_url, &redact_url(&client.base_url));
    if !client.api_token.is_empty() {
        message = message.replace(&client.api_token, "[redacted]");
    }
    safe_display_text(&message, 240)
}

fn sanitize_display(value: &str, max_graphemes: usize) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| if ch.is_control() { '�' } else { ch })
        .collect();
    let mut graphemes = sanitized.graphemes(true);
    let mut output = String::new();
    for _ in 0..max_graphemes {
        let Some(grapheme) = graphemes.next() else {
            return output;
        };
        output.push_str(grapheme);
    }
    if graphemes.next().is_some() {
        output.push('…');
    }
    output
}

fn redact_urls_in_text(value: &str) -> String {
    value
        .split_inclusive(char::is_whitespace)
        .map(|segment| {
            let content = segment.trim_end_matches(char::is_whitespace);
            let whitespace = &segment[content.len()..];
            let Some(scheme_end) = content.find("://") else {
                return segment.to_owned();
            };
            let scheme_start = content[..scheme_end]
                .rfind(|ch: char| !ch.is_ascii_alphanumeric() && !matches!(ch, '+' | '-' | '.'))
                .map_or(0, |index| index + 1);
            let (prefix, url) = content.split_at(scheme_start);
            format!("{prefix}{}{whitespace}", redact_url(url))
        })
        .collect()
}

fn safe_display_text(value: &str, max_graphemes: usize) -> String {
    sanitize_display(&redact_urls_in_text(value), max_graphemes)
}

fn format_command(verb: CmdVerb, zh: bool) -> &'static str {
    match (zh, verb) {
        (true, CmdVerb::Start) => "启动",
        (true, CmdVerb::Stop) => "停止",
        (true, CmdVerb::Restart) => "重启",
        (true, CmdVerb::Disable) => "禁用",
        (true, CmdVerb::Ping) => "探测",
        (true, CmdVerb::Reload) => "重载",
        (false, CmdVerb::Start) => "start",
        (false, CmdVerb::Stop) => "stop",
        (false, CmdVerb::Restart) => "restart",
        (false, CmdVerb::Disable) => "disable",
        (false, CmdVerb::Ping) => "ping",
        (false, CmdVerb::Reload) => "reload",
    }
}

fn localized<'a>(zh: bool, en: &'a str, zh_text: &'a str) -> &'a str {
    if zh {
        zh_text
    } else {
        en
    }
}

#[cfg(test)]
mod tests;
