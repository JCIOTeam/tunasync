use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Row, Table, Wrap};
use ratatui::Frame;
use tunasync_protocol::{ClientCmd, MirrorStatus, SyncStatus};

use super::{format_command, localized, redact_url, safe_display_text, App};
use crate::{fmt_status, fmt_time};

pub(super) fn render(frame: &mut Frame<'_>, app: &mut App, manager: &str) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    render_header(frame, app, manager, chunks[0]);
    render_jobs(frame, app, chunks[1]);
    render_detail(frame, app, chunks[2]);
    render_footer(frame, app, chunks[3]);

    if let Some(command) = &app.pending {
        render_confirmation(frame, command, app.zh);
    }
}

fn render_header(frame: &mut Frame<'_>, app: &App, manager: &str, area: Rect) {
    let manager = safe_display_text(manager, 160);
    let refreshed = app
        .last_refresh
        .map(|time| time.format("%H:%M:%S").to_string())
        .unwrap_or_else(|| "--:--:--".to_owned());
    let activity = if app.refreshing {
        localized(app.zh, "refreshing", "刷新中")
    } else if app.command_in_flight {
        localized(app.zh, "command pending", "命令执行中")
    } else {
        localized(app.zh, "ready", "就绪")
    };
    let title = if app.zh {
        format!(
            " tunasynctl | {manager} | {activity} | 更新 {refreshed} | Worker: {} | 状态: {} ",
            safe_display_text(app.worker_filter_label(), 80),
            app.status_filter_label()
        )
    } else {
        format!(
            " tunasynctl | {manager} | {activity} | updated {refreshed} | worker: {} | status: {} ",
            safe_display_text(app.worker_filter_label(), 80),
            app.status_filter_label()
        )
    };
    frame.render_widget(
        Paragraph::new(title)
            .style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_jobs(frame: &mut Frame<'_>, app: &mut App, area: Rect) {
    let rows = app.visible_jobs().into_iter().map(|job| {
        Row::new(vec![
            safe_display_text(&job.name, 120),
            safe_display_text(&job.worker, 120),
            fmt_status(&job.status),
            fmt_time(&job.last_update),
            display_or_dash(&safe_display_text(&job.size, 60)),
        ])
        .style(status_style(job.status))
    });
    let headings = if app.zh {
        ["镜像", "Worker", "状态", "最后更新", "大小"]
    } else {
        ["Mirror", "Worker", "Status", "Last update", "Size"]
    };
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(28),
            Constraint::Percentage(20),
            Constraint::Length(12),
            Constraint::Length(20),
            Constraint::Min(8),
        ],
    )
    .header(
        Row::new(headings).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(Block::default().borders(Borders::ALL).title(if app.zh {
        format!(" 镜像 ({}) ", app.visible_job_indices().len())
    } else {
        format!(" Mirrors ({}) ", app.visible_job_indices().len())
    }))
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("› ");
    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn render_detail(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let text = app.selected_job().map_or_else(
        || {
            Text::from(localized(
                app.zh,
                "No mirror matches the current filters.",
                "当前筛选条件下没有镜像。",
            ))
        },
        |job| Text::from(detail_lines(job, app.zh)),
    );
    frame.render_widget(
        Paragraph::new(text).wrap(Wrap { trim: true }).block(
            Block::default()
                .borders(Borders::ALL)
                .title(localized(app.zh, " Detail ", " 详情 ")),
        ),
        area,
    );
}

fn detail_lines(job: &MirrorStatus, zh: bool) -> Vec<Line<'static>> {
    let upstream = redact_url(&job.upstream);
    let name = safe_display_text(&job.name, 120);
    let worker = safe_display_text(&job.worker, 120);
    let upstream = safe_display_text(&upstream, 500);
    let error = safe_display_text(&job.error_msg, 1000);
    let master = yes_no(zh, job.is_master);
    let stale = yes_no(zh, job.stale);
    if zh {
        vec![
            Line::from(format!(
                "名称: {}    Worker: {}    状态: {}",
                name,
                worker,
                fmt_status(&job.status)
            )),
            Line::from(format!(
                "主节点: {master}    Stale: {stale}    连续失败: {}",
                job.consecutive_failures
            )),
            Line::from(format!(
                "开始: {}    结束: {}",
                fmt_time(&job.last_started),
                fmt_time(&job.last_ended)
            )),
            Line::from(format!(
                "下次运行: {}    最近传输: {}",
                fmt_time(&job.scheduled),
                format_bytes(job.last_transferred_bytes)
            )),
            Line::from(format!("Upstream: {}", display_or_dash(&upstream))),
            Line::from(format!("错误: {}", display_or_dash(&error))),
        ]
    } else {
        vec![
            Line::from(format!(
                "Name: {}    Worker: {}    Status: {}",
                name,
                worker,
                fmt_status(&job.status)
            )),
            Line::from(format!(
                "Master: {master}    Stale: {stale}    Consecutive failures: {}",
                job.consecutive_failures
            )),
            Line::from(format!(
                "Started: {}    Ended: {}",
                fmt_time(&job.last_started),
                fmt_time(&job.last_ended)
            )),
            Line::from(format!(
                "Next run: {}    Last transferred: {}",
                fmt_time(&job.scheduled),
                format_bytes(job.last_transferred_bytes)
            )),
            Line::from(format!("Upstream: {}", display_or_dash(&upstream))),
            Line::from(format!("Error: {}", display_or_dash(&error))),
        ]
    }
}

fn render_footer(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let help = localized(
        app.zh,
        "↑/↓ j/k select  w worker  s status  r refresh  a start  x stop  R restart  d disable  q quit",
        "↑/↓ j/k 选择  w Worker  s 状态  r 刷新  a 启动  x 停止  R 重启  d 禁用  q 退出",
    );
    let text = Text::from(vec![
        Line::from(Span::styled(
            safe_display_text(&app.status_message, 500),
            Style::default().fg(Color::Cyan),
        )),
        Line::from(help),
    ]);
    frame.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn render_confirmation(frame: &mut Frame<'_>, command: &ClientCmd, zh: bool) {
    let area = centered_rect(64, 7, frame.area());
    let mirror = safe_display_text(&command.mirror_id, 120);
    let worker = safe_display_text(&command.worker_id, 120);
    let action = format_command(command.cmd, zh);
    let prompt = if zh {
        format!(
            "确认对镜像 {mirror:?}（Worker {worker:?}）执行{action}？\n\nEnter/y 确认，Esc/n 取消"
        )
    } else {
        format!(
            "Run {action} on mirror {mirror:?} (worker {worker:?})?\n\nEnter/y confirms, Esc/n cancels"
        )
    };
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(prompt)
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Yellow))
                    .title(localized(zh, " Confirm action ", " 确认操作 ")),
            ),
        area,
    );
}

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let vertical_margin = area.height.saturating_sub(height) / 2;
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(vertical_margin),
            Constraint::Length(height.min(area.height)),
            Constraint::Min(0),
        ])
        .split(area);
    let horizontal_margin = 100u16.saturating_sub(percent_x) / 2;
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(horizontal_margin),
            Constraint::Percentage(percent_x),
            Constraint::Percentage(horizontal_margin),
        ])
        .split(vertical[1])[1]
}

fn status_style(status: SyncStatus) -> Style {
    let color = match status {
        SyncStatus::Success => Color::Green,
        SyncStatus::Failed => Color::Red,
        SyncStatus::Syncing | SyncStatus::PreSyncing => Color::Cyan,
        SyncStatus::Paused => Color::Yellow,
        SyncStatus::Disabled => Color::DarkGray,
        SyncStatus::None => Color::White,
    };
    Style::default().fg(color)
}

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "-".to_owned();
    }
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn display_or_dash(value: &str) -> String {
    if value.is_empty() {
        "-".to_owned()
    } else {
        value.to_owned()
    }
}

fn yes_no(zh: bool, value: bool) -> &'static str {
    match (zh, value) {
        (true, true) => "是",
        (true, false) => "否",
        (false, true) => "yes",
        (false, false) => "no",
    }
}
