//! Blackout time-window parser and checker.
//!
//! Mirrors can declare `blackout = ["08:00-18:00 Mon-Fri"]` in their config to
//! suppress new syncs during busy hours.  In-progress syncs are never
//! interrupted — the check only happens before a sync is *started*.
//!
//! Grammar (lenient): `HH:MM-HH:MM [<days>]`
//!
//! `<days>` is optional (defaults to every day) and accepts:
//! - `Mon-Fri`, `Mon-Sun`, `Sat-Sun`, or any two-letter weekday abbreviation
//!   range using `-` as the separator.
//! - `daily` — equivalent to omitting the day specifier.
//!
//! Midnight wraparound is supported: `22:00-04:00` covers 22:00–23:59 and
//! 00:00–04:00 on the applicable weekdays.
//!
//! # Weekday semantics with wraparound windows
//!
//! The weekday filter applies to the day **the clock currently shows**, not
//! the day the window "started". `22:00-04:00 Mon` therefore means
//! *Monday 00:00–04:00* ∪ *Monday 22:00–24:00* — it does **not** extend into
//! Tuesday's early hours. To cover "Monday night through Tuesday 04:00",
//! use a two-day weekday range:
//!
//! ```toml
//! blackout = ["22:00-04:00 Mon-Tue"]
//! ```
//!
//! Note this also covers Monday 00:00–04:00 and Tuesday 22:00–24:00; if that
//! symmetric coverage is unacceptable, split into per-day windows
//! (`"22:00-23:59 Mon"`, `"00:00-04:00 Tue"` — the end is exclusive, so the
//! final minute 23:59–24:00 is not covered by the first window).

use chrono::{Datelike, NaiveTime, Timelike, Weekday};

/// A single parsed blackout window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlackoutWindow {
    /// Start of the window (inclusive).
    pub start: NaiveTime,
    /// End of the window (exclusive).
    pub end: NaiveTime,
    /// Weekdays this window applies to.  Empty means every day.
    pub weekdays: Vec<Weekday>,
}

impl BlackoutWindow {
    /// Parse a single blackout string such as `"08:00-18:00 Mon-Fri"`.
    ///
    /// Returns `None` for strings that don't match the expected format.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        // Split on whitespace: first token is the time range, rest is days.
        let mut parts = s.splitn(2, char::is_whitespace);
        let time_part = parts.next()?;
        let day_part = parts.next().map(str::trim).unwrap_or("");

        // Parse time range "HH:MM-HH:MM".
        let mut times = time_part.splitn(2, '-');
        let start = parse_hhmm(times.next()?)?;
        let end = parse_hhmm(times.next()?)?;

        // Parse optional weekday range.
        let weekdays = if day_part.is_empty() || day_part.eq_ignore_ascii_case("daily") {
            vec![]
        } else {
            parse_weekday_range(day_part)?
        };

        Some(BlackoutWindow {
            start,
            end,
            weekdays,
        })
    }

    /// Return `true` if `now` falls inside this blackout window.
    pub fn is_active_at<Tz>(&self, now: &chrono::DateTime<Tz>) -> bool
    where
        Tz: chrono::TimeZone,
    {
        // Check weekday first (fast path).
        if !self.weekdays.is_empty() {
            let wd = now.weekday();
            if !self.weekdays.contains(&wd) {
                return false;
            }
        }

        let t = NaiveTime::from_hms_opt(now.hour(), now.minute(), 0).unwrap_or(self.start);

        if self.start <= self.end {
            // Normal range (e.g. 08:00-18:00).
            t >= self.start && t < self.end
        } else {
            // Midnight wraparound (e.g. 22:00-04:00):
            // active if t >= 22:00 OR t < 04:00.
            t >= self.start || t < self.end
        }
    }
}

/// Parse a list of blackout strings from a mirror config.
///
/// Strings that fail to parse are logged at `warn` level and skipped so
/// a single bad entry doesn't disable the mirror entirely.
pub fn parse_blackout_windows(strings: &[String]) -> Vec<BlackoutWindow> {
    strings
        .iter()
        .filter_map(|s| {
            let w = BlackoutWindow::parse(s);
            if w.is_none() {
                tracing::warn!(blackout = %s, "could not parse blackout window — skipping");
            }
            w
        })
        .collect()
}

/// Return `true` if any window in `windows` is active at `now`.
pub fn is_in_blackout<Tz>(windows: &[BlackoutWindow], now: &chrono::DateTime<Tz>) -> bool
where
    Tz: chrono::TimeZone,
{
    windows.iter().any(|w| w.is_active_at(now))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn parse_hhmm(s: &str) -> Option<NaiveTime> {
    let s = s.trim();
    let mut parts = s.splitn(2, ':');
    let h: u32 = parts.next()?.trim().parse().ok()?;
    let m: u32 = parts.next()?.trim().parse().ok()?;
    NaiveTime::from_hms_opt(h, m, 0)
}

fn parse_weekday_range(s: &str) -> Option<Vec<Weekday>> {
    // Expect "Mon-Fri", "Sat-Sun", etc.  We also accept single days like "Mon".
    let parts: Vec<&str> = s.split('-').collect();
    match parts.as_slice() {
        [single] => {
            let wd = parse_weekday(single)?;
            Some(vec![wd])
        }
        [start, end] => {
            let from = parse_weekday(start)?;
            let to = parse_weekday(end)?;
            // Enumerate the range from → to (wrapping Sun→Mon if needed).
            let mut result = Vec::new();
            let mut cur = from;
            loop {
                result.push(cur);
                if cur == to {
                    break;
                }
                cur = next_weekday(cur);
                // Guard against infinite loop if from == to after first iteration.
                if result.len() > 7 {
                    break;
                }
            }
            Some(result)
        }
        _ => None,
    }
}

fn parse_weekday(s: &str) -> Option<Weekday> {
    match s.trim().to_ascii_lowercase().as_str() {
        "mon" | "monday" => Some(Weekday::Mon),
        "tue" | "tuesday" => Some(Weekday::Tue),
        "wed" | "wednesday" => Some(Weekday::Wed),
        "thu" | "thursday" => Some(Weekday::Thu),
        "fri" | "friday" => Some(Weekday::Fri),
        "sat" | "saturday" => Some(Weekday::Sat),
        "sun" | "sunday" => Some(Weekday::Sun),
        _ => None,
    }
}

fn next_weekday(wd: Weekday) -> Weekday {
    match wd {
        Weekday::Mon => Weekday::Tue,
        Weekday::Tue => Weekday::Wed,
        Weekday::Wed => Weekday::Thu,
        Weekday::Thu => Weekday::Fri,
        Weekday::Fri => Weekday::Sat,
        Weekday::Sat => Weekday::Sun,
        Weekday::Sun => Weekday::Mon,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc, Weekday};

    fn utc(year: i32, month: u32, day: u32, h: u32, m: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, h, m, 0).unwrap()
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    #[test]
    fn parse_weekday_range_mon_fri() {
        let w = BlackoutWindow::parse("08:00-18:00 Mon-Fri").unwrap();
        assert_eq!(w.start, NaiveTime::from_hms_opt(8, 0, 0).unwrap());
        assert_eq!(w.end, NaiveTime::from_hms_opt(18, 0, 0).unwrap());
        assert_eq!(
            w.weekdays,
            vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri
            ]
        );
    }

    #[test]
    fn parse_sat_sun() {
        let w = BlackoutWindow::parse("00:00-23:59 Sat-Sun").unwrap();
        assert_eq!(w.weekdays, vec![Weekday::Sat, Weekday::Sun]);
    }

    #[test]
    fn parse_daily_keyword() {
        let w = BlackoutWindow::parse("02:00-06:00 daily").unwrap();
        assert!(w.weekdays.is_empty(), "daily should produce empty weekdays");
    }

    #[test]
    fn parse_no_day_specifier() {
        let w = BlackoutWindow::parse("01:00-05:00").unwrap();
        assert!(w.weekdays.is_empty());
    }

    #[test]
    fn parse_invalid_returns_none() {
        assert!(BlackoutWindow::parse("not a window").is_none());
        assert!(BlackoutWindow::parse("25:00-26:00").is_none());
        assert!(BlackoutWindow::parse("").is_none());
    }

    // ── is_active_at ──────────────────────────────────────────────────────────

    #[test]
    fn active_during_window_on_matching_weekday() {
        // 2024-01-15 is a Monday.
        let w = BlackoutWindow::parse("08:00-18:00 Mon-Fri").unwrap();
        let inside = utc(2024, 1, 15, 12, 0); // Monday 12:00
        assert!(w.is_active_at(&inside));
    }

    #[test]
    fn inactive_outside_time_on_matching_weekday() {
        let w = BlackoutWindow::parse("08:00-18:00 Mon-Fri").unwrap();
        let outside = utc(2024, 1, 15, 20, 0); // Monday 20:00
        assert!(!w.is_active_at(&outside));
    }

    #[test]
    fn inactive_during_window_on_nonmatching_weekday() {
        let w = BlackoutWindow::parse("08:00-18:00 Mon-Fri").unwrap();
        // 2024-01-20 is a Saturday.
        let sat = utc(2024, 1, 20, 12, 0);
        assert!(!w.is_active_at(&sat));
    }

    #[test]
    fn midnight_wraparound_active_before_midnight() {
        let w = BlackoutWindow::parse("22:00-04:00").unwrap();
        let before_midnight = utc(2024, 1, 15, 23, 0);
        assert!(w.is_active_at(&before_midnight));
    }

    #[test]
    fn midnight_wraparound_active_after_midnight() {
        let w = BlackoutWindow::parse("22:00-04:00").unwrap();
        let after_midnight = utc(2024, 1, 15, 3, 0);
        assert!(w.is_active_at(&after_midnight));
    }

    #[test]
    fn midnight_wraparound_inactive_in_middle_of_day() {
        let w = BlackoutWindow::parse("22:00-04:00").unwrap();
        let noon = utc(2024, 1, 15, 12, 0);
        assert!(!w.is_active_at(&noon));
    }

    #[test]
    fn daily_window_active_on_any_weekday() {
        let w = BlackoutWindow::parse("00:00-23:59 daily").unwrap();
        // Saturday — must still fire.
        let sat = utc(2024, 1, 20, 10, 0);
        assert!(w.is_active_at(&sat));
    }

    // ── is_in_blackout ────────────────────────────────────────────────────────

    #[test]
    fn is_in_blackout_returns_true_when_any_window_matches() {
        let windows =
            parse_blackout_windows(&["02:00-06:00".to_string(), "08:00-18:00 Mon-Fri".to_string()]);
        // Monday 12:00 → second window matches.
        let t = utc(2024, 1, 15, 12, 0);
        assert!(is_in_blackout(&windows, &t));
    }

    #[test]
    fn is_in_blackout_returns_false_when_no_window_matches() {
        let windows = parse_blackout_windows(&["08:00-18:00 Mon-Fri".to_string()]);
        // Saturday 12:00 → no match.
        let t = utc(2024, 1, 20, 12, 0);
        assert!(!is_in_blackout(&windows, &t));
    }
}
