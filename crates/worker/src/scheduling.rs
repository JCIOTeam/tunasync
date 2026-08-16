use std::time::Duration as StdDuration;

use chrono::{DateTime, Days, Duration, LocalResult, NaiveDateTime, NaiveTime, TimeZone, Utc};

/// Pure scheduling calculation detached from wall-clock and Tokio time.
#[derive(Clone)]
pub enum SchedulingPolicy {
    FixedDelay {
        interval: StdDuration,
    },
    Cron {
        schedule: Box<cron::Schedule>,
        timezone: chrono_tz::Tz,
    },
    FixedRate {
        interval_minutes: u64,
        anchor: NaiveTime,
        timezone: chrono_tz::Tz,
    },
}

impl SchedulingPolicy {
    pub fn fixed_delay(interval: StdDuration) -> Self {
        Self::FixedDelay { interval }
    }

    pub fn cron(schedule: cron::Schedule, timezone: chrono_tz::Tz) -> Self {
        Self::Cron {
            schedule: Box::new(schedule),
            timezone,
        }
    }

    pub fn fixed_rate(interval_minutes: u64, anchor: NaiveTime, timezone: chrono_tz::Tz) -> Self {
        Self::FixedRate {
            interval_minutes,
            anchor,
            timezone,
        }
    }

    /// Calculate the first occurrence strictly after `after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::FixedDelay { interval } => Duration::from_std(*interval)
                .ok()
                .and_then(|delta| after.checked_add_signed(delta)),
            Self::Cron { schedule, timezone } => schedule
                .after(&after.with_timezone(timezone))
                .next()
                .map(|next| next.with_timezone(&Utc)),
            Self::FixedRate {
                interval_minutes,
                anchor,
                timezone,
            } => next_fixed_rate(*interval_minutes, *anchor, *timezone, after),
        }
    }

    pub fn is_fixed_delay(&self) -> bool {
        matches!(self, Self::FixedDelay { .. })
    }

    pub fn is_wall_clock(&self) -> bool {
        !self.is_fixed_delay()
    }

    /// Startup behavior: fixed-delay preserves immediate/last-completion
    /// semantics, while wall-clock policies choose the next future slot.
    pub fn next_startup_after(
        &self,
        now: DateTime<Utc>,
        last_completion: Option<DateTime<Utc>>,
    ) -> Option<DateTime<Utc>> {
        if self.is_fixed_delay() {
            return match last_completion {
                Some(completed) => self.next_after(completed),
                None => Some(now),
            };
        }
        self.next_after(now)
    }

    /// Reload Add/Modify behavior: fixed-delay may run immediately; wall-clock
    /// policies wait for their next future occurrence.
    pub fn next_reload_after(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.is_fixed_delay() {
            Some(now)
        } else {
            self.next_after(now)
        }
    }

    /// Blackout behavior: fixed-delay retries in five minutes; wall-clock
    /// policies skip the blocked occurrence.
    pub fn next_after_blackout(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if self.is_fixed_delay() {
            Some(now + Duration::minutes(5))
        } else {
            self.next_after(now)
        }
    }
}

/// Parse exactly `HH:MM` with zero-padded decimal fields.
pub fn parse_fixed_rate_anchor(value: &str) -> Result<NaiveTime, String> {
    if value.len() != 5 || value.as_bytes()[2] != b':' {
        return Err("anchor must use strict HH:MM format".into());
    }
    if !value[..2].bytes().all(|b| b.is_ascii_digit())
        || !value[3..].bytes().all(|b| b.is_ascii_digit())
    {
        return Err("anchor must use strict HH:MM format".into());
    }
    NaiveTime::parse_from_str(value, "%H:%M")
        .map_err(|_| "anchor must be a valid local time in strict HH:MM format".into())
}

fn next_fixed_rate(
    interval_minutes: u64,
    anchor: NaiveTime,
    timezone: chrono_tz::Tz,
    after: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let slots = 1440 / interval_minutes;
    let anchor_minutes = u64::from(anchor.hour()) * 60 + u64::from(anchor.minute());
    let mut minute_slots: Vec<u64> = (0..slots)
        .map(|index| (anchor_minutes + index * interval_minutes) % 1440)
        .collect();
    minute_slots.sort_unstable();

    let mut date = after.with_timezone(&timezone).date_naive();
    for _ in 0..=370 {
        for minute in &minute_slots {
            let time = NaiveTime::from_hms_opt((minute / 60) as u32, (minute % 60) as u32, 0)?;
            let local = NaiveDateTime::new(date, time);
            let candidate = match timezone.from_local_datetime(&local) {
                LocalResult::Single(value) => value.with_timezone(&Utc),
                LocalResult::Ambiguous(first, second) => {
                    std::cmp::min(first.with_timezone(&Utc), second.with_timezone(&Utc))
                }
                LocalResult::None => continue,
            };
            if candidate > after {
                return Some(candidate);
            }
        }
        date = date.checked_add_days(Days::new(1))?;
    }
    None
}

use chrono::Timelike;

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, NaiveTime, Utc};

    use super::{parse_fixed_rate_anchor, SchedulingPolicy};

    fn utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn fixed_delay_uses_completion_plus_interval() {
        let policy = SchedulingPolicy::fixed_delay(Duration::minutes(90).to_std().unwrap());
        assert_eq!(
            policy.next_after(utc("2026-01-02T03:04:05Z")).unwrap(),
            utc("2026-01-02T04:34:05Z")
        );
    }

    #[test]
    fn cron_is_strictly_after_supplied_utc_in_effective_timezone() {
        let schedule = crate::worker::parse_cron_lenient("0 3 * * *").unwrap();
        let policy = SchedulingPolicy::cron(schedule, chrono_tz::Asia::Shanghai);
        assert_eq!(
            policy.next_after(utc("2026-01-01T19:00:00Z")).unwrap(),
            utc("2026-01-02T19:00:00Z")
        );
    }

    #[test]
    fn fixed_rate_uses_daily_anchor_and_interval_slots() {
        let policy = SchedulingPolicy::fixed_rate(
            60,
            NaiveTime::from_hms_opt(0, 15, 0).unwrap(),
            chrono_tz::UTC,
        );
        assert_eq!(
            policy.next_after(utc("2026-01-01T10:15:00Z")).unwrap(),
            utc("2026-01-01T11:15:00Z")
        );
        assert_eq!(
            policy.next_after(utc("2026-01-01T10:59:59Z")).unwrap(),
            utc("2026-01-01T11:15:00Z")
        );
    }

    #[test]
    fn fixed_rate_skips_nonexistent_dst_slot() {
        let policy = SchedulingPolicy::fixed_rate(
            1440,
            NaiveTime::from_hms_opt(2, 30, 0).unwrap(),
            chrono_tz::America::New_York,
        );
        assert_eq!(
            policy.next_after(utc("2026-03-08T05:00:00Z")).unwrap(),
            utc("2026-03-09T06:30:00Z")
        );
    }

    #[test]
    fn fixed_rate_ambiguous_slot_occurs_once_at_earlier_utc() {
        let policy = SchedulingPolicy::fixed_rate(
            1440,
            NaiveTime::from_hms_opt(1, 30, 0).unwrap(),
            chrono_tz::America::New_York,
        );
        assert_eq!(
            policy.next_after(utc("2026-11-01T04:00:00Z")).unwrap(),
            utc("2026-11-01T05:30:00Z")
        );
        assert_eq!(
            policy.next_after(utc("2026-11-01T05:30:00Z")).unwrap(),
            utc("2026-11-02T06:30:00Z")
        );
    }

    #[test]
    fn fixed_rate_anchor_parser_is_strict() {
        assert_eq!(
            parse_fixed_rate_anchor("03:07").unwrap(),
            NaiveTime::from_hms_opt(3, 7, 0).unwrap()
        );
        for invalid in ["3:07", "03:7", "03:07:00", "24:00", "03:60", " 03:07"] {
            assert!(parse_fixed_rate_anchor(invalid).is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn startup_reload_and_blackout_semantics_are_deterministic() {
        let now = utc("2026-01-01T10:00:00Z");
        let delay = SchedulingPolicy::fixed_delay(Duration::minutes(60).to_std().unwrap());
        assert_eq!(delay.next_startup_after(now, None).unwrap(), now);
        assert_eq!(
            delay
                .next_startup_after(now, Some(utc("2026-01-01T09:30:00Z")))
                .unwrap(),
            utc("2026-01-01T10:30:00Z")
        );
        assert_eq!(
            delay
                .next_startup_after(now, Some(utc("2026-01-01T08:00:00Z")))
                .unwrap(),
            utc("2026-01-01T09:00:00Z")
        );
        assert_eq!(delay.next_reload_after(now).unwrap(), now);
        assert_eq!(
            delay.next_after_blackout(now).unwrap(),
            utc("2026-01-01T10:05:00Z")
        );

        let rate = SchedulingPolicy::fixed_rate(
            60,
            NaiveTime::from_hms_opt(0, 15, 0).unwrap(),
            chrono_tz::UTC,
        );
        let next_slot = utc("2026-01-01T10:15:00Z");
        assert_eq!(rate.next_startup_after(now, None).unwrap(), next_slot);
        assert_eq!(rate.next_reload_after(now).unwrap(), next_slot);
        assert_eq!(rate.next_after_blackout(now).unwrap(), next_slot);
    }
}
