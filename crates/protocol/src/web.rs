//! Presentational mirror status for the `/jobs` HTTP endpoint.
//!
//! Go's `internal/status_web.go` defines `WebMirrorStatus` with two time
//! representations per timestamp:
//!
//! - a **human-readable** string field (`"2006-01-02 15:04:05 -0700"`)
//! - a **unix-timestamp** integer `_ts` field
//!
//! Status pages and the TUNA mirrors website consume this format, so we must
//! reproduce it exactly.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::status::SyncStatus;
use crate::MirrorStatus;

// ---------------------------------------------------------------------------
// Serialisation helpers for Go's two custom time formats
// ---------------------------------------------------------------------------

/// Serialise a `DateTime<Utc>` as `"YYYY-MM-DD HH:MM:SS +HHMM"`.
///
/// Matches Go's `textTime.MarshalJSON`:
/// `t.Format("2006-01-02 15:04:05 -0700")`
mod text_time {
    use super::*;
    use serde::Deserializer;

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        // chrono %z = "+HHMM" (no colon), matching Go's "-0700" token.
        s.serialize_str(&t.format("%Y-%m-%d %H:%M:%S %z").to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        use serde::de::Error;
        let s = String::deserialize(d)?;
        DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S %z")
            .map(|t| t.with_timezone(&Utc))
            .map_err(Error::custom)
    }
}

/// Serialise a `DateTime<Utc>` as a Unix timestamp (integer seconds).
///
/// Matches Go's `stampTime.MarshalJSON`: `json.Marshal(t.Unix())`.
mod stamp_time {
    use super::*;
    use serde::Deserializer;

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(t.timestamp())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        use serde::de::Error;
        let ts = i64::deserialize(d)?;
        DateTime::from_timestamp(ts, 0)
            .ok_or_else(|| Error::custom(format!("invalid unix timestamp: {ts}")))
    }
}

// ---------------------------------------------------------------------------
// WebMirrorStatus
// ---------------------------------------------------------------------------

/// Presentational mirror status returned by `GET /jobs`.
///
/// Wire-compatible with Go's `internal.WebMirrorStatus`. Each timestamp
/// appears twice: once as a human-readable string and once as a unix integer.
/// Note that `worker` is intentionally absent — status pages show per-mirror
/// info without exposing which worker owns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebMirrorStatus {
    /// Mirror name.
    pub name: String,
    /// Whether this worker is the master for the mirror.
    pub is_master: bool,
    /// Current sync status.
    pub status: SyncStatus,

    /// Human-readable last-update timestamp.
    #[serde(with = "text_time")]
    pub last_update: DateTime<Utc>,
    /// Unix timestamp of last update.
    #[serde(with = "stamp_time")]
    pub last_update_ts: DateTime<Utc>,

    /// Human-readable last-started timestamp.
    #[serde(with = "text_time")]
    pub last_started: DateTime<Utc>,
    /// Unix timestamp of last start.
    #[serde(with = "stamp_time")]
    pub last_started_ts: DateTime<Utc>,

    /// Human-readable last-ended timestamp.
    #[serde(with = "text_time")]
    pub last_ended: DateTime<Utc>,
    /// Unix timestamp of last end.
    #[serde(with = "stamp_time")]
    pub last_ended_ts: DateTime<Utc>,

    /// Next scheduled run, human-readable.
    #[serde(rename = "next_schedule", with = "text_time")]
    pub scheduled: DateTime<Utc>,
    /// Next scheduled run, unix timestamp.
    #[serde(rename = "next_schedule_ts", with = "stamp_time")]
    pub scheduled_ts: DateTime<Utc>,

    /// Upstream URL.
    pub upstream: String,
    /// Approximate mirror size (e.g. `"1.2T"`).
    pub size: String,
}

impl WebMirrorStatus {
    /// Build a `WebMirrorStatus` from the internal [`MirrorStatus`] wire type.
    ///
    /// Mirrors Go's `BuildWebMirrorStatus`.
    pub fn from_mirror_status(m: &MirrorStatus) -> Self {
        Self {
            name: m.name.clone(),
            is_master: m.is_master,
            status: m.status,
            last_update: m.last_update,
            last_update_ts: m.last_update,
            last_started: m.last_started,
            last_started_ts: m.last_started,
            last_ended: m.last_ended,
            last_ended_ts: m.last_ended,
            scheduled: m.scheduled,
            scheduled_ts: m.scheduled,
            upstream: m.upstream.clone(),
            size: m.size.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the human-readable format matches Go's `textTime.MarshalJSON` output.
    #[test]
    fn text_time_format_matches_go() {
        use chrono::TimeZone;
        let t = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        let ws = WebMirrorStatus {
            name: "test".into(),
            is_master: true,
            status: SyncStatus::Success,
            last_update: t,
            last_update_ts: t,
            last_started: t,
            last_started_ts: t,
            last_ended: t,
            last_ended_ts: t,
            scheduled: t,
            scheduled_ts: t,
            upstream: "rsync://example.com/".into(),
            size: "1.0T".into(),
        };
        let json: serde_json::Value = serde_json::from_str(&serde_json::to_string(&ws).unwrap()).unwrap();
        // Go produces "2024-06-15 10:30:45 +0000" for UTC.
        assert_eq!(json["last_update"], "2024-06-15 10:30:45 +0000");
        // Unix timestamp
        assert_eq!(json["last_update_ts"], t.timestamp());
    }
}
