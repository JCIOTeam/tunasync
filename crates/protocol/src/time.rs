//! Time helpers for Go-compatible JSON wire format.
//!
//! Go's `time.Time{}` zero value serialises to `"0001-01-01T00:00:00Z"`
//! (Year 1, January 1st, midnight UTC). The Go tunasync codebase relies on
//! this zero value to mean "field not yet populated" — for example, a freshly
//! registered worker has `last_online == time.Time{}` until its first ping.
//!
//! `chrono::DateTime<Utc>::default()` is the Unix epoch (1970), which is a
//! *different* sentinel value, so we cannot use `Default::default` for this
//! purpose. Instead, populate fields with [`zero_time`] when no value is yet
//! available, and check with [`is_zero_time`].
//!
//! Both Go and `chrono` use RFC 3339 with auto-suppressed trailing zeros, so
//! values produced by either side parse cleanly on the other.

use chrono::{DateTime, TimeZone, Utc};

/// Returns the Go `time.Time{}` zero value: `0001-01-01T00:00:00Z`.
pub fn zero_time() -> DateTime<Utc> {
    // Year 1 is well within `chrono::DateTime<Utc>`'s representable range
    // (≈ ±262 144 years from the epoch), so this never panics.
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("0001-01-01T00:00:00Z is a valid UTC datetime")
}

/// Returns `true` if `t` equals the Go zero-value timestamp.
pub fn is_zero_time(t: &DateTime<Utc>) -> bool {
    *t == zero_time()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_time_serialises_like_go() {
        let zero = zero_time();
        let json = serde_json::to_string(&zero).unwrap();
        assert_eq!(json, r#""0001-01-01T00:00:00Z""#);
    }

    #[test]
    fn parses_go_zero_value() {
        let parsed: DateTime<Utc> = serde_json::from_str(r#""0001-01-01T00:00:00Z""#).unwrap();
        assert!(is_zero_time(&parsed));
    }

    #[test]
    fn unix_epoch_is_not_zero_time() {
        let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        assert!(!is_zero_time(&epoch));
    }

    #[test]
    fn nanosecond_precision_round_trip() {
        // Go emits RFC 3339 with up to nanosecond precision, e.g. tunasync's
        // last_update timestamp. Make sure chrono preserves it.
        let original = "2024-06-15T10:30:45.123456789Z";
        let parsed: DateTime<Utc> = serde_json::from_str(&format!(r#""{original}""#)).unwrap();
        let back = serde_json::to_string(&parsed).unwrap();
        assert_eq!(back, format!(r#""{original}""#));
    }
}
