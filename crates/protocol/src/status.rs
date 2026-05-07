//! Sync status of a mirror job.

use serde::{Deserialize, Serialize};

/// Sync status of a mirror job.
///
/// Wire-compatible with Go's `internal.SyncStatus`. JSON encoding is the
/// lower-case status string (`"none"`, `"failed"`, `"success"`, `"syncing"`,
/// `"pre-syncing"`, `"paused"`, `"disabled"`).
///
/// Note that `pre-syncing` contains a hyphen — this matches Go's
/// `MarshalJSON` output exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum SyncStatus {
    /// No status known yet (e.g. job just registered, never run).
    #[default]
    #[serde(rename = "none")]
    None,
    /// Last sync failed.
    #[serde(rename = "failed")]
    Failed,
    /// Last sync succeeded.
    #[serde(rename = "success")]
    Success,
    /// Sync currently in progress (post-hooks running).
    #[serde(rename = "syncing")]
    Syncing,
    /// Pre-sync hooks running.
    #[serde(rename = "pre-syncing")]
    PreSyncing,
    /// Sync paused by an operator.
    #[serde(rename = "paused")]
    Paused,
    /// Job disabled.
    #[serde(rename = "disabled")]
    Disabled,
}

impl SyncStatus {
    /// Return the Prometheus-label-safe wire string for this status.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Failed => "failed",
            Self::Success => "success",
            Self::Syncing => "syncing",
            Self::PreSyncing => "pre-syncing",
            Self::Paused => "paused",
            Self::Disabled => "disabled",
        }
    }
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Failed => "failed",
            Self::Success => "success",
            Self::Syncing => "syncing",
            Self::PreSyncing => "pre-syncing",
            Self::Paused => "paused",
            Self::Disabled => "disabled",
        };
        f.write_str(s)
    }
}

impl std::str::FromStr for SyncStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "failed" => Ok(Self::Failed),
            "success" => Ok(Self::Success),
            "syncing" => Ok(Self::Syncing),
            "pre-syncing" => Ok(Self::PreSyncing),
            "paused" => Ok(Self::Paused),
            "disabled" => Ok(Self::Disabled),
            other => Err(ParseStatusError(other.to_owned())),
        }
    }
}

/// Error returned by [`SyncStatus::from_str`] when given an unknown variant.
#[derive(Debug, thiserror::Error)]
#[error("invalid SyncStatus: {0:?}")]
pub struct ParseStatusError(String);

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn json_roundtrip() {
        for status in [
            SyncStatus::None,
            SyncStatus::Failed,
            SyncStatus::Success,
            SyncStatus::Syncing,
            SyncStatus::PreSyncing,
            SyncStatus::Paused,
            SyncStatus::Disabled,
        ] {
            let s = serde_json::to_string(&status).unwrap();
            let back: SyncStatus = serde_json::from_str(&s).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn pre_syncing_is_hyphenated() {
        // This is the trickiest variant — Go's MarshalJSON outputs
        // "pre-syncing" with a hyphen, so we must match exactly.
        assert_eq!(
            serde_json::to_string(&SyncStatus::PreSyncing).unwrap(),
            r#""pre-syncing""#
        );
        assert_eq!(
            serde_json::from_str::<SyncStatus>(r#""pre-syncing""#).unwrap(),
            SyncStatus::PreSyncing
        );
    }

    #[test]
    fn as_str_matches_wire_format() {
        // as_str() must return exactly the same string as the JSON wire value
        // (without quotes), since it's used for Prometheus label values.
        let cases = [
            (SyncStatus::None, "none"),
            (SyncStatus::Failed, "failed"),
            (SyncStatus::Success, "success"),
            (SyncStatus::Syncing, "syncing"),
            (SyncStatus::PreSyncing, "pre-syncing"),
            (SyncStatus::Paused, "paused"),
            (SyncStatus::Disabled, "disabled"),
        ];
        for (status, expected) in cases {
            assert_eq!(status.as_str(), expected);
            // Must also match Display output.
            assert_eq!(status.to_string(), expected);
        }
    }

    #[test]
    fn from_str_round_trip() {
        assert_eq!(
            SyncStatus::from_str("syncing").unwrap(),
            SyncStatus::Syncing
        );
        assert!(SyncStatus::from_str("Syncing").is_err());
        assert!(SyncStatus::from_str("unknown").is_err());
    }
}
