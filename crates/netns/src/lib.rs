//! Shared protocol and root-policy model for network namespace execution.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Component, Path};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DEFAULT_SOCKET: &str = "/run/tunasync/netns-broker.sock";
pub const MAX_FRAME_LEN: usize = 1024 * 1024;
pub const MAX_OUTPUT_CHUNK: usize = 16 * 1024;
pub const DYNAMIC_LOG_ENV: &str = "TUNASYNC_LOG_FILE";
const DYNAMIC_LOG_MARKER: &str = "<dynamic-log-file>";
const SECRET_ENV_MARKER: &str = "<secret>";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("frame length {0} exceeds limit {MAX_FRAME_LEN}")]
    FrameTooLarge(usize),
    #[error("truncated frame")]
    TruncatedFrame,
    #[error("invalid protocol data: {0}")]
    InvalidData(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchPlan {
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl LaunchPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.argv.is_empty() || self.argv[0].is_empty() {
            return Err("argv must contain a non-empty executable".into());
        }
        validate_absolute_normalized_path(&self.argv[0], "executable")?;
        if self.argv.len() > 4096 {
            return Err("argv contains too many arguments".into());
        }
        if self.argv.iter().any(|arg| arg.contains('\0')) {
            return Err("argv contains NUL".into());
        }
        validate_absolute_normalized_path(&self.cwd, "cwd")?;
        if self.env.len() > 256 {
            return Err("environment contains too many entries".into());
        }
        for (key, value) in &self.env {
            validate_env_key(key)?;
            if value.contains('\0') {
                return Err(format!("environment value for {key:?} contains NUL"));
            }
            if value.len() > 64 * 1024 {
                return Err(format!("environment value for {key:?} is too large"));
            }
        }
        Ok(())
    }

    pub fn normalized_hash(&self) -> Result<String, String> {
        self.validate()?;
        let mut normalized = self.clone();
        for (key, value) in &mut normalized.env {
            if key == DYNAMIC_LOG_ENV {
                *value = DYNAMIC_LOG_MARKER.into();
            } else if is_secret_env_key(key) {
                *value = SECRET_ENV_MARKER.into();
            }
        }
        let encoded = serde_json::to_vec(&normalized).map_err(|e| e.to_string())?;
        let digest = Sha256::digest(encoded);
        Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub generation: String,
    #[serde(default)]
    pub launch: Vec<PolicyLaunch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyLaunch {
    pub mirror: String,
    pub operation: String,
    pub namespace: String,
    pub cwd: String,
    pub plan_sha256: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_env: Vec<String>,
    #[serde(default)]
    pub allow_concurrency: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_root: Option<String>,
}

impl Policy {
    pub fn validate(&self) -> Result<(), String> {
        if self.generation.is_empty() || self.generation.len() > 128 {
            return Err("policy generation must contain 1..=128 characters".into());
        }
        let mut previous: Option<(&str, &str)> = None;
        for entry in &self.launch {
            validate_namespace_name(&entry.namespace)?;
            if entry.mirror.is_empty() || entry.operation.is_empty() {
                return Err("policy mirror and operation must be non-empty".into());
            }
            validate_absolute_normalized_path(&entry.cwd, "policy cwd")?;
            if entry.plan_sha256.len() != 64
                || !entry.plan_sha256.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(format!(
                    "invalid plan_sha256 for {}/{}",
                    entry.mirror, entry.operation
                ));
            }
            if let Some(root) = &entry.log_root {
                validate_absolute_normalized_path(root, "log_root")?;
            }
            let mut previous_secret: Option<&str> = None;
            for key in &entry.secret_env {
                validate_env_key(key)?;
                if !is_secret_env_key(key) {
                    return Err(format!(
                        "policy secret_env key {key:?} is not classified as secret"
                    ));
                }
                if previous_secret.is_some_and(|previous| previous >= key.as_str()) {
                    return Err("policy secret_env keys must be unique and sorted".into());
                }
                previous_secret = Some(key);
            }
            let key = (entry.mirror.as_str(), entry.operation.as_str());
            if previous.is_some_and(|prev| prev >= key) {
                return Err("policy launch entries must be unique and sorted".into());
            }
            previous = Some(key);
        }
        Ok(())
    }

    pub fn authorize(&self, request: &LaunchRequest) -> Result<(), String> {
        if request.generation != self.generation {
            return Err("policy generation mismatch".into());
        }
        validate_namespace_name(&request.namespace)?;
        request.plan.validate()?;
        let entry = self
            .launch
            .iter()
            .find(|entry| {
                entry.mirror == request.mirror
                    && entry.operation == request.operation
                    && entry.namespace == request.namespace
            })
            .ok_or_else(|| "operation is not authorized by policy".to_string())?;
        let actual_hash = request.plan.normalized_hash()?;
        if request.plan.cwd != entry.cwd {
            return Err("launch cwd mismatch".into());
        }
        if !constant_time_eq(actual_hash.as_bytes(), entry.plan_sha256.as_bytes()) {
            return Err("launch plan hash mismatch".into());
        }
        let actual_secret_env = request.plan.secret_env_keys();
        if actual_secret_env != entry.secret_env {
            return Err("launch secret environment key set mismatch".into());
        }
        match request.plan.env.get(DYNAMIC_LOG_ENV) {
            Some(value) => {
                let root = entry
                    .log_root
                    .as_deref()
                    .ok_or_else(|| "dynamic log path is not authorized".to_string())?;
                validate_path_beneath(value, root)?;
            }
            None if entry.log_root.is_some() => {
                return Err("authorized dynamic log path is missing".into());
            }
            None => {}
        }
        Ok(())
    }
}

impl LaunchPlan {
    pub fn secret_env_keys(&self) -> Vec<String> {
        self.env
            .keys()
            .filter(|key| is_secret_env_key(key))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchRequest {
    pub generation: String,
    pub mirror: String,
    pub operation: String,
    pub namespace: String,
    pub plan: LaunchPlan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientRequest {
    Status,
    Launch(LaunchRequest),
    Terminate { job_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    Ready {
        generation: String,
    },
    Started {
        job_id: String,
        pid: u32,
    },
    Stdout {
        data: Vec<u8>,
    },
    Stderr {
        data: Vec<u8>,
    },
    Exit {
        code: Option<i32>,
        signal: Option<i32>,
    },
    Terminated,
    Error {
        kind: String,
        message: String,
    },
}

pub fn is_secret_env_key(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    [
        "PASSWORD",
        "TOKEN",
        "SECRET",
        "API_KEY",
        "PRIVATE_KEY",
        "CREDENTIAL",
    ]
    .iter()
    .any(|needle| upper.contains(needle))
}

pub fn write_frame<W: Write, T: Serialize>(writer: &mut W, value: &T) -> Result<(), Error> {
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge(body.len()));
    }
    writer.write_all(&(body.len() as u32).to_be_bytes())?;
    writer.write_all(&body)?;
    writer.flush()?;
    Ok(())
}

pub fn read_frame<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<T, Error> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::TruncatedFrame
        } else {
            Error::Io(e)
        }
    })?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge(length));
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::TruncatedFrame
        } else {
            Error::Io(e)
        }
    })?;
    Ok(serde_json::from_slice(&body)?)
}

#[cfg(feature = "async")]
pub async fn write_frame_async<W, T>(writer: &mut W, value: &T) -> Result<(), Error>
where
    W: tokio::io::AsyncWrite + Unpin,
    T: Serialize,
{
    use tokio::io::AsyncWriteExt;
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge(body.len()));
    }
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
pub async fn read_frame_async<R, T>(reader: &mut R) -> Result<T, Error>
where
    R: tokio::io::AsyncRead + Unpin,
    T: DeserializeOwned,
{
    use tokio::io::AsyncReadExt;
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::TruncatedFrame
        } else {
            Error::Io(e)
        }
    })?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_LEN {
        return Err(Error::FrameTooLarge(length));
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::TruncatedFrame
        } else {
            Error::Io(e)
        }
    })?;
    Ok(serde_json::from_slice(&body)?)
}

pub fn validate_namespace_name(name: &str) -> Result<(), String> {
    let valid = (1..=63).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        && name != "."
        && name != ".."
        && !name.starts_with('.')
        && !name.ends_with('.');
    if valid {
        Ok(())
    } else {
        Err(format!("unsafe network namespace name {name:?}"))
    }
}

pub fn validate_env_key(key: &str) -> Result<(), String> {
    let mut bytes = key.bytes();
    let valid = bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !valid || key.len() > 128 {
        return Err(format!("invalid environment key {key:?}"));
    }
    let upper = key.to_ascii_uppercase();
    if upper.starts_with("LD_")
        || upper.starts_with("DYLD_")
        || matches!(
            upper.as_str(),
            "BASH_ENV" | "ENV" | "IFS" | "PATH" | "LANG" | "LC_ALL"
        )
    {
        return Err(format!(
            "loader or shell injection environment key {key:?} is forbidden"
        ));
    }
    Ok(())
}

pub fn validate_absolute_normalized_path(value: &str, field: &str) -> Result<(), String> {
    let path = Path::new(value);
    if !path.is_absolute()
        || value.contains('\0')
        || path
            .components()
            .any(|c| matches!(c, Component::CurDir | Component::ParentDir))
    {
        return Err(format!("{field} must be an absolute normalized path"));
    }
    Ok(())
}

fn validate_path_beneath(value: &str, root: &str) -> Result<(), String> {
    validate_absolute_normalized_path(value, DYNAMIC_LOG_ENV)?;
    validate_absolute_normalized_path(root, "log_root")?;
    let value = Path::new(value);
    let root = Path::new(root);
    if value == root || !value.starts_with(root) {
        return Err(format!("{DYNAMIC_LOG_ENV} is outside authorized log root"));
    }
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> LaunchPlan {
        LaunchPlan {
            argv: vec!["/usr/bin/rsync".into(), "--list-only".into()],
            cwd: "/srv/mirrors/debian".into(),
            env: BTreeMap::from([
                ("RSYNC_PASSWORD".into(), "secret".into()),
                (DYNAMIC_LOG_ENV.into(), "/var/log/tunasync/a.log".into()),
            ]),
        }
    }

    #[test]
    fn plan_hash_is_deterministic_and_hides_dynamic_log_value() {
        let mut other = plan();
        other.env.insert(
            DYNAMIC_LOG_ENV.into(),
            "/var/log/tunasync/rotated.log".into(),
        );
        assert_eq!(
            plan().normalized_hash().unwrap(),
            other.normalized_hash().unwrap()
        );
        other
            .env
            .insert("RSYNC_PASSWORD".into(), "different".into());
        assert_eq!(
            plan().normalized_hash().unwrap(),
            other.normalized_hash().unwrap()
        );
    }

    #[test]
    fn secret_key_set_is_bound_but_secret_values_are_not_public_verifiers() {
        let mut changed_value = plan();
        changed_value
            .env
            .insert("service_api_key".into(), "second-secret".into());
        let mut first = plan();
        first
            .env
            .insert("service_api_key".into(), "first-secret".into());
        assert_eq!(
            first.normalized_hash().unwrap(),
            changed_value.normalized_hash().unwrap()
        );

        let mut added_key = first.clone();
        added_key.env.insert("ACCESS_TOKEN".into(), "secret".into());
        assert_ne!(
            first.normalized_hash().unwrap(),
            added_key.normalized_hash().unwrap()
        );
        assert_ne!(first.secret_env_keys(), added_key.secret_env_keys());

        let mut changed_public = first;
        changed_public.env.insert("USER".into(), "other".into());
        assert_ne!(
            plan().normalized_hash().unwrap(),
            changed_public.normalized_hash().unwrap()
        );
    }

    #[test]
    fn rejects_unsafe_namespace_and_environment() {
        for name in ["", "../host", "/run/netns/x", ".hidden", "a/b"] {
            assert!(validate_namespace_name(name).is_err(), "accepted {name:?}");
        }
        for key in ["LD_PRELOAD", "DYLD_INSERT_LIBRARIES", "BASH_ENV", "A=B"] {
            assert!(validate_env_key(key).is_err(), "accepted {key:?}");
        }
    }

    #[test]
    fn generation_and_dynamic_log_path_fail_closed() {
        let request = LaunchRequest {
            generation: "g1".into(),
            mirror: "debian".into(),
            operation: "sync".into(),
            namespace: "warp0".into(),
            plan: plan(),
        };
        let policy = Policy {
            generation: "g2".into(),
            launch: vec![PolicyLaunch {
                mirror: "debian".into(),
                operation: "sync".into(),
                namespace: "warp0".into(),
                cwd: "/srv/mirrors/debian".into(),
                plan_sha256: request.plan.normalized_hash().unwrap(),
                secret_env: request.plan.secret_env_keys(),
                allow_concurrency: false,
                log_root: Some("/var/log/tunasync".into()),
            }],
        };
        assert!(policy
            .authorize(&request)
            .unwrap_err()
            .contains("generation"));
        let mut matching = policy;
        matching.generation = "g1".into();
        assert!(matching.authorize(&request).is_ok());
        let mut escaped = request;
        escaped.plan.env.insert(
            DYNAMIC_LOG_ENV.into(),
            "/var/log/tunasync-other/a.log".into(),
        );
        assert!(matching.authorize(&escaped).is_err());
    }

    #[test]
    fn policy_authorization_binds_exact_secret_key_set() {
        let request = LaunchRequest {
            generation: "g1".into(),
            mirror: "debian".into(),
            operation: "sync".into(),
            namespace: "warp0".into(),
            plan: plan(),
        };
        let policy = Policy {
            generation: "g1".into(),
            launch: vec![PolicyLaunch {
                mirror: request.mirror.clone(),
                operation: request.operation.clone(),
                namespace: request.namespace.clone(),
                cwd: request.plan.cwd.clone(),
                plan_sha256: request.plan.normalized_hash().unwrap(),
                secret_env: request.plan.secret_env_keys(),
                allow_concurrency: false,
                log_root: Some("/var/log/tunasync".into()),
            }],
        };
        assert!(policy.authorize(&request).is_ok());

        let mut changed_value = request.clone();
        changed_value
            .plan
            .env
            .insert("RSYNC_PASSWORD".into(), "rotated".into());
        assert!(policy.authorize(&changed_value).is_ok());

        let mut added_key = changed_value.clone();
        added_key
            .plan
            .env
            .insert("ACCESS_TOKEN".into(), "new".into());
        assert!(policy.authorize(&added_key).is_err());

        let mut removed_key = changed_value;
        removed_key.plan.env.remove("RSYNC_PASSWORD");
        assert!(policy.authorize(&removed_key).is_err());
    }

    #[test]
    fn framing_rejects_oversized_declared_length() {
        let encoded = ((MAX_FRAME_LEN as u32) + 1).to_be_bytes();
        let mut bytes = encoded.as_slice();
        let result = read_frame::<_, ClientRequest>(&mut bytes);
        assert!(matches!(result, Err(Error::FrameTooLarge(_))));
    }
}
