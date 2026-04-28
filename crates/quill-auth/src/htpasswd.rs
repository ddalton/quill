use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use dashmap::DashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HtpasswdError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid line {line_no} in htpasswd file")]
    InvalidLine { line_no: usize },
    #[error("bcrypt: {0}")]
    Bcrypt(#[from] bcrypt::BcryptError),
}

/// htpasswd store with a small successful-auth cache.
///
/// bcrypt verification is intentionally CPU-expensive (~50–100 ms per call).
/// To avoid paying that on every request, we cache successful `(user, password-hash)`
/// auth results for a short TTL keyed by the *raw Authorization header* — the
/// next request from the same client carries the same header and hits cache.
pub struct HtpasswdStore {
    file_path: PathBuf,
    /// username -> bcrypt hash from the file
    users: DashMap<String, String>,
    /// Authorization header value -> (username, valid_until)
    auth_cache: DashMap<String, (String, Instant)>,
    auth_cache_ttl: Duration,
}

impl HtpasswdStore {
    pub fn load(path: impl AsRef<Path>) -> Result<Arc<Self>, HtpasswdError> {
        let path = path.as_ref().to_path_buf();
        let users = parse_file(&path)?;
        let dm = DashMap::new();
        for (k, v) in users {
            dm.insert(k, v);
        }
        Ok(Arc::new(Self {
            file_path: path,
            users: dm,
            auth_cache: DashMap::new(),
            auth_cache_ttl: Duration::from_secs(300),
        }))
    }

    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Verify a `Basic <base64(user:pass)>` Authorization header.
    /// Returns `Some(username)` on success.
    pub fn verify_basic(&self, authz_header_value: &str) -> Option<String> {
        if let Some((user, until)) = self.auth_cache.get(authz_header_value).map(|e| e.clone()) {
            if until > Instant::now() {
                return Some(user);
            }
        }

        let creds = authz_header_value.strip_prefix("Basic ")?;
        let decoded_bytes = BASE64_STANDARD.decode(creds.trim()).ok()?;
        let decoded = String::from_utf8(decoded_bytes).ok()?;
        let (user, password) = decoded.split_once(':')?;
        let hash = self.users.get(user).map(|e| e.clone())?;
        match bcrypt::verify(password, &hash) {
            Ok(true) => {
                self.auth_cache.insert(
                    authz_header_value.to_string(),
                    (user.to_string(), Instant::now() + self.auth_cache_ttl),
                );
                Some(user.to_string())
            }
            _ => None,
        }
    }
}

fn parse_file(path: &Path) -> Result<HashMap<String, String>, HtpasswdError> {
    let body = std::fs::read_to_string(path)?;
    let mut out = HashMap::new();
    for (i, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (user, hash) = line
            .split_once(':')
            .ok_or(HtpasswdError::InvalidLine { line_no: i + 1 })?;
        out.insert(user.to_string(), hash.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn b64(input: &[u8]) -> String {
        BASE64_STANDARD.encode(input)
    }

    #[test]
    fn verifies_known_user() {
        let hash = bcrypt::hash("hunter2", bcrypt::DEFAULT_COST - 4).unwrap();
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "alice:{}", hash).unwrap();
        let store = HtpasswdStore::load(f.path()).unwrap();
        let header = format!("Basic {}", b64(b"alice:hunter2"));
        assert_eq!(store.verify_basic(&header).as_deref(), Some("alice"));
    }

    #[test]
    fn rejects_wrong_password() {
        let hash = bcrypt::hash("hunter2", bcrypt::DEFAULT_COST - 4).unwrap();
        let mut f = NamedTempFile::new().unwrap();
        writeln!(f, "alice:{}", hash).unwrap();
        let store = HtpasswdStore::load(f.path()).unwrap();
        let header = format!("Basic {}", b64(b"alice:wrong"));
        assert!(store.verify_basic(&header).is_none());
    }
}
