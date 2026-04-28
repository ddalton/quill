use std::collections::HashMap;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use serde::Deserialize;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Clone)]
pub enum AuthMode {
    Anonymous,
    Basic { username: String, password: String },
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("missing or malformed WWW-Authenticate header")]
    MalformedChallenge,
    #[error("token endpoint returned status {0}")]
    TokenStatus(u16),
}

/// Parsed Bearer challenge from `WWW-Authenticate: Bearer realm="...", service="...", scope="..."`.
#[derive(Debug, Clone)]
pub struct TokenRequest {
    pub realm: String,
    pub service: Option<String>,
    pub scope: Option<String>,
}

impl TokenRequest {
    pub fn parse(header: &str) -> Option<Self> {
        let rest = header.trim().strip_prefix("Bearer ")?;
        let mut params: HashMap<String, String> = HashMap::new();
        for kv in rest.split(',') {
            let kv = kv.trim();
            if let Some((k, v)) = kv.split_once('=') {
                let v = v.trim().trim_matches('"');
                params.insert(k.trim().to_string(), v.to_string());
            }
        }
        let realm = params.remove("realm")?;
        Some(TokenRequest {
            realm,
            service: params.remove("service"),
            scope: params.remove("scope"),
        })
    }

    pub fn cache_key(&self) -> (String, String) {
        (
            self.realm.clone(),
            self.scope.clone().unwrap_or_default(),
        )
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    token: Option<String>,
    #[serde(default, rename = "access_token")]
    access_token: Option<String>,
    #[serde(default, rename = "expires_in")]
    expires_in: Option<u64>,
}

#[derive(Debug, Clone)]
struct CachedToken {
    bearer: String,
    valid_until: Instant,
}

/// In-memory bearer-token cache, keyed by (realm, scope). Single-flight refresh
/// would be a Phase 4 polish item; the simple approach here is fine for one user.
pub struct BearerCache {
    entries: DashMap<(String, String), CachedToken>,
}

impl BearerCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    pub fn get(&self, key: &(String, String)) -> Option<String> {
        let entry = self.entries.get(key)?;
        if entry.valid_until > Instant::now() {
            Some(entry.bearer.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, key: (String, String), bearer: String, ttl: Duration) {
        self.entries.insert(
            key,
            CachedToken {
                bearer,
                valid_until: Instant::now() + ttl,
            },
        );
    }

    /// Fetch a bearer token for the given challenge using `auth_mode` for
    /// upstream-token-server auth (Docker Hub allows anonymous; private
    /// registries usually want Basic).
    pub async fn fetch(
        &self,
        client: &reqwest::Client,
        req: &TokenRequest,
        auth_mode: &AuthMode,
    ) -> Result<String, AuthError> {
        let key = req.cache_key();
        if let Some(t) = self.get(&key) {
            return Ok(t);
        }
        let mut url = url::Url::parse(&req.realm).map_err(|_| AuthError::MalformedChallenge)?;
        {
            let mut q = url.query_pairs_mut();
            if let Some(s) = &req.service {
                q.append_pair("service", s);
            }
            if let Some(sc) = &req.scope {
                q.append_pair("scope", sc);
            }
        }
        let mut http_req = client.get(url);
        if let AuthMode::Basic { username, password } = auth_mode {
            http_req = http_req.basic_auth(username, Some(password));
        }
        let resp = http_req.send().await?;
        if !resp.status().is_success() {
            return Err(AuthError::TokenStatus(resp.status().as_u16()));
        }
        let parsed: TokenResponse = resp.json().await?;
        let bearer = parsed
            .token
            .or(parsed.access_token)
            .ok_or(AuthError::MalformedChallenge)?;
        let ttl = Duration::from_secs(parsed.expires_in.unwrap_or(60).max(30).saturating_sub(15));
        debug!(realm = %req.realm, "minted upstream bearer token");
        self.insert(key, bearer.clone(), ttl);
        Ok(bearer)
    }
}

impl Default for BearerCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_challenge() {
        let h = r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull""#;
        let r = TokenRequest::parse(h).unwrap();
        assert_eq!(r.realm, "https://auth.docker.io/token");
        assert_eq!(r.service.as_deref(), Some("registry.docker.io"));
        assert_eq!(
            r.scope.as_deref(),
            Some("repository:library/redis:pull")
        );
    }

    #[test]
    fn rejects_basic_challenge() {
        assert!(TokenRequest::parse(r#"Basic realm="quill""#).is_none());
    }
}
