use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml parse error: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub http: Http,
    pub storage: Storage,
    #[serde(default)]
    pub upstream: Vec<Upstream>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Http {
    /// Bind address, e.g. "127.0.0.1:5000".
    pub address: String,
    /// TLS config. If absent and address is a localhost bind, a self-signed cert
    /// is auto-generated. If absent and address is a non-localhost bind, startup fails.
    #[serde(default)]
    pub tls: Option<Tls>,
    #[serde(default)]
    pub auth: Option<Auth>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Tls {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Auth {
    #[serde(default)]
    pub htpasswd: Option<HtpasswdAuth>,
    #[serde(default)]
    pub token: Option<TokenAuth>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HtpasswdAuth {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenAuth {
    pub realm: String,
    pub service: String,
    pub issuer_key: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Storage {
    /// Filesystem root for the CAS layout.
    pub root: PathBuf,
    /// In-memory blob metadata cache TTL in seconds.
    #[serde(default = "default_blob_meta_ttl")]
    pub blob_meta_ttl_secs: u64,
}

fn default_blob_meta_ttl() -> u64 {
    60
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Upstream {
    /// Friendly name, used in URL prefix matching.
    pub name: String,
    /// e.g. "https://123456789.dkr.ecr.us-east-1.amazonaws.com"
    pub url: String,
    pub kind: UpstreamKind,
    /// Repo prefix; pulls of /v2/<prefix>/... go through this upstream.
    pub repo_prefix: String,
    #[serde(default)]
    pub auth: Option<UpstreamAuth>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UpstreamKind {
    Ecr,
    Harbor,
    Generic,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum UpstreamAuth {
    /// HTTP basic auth. Covers Harbor (username/password) and ECR (username
    /// `AWS`, password from `aws ecr get-login-password`).
    Basic { username: String, password: String },
}

impl Upstream {
    /// Convenience for callers that don't care about the auth variant directly.
    pub fn auth_or_anonymous(&self) -> Option<&UpstreamAuth> {
        self.auth.as_ref()
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let body = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&body)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let bind = &self.http.address;
        let is_localhost = bind.starts_with("127.0.0.1:")
            || bind.starts_with("[::1]:")
            || bind.starts_with("localhost:");
        if self.http.tls.is_none() && !is_localhost {
            return Err(ConfigError::Invalid(
                "TLS required for non-localhost bind addresses; \
                 set [http.tls] or bind to 127.0.0.1"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let toml_str = r#"
            [http]
            address = "127.0.0.1:5000"

            [storage]
            root = "/tmp/quill"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.http.address, "127.0.0.1:5000");
        assert_eq!(cfg.storage.blob_meta_ttl_secs, 60);
    }

    #[test]
    fn rejects_non_localhost_without_tls() {
        let toml_str = r#"
            [http]
            address = "0.0.0.0:5000"

            [storage]
            root = "/tmp/quill"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(cfg.validate().is_err());
    }
}
