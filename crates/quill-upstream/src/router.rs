use std::sync::Arc;

use quill_config::{Upstream, UpstreamAuth};

use crate::auth::AuthMode;
use crate::client::{HttpUpstream, UpstreamClient, UpstreamError};

pub struct UpstreamEntry {
    pub config: Upstream,
    pub client: Arc<dyn UpstreamClient>,
}

pub struct UpstreamRouter {
    entries: Vec<Arc<UpstreamEntry>>,
}

impl UpstreamRouter {
    /// Build clients for each configured upstream. Failures are surfaced so the
    /// server fails fast at startup rather than masking bad config.
    pub fn build(upstreams: Vec<Upstream>) -> Result<Self, UpstreamError> {
        let mut out = Vec::with_capacity(upstreams.len());
        for u in upstreams {
            let auth_mode = match &u.auth {
                None => AuthMode::Anonymous,
                Some(UpstreamAuth::Basic { username, password }) => AuthMode::Basic {
                    username: username.clone(),
                    password: password.clone(),
                },
            };
            let client = HttpUpstream::new(&u.name, &u.url, auth_mode)?;
            out.push(Arc::new(UpstreamEntry {
                config: u,
                client,
            }));
        }
        Ok(Self { entries: out })
    }

    pub fn empty() -> Self {
        Self { entries: vec![] }
    }

    /// Construct from pre-built entries. Used by integration tests that
    /// inject a mock upstream client. Not intended for production.
    #[doc(hidden)]
    pub fn __with_entries_for_test(entries: Vec<Arc<UpstreamEntry>>) -> Self {
        Self { entries }
    }

    /// Find the first upstream whose `repo_prefix` is a prefix of `repo`.
    pub fn route(&self, repo: &str) -> Option<&Arc<UpstreamEntry>> {
        self.entries
            .iter()
            .find(|u| repo.starts_with(&u.config.repo_prefix))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
