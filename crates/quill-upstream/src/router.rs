use std::sync::Arc;

use quill_config::{Upstream, UpstreamAuth};

use crate::auth::AuthMode;
use crate::client::{FallbackUpstream, HttpUpstream, UpstreamClient, UpstreamError};

fn auth_mode_of(auth: &Option<UpstreamAuth>) -> AuthMode {
    match auth {
        None => AuthMode::Anonymous,
        Some(UpstreamAuth::Basic { username, password }) => AuthMode::Basic {
            username: username.clone(),
            password: password.clone(),
        },
    }
}

pub struct UpstreamEntry {
    pub config: Upstream,
    pub client: Arc<dyn UpstreamClient>,
}

impl UpstreamEntry {
    /// Translate a client-facing repo into the repo path sent upstream.
    ///
    /// When `rewrite_prefix` is unset the repo is forwarded verbatim (the
    /// transparent-proxy default). When set, the matched `repo_prefix` is
    /// replaced by `rewrite_prefix` (`""` strips it entirely), so an OCI
    /// endpoint like `.../cloudera-oci-virtual` can proxy to a different
    /// upstream whose path layout differs.
    pub fn translate_repo(&self, repo: &str) -> String {
        match &self.config.rewrite_prefix {
            None => repo.to_string(),
            Some(dest) => {
                let rest = repo.strip_prefix(&self.config.repo_prefix).unwrap_or(repo);
                format!("{dest}{rest}")
            }
        }
    }
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
            let auth_mode = auth_mode_of(&u.auth);
            let primary = HttpUpstream::new(&u.name, &u.url, auth_mode)?;

            // If a fallback registry is configured, wrap the primary so not-found
            // / unreachable results retry against it (same rewritten repo path).
            let client: Arc<dyn UpstreamClient> = match &u.fallback_url {
                Some(fb_url) if !fb_url.is_empty() => {
                    let fb_mode = auth_mode_of(&u.fallback_auth);
                    let fallback =
                        HttpUpstream::new(&format!("{}-fallback", u.name), fb_url, fb_mode)?;
                    FallbackUpstream::new(primary, fallback)
                }
                _ => primary,
            };

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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use quill_config::UpstreamKind;
    use quill_storage::Digest;

    use crate::client::{BlobStream, ManifestResponse, UpstreamClient, UpstreamError};

    struct DummyClient;

    #[async_trait]
    impl UpstreamClient for DummyClient {
        async fn resolve_tag(&self, _repo: &str, _reference: &str) -> Result<Digest, UpstreamError> {
            unreachable!("translate_repo tests never call the client")
        }
        async fn get_manifest(
            &self,
            _repo: &str,
            _reference: &str,
        ) -> Result<ManifestResponse, UpstreamError> {
            unreachable!("translate_repo tests never call the client")
        }
        async fn stream_blob(
            &self,
            _repo: &str,
            _digest: &Digest,
        ) -> Result<BlobStream, UpstreamError> {
            unreachable!("translate_repo tests never call the client")
        }
        async fn list_tags(&self, _repo: &str) -> Result<Vec<String>, UpstreamError> {
            unreachable!("translate_repo tests never call the client")
        }
    }

    fn entry(repo_prefix: &str, rewrite_prefix: Option<&str>) -> UpstreamEntry {
        UpstreamEntry {
            config: Upstream {
                name: "test".into(),
                url: "https://upstream.invalid".into(),
                kind: UpstreamKind::Generic,
                repo_prefix: repo_prefix.into(),
                rewrite_prefix: rewrite_prefix.map(|s| s.to_string()),
                auth: None,
                fallback_url: None,
                fallback_auth: None,
            },
            client: Arc::new(DummyClient),
        }
    }

    #[test]
    fn translate_verbatim_when_unset() {
        let e = entry("cloudera", None);
        assert_eq!(e.translate_repo("cloudera/img"), "cloudera/img");
    }

    #[test]
    fn translate_strips_prefix_when_empty() {
        let e = entry("cloudera-oci-virtual/", Some(""));
        assert_eq!(
            e.translate_repo("cloudera-oci-virtual/cloudera/img"),
            "cloudera/img"
        );
    }

    #[test]
    fn translate_replaces_prefix() {
        let e = entry("cloudera-oci-virtual/", Some("mirror/"));
        assert_eq!(
            e.translate_repo("cloudera-oci-virtual/cloudera/img"),
            "mirror/cloudera/img"
        );
    }

    #[test]
    fn translate_leaves_non_matching_repo_when_rewrite_set() {
        let e = entry("cloudera-oci-virtual/", Some("mirror/"));
        // repo doesn't carry the prefix: nothing to strip, dest still prepended.
        assert_eq!(e.translate_repo("other/img"), "mirror/other/img");
    }
}
