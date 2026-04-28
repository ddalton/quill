use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use quill_auth::{AuthLayer, AuthState, HtpasswdStore};
use quill_config::Config;
use quill_pullthrough::PullThroughTable;
use quill_registry::{router as registry_router, RegistryState, UpstreamTagCache};
use quill_storage::{CasLayout, LocalStorage, LocalTagsStore};
use quill_tls::{install_default_crypto_provider, server_config_from_files, server_config_self_signed};
use quill_upstream::UpstreamRouter;

#[derive(Parser, Debug)]
#[command(name = "quill", about = "Single-user OCI registry with pull-through caching")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Parser, Debug)]
enum Command {
    /// Start the registry server.
    Serve {
        /// Path to a TOML config file.
        #[arg(short, long, default_value = "quill.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,quill=debug")),
        )
        .with_target(true)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Command::Serve { config } => serve(config).await,
    }
}

async fn serve(config_path: PathBuf) -> Result<()> {
    install_default_crypto_provider();

    let cfg = Config::from_file(&config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;
    info!(address = %cfg.http.address, "loaded config");

    // --- storage ---
    let layout = CasLayout::new(&cfg.storage.root);
    std::fs::create_dir_all(&cfg.storage.root)
        .with_context(|| format!("creating storage root {}", cfg.storage.root.display()))?;
    let storage = Arc::new(LocalStorage::new(
        layout.clone(),
        Duration::from_secs(cfg.storage.blob_meta_ttl_secs),
    ));
    let local_tags = Arc::new(LocalTagsStore::new(layout.clone()));
    load_existing_local_tags(&local_tags, &cfg.storage.root)?;

    // --- pull-through machinery + upstreams ---
    let pullthrough = Arc::new(PullThroughTable::new());
    let upstreams = Arc::new(
        UpstreamRouter::build(cfg.upstream.clone())
            .with_context(|| "building upstream clients")?,
    );
    if upstreams.is_empty() {
        info!("no upstreams configured; running as local-only registry");
    } else {
        info!(count = cfg.upstream.len(), "configured upstreams");
    }
    let upstream_tag_cache = Arc::new(UpstreamTagCache::new(Duration::from_secs(300)));

    // --- auth ---
    let htpasswd_store = match cfg
        .http
        .auth
        .as_ref()
        .and_then(|a| a.htpasswd.as_ref())
    {
        Some(h) => {
            let store = HtpasswdStore::load(&h.path).with_context(|| {
                format!("loading htpasswd file {}", h.path.display())
            })?;
            info!(path = %h.path.display(), "htpasswd loaded");
            Some(store)
        }
        None => {
            warn!("no htpasswd configured; auth is disabled (anyone with network access can pull and push)");
            None
        }
    };
    let auth_state = AuthState::new(htpasswd_store);

    // --- routes ---
    let state = RegistryState::new(
        storage,
        local_tags,
        pullthrough,
        upstreams,
        upstream_tag_cache,
    );
    let app: Router = registry_router(state).layer(AuthLayer::new(auth_state));

    // --- TLS ---
    let tls_cfg = match cfg.http.tls.as_ref() {
        Some(tls) => Some(server_config_from_files(&tls.cert, &tls.key)?),
        None => {
            // Self-signed for localhost only.
            let self_signed_dir = cfg.storage.root.join("_quill");
            std::fs::create_dir_all(&self_signed_dir)?;
            let cert_path = self_signed_dir.join("self-signed.crt");
            let key_path = self_signed_dir.join("self-signed.key");
            Some(server_config_self_signed(
                &cert_path,
                &key_path,
                &["localhost", "127.0.0.1"],
            )?)
        }
    };

    let listener = TcpListener::bind(&cfg.http.address)
        .await
        .with_context(|| format!("binding {}", cfg.http.address))?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "quill listening");

    let tls_acceptor = tls_cfg.map(|c| TlsAcceptor::from(Arc::new(c)));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "accept failed");
                continue;
            }
        };
        let app = app.clone();
        let tls_acceptor = tls_acceptor.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_conn(stream, peer, app, tls_acceptor).await {
                warn!(%peer, error = %e, "connection error");
            }
        });
    }
}

async fn serve_conn(
    stream: tokio::net::TcpStream,
    _peer: std::net::SocketAddr,
    app: Router,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<()> {
    use hyper::service::service_fn;
    let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let app = app.clone();
        async move {
            let req = req.map(axum::body::Body::new);
            let resp = app.oneshot(req).await;
            // axum::Router::oneshot returns Result<_, Infallible>, so this is unreachable.
            Ok::<_, std::convert::Infallible>(resp.unwrap_or_else(|e| match e {}))
        }
    });

    let builder = auto::Builder::new(TokioExecutor::new());
    match tls_acceptor {
        Some(acc) => {
            let tls_stream = acc.accept(stream).await?;
            let io = TokioIo::new(tls_stream);
            builder
                .serve_connection(io, svc)
                .await
                .map_err(|e| anyhow::anyhow!("hyper: {e}"))?;
        }
        None => {
            let io = TokioIo::new(stream);
            builder
                .serve_connection(io, svc)
                .await
                .map_err(|e| anyhow::anyhow!("hyper: {e}"))?;
        }
    }
    Ok(())
}

fn load_existing_local_tags(store: &Arc<LocalTagsStore>, root: &std::path::Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(path = %dir.display(), error = %e, "skipping dir during local-tags scan");
                continue;
            }
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if entry.file_name() == "blobs" || entry.file_name() == "_uploads" {
                    continue;
                }
                stack.push(p);
            } else if p.file_name().is_some_and(|n| n == "_local_tags.json") {
                if let Some(repo_dir) = p.parent() {
                    if let Ok(repo_rel) = repo_dir.strip_prefix(root) {
                        let repo_str = repo_rel.to_string_lossy().to_string();
                        if let Err(e) = store.load_repo(&repo_str) {
                            error!(repo = %repo_str, error = %e, "failed to load _local_tags.json");
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// `axum::Router::oneshot` returns an Infallible error; suppress unused-import warning.
#[allow(dead_code)]
fn _phantom() {
    let _ = std::marker::PhantomData::<axum::Router>;
}
