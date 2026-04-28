use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("no private key found in {0}")]
    NoPrivateKey(PathBuf),
    #[error("self-signed cert generation: {0}")]
    SelfSigned(#[from] rcgen::Error),
}

/// Build a `rustls::ServerConfig` from PEM-encoded cert and key files.
pub fn server_config_from_files(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> Result<ServerConfig, TlsError> {
    let certs = load_certs(cert_path.as_ref())?;
    let key = load_private_key(key_path.as_ref())?;
    let mut cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(cfg)
}

/// Generate a self-signed cert + key for the given subjects (e.g. ["localhost", "127.0.0.1"]),
/// persist to `cert_path`/`key_path`, and return a `ServerConfig`.
///
/// If both files already exist, load them instead of regenerating.
pub fn server_config_self_signed(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    subjects: &[&str],
) -> Result<ServerConfig, TlsError> {
    let cert_path = cert_path.as_ref();
    let key_path = key_path.as_ref();
    if cert_path.exists() && key_path.exists() {
        return server_config_from_files(cert_path, key_path);
    }
    if let Some(parent) = cert_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let san: Vec<String> = subjects.iter().map(|s| s.to_string()).collect();
    let cert = rcgen::generate_simple_self_signed(san)?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    std::fs::write(cert_path, &cert_pem)?;
    std::fs::write(key_path, &key_pem)?;
    info!(
        cert = %cert_path.display(),
        key = %key_path.display(),
        "generated self-signed TLS cert"
    );
    server_config_from_files(cert_path, key_path)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = io::BufReader::new(std::fs::File::open(path)?);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = io::BufReader::new(std::fs::File::open(path)?);
    let key = rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| TlsError::NoPrivateKey(path.to_path_buf()))?;
    Ok(key)
}

pub fn install_default_crypto_provider() {
    // Required by rustls 0.23 to pick a default provider.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

pub type ServerConfigArc = Arc<ServerConfig>;
