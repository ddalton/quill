//! OCI Distribution Spec error envelope.
//!
//! https://distribution.github.io/distribution/spec/api/#errors

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RegistryErrorCode {
    Unknown,
    Unauthorized,
    Denied,
    Unsupported,
    NameInvalid,
    NameUnknown,
    BlobUnknown,
    BlobUploadInvalid,
    BlobUploadUnknown,
    DigestInvalid,
    ManifestUnknown,
    ManifestInvalid,
    SizeInvalid,
    Unavailable,
}

#[derive(Debug, Error)]
#[error("{code:?}: {message}")]
pub struct RegistryError {
    pub status: StatusCode,
    pub code: RegistryErrorCode,
    pub message: String,
}

impl RegistryError {
    pub fn new(status: StatusCode, code: RegistryErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn name_unknown(name: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            RegistryErrorCode::NameUnknown,
            format!("repository name not known: {}", name.into()),
        )
    }

    pub fn blob_unknown(digest: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            RegistryErrorCode::BlobUnknown,
            format!("blob unknown: {}", digest.into()),
        )
    }

    pub fn manifest_unknown(reference: impl Into<String>) -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            RegistryErrorCode::ManifestUnknown,
            format!("manifest unknown: {}", reference.into()),
        )
    }

    pub fn digest_invalid(digest: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            RegistryErrorCode::DigestInvalid,
            format!("invalid digest: {}", digest.into()),
        )
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    errors: Vec<ErrorDetail>,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: RegistryErrorCode,
    message: String,
}

pub fn registry_error_response(err: RegistryError) -> Response {
    let body = ErrorEnvelope {
        errors: vec![ErrorDetail {
            code: err.code,
            message: err.message,
        }],
    };
    (err.status, Json(body)).into_response()
}

impl IntoResponse for RegistryError {
    fn into_response(self) -> Response {
        registry_error_response(self)
    }
}
