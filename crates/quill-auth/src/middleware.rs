use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::http::{header, Request, Response, StatusCode};
use tower::{Layer, Service};

use crate::htpasswd::HtpasswdStore;

/// Shared auth state. v1: htpasswd basic auth. Bearer-token flow lives in the
/// server crate's `/v2/token` handler, signing JWTs that this layer accepts.
#[derive(Clone)]
pub struct AuthState {
    htpasswd: Option<Arc<HtpasswdStore>>,
}

impl AuthState {
    pub fn new(htpasswd: Option<Arc<HtpasswdStore>>) -> Self {
        Self { htpasswd }
    }

    /// Returns true if no authentication is configured. In that case, the
    /// middleware is a no-op (intentional: a localhost laptop binding with
    /// no htpasswd file is the simplest "just let me push and pull" mode).
    pub fn is_disabled(&self) -> bool {
        self.htpasswd.is_none()
    }

    /// Verify an Authorization header. Returns `Some(username)` on success,
    /// `None` on failure or if no Authorization header was supplied.
    pub fn verify(&self, authz_header: Option<&str>) -> Option<String> {
        let header = authz_header?;
        if let Some(store) = &self.htpasswd {
            return store.verify_basic(header);
        }
        None
    }
}

#[derive(Clone)]
pub struct AuthLayer {
    state: AuthState,
}

impl AuthLayer {
    pub fn new(state: AuthState) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;
    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    state: AuthState,
}

impl<S> Service<Request<Body>> for AuthMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if self.state.is_disabled() {
            let fut = self.inner.call(req);
            return Box::pin(fut);
        }
        let header = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        match self.state.verify(header.as_deref()) {
            Some(_user) => {
                let fut = self.inner.call(req);
                Box::pin(fut)
            }
            None => {
                let resp = Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header(header::WWW_AUTHENTICATE, "Basic realm=\"quill\"")
                    .body(Body::empty())
                    .expect("static response");
                Box::pin(async move { Ok(resp) })
            }
        }
    }
}
