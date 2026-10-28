use anyhow::Result;
use axum::{
    Json, Router,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::{
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::{Span, debug, info, info_span};

use crate::{coordinator, db, error, metrics as counter, registry};

pub mod load;
pub mod metrics;
pub mod status;
pub mod store;

#[derive(Clone)]
pub struct Dependencies {
    metrics: counter::Metrics,
    db: db::DynJobStorer,
    coordinator: coordinator::Coordinator,
}

impl Dependencies {
    pub fn new(
        metrics: counter::Metrics,
        db: db::DynJobStorer,
        coordinator: coordinator::Coordinator,
    ) -> Self {
        Self {
            metrics,
            db,
            coordinator,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Server {
    metrics: counter::Metrics,
    db: db::DynJobStorer,
    coordinator: coordinator::Coordinator,
}

impl Server {
    pub fn new(
        metrics: counter::Metrics,
        db: db::DynJobStorer,
        coordinator: coordinator::Coordinator,
    ) -> Self {
        debug!("create new server");
        Self {
            metrics,
            db,
            coordinator,
        }
    }

    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        let state = Dependencies::new(
            self.metrics.clone(),
            self.db.clone(),
            self.coordinator.clone(),
        );
        let router = create_router(state);
        info!(address = ?listener.local_addr()?, "serving on address");
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_token.cancelled_owned())
        .await?;
        debug!("server stopped");
        Ok(())
    }
}

fn create_router(deps: Dependencies) -> Router {
    let api_v1_router = load::create_router(deps.clone()).merge(store::create_router(deps.clone()));
    let api_router = status::create_router()
        .merge(metrics::create_router(deps.clone()))
        .nest("/api/v1", api_v1_router);

    let trace_mw = TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            info_span!(
                "handle_request",
                method = %request.method(),
                uri = %request.uri(),
                status = tracing::field::Empty,
                latency_ms = tracing::field::Empty,
            )
        })
        .on_request(())
        .on_response(|response: &Response<_>, latency: Duration, span: &Span| {
            span.record("status", response.status().as_u16());
            span.record("latency_ms", latency.as_millis());
            info!("request completed");
        })
        .on_body_chunk(())
        .on_eos(());

    let serve_dir =
        ServeDir::new("server/web/dist").fallback(ServeFile::new("server/web/dist/index.html"));

    Router::new()
        .merge(api_router)
        .fallback_service(serve_dir)
        .layer(trace_mw)
}

impl IntoResponse for error::AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            error::AppError::JobStore(db::JobStoreError::NotFound { id }) => {
                (StatusCode::NOT_FOUND, format!("job id {} not found", id))
            }
            error::AppError::JobStore(db::JobStoreError::Database(_))
            | error::AppError::JobStore(db::JobStoreError::Pool(_))
            | error::AppError::JobStore(db::JobStoreError::InvalidUuid(_))
            | error::AppError::JobStore(db::JobStoreError::Postgres(_)) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
            error::AppError::JobRegistry(registry::RegistryError::JobAlreadyRunning { job_id }) => {
                (
                    StatusCode::CONFLICT,
                    format!("job id {} already exists", job_id),
                )
            }
            error::AppError::Validation(e) => (StatusCode::BAD_REQUEST, e.to_string()),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
