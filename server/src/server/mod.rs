use anyhow::Result;
use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server,
};
use serde_json::json;
use std::{convert::Infallible, net::SocketAddr};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::{Service, ServiceExt};
use tracing::{debug, info, warn};

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
    addr: String,
    metrics: counter::Metrics,
    db: db::DynJobStorer,
    coordinator: coordinator::Coordinator,
}

impl Server {
    pub fn new(
        addr: String,
        metrics: counter::Metrics,
        db: db::DynJobStorer,
        coordinator: coordinator::Coordinator,
    ) -> Self {
        debug!(address = addr, "create new server");
        Self {
            addr,
            metrics,
            db,
            coordinator,
        }
    }

    pub async fn serve(&self, shutdown_token: CancellationToken) {
        let state = Dependencies::new(
            self.metrics.clone(),
            self.db.clone(),
            self.coordinator.clone(),
        );
        let router = create_router(state.clone());
        let mut make_service = router.into_make_service_with_connect_info::<SocketAddr>();
        let listener = TcpListener::bind(self.addr.clone()).await.unwrap();
        info!(address = self.addr, "serving on address");

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, remote_addr)) => {
                            let tower_service = unwrap_infallible(make_service.call(remote_addr).await);
                            tokio::spawn(async move {
                                let socket = TokioIo::new(socket);
                                let hyper_service =
                                    hyper::service::service_fn(move |request: Request<Incoming>| {
                                        tower_service.clone().oneshot(request)
                                    });
                                if let Err(err) = server::conn::auto::Builder::new(TokioExecutor::new())
                                    .serve_connection(socket, hyper_service)
                                    .await
                                {
                                    warn!(err = ?err, "fail serve connection")
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = ?e, "fail accept connection");
                        }
                    }
                }
                _ = shutdown_token.cancelled() => {
                    debug!("shutdown signal received, stopping server");
                    break;
                }
            }
        }
        debug!("server stopped accepting connections");
    }
}

fn unwrap_infallible<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => match err {},
    }
}

fn create_router(deps: Dependencies) -> Router {
    status::create_router()
        .merge(metrics::create_router(deps.clone()))
        .merge(load::create_router(deps.clone()))
        .merge(store::create_router(deps))
}

impl IntoResponse for error::AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            error::AppError::JobStore(db::JobStoreError::NotFound { id }) => {
                (StatusCode::NOT_FOUND, format!("job id {} not found", id))
            }
            error::AppError::JobStore(db::JobStoreError::Database(_))
            | error::AppError::JobStore(db::JobStoreError::Pool(_))
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
            error::AppError::JobRegistry(registry::RegistryError::JobNotFound { job_id }) => (
                StatusCode::NOT_FOUND,
                format!("job id {} not found", job_id),
            ),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}
