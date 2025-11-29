use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use serde_json::json;

use anyhow::Result;
use std::convert::Infallible;
use std::net::SocketAddr;

use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server;
use tokio::net::TcpListener;
use tower::{Service, ServiceExt};
use tracing::{info, warn};

use crate::db;
use crate::io;
use crate::metrics as counter;

pub mod load;
pub mod metrics;
pub mod status;
pub mod store;

#[derive(Clone)]
pub struct Dependencies {
    metrics: counter::Metrics,
    io: io::IO,
    db: db::DynStorer,
}

impl Dependencies {
    pub fn new(metrics: counter::Metrics, io: io::IO, db: db::DynStorer) -> Self {
        Self { metrics, io, db }
    }
}

#[derive(Debug, Clone)]
pub struct Server {
    addr: String,
    metrics: counter::Metrics,
    io: io::IO,
    db: db::DynStorer,
}

impl Server {
    pub fn new(addr: String, metrics: counter::Metrics, io: io::IO, db: db::DynStorer) -> Self {
        Self {
            addr,
            metrics,
            io,
            db,
        }
    }

    pub async fn serve(&self) {
        let state = Dependencies::new(self.metrics.clone(), self.io.clone(), self.db.clone());
        let router = create_router(state.clone());
        let mut make_service = router.into_make_service_with_connect_info::<SocketAddr>();
        let listener = TcpListener::bind(self.addr.clone()).await.unwrap();
        info!("serving on addr {:?}", self.addr);

        loop {
            let (socket, remote_addr) = listener.accept().await.unwrap();
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
                    warn!(err = err, "failed to serve connection")
                }
            });
        }
    }
}

fn unwrap_infallible<T>(result: Result<T, Infallible>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => match err {},
    }
}

fn create_router(deps: Dependencies) -> Router {
    let app = status::create_router()
        .merge(metrics::create_router(deps.clone()))
        .merge(load::create_router(deps))
        .merge(store::create_router());
    return app;
}

enum ServerError {
    // something went wrong when calling the job store
    JobStore(db::JobStoreError),
}

// enable conversion of `db::JobStoreError` -> `ServerError` with `?` operator
impl From<db::JobStoreError> for ServerError {
    fn from(inner: db::JobStoreError) -> Self {
        ServerError::JobStore(inner)
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ServerError::JobStore(db::JobStoreError::NotFound { id }) => {
                (StatusCode::NOT_FOUND, format!("job id {} not found", id))
            } // _ => (
              //     StatusCode::INTERNAL_SERVER_ERROR,
              //     "unknown error".to_string(),
              // ),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        return (status, body).into_response();
    }
}
