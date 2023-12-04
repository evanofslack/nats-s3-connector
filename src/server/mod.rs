use anyhow::Result;
use std::convert::Infallible;
use std::net::SocketAddr;

use axum::{extract::Request, Router};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server;
use tokio::net::TcpListener;
use tower::{Service, ServiceExt};
use tracing::{info, warn};

use crate::io;

pub mod load;
pub mod status;
pub mod store;

#[derive(Clone)]
pub struct Dependencies {
    io: io::IO,
}

impl Dependencies {
    pub fn new(io: io::IO) -> Self {
        Self { io }
    }
}

#[derive(Debug, Clone)]
pub struct Server {
    addr: String,
    io: io::IO,
}

impl Server {
    pub fn new(addr: String, io: io::IO) -> Self {
        Self { addr, io }
    }

    pub async fn serve(&self) {
        let state = Dependencies::new(self.io.clone());
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
        .merge(load::create_router(deps))
        .merge(store::create_router());
    return app;
}
