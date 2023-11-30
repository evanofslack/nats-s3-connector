use anyhow::{Error, Result};
use std::convert::Infallible;
use std::net::SocketAddr;

use axum::{extract::Request, routing::get, Router};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server;
use tokio::net::TcpListener;
use tower::{Service, ServiceExt};

pub struct Server {
    addr: String,
}

impl Server {
    pub fn new(addr: String) -> Result<Self, Error> {
        let server = Self { addr };
        Ok(server)
    }

    pub async fn serve(&self) {
        let app = create_router();
        let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();
        let listener = TcpListener::bind(self.addr.clone()).await.unwrap();
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
                    eprintln!("failed to serve connection: {err:#}");
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

async fn root() -> &'static str {
    "nats-s3-connect"
}

pub fn create_router() -> Router {
    let router: Router = Router::new().route("/", get(root));
    return router;
}
