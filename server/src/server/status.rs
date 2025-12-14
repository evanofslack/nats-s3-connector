use axum::{http::StatusCode, routing::get, Router};
use tracing::info;

async fn ping() -> (StatusCode, &'static str) {
    info!(route = "/ping", method = "GET", "handle request");
    (StatusCode::OK, "pong")
}

async fn ready() -> (StatusCode, &'static str) {
    info!(route = "/ready", method = "GET", "handle request");
    (StatusCode::OK, "ready")
}

pub fn create_router() -> Router {
    let router: Router = Router::new()
        .route("/ping", get(ping))
        .route("/ready", get(ready));

    router
}
