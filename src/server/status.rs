use axum::{http::StatusCode, routing::get, Router};

async fn ping() -> (StatusCode, &'static str) {
    (StatusCode::OK, "pong")
}

async fn ready() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ready")
}

pub fn create_router() -> Router {
    let router: Router = Router::new()
        .route("/ping", get(ping))
        .route("/ready", get(ready));

    router
}
