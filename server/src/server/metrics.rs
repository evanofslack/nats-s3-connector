use crate::server::Dependencies;
use axum::{debug_handler, extract::State, http::StatusCode, routing::get, Router};
use prometheus_client::encoding::text::encode;
use tracing::info;

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/metrics", get(metrics))
        .with_state(deps);

    router
}

#[debug_handler]
async fn metrics(State(state): State<Dependencies>) -> (StatusCode, String) {
    info!(route = "/metrics", method = "GET", "handle request");

    let registry = state.metrics.registry;
    let mut body = String::new();

    match encode(&mut body, &registry) {
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, body),
        Ok(_) => (StatusCode::OK, body),
    }
}
