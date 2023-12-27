use crate::server::Dependencies;
use axum::{debug_handler, extract::State, http::StatusCode, routing::get, Router};
use prometheus_client::encoding::text::encode;

#[debug_handler]
async fn metrics(State(state): State<Dependencies>) -> (StatusCode, String) {
    let registry = state.metrics.registry.write().expect("lock not poisoned");
    let mut body = String::new();
    encode(&mut body, &registry).expect("read metrics registry");
    return (StatusCode::OK, body);
}

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/metrics", get(metrics))
        .with_state(deps);

    return router;
}
