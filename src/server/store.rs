use axum::{http::StatusCode, routing::get, Router};

async fn store() -> StatusCode {
    return StatusCode::NO_CONTENT;
}

pub fn create_router() -> Router {
    let router: Router = Router::new().route("/store", get(store));
    return router;
}
