use axum::{http::StatusCode, routing::get, Router};

async fn load() -> StatusCode {
    return StatusCode::NO_CONTENT;
}

pub fn create_router() -> Router {
    let router: Router = Router::new().route("/load", get(load));
    return router;
}
