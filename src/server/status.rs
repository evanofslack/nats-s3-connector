use axum::{http::StatusCode, routing::get, Router};

async fn ping() -> StatusCode {
    return StatusCode::NO_CONTENT;
}

pub fn create_router() -> Router {
    let router: Router = Router::new().route("/ping", get(ping));

    return router;
}
