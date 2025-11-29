use axum::{http::StatusCode, routing::get, Router};
use tracing::{debug, warn};

async fn store() -> StatusCode {
    return StatusCode::NO_CONTENT;
}

pub fn create_router() -> Router {
    let router: Router = Router::new().route("/store", get(store));
    return router;
}

#[debug_handler]
async fn get_store_jobs(
    State(state): State<Dependencies>,
) -> Result<Json<Vec<jobs::StoreJob>>, ServerError> {
    debug!(route = "/jobs/store", method = "GET", "handle request");

    // fetch store jobs from db
    let jobs = state.db.get_store_jobs().await?;

    return Ok(Json(jobs));
}
