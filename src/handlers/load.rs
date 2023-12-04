use crate::handlers;
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::debug;

pub fn create_router(state: handlers::State) -> Router {
    let router: Router = Router::new()
        .route("/load", post(create_load_job))
        .with_state(state);
    return router;
}

async fn create_load_job(
    State(_state): State<handlers::State>,
    Json(payload): Json<CreateLoadJob>,
) -> (StatusCode, Json<LoadJob>) {
    debug!(
        route = "/load",
        method = "PUT",
        bucket = payload.bucket,
        read_subject = payload.read_subject,
        write_subject = payload.write_subject,
        stream = payload.stream,
        "handle request"
    );

    let job = LoadJob { id: 0 };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(job))
}

#[derive(Deserialize)]
struct CreateLoadJob {
    bucket: String,
    read_subject: String,
    stream: String,
    write_subject: String,
}

#[derive(Serialize)]
struct LoadJob {
    id: u64,
}
