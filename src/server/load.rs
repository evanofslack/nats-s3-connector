use axum::{debug_handler, extract::State, routing::post, Json, Router};
use tracing::{debug, warn};

use crate::jobs;
use crate::server::{Dependencies, ServerError};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/jobs/load", post(start_load_job))
        .with_state(deps);
    return router;
}

#[debug_handler]
async fn start_load_job(
    State(state): State<Dependencies>,
    Json(payload): Json<jobs::CreateLoadJob>,
) -> Result<Json<jobs::LoadJob>, ServerError> {
    debug!(
        route = "/jobs/load",
        method = "PUT",
        bucket = payload.bucket,
        read_subject = payload.read_subject,
        write_subject = payload.write_subject,
        "handle request"
    );

    let job = jobs::LoadJob::new(
        payload.bucket.clone(),
        payload.read_stream.clone(),
        payload.read_subject.clone(),
        payload.write_stream.clone(),
        payload.write_subject.clone(),
    );

    // store job in db
    state.db.create_load_job(job.clone()).await?;

    // spawn a background task loading the messages
    // from S3 and publishing to stream.
    tokio::spawn(async move {
        if let Err(err) = state
            .io
            .publish_stream(
                payload.read_stream,
                payload.read_subject,
                payload.write_stream,
                payload.write_subject,
                payload.bucket,
            )
            .await
        {
            warn!("{}", err);
        }
    });

    // return a 201 resp
    // TODO: write location header
    // Ok((Json(job), StatusCode::ACCEPTED))
    return Ok(Json(job));
}
