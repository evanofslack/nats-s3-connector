use axum::{
    debug_handler,
    extract::State,
    routing::{get, post},
    Json, Router,
};
use tracing::{debug, warn};

use crate::jobs;
use crate::server::{Dependencies, ServerError};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/load", get(get_load_jobs))
        .route("/load", post(start_load_job))
        .with_state(deps);
    return router;
}

#[debug_handler]
async fn get_load_jobs(
    State(state): State<Dependencies>,
) -> Result<Json<Vec<jobs::LoadJob>>, ServerError> {
    debug!(route = "/jobs/load", method = "GET", "handle request");

    // fetch jobs from db
    let jobs = state.db.get_load_jobs().await?;

    return Ok(Json(jobs));
}

#[debug_handler]
async fn start_load_job(
    State(state): State<Dependencies>,
    Json(payload): Json<jobs::CreateLoadJob>,
) -> Result<Json<jobs::LoadJob>, ServerError> {
    debug!(
        route = "/load",
        method = "PUT",
        bucket = payload.bucket,
        read_subject = payload.read_subject,
        write_subject = payload.write_subject,
        delete_chunks = payload.delete_chunks,
        start = payload.start,
        end = payload.end,
        "handle request"
    );

    let job = jobs::LoadJob::new(
        payload.bucket.clone(),
        payload.prefix.clone(),
        payload.read_stream.clone(),
        payload.read_subject.clone(),
        payload.write_stream.clone(),
        payload.write_subject.clone(),
        payload.delete_chunks,
        payload.start,
        payload.end,
    );
    let job_id = job.clone().id;

    // store job in db
    state.db.create_load_job(job.clone()).await?;

    // spawn a background task loading the messages
    // from S3 and publishing to stream.
    tokio::spawn(async move {
        if let Err(err) = state
            .db
            .update_load_job(job_id.clone(), jobs::LoadJobStatus::Running)
            .await
        {
            warn!("{}", err)
        }

        let mut success = true;
        if let Err(err) = state
            .io
            .publish_stream(
                payload.read_stream,
                payload.read_subject,
                payload.write_stream,
                payload.write_subject,
                payload.bucket,
                payload.prefix,
                payload.delete_chunks,
                payload.start,
                payload.end,
            )
            .await
        {
            success = false;
            warn!("{}", err);
        }

        // update job when finished
        let status = match success {
            true => jobs::LoadJobStatus::Success,
            false => jobs::LoadJobStatus::Failure,
        };
        if let Err(err) = state.db.update_load_job(job_id, status).await {
            warn!("{}", err)
        }
    });

    // return a 201 resp
    // TODO: write location header
    return Ok(Json(job));
}
