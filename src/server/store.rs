use axum::{
    debug_handler,
    extract::State,
    routing::{get, post},
    Json, Router,
};
use tracing::{debug, warn};

use crate::jobs;
use crate::metrics;
use crate::server::{Dependencies, ServerError};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/store", get(get_store_jobs))
        .route("/store", post(start_store_job))
        .with_state(deps);
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

#[debug_handler]
async fn start_store_job(
    State(state): State<Dependencies>,
    Json(payload): Json<jobs::CreateStoreJob>,
) -> Result<Json<jobs::StoreJob>, ServerError> {
    debug!(
        route = "/store",
        method = "PUT",
        name = payload.name,
        stream = payload.stream,
        subject = payload.subject,
        bucket = payload.bucket,
        prefix = payload.prefix,
        "handle request"
    );

    let job = jobs::StoreJob::new(
        payload.name.clone(),
        payload.stream.clone(),
        payload.subject.clone(),
        payload.bucket.clone(),
        payload.prefix.clone(),
        payload.batch.unwrap_or_default(),
        payload.encoding.unwrap_or_default(),
    );
    let job_id = job.clone().id;

    // store job in db
    state
        .db
        .create_store_job(job.clone())
        .await
        .map_err(|err| {
            warn!(job_id = job.id, "fail create store job: {err}");
            ServerError::JobStore(err)
        })?;

    // change state to running
    state
        .db
        .update_store_job(job_id.clone(), jobs::StoreJobStatus::Running)
        .await
        .map_err(|err| {
            warn!(job_id = job_id, "fail update store job status: {err}");
            ServerError::JobStore(err)
        })?;

    let job_clone = job.clone();
    tokio::spawn(async move {
        // spawn background thread consuming messages from stream
        // and writing to s3.
        if let Err(err) = state
            .io
            .consume_stream(
                job_clone.stream.clone(),
                job_clone.subject.clone(),
                job_clone.bucket.clone(),
                job_clone.prefix.clone(),
                job_clone.batch.max_bytes,
                job_clone.batch.max_count,
                job_clone.encoding.codec,
            )
            .await
        {
            warn!(id = job_id, "store job terminated with error: {err}");
            state
                .io
                .metrics
                .jobs
                .write()
                .await
                .store_jobs
                .get_or_create(&metrics::JobLabels {
                    stream: job_clone.stream.clone(),
                    subject: job_clone.subject.clone(),
                    bucket: job_clone.bucket.clone(),
                })
                .dec();
        }
    });

    // return a 201 resp
    return Ok(Json(job));
}
