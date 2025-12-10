use axum::{
    debug_handler,
    extract::{Query, State},
    routing::{delete, get, post},
    Json, Router,
};
use nats3_types::{CreateStoreJob, StoreJob, StoreJobStatus};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::io;
use crate::metrics;
use crate::server::{Dependencies, ServerError};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/store/job", get(get_store_job))
        .route("/store/job", delete(delete_store_job))
        .route("/store/job", post(start_store_job))
        .route("/store/jobs", get(get_store_jobs))
        .with_state(deps);
    router
}

#[derive(Deserialize)]
struct GetJobParams {
    job_id: String,
}

#[debug_handler]
async fn get_store_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<StoreJob>, ServerError> {
    debug!(
        route = "/store/job",
        method = "GET",
        job_id = params.job_id,
        "handle request"
    );

    // fetch store jobs from db
    let job = state.db.get_store_job(params.job_id).await?;

    Ok(Json(job))
}

async fn get_store_jobs(
    State(state): State<Dependencies>,
) -> Result<Json<Vec<StoreJob>>, ServerError> {
    debug!(route = "/store/jobs", method = "GET", "handle request");

    // fetch store jobs from db
    let jobs = state.db.get_store_jobs().await?;

    Ok(Json(jobs))
}

#[debug_handler]
async fn delete_store_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<(), ServerError> {
    debug!(
        route = "/store/job",
        method = "DELETE",
        job_id = params.job_id,
        "handle request"
    );

    state.db.delete_store_job(params.job_id).await?;
    Ok(())
}

#[debug_handler]
async fn start_store_job(
    State(state): State<Dependencies>,
    Json(payload): Json<CreateStoreJob>,
) -> Result<Json<StoreJob>, ServerError> {
    debug!(
        route = "/store/job",
        method = "PUT",
        name = payload.name,
        stream = payload.stream,
        subject = payload.subject,
        bucket = payload.bucket,
        prefix = payload.prefix,
        "handle request"
    );

    let job = StoreJob::new(
        payload.name.clone(),
        payload.stream.clone(),
        payload.consumer.clone(),
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
        .update_store_job(job_id.clone(), StoreJobStatus::Running)
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
            .consume_stream(io::ConsumeConfig {
                stream: job_clone.stream.clone(),
                consumer: job_clone.consumer.clone(),
                subject: job_clone.subject.clone(),
                bucket: job_clone.bucket.clone(),
                prefix: job_clone.prefix.clone(),
                bytes_max: job_clone.batch.max_bytes,
                messages_max: job_clone.batch.max_count,
                codec: job_clone.encoding.codec,
            })
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
    Ok(Json(job))
}
