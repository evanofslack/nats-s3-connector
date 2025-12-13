use anyhow::Result;
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
use crate::jobs;
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

    // Ensure job with this id isn't already running
    if state.registry.is_store_job_running(&job.id).await {
        warn!(
            job_id = job.id,
            "handle start store job, already registered"
        );
        return Err(jobs::RegistryError::JobAlreadyRunning { job_id }.into());
    }

    // store job in db
    state.db.create_store_job(job.clone()).await?;
    let config = io::ConsumeConfig {
        stream: job.stream.clone(),
        consumer: job.consumer.clone(),
        subject: job.subject.clone(),
        bucket: job.bucket.clone(),
        prefix: job.prefix.clone(),
        bytes_max: job.batch.max_bytes,
        messages_max: job.batch.max_count,
        codec: job.clone().encoding.codec,
    };
    let registry_config = config.clone();
    let handle: tokio::task::JoinHandle<Result<()>> =
        tokio::spawn(async move { state.io.consume_stream(config).await });
    let registered = state
        .registry
        .try_register_store_job(job_id.clone(), handle, registry_config)
        .await;
    let status = match registered {
        true => StoreJobStatus::Running,
        false => StoreJobStatus::Failure,
    };

    if let Err(err) = state
        .db
        .update_store_job(job_id.clone(), status.clone())
        .await
    {
        warn!(
            error = err.to_string(),
            job_id = job_id,
            status = status.to_string(),
            "fail update store job status after register"
        )
    }

    // change state to running
    state
        .db
        .update_store_job(job_id.clone(), StoreJobStatus::Running)
        .await
        .map_err(|err| {
            warn!(job_id = job_id, "fail update store job status: {err}");
            ServerError::JobStore(err)
        })?;

    // return a 201 resp
    Ok(Json(job))
}
