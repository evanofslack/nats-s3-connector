use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    routing::{delete, get, post},
    Json, Router,
};
use nats3_types::{CreateLoadJob, LoadJob, LoadJobStatus};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::server::{Dependencies, ServerError};
use crate::{io, jobs};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/load/job", get(get_load_job))
        .route("/load/job", delete(delete_load_job))
        .route("/load/job", post(start_load_job))
        .route("/load/jobs", get(get_load_jobs))
        .with_state(deps);
    router
}

#[derive(Deserialize)]
struct GetJobParams {
    job_id: String,
}

#[debug_handler]
async fn get_load_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<LoadJob>, ServerError> {
    debug!(
        route = "/load/job",
        method = "GET",
        job_id = params.job_id,
        "handle request"
    );

    // fetch load jobs from db
    let job = state.db.get_load_job(params.job_id).await?;

    Ok(Json(job))
}

#[debug_handler]
async fn delete_load_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<(), ServerError> {
    debug!(
        route = "/load/job",
        method = "DELETE",
        job_id = params.job_id,
        "handle request"
    );

    state.db.delete_load_job(params.job_id).await?;
    Ok(())
}

#[debug_handler]
async fn get_load_jobs(
    State(state): State<Dependencies>,
) -> Result<Json<Vec<LoadJob>>, ServerError> {
    debug!(route = "/load/jobs", method = "GET", "handle request");

    // fetch load jobs from db
    let jobs = state.db.get_load_jobs().await?;

    Ok(Json(jobs))
}

#[debug_handler]
async fn start_load_job(
    State(state): State<Dependencies>,
    Json(payload): Json<CreateLoadJob>,
) -> Result<Json<LoadJob>, ServerError> {
    debug!(
        route = "/load/job",
        method = "PUT",
        bucket = payload.bucket,
        read_subject = payload.read_subject,
        write_subject = payload.write_subject,
        delete_chunks = payload.delete_chunks,
        start = payload.start,
        end = payload.end,
        "handle request"
    );

    let job = LoadJob::new(
        payload.bucket.clone(),
        payload.prefix.clone(),
        payload.read_stream.clone(),
        payload.read_consumer.clone(),
        payload.read_subject.clone(),
        payload.write_subject.clone(),
        payload.delete_chunks,
        payload.start,
        payload.end,
    );
    let job_id = job.clone().id;

    // Ensure job with this id isn't already running
    if state.registry.is_load_job_running(&job.id).await {
        warn!(job_id = job.id, "handle start load job, already registered");
        return Err(jobs::RegistryError::JobAlreadyRunning { job_id }.into());
    }

    // store job in db
    state.db.create_load_job(job.clone()).await?;

    // spawn a background task loading the messages
    // from S3 and publishing to stream.
    let config = io::PublishConfig {
        read_stream: payload.read_stream,
        read_consumer: payload.read_consumer,
        read_subject: payload.read_subject,
        write_subject: payload.write_subject,
        bucket: payload.bucket,
        prefix: payload.prefix,
        delete_chunks: payload.delete_chunks,
        start: payload.start,
        end: payload.end,
    };
    let registry_config = config.clone();
    let handle: tokio::task::JoinHandle<Result<()>> =
        tokio::spawn(async move { state.io.publish_stream(config).await });
    let registered = state
        .registry
        .try_register_load_job(job_id.clone(), handle, registry_config)
        .await;
    let status = match registered {
        true => LoadJobStatus::Running,
        false => LoadJobStatus::Failure,
    };

    if let Err(err) = state
        .db
        .update_load_job(job_id.clone(), status.clone())
        .await
    {
        warn!(
            error = err.to_string(),
            job_id = job_id,
            status = status.to_string(),
            "fail update load job status after register"
        )
    }

    // return a 201 resp
    // TODO: write location header
    Ok(Json(job))
}
