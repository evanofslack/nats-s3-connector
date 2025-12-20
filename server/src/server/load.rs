use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    routing::{delete, get, post},
    Json, Router,
};
use nats3_types::{CreateLoadJob, LoadJob};
use serde::Deserialize;

use crate::{error::AppError, io, server::Dependencies};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/load/job", get(get_load_job))
        .route("/load/job", delete(delete_load_job))
        .route("/load/job", post(start_load_job))
        .route("/load/job/pause", post(pause_load_job))
        .route("/load/job/resume", post(resume_load_job))
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
) -> Result<Json<LoadJob>, AppError> {
    // fetch load jobs from db
    let job = state.db.get_load_job(params.job_id).await?;

    Ok(Json(job))
}

#[debug_handler]
async fn delete_load_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<(), AppError> {
    state.coordinator.stop_load_job(params.job_id.clone()).await;
    state.db.delete_load_job(params.job_id).await?;
    Ok(())
}

#[debug_handler]
async fn get_load_jobs(State(state): State<Dependencies>) -> Result<Json<Vec<LoadJob>>, AppError> {
    let jobs = state.db.get_load_jobs(None).await?;
    Ok(Json(jobs))
}

#[debug_handler]
async fn start_load_job(
    State(state): State<Dependencies>,
    Json(payload): Json<CreateLoadJob>,
) -> Result<Json<LoadJob>, AppError> {
    let job = LoadJob::new(
        payload.bucket.clone(),
        payload.prefix.clone(),
        payload.read_stream.clone(),
        payload.read_consumer.clone(),
        payload.read_subject.clone(),
        payload.write_subject.clone(),
        payload.poll_interval,
        payload.delete_chunks,
        payload.from_time,
        payload.to_time,
    );

    let config: io::PublishConfig = job.clone().into();
    state
        .coordinator
        .start_new_load_job(job.clone(), config)
        .await?;

    // return a 201 resp
    // TODO: write location header
    Ok(Json(job))
}

#[debug_handler]
async fn pause_load_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<LoadJob>, AppError> {
    let job = state.coordinator.pause_load_job(params.job_id).await?;
    Ok(Json(job))
}

#[debug_handler]
async fn resume_load_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<LoadJob>, AppError> {
    let job = state.coordinator.resume_load_job(params.job_id).await?;
    Ok(Json(job))
}
