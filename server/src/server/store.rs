use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    routing::{delete, get, post},
    Json, Router,
};
use nats3_types::{CreateStoreJob, StoreJob, StoreJobConfig};
use serde::Deserialize;

use crate::{error::AppError, io, server::Dependencies};

pub fn create_router(deps: Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/store/job", get(get_store_job))
        .route("/store/job", delete(delete_store_job))
        .route("/store/job", post(start_store_job))
        .route("/store/job/pause", post(pause_store_job))
        .route("/store/job/resume", post(resume_store_job))
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
) -> Result<Json<StoreJob>, AppError> {
    // fetch store jobs from db
    let job = state.db.get_store_job(params.job_id).await?;

    Ok(Json(job))
}

async fn get_store_jobs(
    State(state): State<Dependencies>,
) -> Result<Json<Vec<StoreJob>>, AppError> {
    // fetch store jobs from db
    let jobs = state.db.get_store_jobs(None).await?;

    Ok(Json(jobs))
}

#[debug_handler]
async fn delete_store_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<(), AppError> {
    state
        .coordinator
        .stop_store_job(params.job_id.clone())
        .await;
    state.db.delete_store_job(params.job_id).await?;
    Ok(())
}

#[debug_handler]
async fn start_store_job(
    State(state): State<Dependencies>,
    Json(payload): Json<CreateStoreJob>,
) -> Result<Json<StoreJob>, AppError> {
    let job_config = StoreJobConfig {
        name: payload.name.clone(),
        stream: payload.stream.clone(),
        consumer: payload.consumer.clone(),
        subject: payload.subject.clone(),
        bucket: payload.bucket.clone(),
        prefix: payload.prefix.clone(),
        batch: payload.batch.unwrap_or_default(),
        encoding: payload.encoding.unwrap_or_default(),
    };

    let job = StoreJob::new(job_config);
    let config: io::ConsumeConfig = job.clone().into();
    state
        .coordinator
        .start_store_job(job.clone(), config, false)
        .await?;

    // return a 201 resp
    Ok(Json(job))
}

#[debug_handler]
async fn pause_store_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<StoreJob>, AppError> {
    let job = state.coordinator.pause_store_job(params.job_id).await?;
    Ok(Json(job))
}

#[debug_handler]
async fn resume_store_job(
    State(state): State<Dependencies>,
    Query(params): Query<GetJobParams>,
) -> Result<Json<StoreJob>, AppError> {
    let job = state.coordinator.resume_store_job(params.job_id).await?;
    Ok(Json(job))
}
