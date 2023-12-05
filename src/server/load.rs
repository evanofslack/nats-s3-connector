use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::server;

pub fn create_router(deps: server::Dependencies) -> Router {
    let router: Router = Router::new()
        .route("/load", post(start_load_job))
        .with_state(deps);
    return router;
}

async fn start_load_job(
    State(state): State<server::Dependencies>,
    Json(payload): Json<CreateLoadJob>,
) -> (StatusCode, Json<LoadJob>) {
    debug!(
        route = "/load",
        method = "PUT",
        bucket = payload.bucket,
        read_subject = payload.read_subject,
        write_subject = payload.write_subject,
        "handle request"
    );

    let job = LoadJob {
        id: 0,
        status: LoadJobStatus::Pending,
        bucket: payload.bucket.clone(),
        read_stream: payload.read_stream.clone(),
        read_subject: payload.read_subject.clone(),
        write_stream: payload.write_stream.clone(),
        write_subject: payload.write_subject.clone(),
    };

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
    (StatusCode::ACCEPTED, Json(job))
}

#[derive(Deserialize)]
struct CreateLoadJob {
    bucket: String,
    read_stream: String,
    read_subject: String,
    write_stream: String,
    write_subject: String,
}

#[derive(Serialize)]
struct LoadJob {
    id: u64,
    status: LoadJobStatus,
    bucket: String,
    read_stream: String,
    read_subject: String,
    write_stream: String,
    write_subject: String,
}

#[derive(Serialize)]
enum LoadJobStatus {
    Pending,
    _Finished,
    _Unknown,
}
