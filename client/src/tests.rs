use crate::{Client, ClientError};
use chrono::Utc;
use nats3_types::{
    Batch, Encoding, LoadJob, LoadJobCreate, LoadJobStatus, StoreJob, StoreJobCreate,
    StoreJobStatus,
};

#[cfg(test)]
fn new_load_job() -> LoadJob {
    LoadJob {
        id: "test-id".to_string(),
        name: "test".to_string(),
        status: LoadJobStatus::Running,
        bucket: "test-bucket".to_string(),
        prefix: None,
        read_stream: "read-stream".to_string(),
        read_consumer: None,
        read_subject: "read.subject".to_string(),
        write_subject: "write.subject".to_string(),
        poll_interval: None,
        delete_chunks: false,
        from_time: None,
        to_time: None,
        created: Utc::now(),
        updated: Utc::now(),
    }
}

#[cfg(test)]
fn new_store_job() -> StoreJob {
    StoreJob {
        id: "test-id".to_string(),
        name: "test".to_string(),
        status: StoreJobStatus::Running,
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test-subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: None,
        batch: Batch::default(),
        encoding: Encoding::default(),
        created: Utc::now(),
        updated: Utc::now(),
    }
}

#[cfg(test)]
fn new_load_job_create() -> LoadJobCreate {
    LoadJobCreate {
        name: "test".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: None,
        read_stream: "read-stream".to_string(),
        read_consumer: None,
        read_subject: "read.subject".to_string(),
        write_subject: "write.subject".to_string(),
        poll_interval: None,
        delete_chunks: false,
        from_time: None,
        to_time: None,
    }
}

#[cfg(test)]
fn new_store_job_create() -> StoreJobCreate {
    StoreJobCreate {
        name: "test".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: None,
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test-subject".to_string(),
        batch: Batch::default(),
        encoding: Encoding::default(),
    }
}

#[tokio::test]
async fn test_get_load_jobs_success() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/load/jobs")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[]"#)
        .create();

    let client = Client::new(server.url());
    let jobs = client.get_load_jobs().await.unwrap();

    assert_eq!(jobs.len(), 0);
    mock.assert();
}

#[tokio::test]
async fn test_get_load_jobs_http_error() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/load/jobs")
        .with_status(500)
        .with_body("Internal Server Error")
        .create();

    let client = Client::new(server.url());
    let result = client.get_load_jobs().await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Http { status, .. } => assert_eq!(status, 500),
        _ => panic!("Expected Http error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_create_load_job_success() {
    let mut server = mockito::Server::new_async().await;
    let job = new_load_job();

    let mock = server
        .mock("POST", "/api/v1/load/job")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let create_job = new_load_job_create();
    let result = client.create_load_job(create_job.clone()).await.unwrap();

    assert_eq!(result.bucket, create_job.bucket);
    mock.assert();
}

#[tokio::test]
async fn test_get_store_jobs_success() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/store/jobs")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"[]"#)
        .create();

    let client = Client::new(server.url());
    let jobs = client.get_store_jobs().await.unwrap();

    assert_eq!(jobs.len(), 0);
    mock.assert();
}

#[tokio::test]
async fn test_create_store_job_success() {
    let mut server = mockito::Server::new_async().await;
    let job = new_store_job();

    let mock = server
        .mock("POST", "/api/v1/store/job")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let create_job = new_store_job_create();
    let result = client.create_store_job(create_job.clone()).await.unwrap();
    assert_eq!(result.name, create_job.name);
    mock.assert();
}

#[tokio::test]
async fn test_invalid_json_response() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/load/jobs")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"invalid json"#)
        .create();

    let client = Client::new(server.url());
    let result = client.get_load_jobs().await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Deserialization(_) => (),
        _ => panic!("Expected Deserialization error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_get_load_job_success() {
    let mut server = mockito::Server::new_async().await;

    let mut job = new_load_job();
    job.id = "test-job-123".to_string();
    let mock = server
        .mock("GET", "/api/v1/load/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "test-job-123".into(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let result = client
        .get_load_job("test-job-123".to_string())
        .await
        .unwrap();

    assert_eq!(result.bucket, "test-bucket");
    mock.assert();
}

#[tokio::test]
async fn test_get_load_job_not_found() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/load/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "nonexistent".into(),
        ))
        .with_status(404)
        .with_body("Job not found")
        .create();

    let client = Client::new(server.url());
    let result = client.get_load_job("nonexistent".to_string()).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Http { status, .. } => assert_eq!(status, 404),
        _ => panic!("Expected Http error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_delete_load_job_success() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("DELETE", "/api/v1/load/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "test-job-456".into(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"null"#)
        .create();

    let client = Client::new(server.url());
    let result = client.delete_load_job("test-job-456".to_string()).await;

    assert!(result.is_ok());
    mock.assert();
}

#[tokio::test]
async fn test_delete_load_job_http_error() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("DELETE", "/api/v1/load/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "test-job".into(),
        ))
        .with_status(500)
        .with_body("Failed to delete job")
        .create();

    let client = Client::new(server.url());
    let result = client.delete_load_job("test-job".to_string()).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Http { status, .. } => assert_eq!(status, 500),
        _ => panic!("Expected Http error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_get_store_job_success() {
    let mut server = mockito::Server::new_async().await;

    let mut job = new_store_job();
    job.id = "test-job-789".to_string();
    let mock = server
        .mock("GET", "/api/v1/store/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "test-job-789".into(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let result = client
        .get_store_job("test-job-789".to_string())
        .await
        .unwrap();

    assert_eq!(result.name, "test");
    mock.assert();
}

#[tokio::test]
async fn test_get_store_job_not_found() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/api/v1/store/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "nonexistent".into(),
        ))
        .with_status(404)
        .with_body("Job not found")
        .create();

    let client = Client::new(server.url());
    let result = client.get_store_job("nonexistent".to_string()).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Http { status, .. } => assert_eq!(status, 404),
        _ => panic!("Expected Http error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_delete_store_job_success() {
    let mut server = mockito::Server::new_async().await;

    let job = new_store_job();
    let mock = server
        .mock("DELETE", "/api/v1/store/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "job-to-delete".into(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    client
        .delete_store_job("job-to-delete".to_string())
        .await
        .unwrap();

    mock.assert();
}

#[tokio::test]
async fn test_delete_store_job_http_error() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("DELETE", "/api/v1/store/job")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            "job-fail".into(),
        ))
        .with_status(403)
        .with_body("Forbidden")
        .create();

    let client = Client::new(server.url());
    let result = client.delete_store_job("job-fail".to_string()).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        ClientError::Http { status, .. } => assert_eq!(status, 403),
        _ => panic!("Expected Http error"),
    }
    mock.assert();
}

#[tokio::test]
async fn test_pause_load_job_success() {
    let mut server = mockito::Server::new_async().await;
    let job = new_load_job();
    let mock = server
        .mock("POST", "/api/v1/load/job/pause")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            job.id.clone(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let result = client.pause_load_job(job.id).await.unwrap();

    assert_eq!(result.bucket, "test-bucket");
    mock.assert();
}

#[tokio::test]
async fn test_resume_store_job_success() {
    let mut server = mockito::Server::new_async().await;
    let job = new_store_job();
    let mock = server
        .mock("POST", "/api/v1/store/job/resume")
        .match_query(mockito::Matcher::UrlEncoded(
            "job_id".into(),
            job.id.clone(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let result = client.resume_store_job(job.id).await.unwrap();

    assert_eq!(result.name, "test");
    mock.assert();
}
