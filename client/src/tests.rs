use crate::{Client, ClientError};
use nats3_types::{Batch, CreateLoadJob, CreateStoreJob, Encoding, LoadJob, StoreJob};

#[tokio::test]
async fn test_get_load_jobs_success() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/load/jobs")
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
        .mock("GET", "/load/jobs")
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

    let job = LoadJob::new(
        "test-bucket".to_string(),
        Some("prefix/".to_string()),
        "read-stream".to_string(),
        None,
        "read.subject".to_string(),
        "write.subject".to_string(),
        false,
        None,
        None,
    );

    let mock = server
        .mock("POST", "/load/job")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let create_job = CreateLoadJob {
        bucket: "test-bucket".to_string(),
        prefix: Some("prefix/".to_string()),
        read_stream: "read-stream".to_string(),
        read_consumer: None,
        read_subject: "read.subject".to_string(),
        write_subject: "write.subject".to_string(),
        delete_chunks: false,
        start: None,
        end: None,
    };

    let result = client.create_load_job(create_job).await.unwrap();

    assert_eq!(result.bucket, "test-bucket");
    mock.assert();
}

#[tokio::test]
async fn test_get_store_jobs_success() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/store/jobs")
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

    let job = StoreJob::new(
        "test-job".to_string(),
        "test-stream".to_string(),
        None,
        "test.subject".to_string(),
        "test-bucket".to_string(),
        Some("prefix/".to_string()),
        Batch::default(),
        Encoding::default(),
    );

    let mock = server
        .mock("POST", "/store/job")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(serde_json::to_string(&job).unwrap())
        .create();

    let client = Client::new(server.url());
    let create_job = CreateStoreJob {
        name: "test-job".to_string(),
        stream: "test-stream".to_string(),
        consumer: None,
        subject: "test.subject".to_string(),
        bucket: "test-bucket".to_string(),
        prefix: Some("prefix/".to_string()),
        batch: None,
        encoding: None,
    };

    let result = client.create_store_job(create_job).await.unwrap();

    assert_eq!(result.name, "test-job");
    mock.assert();
}

#[tokio::test]
async fn test_invalid_json_response() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/load/jobs")
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

    let job = LoadJob::new(
        "test-bucket".to_string(),
        Some("prefix/".to_string()),
        "read-stream".to_string(),
        None,
        "read.subject".to_string(),
        "write.subject".to_string(),
        false,
        None,
        None,
    );

    let mock = server
        .mock("GET", "/load/job")
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
        .mock("GET", "/load/job")
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
        .mock("DELETE", "/load/job")
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
        .mock("DELETE", "/load/job")
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

    let job = StoreJob::new(
        "test-job".to_string(),
        "test-stream".to_string(),
        None,
        "test.subject".to_string(),
        "test-bucket".to_string(),
        Some("prefix/".to_string()),
        Batch::default(),
        Encoding::default(),
    );

    let mock = server
        .mock("GET", "/store/jobs")
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

    assert_eq!(result.name, "test-job");
    mock.assert();
}

#[tokio::test]
async fn test_get_store_job_not_found() {
    let mut server = mockito::Server::new_async().await;
    let mock = server
        .mock("GET", "/store/jobs")
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

    let job = StoreJob::new(
        "deleted-job".to_string(),
        "test-stream".to_string(),
        None,
        "test.subject".to_string(),
        "test-bucket".to_string(),
        None,
        Batch::default(),
        Encoding::default(),
    );

    let mock = server
        .mock("DELETE", "/store/job")
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
        .mock("DELETE", "/store/job")
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
