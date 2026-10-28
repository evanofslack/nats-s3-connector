use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use nats3_types::{Batch, Encoding, StoreJobCreate, StoreJobStatus};

use super::Coordinator;
use crate::{
    completer::TaskCompleter,
    db::{self, StoreJobStorer},
    error, io_tests, metrics, registry,
};

async fn wait_for_store_failure(
    store: &db::PostgresStore,
    registry: &registry::Registry,
    job_id: &str,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let job = store.get_store_job(job_id.to_string()).await.unwrap();
            if job.status == StoreJobStatus::Failure && !registry.is_store_job_running(job_id).await
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timeout wait for store job failure");
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn test_store_job_missing_stream_fails() {
    let env = io_tests::setup().await;
    let shutdown = CancellationToken::new();
    let registry = Arc::new(registry::Registry::new(shutdown.clone()));
    let job_db: db::DynJobStorer = Arc::new(env.store.clone());
    let metrics = env.io.metrics.clone();
    let coordinator = Coordinator::new(
        registry.clone(),
        env.io.clone(),
        job_db.clone(),
        metrics.clone(),
    );
    TaskCompleter::new(job_db, registry.clone(), metrics.clone()).start(shutdown.clone());

    let job = coordinator
        .start_new_store_job(StoreJobCreate {
            name: "missing".to_string(),
            bucket: "test-bucket".to_string(),
            prefix: None,
            stream: "MISSING".to_string(),
            consumer: None,
            subject: "missing.test".to_string(),
            batch: Batch::default(),
            encoding: Encoding::default(),
        })
        .await
        .expect("fail start store job");
    assert_eq!(job.consumer, Some(format!("nats3-{}", job.id)));

    wait_for_store_failure(&env.store, &registry, &job.id).await;

    let result = coordinator.resume_store_job(job.id.clone()).await;
    assert!(
        !matches!(
            result,
            Err(error::AppError::JobRegistry(
                registry::RegistryError::JobAlreadyRunning { .. }
            ))
        ),
        "{result:?}"
    );

    wait_for_store_failure(&env.store, &registry, &job.id).await;

    let current = metrics
        .jobs
        .jobs_current
        .get_or_create(&metrics::JobTypeLabel {
            job_type: metrics::JOB_TYPE_STORE.to_string(),
        })
        .get();
    assert_eq!(current, 0);
    shutdown.cancel();
}
