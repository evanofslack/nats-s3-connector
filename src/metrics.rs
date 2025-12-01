use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct NatsLabels {
    pub subject: String,
    pub stream: String,
}

#[derive(Default, Debug, Clone)]
pub struct NatsMetrics {
    pub load_messages: Family<NatsLabels, Counter>,
    pub load_bytes: Family<NatsLabels, Counter>,
    pub store_messages: Family<NatsLabels, Counter>,
    pub store_bytes: Family<NatsLabels, Counter>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobLabels {
    pub stream: String,
    pub subject: String,
    pub bucket: String,
}

#[derive(Default, Debug, Clone)]
pub struct JobMetrics {
    pub load_jobs: Family<JobLabels, Gauge>,
    pub store_jobs: Family<JobLabels, Gauge>,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub registry: Arc<RwLock<Registry>>,
    pub nats: Arc<RwLock<NatsMetrics>>,
    pub jobs: Arc<RwLock<JobMetrics>>,
}

impl Metrics {
    pub async fn new() -> Self {
        let metrics = Metrics {
            registry: Arc::new(RwLock::new(<Registry>::default())),
            nats: Arc::new(RwLock::new(NatsMetrics::default())),
            jobs: Arc::new(RwLock::new(JobMetrics::default())),
        };

        let mut registry = metrics.registry.write().await;
        let nats = metrics.nats.read().await;
        let jobs = metrics.jobs.read().await;

        registry.register(
            "nats3_store_messages",
            "Total count of stored messages",
            nats.store_messages.clone(),
        );
        registry.register(
            "nats3_store_bytes",
            "Total size of stored messages in bytes",
            nats.store_bytes.clone(),
        );
        registry.register(
            "nats3_load_messages",
            "Total count of loaded messages",
            nats.load_messages.clone(),
        );
        registry.register(
            "nats3_load_bytes",
            "Total size of loaded messages in bytes",
            nats.load_bytes.clone(),
        );
        registry.register(
            "nats3_store_jobs",
            "Number of store jobs in progress",
            jobs.store_jobs.clone(),
        );
        registry.register(
            "nats3_load_jobs",
            "Number of load jobs in progress",
            jobs.load_jobs.clone(),
        );

        drop(registry);
        drop(nats);
        drop(jobs);
        metrics
    }
}
