use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
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
    pub load_messages_total: Family<NatsLabels, Counter>,
    pub load_bytes_total: Family<NatsLabels, Counter>,
    pub store_messages_total: Family<NatsLabels, Counter>,
    pub store_bytes_total: Family<NatsLabels, Counter>,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub registry: Arc<RwLock<Registry>>,
    pub nats: Arc<RwLock<NatsMetrics>>,
}

impl Metrics {
    pub async fn new() -> Self {
        let metrics = Metrics {
            registry: Arc::new(RwLock::new(<Registry>::default())),
            nats: Arc::new(RwLock::new(NatsMetrics::default())),
        };

        let mut registry = metrics.registry.write().await;
        let nats = metrics.nats.read().await;

        registry.register(
            "store_messages_total",
            "Total count of stored messages",
            nats.store_messages_total.clone(),
        );
        registry.register(
            "store_bytes_total",
            "Total size of stored messages in bytes",
            nats.store_bytes_total.clone(),
        );
        registry.register(
            "load_messages_total",
            "Total count of loaded messages",
            nats.load_messages_total.clone(),
        );
        registry.register(
            "load_bytes_total",
            "Total size of loaded messages in bytes",
            nats.load_bytes_total.clone(),
        );

        drop(registry);
        drop(nats);
        return metrics;
    }
}
