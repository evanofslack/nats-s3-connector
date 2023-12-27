use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct NatsLabels {
    pub subject: String,
    pub stream: String,
}

#[derive(Default, Debug, Clone)]
pub struct NatsMetrics {
    publish_bytes_total: Family<NatsLabels, Counter>,
    publish_messages_total: Family<NatsLabels, Counter>,
    fetch_bytes_total: Family<NatsLabels, Counter>,
    fetch_messages_total: Family<NatsLabels, Counter>,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub registry: Arc<RwLock<Registry>>,
    pub nats: Arc<RwLock<NatsMetrics>>,
}

impl Metrics {
    pub fn new() -> Self {
        let metrics = Metrics {
            registry: Arc::new(RwLock::new(<Registry>::default())),
            nats: Arc::new(RwLock::new(NatsMetrics::default())),
        };

        let mut registry = metrics.registry.write().expect("lock not poisoned");
        let nats = metrics.nats.read().expect("lock not poisoned");

        registry.register(
            "publish_bytes_total",
            "Total size of published messages in bytes",
            nats.publish_bytes_total.clone(),
        );
        registry.register(
            "publish_messages_total",
            "Total count of published messages",
            nats.publish_messages_total.clone(),
        );
        registry.register(
            "fetch_bytes_total",
            "Total size of fetched messages in bytes",
            nats.fetch_bytes_total.clone(),
        );
        registry.register(
            "fetch_messages_total",
            "Total count of fetched messages",
            nats.fetch_messages_total.clone(),
        );

        drop(registry);
        drop(nats);
        return metrics;
    }
}
