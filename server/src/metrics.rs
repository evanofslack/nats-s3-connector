use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobLabels {
    pub job_type: String,
    pub status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobTypeLabel {
    pub job_type: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectionLabel {
    pub direction: String,
}

#[derive(Default, Debug, Clone)]
pub struct JobMetrics {
    pub jobs_total: Family<JobLabels, Counter>,
    pub jobs_current: Family<JobTypeLabel, Gauge>,
}

#[derive(Default, Debug, Clone)]
pub struct IoMetrics {
    pub nats_messages_total: Family<DirectionLabel, Counter>,
    pub nats_bytes_total: Family<DirectionLabel, Counter>,
    pub s3_objects_total: Family<DirectionLabel, Counter>,
    pub s3_bytes_total: Family<DirectionLabel, Counter>,
}

#[derive(Default, Debug, Clone)]
pub struct Metrics {
    pub registry: Arc<Registry>,
    pub jobs: JobMetrics,
    pub io: IoMetrics,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let jobs = JobMetrics::default();
        let io = IoMetrics::default();

        registry.register(
            "nats3_jobs_total",
            "Total jobs completed or failed",
            jobs.jobs_total.clone(),
        );
        registry.register(
            "nats3_jobs_current",
            "Currently running jobs",
            jobs.jobs_current.clone(),
        );
        registry.register(
            "nats3_nats_messages_total",
            "Total NATS messages processed",
            io.nats_messages_total.clone(),
        );
        registry.register(
            "nats3_nats_bytes_total",
            "Total NATS bytes processed",
            io.nats_bytes_total.clone(),
        );
        registry.register(
            "nats3_s3_objects_total",
            "Total S3 objects processed",
            io.s3_objects_total.clone(),
        );
        registry.register(
            "nats3_s3_bytes_total",
            "Total S3 bytes processed",
            io.s3_bytes_total.clone(),
        );

        Metrics {
            registry: Arc::new(registry),
            jobs,
            io,
        }
    }
}

// Usage constants
pub const JOB_TYPE_STORE: &str = "store";
pub const JOB_TYPE_LOAD: &str = "load";
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";
pub const DIRECTION_IN: &str = "in";
pub const DIRECTION_OUT: &str = "out";
