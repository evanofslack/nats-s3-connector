use anyhow::{Context, Error, Result};
use async_nats::{
    header::HeaderMap,
    jetstream::{
        self,
        consumer::{pull::Stream, PullConsumer},
    },
};
use bytes::Bytes;
use std::collections::BTreeMap;
use tracing::{debug, trace};

use crate::metrics;

#[derive(Clone, Debug)]
pub struct Client {
    client: async_nats::Client,
    metrics: metrics::Metrics,
}

impl Client {
    pub async fn new(url: String, metrics: metrics::Metrics) -> Result<Self, Error> {
        debug!(url = url, "create new nats client");
        let client = async_nats::connect(url.clone())
            .await
            .context("fail connect to nats server")?;
        let client = Client { client, metrics };
        Ok(client)
    }

    pub async fn consume(
        &self,
        stream_name: String,
        subject: String,
        max_ack_pending: i64,
    ) -> Result<Stream, Error> {
        debug!(stream = stream_name, subject = subject, "consume stream");
        let jetstream = jetstream::new(self.client.clone());

        let stream = jetstream.get_stream(stream_name.clone()).await?;

        // TODO: option to subscribe to existing consumer
        // let consumer: PullConsumer = stream.get_consumer(&consumer_name).await?;

        // need to replace special chars for consumer names
        let name = subject
            .replace(".", "_")
            .replace(">", "_")
            .replace("*", "_");

        let filter_subject = subject.clone();

        debug!(
            name = name,
            filter_subject = subject.clone(),
            "create consumer"
        );

        let consumer: PullConsumer = stream
            .get_or_create_consumer(
                name.as_str(),
                jetstream::consumer::pull::Config {
                    filter_subject,
                    durable_name: Some(name.clone()),
                    max_ack_pending,
                    ..Default::default()
                },
            )
            .await?;

        let messages = consumer.messages().await?;
        Ok(messages)
    }

    pub async fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: Option<BTreeMap<String, Vec<String>>>,
    ) -> Result<(), Error> {
        let byte_count = payload.len();
        trace!(bytes = byte_count, subject = subject, "publish message");
        let jetstream = jetstream::new(self.client.clone());

        let ack = if let Some(headers_map) = headers {
            let mut nats_headers = HeaderMap::new();
            for (key, values) in headers_map {
                for value in values {
                    nats_headers.append(key.as_str(), value.as_str());
                }
            }
            jetstream
                .publish_with_headers(subject, nats_headers, payload)
                .await?
        } else {
            jetstream.publish(subject, payload).await?
        };

        ack.await?;

        self.metrics
            .io
            .nats_messages_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_IN.to_string(),
            })
            .inc();
        self.metrics
            .io
            .nats_bytes_total
            .get_or_create(&metrics::DirectionLabel {
                direction: metrics::DIRECTION_IN.to_string(),
            })
            .inc_by(byte_count as u64);
        Ok(())
    }
}
