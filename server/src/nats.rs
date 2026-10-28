use anyhow::{Context, Error, Result, bail};
use async_nats::{
    header::HeaderMap,
    jetstream::{
        self,
        consumer::{PullConsumer, pull::Stream},
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
        consumer_name: String,
        max_ack_pending: i64,
    ) -> Result<Stream, Error> {
        debug!(
            stream = stream_name,
            subject = subject,
            consumer = consumer_name,
            "consume stream"
        );
        let jetstream = jetstream::new(self.client.clone());
        let stream = jetstream.get_stream(stream_name.clone()).await?;

        let consumer: PullConsumer = stream
            .get_or_create_consumer(
                consumer_name.as_str(),
                jetstream::consumer::pull::Config {
                    filter_subject: subject.clone(),
                    durable_name: Some(consumer_name.clone()),
                    max_ack_pending,
                    ..Default::default()
                },
            )
            .await?;

        let config = &consumer.cached_info().config;
        let filter_matches =
            config.filter_subject == subject || config.filter_subjects == [subject.clone()];
        if !filter_matches {
            bail!(
                "consumer {} has filter subject {:?}, expected {}",
                consumer_name,
                config.filter_subject,
                subject
            );
        }

        let messages = consumer.messages().await?;
        Ok(messages)
    }

    pub async fn delete_consumer(&self, stream_name: String, consumer_name: String) -> Result<()> {
        debug!(
            stream = stream_name,
            consumer = consumer_name,
            "delete consumer"
        );
        let jetstream = jetstream::new(self.client.clone());
        let stream = jetstream.get_stream(stream_name).await?;
        stream.delete_consumer(&consumer_name).await?;
        Ok(())
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
