use anyhow::{Context, Error, Result};
use async_nats::jetstream::{
    self,
    consumer::{pull::Stream, PullConsumer},
};

use bytes::Bytes;
use tracing::{debug, trace};

#[derive(Clone, Debug)]
pub struct Client {
    client: async_nats::Client,
}

impl Client {
    pub async fn new(url: String) -> Result<Self, Error> {
        debug!(url = url, "create new nats client");
        let client = async_nats::connect(url.clone())
            .await
            .context("fail connect to nats server")?;
        let client = Client { client };
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

    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
        trace!(bytes = payload.len(), subject = subject, "publish message");
        let jetstream = jetstream::new(self.client.clone());
        jetstream.publish(subject, payload).await?.await?;
        Ok(())
    }
}
