use anyhow::{Context, Error, Result};
use async_nats::jetstream::{self, consumer::pull::Stream, consumer::PullConsumer};

use bytes::Bytes;

pub struct Client {
    client: async_nats::Client,
}

impl Client {
    pub async fn new(url: String) -> Result<Self, Error> {
        let client = async_nats::connect(url.clone())
            .await
            .context("failed to connect to nats server")?;
        let client = Client { client };
        return Ok(client);
    }

    pub async fn consume(&self, stream_name: String, subject: String) -> Result<Stream, Error> {
        let jetstream = jetstream::new(self.client.clone());

        let stream = jetstream.get_stream(stream_name.clone()).await?;
        // let consumer: PullConsumer = stream.get_consumer(&consumer_name).await?;

        let consumer: PullConsumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some(subject.into()),
                ..Default::default()
            })
            .await?;
        let messages = consumer.messages().await?;
        return Ok(messages);
    }

    pub async fn publish(&self, subject: String, payload: Bytes) -> Result<(), Error> {
        let jetstream = jetstream::new(self.client.clone());
        jetstream.publish(subject, payload).await?;
        return Ok(());
    }
}
