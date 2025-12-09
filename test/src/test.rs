use crate::config::Config;
use crate::message::{generate_messages, TestMessage};
use crate::verify::{consume_and_verify, TestResults, Verifier};
use async_nats::jetstream::consumer::{
    pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy, PullConsumer,
};
use tracing::info;

pub async fn run(config: &Config) -> anyhow::Result<TestResults> {
    let messages = generate_messages(config);
    info!("Generated {} test messages", messages.len());

    let nats_client = async_nats::connect(&config.nats_url).await?;
    let jetstream = async_nats::jetstream::new(nats_client);

    publish_messages(&jetstream, config, &messages).await?;

    let consumer = create_consumer(&jetstream, config).await?;
    let verifier = Verifier::new(messages);

    let results = consume_and_verify(consumer, verifier, config.idle_timeout).await?;

    Ok(results)
}

async fn publish_messages(
    jetstream: &async_nats::jetstream::Context,
    config: &Config,
    messages: &[TestMessage],
) -> anyhow::Result<()> {
    info!(
        "Publishing {} messages to {}",
        messages.len(),
        config.input_subject
    );

    for (i, msg) in messages.iter().enumerate() {
        let mut headers = async_nats::HeaderMap::new();
        for (k, v) in &msg.headers {
            headers.insert(k.as_str(), v.as_str());
        }

        jetstream
            .publish_with_headers(
                config.input_subject.clone(),
                headers,
                msg.payload.clone().into(),
            )
            .await?
            .await?;

        if (i + 1) % 10 == 0 {
            info!("Published {}/{} messages", i + 1, messages.len());
        }
    }

    info!("All messages published");
    Ok(())
}

async fn create_consumer(
    jetstream: &async_nats::jetstream::Context,
    config: &Config,
) -> anyhow::Result<PullConsumer> {
    info!("Creating consumer for stream: {}", config.output_stream);

    let consumer_config = ConsumerConfig {
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ..Default::default()
    };

    let consumer = jetstream
        .create_consumer_on_stream(consumer_config, &config.output_stream)
        .await?;

    Ok(consumer)
}
