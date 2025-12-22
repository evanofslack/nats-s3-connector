use anyhow::Result;
use chrono::{DateTime, Utc};
use inquire::{Confirm, Text};
use nats3_types::{Batch, Codec, CreateLoadJob, CreateStoreJob, Encoding};

pub fn prompt_create_load_job() -> Result<CreateLoadJob> {
    let name = Text::new("Job name (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;
    let bucket = Text::new("Bucket name:").prompt()?;

    let prefix = Text::new("Key prefix (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;

    let read_stream = Text::new("Read stream:").prompt()?;
    let read_consumer = Text::new("Read consumer (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;
    let read_subject = Text::new("Read subject:").prompt()?;
    let write_subject = Text::new("Write subject:").prompt()?;

    let poll_interval = Text::new("Poll interval (optional)?")
        .with_help_message("Duration to keep trying load (e.g. 5sec, 1min). Press Enter to skip")
        .prompt_skippable()?
        .map(|s| humantime::parse_duration(&s))
        .transpose()?;

    let delete_chunks = Confirm::new("Delete chunks after load?")
        .with_default(false)
        .prompt()?;

    let from_time = Text::new("To time (optional):")
        .with_help_message("RFC3339 format (e.g. 2024-12-14T18:00:00Z). Press Enter to skip")
        .prompt_skippable()?
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let to_time = Text::new("To time (optional):")
        .with_help_message("RFC3339 format (e.g. 2024-12-14T18:00:00Z). Press Enter to skip")
        .prompt_skippable()?
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    Ok(CreateLoadJob {
        name,
        bucket,
        prefix,
        read_stream,
        read_consumer,
        read_subject,
        write_subject,
        poll_interval,
        delete_chunks,
        from_time,
        to_time,
    })
}

pub fn prompt_create_store_job() -> Result<CreateStoreJob> {
    let name = Text::new("Job name:").prompt()?;
    let stream = Text::new("Stream:").prompt()?;
    let consumer = Text::new("Consumer (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;
    let subject = Text::new("Subject:").prompt()?;
    let bucket = Text::new("Bucket:").prompt()?;

    let prefix = Text::new("Key prefix (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;

    let configure_batch = Confirm::new("Configure batch settings?")
        .with_default(false)
        .prompt()?;

    let batch = if configure_batch {
        let max_bytes = Text::new("Max bytes per batch:")
            .with_default("1000000")
            .prompt()?
            .parse()?;

        let max_count = Text::new("Max messages per batch:")
            .with_default("1000")
            .prompt()?
            .parse()?;

        Some(Batch {
            max_bytes,
            max_count,
        })
    } else {
        None
    };

    let configure_encoding = Confirm::new("Configure encoding?")
        .with_default(false)
        .prompt()?;

    let encoding = if configure_encoding {
        let codec_str = Text::new("Codec (json/binary):")
            .with_default("binary")
            .prompt()?;

        let codec = codec_str.parse::<Codec>()?;

        Some(Encoding { codec })
    } else {
        None
    };

    Ok(CreateStoreJob {
        name,
        stream,
        consumer,
        subject,
        bucket,
        prefix,
        batch,
        encoding,
    })
}

pub fn prompt_job_id() -> Result<String> {
    let job_id = Text::new("Job id:").prompt()?;
    Ok(job_id)
}
