use anyhow::Result;
use inquire::{Confirm, Text};
use nats3_types::{Batch, Codec, CreateLoadJob, CreateStoreJob, Encoding};

pub fn prompt_create_load_job() -> Result<CreateLoadJob> {
    let bucket = Text::new("Bucket name:").prompt()?;

    let prefix = Text::new("Key prefix (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?;

    let read_stream = Text::new("Read stream:").prompt()?;
    let read_subject = Text::new("Read subject:").prompt()?;
    let write_stream = Text::new("Write stream:").prompt()?;
    let write_subject = Text::new("Write subject:").prompt()?;

    let delete_chunks = Confirm::new("Delete chunks after load?")
        .with_default(false)
        .prompt()?;

    let start = Text::new("Start index (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?
        .and_then(|s| s.parse().ok());

    let end = Text::new("End index (optional):")
        .with_help_message("Press Enter to skip")
        .prompt_skippable()?
        .and_then(|s| s.parse().ok());

    Ok(CreateLoadJob {
        bucket,
        prefix,
        read_stream,
        read_subject,
        write_stream,
        write_subject,
        delete_chunks,
        start,
        end,
    })
}

pub fn prompt_create_store_job() -> Result<CreateStoreJob> {
    let name = Text::new("Job name:").prompt()?;
    let stream = Text::new("Stream:").prompt()?;
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
        subject,
        bucket,
        prefix,
        batch,
        encoding,
    })
}
