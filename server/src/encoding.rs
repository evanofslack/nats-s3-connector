use anyhow::{Context, Result};
use async_nats::{header, jetstream};
use bytes::Bytes;
use nats3_types::Codec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, fmt};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use crate::{db::CreateChunkMetadata, io::ConsumeConfig};

const MAGIC_NUMBER: &str = "NATS3";
const VERSION: &str = "1.0";

// our repr of a NATS message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    // subject to which message is published to.
    pub subject: String,
    // payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    // optional headers.
    pub headers: Option<HashMap<String, Vec<String>>>,
    pub length: usize,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub sequence: u64,
}

impl From<jetstream::Message> for Message {
    fn from(source: jetstream::Message) -> Message {
        let headers_ref = source.headers.as_ref();
        let timestamp = headers_ref
            .and_then(|h| h.get_last(header::NATS_TIME_STAMP))
            .and_then(|ts| OffsetDateTime::parse(ts.as_str(), &Rfc3339).ok())
            .and_then(|odt| {
                chrono::DateTime::<chrono::Utc>::from_timestamp(
                    odt.unix_timestamp(),
                    odt.nanosecond(),
                )
            })
            .unwrap_or_else(chrono::Utc::now);

        let sequence = headers_ref
            .and_then(|h| h.get_last(header::NATS_SEQUENCE))
            .and_then(|seq| seq.as_str().parse::<u64>().ok())
            .unwrap_or(0);

        let headers = source.headers.as_ref().map(|h| {
            h.iter()
                .map(|(k, values)| {
                    (
                        k.to_string(),
                        values.iter().map(|v| v.to_string()).collect(),
                    )
                })
                .collect()
        });

        Message {
            subject: source.subject.to_string(),
            payload: source.payload.clone(),
            headers,
            length: source.length,
            timestamp,
            sequence,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageBlock {
    pub messages: Vec<Message>,
    pub timestamp_min: chrono::DateTime<chrono::Utc>,
    pub timestamp_max: chrono::DateTime<chrono::Utc>,
    pub bytes_total: usize,
}

impl MessageBlock {
    pub fn hash(&self) -> Bytes {
        let config = bincode::config::legacy();
        let payload: Vec<u8> = bincode::serde::encode_to_vec(self, config).unwrap();
        Bytes::from(Sha256::digest(&payload).to_vec())
    }
}

impl From<Vec<jetstream::Message>> for MessageBlock {
    fn from(js_messages: Vec<jetstream::Message>) -> MessageBlock {
        let messages: Vec<Message> = js_messages.into_iter().map(Message::from).collect();

        let timestamp_min = messages
            .iter()
            .min_by_key(|m| m.timestamp)
            .map(|m| m.timestamp)
            .unwrap_or_else(chrono::Utc::now);

        let timestamp_max = messages
            .iter()
            .max_by_key(|m| m.timestamp)
            .map(|m| m.timestamp)
            .unwrap_or_else(chrono::Utc::now);

        let bytes_total = messages.iter().map(|m| m.length).sum();

        MessageBlock {
            messages,
            timestamp_min,
            timestamp_max,
            bytes_total,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Chunk {
    pub block: MessageBlock,
    magic_number: String,
    version: String,
    pub hash: Bytes,
}

impl Chunk {
    pub fn serialize(&self, codec: Codec) -> Result<Vec<u8>> {
        match codec {
            Codec::Json => serde_json::to_vec(&self).context("json serialization"),
            Codec::Binary => {
                let config = bincode::config::legacy();
                bincode::serde::encode_to_vec(self, config).context("binary serialization")
            }
        }
    }
    pub fn deserialize(data: Vec<u8>, codec: Codec) -> Result<Self> {
        match codec {
            Codec::Json => serde_json::from_slice(&data).context("binary deserialization"),
            Codec::Binary => {
                let config = bincode::config::legacy();
                let (chunk, _) = bincode::serde::decode_from_slice(&data, config)
                    .context("binary deserialization")?;
                Ok(chunk)
            }
        }
    }
    pub fn to_chunk_metadata(
        &self,
        config: &ConsumeConfig,
        key: &str,
        serialized_size: usize,
    ) -> CreateChunkMetadata {
        CreateChunkMetadata {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            key: key.to_string(),
            stream: config.stream.clone(),
            consumer: config.consumer.clone(),
            subject: config.subject.clone(),
            timestamp_start: self.block.timestamp_min,
            timestamp_end: self.block.timestamp_max,
            message_count: self.block.messages.len() as i64,
            size_bytes: serialized_size as i64,
            codec: config.codec.clone(),
            hash: self.hash.clone(),
            version: self.version.clone(),
        }
    }

    pub fn key(&self, codec: Codec) -> ChunkKey {
        ChunkKey {
            timestamp: self.block.timestamp_min.timestamp(),
            message_count: self.block.messages.len(),
            codec,
        }
    }
}

impl From<MessageBlock> for Chunk {
    fn from(block: MessageBlock) -> Self {
        let hash = block.hash();
        Chunk {
            magic_number: MAGIC_NUMBER.to_string(),
            version: VERSION.to_string(),
            block,
            hash,
        }
    }
}

pub struct ChunkKey {
    pub timestamp: i64,
    pub message_count: usize,
    pub codec: Codec,
}

impl fmt::Display for ChunkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}.{}",
            self.timestamp,
            self.message_count,
            self.codec.to_extension()
        )
    }
}
