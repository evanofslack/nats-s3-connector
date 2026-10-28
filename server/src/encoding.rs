use anyhow::{Context, Result};
use async_nats::jetstream;
use bytes::Bytes;
use nats3_types::Codec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt};

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
    // must use ordered map for determinstic hashing.
    pub headers: Option<BTreeMap<String, Vec<String>>>,
    pub length: usize,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub sequence: u64,
}

impl From<jetstream::Message> for Message {
    fn from(source: jetstream::Message) -> Message {
        let (timestamp, sequence) = match source.info() {
            Ok(info) => (
                chrono::DateTime::<chrono::Utc>::from_timestamp(
                    info.published.unix_timestamp(),
                    info.published.nanosecond(),
                )
                .unwrap_or_else(chrono::Utc::now),
                info.stream_sequence,
            ),
            Err(_) => (chrono::Utc::now(), 0),
        };

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
    pub fn deserialize(data: &Bytes, codec: Codec) -> Result<Self> {
        match codec {
            Codec::Json => serde_json::from_slice(data).context("binary deserialization"),
            Codec::Binary => {
                let config = bincode::config::legacy();
                let (chunk, _) = bincode::serde::decode_from_slice(data, config)
                    .context("binary deserialization")?;
                Ok(chunk)
            }
        }
    }
    pub fn to_chunk_metadata(
        &self,
        config: &ConsumeConfig,
        key: &str,
        byte_count: usize,
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
            size_bytes: byte_count as i64,
            codec: config.codec.clone(),
            hash: self.hash.clone(),
            version: self.version.clone(),
        }
    }

    pub fn key(&self, codec: Codec) -> ChunkKey {
        let sequences = self.block.messages.iter().map(|m| m.sequence);
        ChunkKey {
            timestamp_ms: self.block.timestamp_min.timestamp_millis(),
            sequence_min: sequences.clone().min().unwrap_or(0),
            sequence_max: sequences.max().unwrap_or(0),
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
    pub timestamp_ms: i64,
    pub sequence_min: u64,
    pub sequence_max: u64,
    pub codec: Codec,
}

impl fmt::Display for ChunkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}.{}",
            self.timestamp_ms,
            self.sequence_min,
            self.sequence_max,
            self.codec.to_extension()
        )
    }
}
