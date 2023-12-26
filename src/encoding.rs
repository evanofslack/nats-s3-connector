use anyhow::{Context, Result};
use async_nats::jetstream;
use bincode;
use sha2::{Digest, Sha256};
use std::time::SystemTime;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use strum_macros::Display;
use thiserror::Error;

const MAGIC_NUMBER: &'static str = "NATS3";
const VERSION: &'static str = "1.0";

#[derive(Error, Debug)]
pub enum ChunkKeyError {
    #[error("invalid key: {key}")]
    InvalidKey { key: String },
    #[error("invalid extension: {ext}")]
    InvalidExt { ext: String },
}

#[derive(Deserialize, Clone, Debug, Display)]
pub enum Codec {
    #[serde(alias = "json", alias = "JSON")]
    Json,
    #[serde(alias = "binary", alias = "bin")]
    Binary,
}

impl Codec {
    pub fn to_extension(&self) -> &str {
        match self {
            Codec::Json => "json",
            Codec::Binary => "bin",
        }
    }
    pub fn from_string(input: String) -> Result<Self> {
        match input.as_str() {
            "json" => Ok(Self::Json),
            "bin" => Ok(Self::Binary),
            _ => Err(ChunkKeyError::InvalidExt { ext: input }.into()),
        }
    }
}

// our repr of a NATS message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    // subject to which message is published to.
    pub subject: String,
    // payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    // optional headers.
    pub headers: Option<HashMap<String, String>>,
    pub length: usize,
}

impl From<jetstream::Message> for Message {
    fn from(source: jetstream::Message) -> Message {
        Message {
            subject: source.subject.clone(),
            payload: source.payload.clone(),
            headers: None,
            length: source.length,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageBlock {
    pub messages: Vec<Message>,
}

impl From<Vec<jetstream::Message>> for MessageBlock {
    fn from(js_messages: Vec<jetstream::Message>) -> MessageBlock {
        let mut messages = Vec::new();
        for m in js_messages {
            messages.push(Message::from(m))
        }
        MessageBlock { messages }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Chunk {
    pub block: MessageBlock,
    magic_number: String,
    version: String,
    hash: Vec<u8>,
    timestamp: u128,
}

impl Chunk {
    pub fn from_block(block: MessageBlock) -> Self {
        let payload: Vec<u8> = bincode::serialize(&block).unwrap();
        let hash = Sha256::digest(&payload);

        Chunk {
            magic_number: MAGIC_NUMBER.to_string(),
            version: VERSION.to_string(),
            block,
            hash: hash.to_vec(),
            timestamp: timestamp(),
        }
    }
    pub fn serialize(&self, codec: Codec) -> Result<Vec<u8>> {
        match codec {
            Codec::Json => return serde_json::to_vec(&self).context("json serialization"),
            Codec::Binary => return bincode::serialize(&self).context("binary serialization"),
        };
    }
    pub fn deserialize(data: Vec<u8>, codec: Codec) -> Result<Self> {
        match codec {
            Codec::Json => return serde_json::from_slice(&data).context("binary deserialization"),
            Codec::Binary => return bincode::deserialize(&data).context("binary deserialization"),
        };
    }

    pub fn key(&self, codec: Codec) -> ChunkKey {
        // let mime_type = match codec {
        //     Codec::Binary => "bin",
        //     Codec::Json => "json",
        // };
        ChunkKey {
            timestamp: self.timestamp,
            message_count: self.block.messages.len(),
            codec,
        }
    }
}

pub struct ChunkKey {
    pub timestamp: u128,
    pub message_count: usize,
    pub codec: Codec,
}

impl ChunkKey {
    pub fn to_string(&self) -> String {
        format!(
            "{}-{}.{}",
            self.timestamp,
            self.message_count,
            self.codec.to_extension()
        )
    }
    pub fn from_string(input: String) -> Result<Self> {
        let input_field = input.clone();
        let (timestamp, rest) = input.split_once("-").ok_or(ChunkKeyError::InvalidKey {
            key: input_field.clone(),
        })?;
        let (count, ext) = rest
            .split_once(".")
            .ok_or(ChunkKeyError::InvalidKey { key: input_field })?;

        let key = Self {
            timestamp: timestamp.parse::<u128>()?,
            message_count: count.parse::<usize>()?,
            codec: Codec::from_string(ext.to_string())?,
        };
        return Ok(key);
    }
}

fn timestamp() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_micros(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
