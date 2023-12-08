use async_nats::jetstream;
use bincode;
use sha2::{Digest, Sha256};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const MAGIC_NUMBER: &'static str = "S3NATSCONNECT";
const VERSION: &'static str = "1";

// Our repr of a NATS message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// Subject to which message is published to.
    pub subject: String,
    /// Payload of the message. Can be any arbitrary data format.
    pub payload: Bytes,
    /// Optional headers.
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
}

impl Chunk {
    pub fn from_block(block: MessageBlock) -> Self {
        let payload: Vec<u8> = bincode::serialize(&block).unwrap();
        let _hash = Sha256::digest(&payload);

        Chunk {
            magic_number: MAGIC_NUMBER.to_string(),
            version: VERSION.to_string(),
            block,
            hash: _hash.to_vec(),
        }
    }
}
