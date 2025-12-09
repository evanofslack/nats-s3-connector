use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::config::Config;

#[derive(Debug, Clone)]
pub struct TestMessage {
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MessagePayload {
    test_run_id: String,
    sequence: usize,
    timestamp: String,
    data: String,
}

pub fn generate_messages(config: &Config) -> Vec<TestMessage> {
    let test_run_id = Uuid::new_v4().to_string();

    (0..config.num_messages)
        .map(|seq| {
            let headers = generate_headers(&test_run_id, seq, config);
            let payload = generate_payload(&test_run_id, seq, config.message_size);

            TestMessage { headers, payload }
        })
        .collect()
}

fn generate_headers(test_run_id: &str, seq: usize, config: &Config) -> HashMap<String, String> {
    if config.header_none {
        return HashMap::new();
    }

    let mut headers = HashMap::new();
    headers.insert("test-run-id".to_string(), test_run_id.to_string());
    headers.insert("msg-sequence".to_string(), seq.to_string());
    headers.insert("msg-timestamp".to_string(), chrono::Utc::now().to_rfc3339());

    if config.header_random {
        let random_data = generate_random_string(config.header_length);
        headers.insert("random-data".to_string(), random_data);
    }

    headers
}

fn generate_payload(test_run_id: &str, seq: usize, size: usize) -> Vec<u8> {
    let base_payload = MessagePayload {
        test_run_id: test_run_id.to_string(),
        sequence: seq,
        timestamp: chrono::Utc::now().to_rfc3339(),
        data: String::new(),
    };

    let base_size = serde_json::to_vec(&base_payload).unwrap().len();
    let remaining_size = size.saturating_sub(base_size + 20);

    let payload = MessagePayload {
        data: generate_random_string(remaining_size),
        ..base_payload
    };

    serde_json::to_vec(&payload).unwrap()
}

fn generate_random_string(length: usize) -> String {
    let mut rng = rand::rng();
    (0..length)
        .map(|_| rng.sample(rand::distr::Alphanumeric) as char)
        .collect()
}

pub fn extract_sequence(headers: &async_nats::HeaderMap) -> Option<usize> {
    headers
        .get("msg-sequence")
        .and_then(|v| Some(v.as_str()))
        .and_then(|s| s.parse().ok())
}
