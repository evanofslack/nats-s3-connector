use crate::message::{extract_sequence, TestMessage};
use async_nats::jetstream::consumer::PullConsumer;
use futures::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tracing::{debug, info, warn};

pub struct Verifier {
    expected: HashMap<usize, TestMessage>,
    received: Vec<usize>,
    missing: HashSet<usize>,
    mismatches: Vec<Mismatch>,
}

#[derive(Debug)]
pub enum Mismatch {
    HeaderMismatch { seq: usize, key: String },
    PayloadMismatch { seq: usize },
    UnexpectedMessage { seq: usize },
}

#[derive(Debug)]
pub struct OrderingViolation {
    pub expected_after: usize,
    pub got: usize,
}

pub struct TestResults {
    pub total_expected: usize,
    pub total_received: usize,
    pub missing: Vec<usize>,
    pub mismatches: Vec<Mismatch>,
    pub ordering_violations: Vec<OrderingViolation>,
    pub duration: Duration,
    pub success: bool,
}

impl Verifier {
    pub fn new(messages: Vec<TestMessage>) -> Self {
        let expected: HashMap<_, _> = messages
            .into_iter()
            .enumerate()
            .map(|(i, msg)| (i, msg))
            .collect();

        let missing = expected.keys().copied().collect();

        Self {
            expected,
            received: Vec::new(),
            missing,
            mismatches: Vec::new(),
        }
    }

    pub fn verify_message(&mut self, msg: &async_nats::Message) -> bool {
        let Some(seq) = extract_sequence(&msg.headers.clone().expect("message has header")) else {
            debug!("Message missing sequence header");
            return false;
        };

        let Some(expected) = self.expected.get(&seq) else {
            self.mismatches.push(Mismatch::UnexpectedMessage { seq });
            return false;
        };

        for (key, value) in &expected.headers {
            match msg
                .headers
                .clone()
                .expect("message has headers")
                .get(key.as_str())
            {
                Some(header_value) => {
                    if header_value.as_str() != value.as_str() {
                        self.mismatches.push(Mismatch::HeaderMismatch {
                            seq,
                            key: key.clone(),
                        });
                        return false;
                    }
                }
                None => {
                    self.mismatches.push(Mismatch::HeaderMismatch {
                        seq,
                        key: key.clone(),
                    });
                    return false;
                }
            }
        }

        if msg.payload.as_ref() != expected.payload.as_slice() {
            self.mismatches.push(Mismatch::PayloadMismatch { seq });
            return false;
        }

        self.received.push(seq);
        self.missing.remove(&seq);

        debug!("Message {} verified successfully", seq);
        true
    }

    fn check_ordering(&self) -> Vec<OrderingViolation> {
        let mut violations = Vec::new();

        for i in 1..self.received.len() {
            let prev = self.received[i - 1];
            let curr = self.received[i];

            if curr != prev + 1 {
                violations.push(OrderingViolation {
                    expected_after: prev,
                    got: curr,
                });
            }
        }

        violations
    }

    pub fn into_results(self, duration: Duration) -> TestResults {
        let ordering_violations = self.check_ordering();
        let mut missing: Vec<_> = self.missing.into_iter().collect();
        missing.sort();

        let success =
            missing.is_empty() && self.mismatches.is_empty() && ordering_violations.is_empty();

        TestResults {
            total_expected: self.expected.len(),
            total_received: self.received.len(),
            missing,
            mismatches: self.mismatches,
            ordering_violations,
            duration,
            success,
        }
    }
}

pub async fn consume_and_verify(
    consumer: PullConsumer,
    mut verifier: Verifier,
    idle_timeout: u64,
) -> anyhow::Result<TestResults> {
    info!("Starting message consumption and verification");

    let start = Instant::now();
    let mut last_message_time = Instant::now();
    let idle_duration = Duration::from_secs(idle_timeout);

    let mut messages = consumer.messages().await?;

    loop {
        if last_message_time.elapsed() >= idle_duration {
            info!("Idle timeout reached, completing test");
            break;
        }

        match timeout(Duration::from_secs(1), messages.next()).await {
            Ok(Some(msg)) => {
                let msg = msg?;
                last_message_time = Instant::now();

                if verifier.verify_message(&msg) {
                    match msg.ack().await {
                        Ok(()) => {}
                        Err(err) => warn!(err = err, "message ack"),
                    }
                } else {
                    match msg.ack().await {
                        Ok(()) => {}
                        Err(err) => warn!(err = err, "message ack"),
                    }
                }

                if verifier.missing.is_empty() {
                    info!("All messages received and verified");
                    break;
                }

                if (verifier.received.len() % 10) == 0 {
                    info!(
                        "Progress: {}/{} messages verified",
                        verifier.received.len(),
                        verifier.expected.len()
                    );
                }
            }
            Ok(None) => {
                debug!("Stream ended");
                break;
            }
            Err(_) => {
                continue;
            }
        }
    }

    let duration = start.elapsed();
    info!("Verification complete in {:?}", duration);

    Ok(verifier.into_results(duration))
}

impl TestResults {
    pub fn print(&self) {
        println!();
        println!("E2E Test Results");
        println!("Duration: {:?}", self.duration);
        println!("Expected: {}", self.total_expected);
        println!("Received: {}", self.total_received);
        println!();

        if self.success {
            println!("All messages verified successfully");
            println!("Message ordering preserved");
        } else {
            if !self.missing.is_empty() {
                println!("Missing messages: {}", self.missing.len());
                if self.missing.len() <= 20 {
                    println!("Missing sequences: {:?}", self.missing);
                } else {
                    println!("Missing sequences (first 20): {:?}", &self.missing[..20]);
                }
            }

            if !self.mismatches.is_empty() {
                println!("Mismatches: {}", self.mismatches.len());
                for (i, m) in self.mismatches.iter().enumerate() {
                    if i >= 10 {
                        println!("... and {} more mismatches", self.mismatches.len() - i);
                        break;
                    }
                    match m {
                        Mismatch::HeaderMismatch { seq, key } => {
                            println!("  Sequence {}: header mismatch on key '{}'", seq, key);
                        }
                        Mismatch::PayloadMismatch { seq } => {
                            println!("  Sequence {}: payload mismatch", seq);
                        }
                        Mismatch::UnexpectedMessage { seq } => {
                            println!("  Sequence {}: unexpected message", seq);
                        }
                    }
                }
            }

            if !self.ordering_violations.is_empty() {
                println!("Ordering violations: {}", self.ordering_violations.len());
                for (i, v) in self.ordering_violations.iter().enumerate() {
                    if i >= 10 {
                        println!(
                            "... and {} more violations",
                            self.ordering_violations.len() - i
                        );
                        break;
                    }
                    println!(
                        "  Expected sequence after {} but got {}",
                        v.expected_after, v.got
                    );
                }
            }
        }

        println!();
    }
}
