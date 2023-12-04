pub mod load;
pub mod status;
pub mod store;

use crate::nats;
use crate::s3;

#[derive(Clone)]
pub struct State {
    pub s3_client: s3::Client,
    pub nats_client: nats::Client,
}

impl State {
    pub fn new(s3_client: s3::Client, nats_client: nats::Client) -> Self {
        Self {
            s3_client,
            nats_client,
        }
    }
}
