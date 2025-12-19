use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct ShutdownCoordinator {
    shutdown_token: CancellationToken,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        Self {
            shutdown_token: CancellationToken::new(),
        }
    }

    pub fn subscribe(&self) -> CancellationToken {
        self.shutdown_token.child_token()
    }

    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}
