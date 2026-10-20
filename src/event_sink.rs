use crate::walkeep_utils::RawWalFrame;
use async_trait::async_trait;

/// Abstract handler interface. Implementing this allows us to plug in Kafka, Redist or an S3
/// to the WalKeepEngine
#[async_trait]
pub trait EventSink {
    async fn handle_event(&self, frame: RawWalFrame) -> Result<(), Box<dyn std::error::Error>>;
}
