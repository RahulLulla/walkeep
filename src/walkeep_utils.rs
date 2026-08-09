use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug)]
pub struct RawWalFrame {
    pub lsn: u64,
    pub data: Bytes,
}

#[derive(serde::Serialize)]
pub struct OutboxEvent {
    pub id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// Abstract handler interface. Implementing this allows us to plug in Kafka, Redist or an S3
/// to the WalKeepEngine
#[async_trait]
pub trait EventSink {
    async fn handle_event(&self, frame: RawWalFrame) -> Result<(), Box<dyn std::error::Error>>;
}
