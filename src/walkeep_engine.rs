use std::sync::Arc;

use pgwire_replication::{ReplicationClient, ReplicationConfig, ReplicationEvent};

use crate::walkeep_utils::{EventSink, RawWalFrame};

pub struct WalKeepEngine {
    config: ReplicationConfig,
    sink: Arc<dyn EventSink>,
}

impl WalKeepEngine {
    pub fn new(config: ReplicationConfig, sink: Arc<dyn EventSink>) -> WalKeepEngine {
        WalKeepEngine { config, sink }
    }

    pub async fn start_pipeline(self) -> Result<(), Box<dyn std::error::Error>> {
        println!("[ENGINE] Connecting to PostgreSQL WAL Stream");

        let mut client: ReplicationClient = ReplicationClient::connect(self.config).await?;
        println!("Handshake verified. Awaiting streaming binary transactions...");

        while let Some(event) = client.recv().await? {
            match event {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    println!("Got data {}: {} bytes", wal_end, data.len());
                    // Acknowledge LSN offset back to Postgres AFTER successful processing
                    let frame = RawWalFrame {
                        lsn: wal_end.into(),
                        data,
                    };
                    
                    self.sink.handle_event(frame).await?;
                    client.update_applied_lsn(wal_end);
                }
                ReplicationEvent::KeepAlive { .. } => {}
                _ => {}
            }
        }
        Ok(())
    }
}
