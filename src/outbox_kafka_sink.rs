use std::time::Duration;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

use crate::walkeep_utils::{EventSink, OutboxEvent, RawWalFrame};
pub struct OutboxKafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl OutboxKafkaSink {
    pub fn new(broker: &str, topic: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("enable.idempotence", "true")
            .create()
            .expect("Failed to initialize Kafka producer");

        Self {
            producer,
            topic: topic.to_string(),
        }
    }

    /**
     * Insert pgout structure:
     * Message Type Prefix - 1 Byte
     * Relation ID - 4 Bytes
     * Tuple Data Flag - 1 Byte
     * Column Count - 2 Bytes
     * Column Values - Each column is 4 bytes
     */
    fn parse_pgoutput(&self, mut data: Bytes) -> Option<OutboxEvent> {
        if data.is_empty() {
            return None;
        }

        if data.get_u8() as char != 'I' {
            return None;
        }

        if data.len() < 5 {
            return None;
        }
        data.advance(4);
        let tuple_flag = data.get_u8() as char;
        if tuple_flag != 'N' {
            // N for new flag
            return None;
        }

        let column_count = data.get_u16();
        let mut columns: Vec<String> = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            if data.is_empty() {
                return None;
            }
            let col_type = data.get_u8() as char;
            // t for text/binary and n for null
            match col_type {
                't' => {
                    let val_len = data.get_u32() as usize;

                    let val_bytes = &data[..val_len];
                    let val_str = String::from_utf8_lossy(val_bytes).into_owned();

                    columns.push(val_str);
                    data.advance(val_len);
                }
                'n' => {
                    columns.push(String::new());
                }
                _ => return None,
            }
        }

        if columns.len() >= 5 {
            let payload_json: serde_json::Value =
                serde_json::from_str(&columns[4]).unwrap_or(serde_json::Value::Null);
            println!("[DECODER] Captured Insert Event in WalKeep Stream");
            return Some(OutboxEvent {
                id: columns[0].clone(),
                aggregate_type: columns[1].clone(),
                aggregate_id: columns[2].clone(),
                event_type: columns[3].clone(),
                payload: payload_json,
            });
        }
        None
    }
}

#[async_trait]
impl EventSink for OutboxKafkaSink {
    async fn handle_event(&self, frame: RawWalFrame) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(event) = self.parse_pgoutput(frame.data) {
            let serialized = serde_json::to_string(&event)?;

            let record = FutureRecord::to(&self.topic)
                .payload(&serialized)
                .key(&event.aggregate_id);
            match self.producer.send(record, Duration::from_secs(2)).await {
                Ok((partition, offset)) => {
                    println!(
                        "[KAFKA PUBLISHED] Event ID: {} |
                    Topic: {} | Partition: {} | Offset: {}",
                        event.id, self.topic, partition, offset
                    );
                }
                Err((err, _)) => return Err(Box::new(err)),
            }
        }
        Ok(())
    }
}
