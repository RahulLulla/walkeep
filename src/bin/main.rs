extern crate walkeep;
use std::sync::Arc;

use pgwire_replication::ReplicationConfig;
use walkeep::outbox_kafka_sink::OutboxKafkaSink;
use walkeep::read_kafka_config::KafkaConfig;
use walkeep::read_kafka_config::read_config as read_kafka_config;
use walkeep::read_repl_config::read_config as read_replication_config;
use walkeep::walkeep_engine::WalKeepEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing WalKeep Pipeline...");

    let config: ReplicationConfig = match read_replication_config() {
        Ok(cfg) => cfg,
        Err(err) => panic!("Error: {}", err),
    };

    let kakfa_config: KafkaConfig = match read_kafka_config() {
        Ok(cfg) => cfg,
        Err(err) => panic!("Error: {}", err),
    };

    let sink: Arc<OutboxKafkaSink> = Arc::new(OutboxKafkaSink::new(
        &kakfa_config.broker,
        &kakfa_config.topic,
    ));

    let engine = WalKeepEngine::new(config, sink);

    if let Err(err) = engine.start_pipeline().await {
        panic!("[WALKEEP ENGINE FATAL ERROR]: {}", err);
    }

    Ok(())
}
