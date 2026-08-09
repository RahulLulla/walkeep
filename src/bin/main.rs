extern crate walkeep;
use std::sync::Arc;

use pgwire_replication::ReplicationConfig;
use walkeep::outbox_kafka_sink::OutboxKafkaSink;
use walkeep::read_kafka_config;
use walkeep::read_kafka_config::KafkaConfig;
use walkeep::read_repl_config;
use walkeep::walkeep_engine::WalKeepEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing WalKeep Pipeline...");

    let config: ReplicationConfig = match read_repl_config::read_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    };

    let kakfa_config: KafkaConfig = match read_kafka_config::read_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    };

    let sink: Arc<OutboxKafkaSink> = Arc::new(OutboxKafkaSink::new(
        &kakfa_config.broker,
        &kakfa_config.topic,
    ));

    let engine = WalKeepEngine::new(config, sink);

    if let Err(err) = engine.start_pipeline().await {
        eprint!("[WALKEEP ENGINE FATAL ERROR]: {}", err);
    }

    Ok(())
}
