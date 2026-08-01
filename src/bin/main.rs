use pgwire_replication::ReplicationClient;

extern crate walkeep;
use walkeep::event_handlers;
use walkeep::read_repl_config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing WalKeep Pipeline...");

    let config = match read_repl_config::read_config() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Error: {}", err);
            std::process::exit(1);
        }
    };
    let mut client: ReplicationClient = ReplicationClient::connect(config).await?;

    println!("Handshake verified. Awaiting streaming binary transactions...");

    while let Some(event) = client.recv().await? {
        event_handlers::handle_replication_event(event, &client);
    }

    Ok(())
}
