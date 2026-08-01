use bytes::Bytes;
use pgwire_replication::{ReplicationClient, ReplicationEvent};

/// Function for handling ReplicationEvent for a ReplicationClient
/// Processing binary data and keeping connection alive
pub fn handle_replication_event(event: ReplicationEvent, client: &ReplicationClient) {
    match event {
        ReplicationEvent::XLogData {
            wal_start,
            wal_end,
            data,
            ..
        } => {
            println!(
                "Got data from {} to {}: {} bytes",
                wal_start,
                wal_end,
                data.len()
            );
            process_binary_payload(data);
            client.update_applied_lsn(wal_end);
        }
        ReplicationEvent::KeepAlive {
            wal_end,
            reply_requested,
            ..
        } => {
            println!(
                "Keepalive at {}. Reply Requested:{}",
                wal_end, reply_requested
            );
        }
        _ => {}
    }
}

/// For processing binary payload and providing info about postgres stream
fn process_binary_payload(data: Bytes) {
    if data.is_empty() {
        return;
    }

    let message_type = data[0] as char;
    match message_type {
        'I' => println!(
            "[INSERT INFERRED] Raw Mutation Block: {} bytes parsed.",
            data.len()
        ),
        'U' => println!("[UPDATE INFERRED] Row altered in memory."),
        'D' => println!("[DELETE INFERRED] Destructive write caught."),
        _ => println!("[UNKNOWN CAUGHT]"),
    }
}
