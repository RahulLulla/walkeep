# Asynchronous PostgreSQL WAL(Write-Ahead Log) Parsing and Streaming Log-Engine

When PostgreSQL updates data, it writes those changes to a sequential file first called the Write-Ahead Log (WAL) for crash recovery.

Building a lightweight and fast CLI tool in Rust that monitors a live Postgres database, reads its binary WAL stream directly, parses the database mutations (Inserts, Updates, Deletes) in real-time, and surfaces them into a high-performance memory buffer for other systems to consume.

Leveraging Rust's memory safety without GC, strict concurrency and predictable performance

Blueprint:

- PostgreSQL -> WAL Reefer/Rust application parsing bytes safely (Passing Binary Stream)
- WAL Reefer -> PostgreSQL (Keep Alive)
- WAL Reefer -> ASYNC TOKIO EVENT LOOP/Distributes raw events to disk/network (Concurrent Zero-Copy Arc)

Components:

- tokio: performing asynchronous activity such handshake with postgres, send `START_REPLICATION` to postgres
- nom: parse the binary data sent by postgres into rust structs
- postgres-protocol: To handle the physical replication protocol handshake with PostgreSQL

---

### Guideline

1. Add following in your postgresql.conf

```conf
# postgresql.conf
wal_level = logical             # Tells Postgres to write full mutation bytes to the WAL
max_replication_slots = 4       # Reserves channels for engines like yours
max_wal_senders = 4            # Allows background streaming processes
```

2. Creating Sample slot, pub and table for logical replication

```SQL
-- Created an isolated user for streaming
CREATE ROLE walkeep_user WITH REPLICATION LOGIN PASSWORD 'secure_pass';

-- Create the publication
create publication walkeep_sub for table production_orders;

-- Create the logical Replication Slot
select pg_create_logical_replication_slot('walkeep_slot', 'pgoutput')

-- Create a mock table to stream mutations from later
CREATE TABLE production_orders (
    id SERIAL PRIMARY KEY,
    item_name TEXT,
    price NUMERIC
);

-- Tell Postgres to track ALL row changes for this table
ALTER TABLE production_orders REPLICA IDENTITY FULL;
```
