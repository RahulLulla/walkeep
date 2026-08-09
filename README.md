# WalKeep - Asynchronous PostgreSQL WAL(Write-Ahead Log) Parsing and Streaming Log-Engine

When PostgreSQL updates data, it writes those changes to a sequential file first called the Write-Ahead Log (WAL) for crash recovery.

Building a lightweight and fast CLI tool in Rust that monitors a live Postgres database, reads its binary WAL stream directly, parses the database mutations (Inserts, Updates, Deletes) in real-time, and surfaces them into a high-performance memory buffer for other systems to consume.

Here a Real-Time transactional outbox worker is built on top of WalKeep pipeline to pass the parsed binary stream to message broker (kakfa). This could be changed to different brokers, redis, sqlite, S3.

Leveraging Rust's memory safety without GC, strict concurrency and predictable performance

**Blueprint:**

- PostgreSQL -> WalKeep Engine/Rust application parsing bytes safely (Passing Binary Stream)
- WalKeep Engine -> PostgreSQL (Keep Alive)
- WalKeep Engine -> ASYNC TOKIO EVENT LOOP/Distributes raw events to disk/network (Concurrent Zero-Copy Arc)

**WalKeep Engine**

- Parse Binary Stream -> Filter for 'outbox_events' mutations
- Deserialize Tuple Payload -> Map to Event Struct
- Publish to Kafka/Redpanda Asynchronously

**Components:**

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

-- Create a mock table to stream mutations from later
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tell Postgres to track ALL row changes for this table
ALTER TABLE outbox_events REPLICA IDENTITY FULL;

-- Grant Permission to user:
GRANT SELECT ON ALL TABLES IN SCHEMA public TO walkeep_user;

-- Create the publication
CREATE PUBLICATION walkeep_pub for table production_orders;

-- Create the logical Replication Slot
SELECT pg_create_logical_replication_slot('walkeep_slot', 'pgoutput')

```

3. Update the postgres db environment inputs in config.ini

```ini
[DB_CONFIG]
db_host=
db_name=
db_port=
walkeep_user=
secure_pass=
walkeep_pub=
walkeep_slot=

[KAFKA_CONFIG]
broker=
topic=
```

4. Setup Apache Kafka, Create topic that would be configured in config.ini

   Refer to [link](https://kafka.apache.org/43/getting-started/quickstart/)
   Ex:

```BAT
a. Start Server
# Generate a Cluster UUID
.\bin\windows\kafka-storage.bat random-uuid

# Format Log Directories
.\bin\windows\kafka-storage.bat format --standalone -t <KAFKA_CLUSTER_ID> .\config\server.properties

# Start Server
.\bin\windows\kafka-server-start.bat .\config\server.properties

b. Create Topic
.\bin\windows\kafka-topics.bat --create --topic walkeepTopic --bootstrap-server localhost:9092 --replication-factor  1 --partitions 1

c. Start Consumer
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic walkeepTopic

```

5. Start the Walkeep or use main.exe

   Perform insert and check the consumer the data should get streamed from DB -> WAL -> WALKeepEngine -> Kafka

```SQL
INSERT INTO public.outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at) VALUES(gen_random_uuid(), 'SUM', '1', 'MATH', '{"name":"Sum was performed"}', now());
```
