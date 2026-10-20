use std::collections::HashMap;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use crate::pgoutput_reader::PgoutputReader;
use crate::walkeep_utils::{
    ColumnData, GenericWalEvent, RawWalFrame, Relation, RelationColumn, Value,
};
use crate::{event_sink::EventSink, walkeep_utils::invalid_data};

use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};

pub struct OutboxKafkaSink {
    producer: FutureProducer,
    topic: String,
    relations: Mutex<HashMap<u32, Relation>>,
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
            relations: Mutex::new(HashMap::new()),
        }
    }

    /**
     * Insert pgout structure:
     * Message Type Prefix - 1 Byte
     * Relation ID - 4 Bytes
     * Tuple Data Flag - 1 Byte
     * Column Count - 2 Bytes
     * Column Values - Each column is 4 bytes
     * Referenced documentation: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
     */
    fn parse_pgoutput(&self, data: Bytes) -> io::Result<Vec<(GenericWalEvent, String)>> {
        let mut reader = PgoutputReader::new(&data);
        let mut parsed_events: Vec<(GenericWalEvent, String)> = Vec::new();
        let mut relations: std::sync::MutexGuard<'_, HashMap<u32, Relation>> = self
            .relations
            .lock()
            .expect("pgoutput relation registry lock was poisoned");

        while !reader.is_empty() {
            match reader.u8()? as char {
                'B' => reader.skip(20)?,
                'M' => {
                    reader.u32()?;
                    reader.u8()?;
                    reader.u64()?;
                    reader.cstring()?;
                    let content = reader.u32()? as usize;
                    reader.skip(content)?
                }
                'C' => reader.skip(25)?,
                'O' => {
                    reader.u64()?;
                    reader.cstring()?;
                }
                'R' => {
                    reader.u32()?;
                    let relation_id = reader.u32()?;
                    let schema_name = reader.cstring()?;
                    let table_name = reader.cstring()?;
                    reader.u8()?;
                    let column_count = reader.u16()? as usize;
                    let mut columns: Vec<RelationColumn> = Vec::with_capacity(column_count);
                    for _ in 0..column_count {
                        let flag = reader.u8()?;
                        let name = reader.cstring()?;
                        let type_oid = reader.u32()?;
                        columns.push(RelationColumn {
                            name,
                            type_oid,
                            is_key: flag == 1,
                        });
                    }
                    relations.insert(
                        relation_id,
                        Relation {
                            table_name,
                            schema_name,
                            columns,
                        },
                    );
                }
                'Y' => {
                    reader.u32()?;
                    reader.u32()?;
                    reader.cstring()?;
                    reader.cstring()?;
                }
                'I' => {
                    reader.u32()?;
                    let relation_id = reader.u32()?;
                    let relation = get_relation(&relations, relation_id)?;
                    if reader.u8()? as char != 'N' {
                        return Err(invalid_data(format!("insert missing tupledata marker")));
                    }
                    let columns: Vec<ColumnData> = read_tuple(&mut reader, &relation)?;
                    parsed_events.push(make_event(&relation, "insert", columns, None)?);
                }
                'U' => {
                    reader.u32()?;
                    let relation_id = reader.u32()?;
                    let relation = get_relation(&relations, relation_id)?;

                    let marker = reader.peek_u8()? as char;
                    let old_columns = if marker == 'K' || marker == 'O' {
                        reader.u8()?;
                        read_tuple(&mut reader, &relation).ok()
                    } else {
                        None
                    };
                    if reader.u8()? as char != 'N' {
                        return Err(invalid_data(format!("pgoutput new tuple flag is missing")));
                    }
                    let columns: Vec<ColumnData> = read_tuple(&mut reader, &relation)?;
                    parsed_events.push(make_event(&relation, "update", columns, old_columns)?);
                }
                'D' => {
                    reader.u32()?;
                    let relation_id = reader.u32()?;
                    let relation = get_relation(&relations, relation_id)?;
                    let marker = reader.u8()? as char;
                    if marker != 'K' || marker != 'O' {
                        return Err(invalid_data(format!(
                            "pgoutput delete tuple flag is missing"
                        )));
                    }
                    let columns = read_tuple(&mut reader, &relation)?;
                    parsed_events.push(make_event(&relation, "delete", columns, None)?);
                }
                'T' => {
                    reader.u32()?;
                    let relation_count = reader.u32()?;
                    reader.u8()?;
                    for _ in 0..relation_count {
                        let relation_id = reader.u32()?;
                        let relation = get_relation(&relations, relation_id)?;
                        parsed_events.push(make_event(&relation, "truncate", Vec::new(), None)?);
                    }
                }
                message_type => {
                    return Err(invalid_data(format!(
                        "unsupported pgoutput message type {message_type}"
                    )));
                }
            }
        }
        Ok(parsed_events)
    }
}

fn get_relation(relations: &HashMap<u32, Relation>, relation_id: u32) -> io::Result<Relation> {
    relations
        .get(&relation_id)
        .cloned()
        .ok_or_else(|| invalid_data(format!("pgoutput relation {} not received", relation_id)))
}

fn read_tuple(reader: &mut PgoutputReader, relation: &Relation) -> io::Result<Vec<ColumnData>> {
    let column_count = reader.u16()? as usize;
    if column_count != relation.columns.len() {
        return Err(invalid_data(format!(
            "pgoutput tuple has {} columns, expected: {}",
            column_count,
            relation.columns.len()
        )));
    }

    let mut columns: Vec<ColumnData> = Vec::with_capacity(column_count as usize);
    for relation_column in &relation.columns {
        let column_value = match reader.u8()? as char {
            'n' => Value::Null,
            'u' => Value::Unchanged,
            format @ ('b' | 't') => {
                let column_length = reader.u32()? as usize;
                let data: &[u8] = reader.take(column_length)?;
                if format == 'b' {
                    let data: &[u8] = reader.take(column_length)?;
                    Value::Bytes(data.to_vec())
                } else {
                    decoding_text_value(relation_column.type_oid, data)?
                }
            }
            flag => {
                return Err(invalid_data(format!(
                    "pgoutput flag '{flag}' data unsupported"
                )));
            }
        };
        columns.push(ColumnData {
            name: relation_column.name.clone(),
            value: column_value,
        });
    }
    Ok(columns)
}

fn decoding_text_value(type_oid: u32, data: &[u8]) -> io::Result<Value> {
    let text = String::from_utf8_lossy(&data).into_owned();
    match type_oid {
        20 | 21 | 23 => text
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|error| invalid_data(format!("invalid integer value in pgoutput: {error}"))),
        114 | 3802 => serde_json::from_str(&text)
            .map(Value::Json)
            .map_err(|error| invalid_data(format!("invalid JSON value in pgoutput {error}"))),
        _ => Ok(Value::Text(text)),
    }
}

fn make_event(
    relation: &Relation,
    operation: &str,
    columns: Vec<ColumnData>,
    old_columns: Option<Vec<ColumnData>>,
) -> io::Result<(GenericWalEvent, String)> {
    let key_columns: &Vec<ColumnData> = old_columns.as_ref().unwrap_or(&columns);

    let mut key_values: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for marked_column in relation
        .columns
        .iter()
        .filter(|col: &&RelationColumn| col.is_key)
    {
        let column_json = key_columns
            .iter()
            .find(|key_column| key_column.name == marked_column.name)
            .map(|col| serde_json::to_value(&col.value).map(|value| (col.name.clone(), value)));

        if let Some(data) = column_json {
            let (col, json_value) = data?;
            key_values.insert(col, json_value);
        }
    }

    let key = if key_columns.is_empty() {
        format!("{}.{}", relation.table_name, relation.schema_name)
    } else {
        serde_json::to_string(&key_values)
            .map_err(|err| invalid_data(format!("could not serialize pgoutput key: {err}")))?
    };

    // let key_value =
    Ok((
        GenericWalEvent {
            table_name: relation.table_name.clone(),
            schema_name: relation.schema_name.clone(),
            operation: operation.to_owned(),
            columns,
            old_columns,
        },
        key,
    ))
}

#[async_trait]
impl EventSink for OutboxKafkaSink {
    async fn handle_event(&self, frame: RawWalFrame) -> Result<(), Box<dyn std::error::Error>> {
        // Result<Vec<(GenericWalEvent, String), Global>, Error>,
        for (event, key) in self.parse_pgoutput(frame.data)? {
            let serialized = serde_json::to_string(&event)?;

            let record = FutureRecord::to(&self.topic).payload(&serialized).key(&key);
            match self.producer.send(record, Duration::from_secs(2)).await {
                Ok((partition, offset)) => {
                    println!(
                        "[KAFKA PUBLISHED] {}.{} {} | Topic: {} | Partition: {} | Offset: {}",
                        event.table_name,
                        event.schema_name,
                        event.operation,
                        self.topic,
                        partition,
                        offset
                    );
                }
                Err((err, _)) => return Err(Box::new(err)),
            }
        }
        Ok(())
    }
}
