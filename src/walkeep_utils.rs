use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io::{self, ErrorKind};

#[derive(Debug)]
pub struct RawWalFrame {
    pub lsn: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Value {
    Null,
    Text(String),
    Int(i64),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Unchanged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnData {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericWalEvent {
    pub table_name: String,
    pub schema_name: String,
    pub operation: String,
    pub columns: Vec<ColumnData>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub old_columns: Option<Vec<ColumnData>>,
}

#[derive(Clone)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub is_key: bool,
}

#[derive(Clone)]
pub struct Relation {
    pub table_name: String,
    pub schema_name: String,
    pub columns: Vec<RelationColumn>,
}

pub fn invalid_data(message: String) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

// nedd to remove
#[derive(serde::Serialize)]
pub struct OutboxEvent {
    pub id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}
