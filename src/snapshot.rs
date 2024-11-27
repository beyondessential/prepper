use std::{collections::HashMap, sync::LazyLock, u64};

use pg_replicate::table::TableId;

pub static SCHEMA: LazyLock<serde_avro_fast::Schema> = LazyLock::new(|| {
    include_str!("record_snapshot_schema.avsc")
        .parse()
        .expect("Failed to parse schema")
});

// TODO: generate schema from type with BuildSchema derive, verify that
//       it's compatible with the handwritten schema/ignore logicalTypes
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Event {
    pub table: Table,
    pub device: Device,
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Snapshot {
    pub id: String,
    pub created_at: u64,
    pub updated_at: u64,
    pub deleted_at: Option<u64>,
    pub sync_tick: i64,
    pub updated_by: Option<String>,
    pub data: HashMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Table {
    pub oid: TableId,
    pub schema: String,
    pub name: String,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Device {
    pub id: [u8; 16], // uuid
    pub ts: u64,      // nanoseconds
}
