use std::{
    collections::HashMap,
    iter,
    path::{Path, PathBuf},
    sync::LazyLock,
    u64,
};

use async_trait::async_trait;
use pg_replicate::{
    conversions::{
        cdc_event::CdcEvent,
        table_row::{Cell, TableRow},
    },
    pipeline::{
        sinks::{BatchSink, SinkError},
        sources::postgres::TableCopyStreamError,
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use tokio::io::AsyncWriteExt as _;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

static RECORD_SNAPSHOT_SCHEMA: LazyLock<serde_avro_fast::Schema> = LazyLock::new(|| {
    include_str!("record_snapshot_schema.avsc")
        .parse()
        .expect("Failed to parse schema")
});

// TODO: generate schema from type with BuildSchema derive, verify that
//       it's compatible with the handwritten schema/ignore logicalTypes
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RecordSnapshot {
    table: RecordTable,
    device: RecordDevice,
    id: String,
    created_at: u64,
    updated_at: u64,
    deleted_at: Option<u64>,
    sync_tick: i64,
    updated_by: Option<String>,
    data: HashMap<String, Vec<u8>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RecordTable {
    oid: TableId,
    schema: String,
    name: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RecordDevice {
    id: [u8; 16], // uuid
    ts: u64,      // nanoseconds
}

#[derive(Debug)]
pub struct AuditSink {
    root: PathBuf,
    state: AuditState,
}

impl AuditSink {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            state: Default::default(),
        }
    }
    
    async fn write_rows(&self, table: &TableDescription, rows: impl Iterator<Item = TableRow>) -> Result<(), AuditSinkError> {
        todo!()
    }
    
    async fn write_delete(&self, table: &TableDescription, row: TableRow) -> Result<(), AuditSinkError> {
        todo!()
    }
    
    async fn write_truncate(&self, table: &TableDescription) -> Result<(), AuditSinkError> {
        todo!()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableDescription {
    pub id: TableId,
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnDescription>,
    pub offsets: Offsets,
}

impl From<TableDescription> for RecordTable {
    fn from(value: TableDescription) -> Self {
        Self {
            oid: value.id,
            schema: value.schema.into(),
            name: value.name.into(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnDescription {
    pub name: String,
    #[serde(skip, default)]
    pub typ: Option<tokio_postgres::types::Type>,
    pub type_oid: postgres_types::Oid,
    pub modifier: i32,
    pub nullable: bool,
    pub identity: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Offsets {
    id: usize,
    created_at: usize,
    updated_at: usize,
    deleted_at: usize,
    sync_tick: usize,
    updated_by: Option<usize>,
}

impl Offsets {
    fn new(columns: &[ColumnDescription]) -> Self {
        let by_name = |name| {
            columns
                .iter()
                .enumerate()
                .find_map(|(index, col)| if col.name == name { Some(index) } else { None })
        };

        Self {
            id: by_name("id").unwrap(),
            created_at: by_name("created_at").unwrap(),
            updated_at: by_name("updated_at").unwrap(),
            deleted_at: by_name("deleted_at").unwrap(),
            sync_tick: by_name("updated_at_sync_tick").unwrap(),
            updated_by: by_name("updated_by"),
        }
    }
}

impl TableDescription {
    pub fn new(id: TableId, schema: TableSchema) -> Self {
        let columns: Vec<ColumnDescription> = schema
            .column_schemas
            .into_iter()
            .map(|col| ColumnDescription {
                name: col.name,
                type_oid: col.typ.oid(),
                typ: Some(col.typ),
                modifier: col.modifier,
                nullable: col.nullable,
                identity: col.identity,
            })
            .collect();

        let offsets = Offsets::new(&columns);

        Self {
            id,
            schema: schema.table_name.schema,
            name: schema.table_name.name,
            offsets,
            columns,
        }
    }

    fn row_to_snapshot(&self, device: RecordDevice, row: TableRow) -> RecordSnapshot {
        fn jb(json: serde_json::Value) -> Vec<u8> {
            let mut bytes = json.to_string().into_bytes();
            bytes.insert(0, b'j');
            bytes.into()
        }

        fn bb(bytes: &Vec<u8>) -> Vec<u8> {
            let mut bytes = bytes.clone();
            bytes.insert(0, b'b');
            bytes.into()
        }

        RecordSnapshot {
            table: self.clone().into(),
            device,
            data: row
                .values
                .iter()
                .enumerate()
                .filter_map(|(index, cell)| {
                    self.columns.get(index).map(|col| {
                        (
                            col.name.clone(),
                            match cell {
                                Cell::Null => jb(serde_json::Value::Null),
                                Cell::Bool(v) => jb(serde_json::Value::Bool(*v)),
                                Cell::String(v) => jb(serde_json::Value::String(v.clone())),
                                Cell::I16(v) => {
                                    jb(serde_json::Value::Number(serde_json::Number::from(*v)))
                                }
                                Cell::I32(v) => {
                                    jb(serde_json::Value::Number(serde_json::Number::from(*v)))
                                }
                                Cell::I64(v) => {
                                    jb(serde_json::Value::Number(serde_json::Number::from(*v)))
                                }
                                Cell::TimeStamp(v) => jb(serde_json::Value::String(v.to_string())),
                                Cell::TimeStampTz(v) => {
                                    jb(serde_json::Value::String(v.to_string()))
                                }
                                Cell::Bytes(v) => bb(v),
                                Cell::Json(v) => jb(v.clone()),
                                _ => vec![].into(),
                            },
                        )
                    })
                })
                .collect(),
            id: match &row.values[self.offsets.id] {
                Cell::String(s) => s.into(),
                cell => panic!("string expected but got {cell:?}"),
            },
            created_at: match &row.values[self.offsets.created_at] {
                Cell::TimeStampTz(d) => d.timestamp_micros().try_into().unwrap_or(u64::MAX),
                cell => panic!("timestamptz expected but got {cell:?}"),
            },
            updated_at: match &row.values[self.offsets.updated_at] {
                Cell::TimeStampTz(d) => d.timestamp_micros().try_into().unwrap_or(u64::MAX),
                cell => panic!("timestamptz expected but got {cell:?}"),
            },
            deleted_at: match &row.values[self.offsets.deleted_at] {
                Cell::TimeStampTz(d) => d.timestamp_micros().try_into().ok(),
                Cell::Null => None,
                cell => panic!("timestamptz expected but got {cell:?}"),
            },
            sync_tick: match &row.values[self.offsets.sync_tick] {
                Cell::I64(t) => *t,
                cell => panic!("i64 expected but got {cell:?}"),
            },
            updated_by: self
                .offsets
                .updated_by
                .map(|index| &row.values[index])
                .and_then(|cell| match cell {
                    Cell::String(s) => Some(s.into()),
                    Cell::Null => None,
                    cell => panic!("string expected but got {cell:?}"),
                }),
        }
    }

    // pub fn path(&self, root: &Path) -> PathBuf {
    //     root.join(format!("{}.{}.json", self.schema, self.table))
    // }

    // pub async fn truncate(&self, root: &Path) -> Result<(), AuditSinkError> {
    //     tokio::fs::remove_file(self.path(root)).await?;
    //     Ok(())
    // }

    // pub async fn write_rows(
    //     &self,
    //     root: &Path,
    //     op: &str,
    //     rows: impl Iterator<Item = TableRow>,
    // ) -> Result<(), AuditSinkError> {
    //     let mut file = tokio::fs::OpenOptions::new()
    //         .create(true)
    //         .append(true)
    //         .open(self.path(root))
    //         .await?;

    //     for row in rows {
    //         // TODO: we should probably store the columns in the state and write an object here
    //         let mut line = serde_json::to_vec(&Self::row_to_json(row, op))?;
    //         line.push(b'\n');
    //         file.write_all(&line).await?;
    //     }

    //     Ok(())
    // }

    // fn row_to_json(row: TableRow, op: &str) -> serde_json::Value {
    //     use pg_replicate::conversions::table_row::Cell::*;

    //     serde_json::Value::Array(
    //         iter::once(serde_json::Value::String(op.into())).chain(
    //         row.values
    //             .into_iter()
    //             .filter_map(|cell| {
    //                 Some()
    //             }))
    //             .collect(),
    //     )
    // }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditState {
    pub last_lsn: u64,
    pub device_id: uuid::Uuid,
    pub tables: HashMap<TableId, TableDescription>,
}

impl AuditState {
    const fn filename() -> &'static str {
        "_state.json"
    }

    fn path(root: &Path) -> PathBuf {
        root.join(Self::filename())
    }

    fn device(&self) -> RecordDevice {
        RecordDevice {
            id: self.device_id.into_bytes(),
            ts: jiff::Timestamp::now()
                .as_nanosecond()
                .try_into()
                .unwrap_or(u64::MAX),
        }
    }

    /// Read the state from the given root directory.
    ///
    /// If the state file does not exist, returns `Ok(None)`.
    pub async fn read(root: &Path) -> Result<Option<Self>, AuditSinkError> {
        match tokio::fs::read(Self::path(root)).await {
            Ok(state) => Ok(Some(serde_json::from_slice(&state)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write(&self, root: &Path) -> Result<(), AuditSinkError> {
        tokio::fs::write(Self::path(root), serde_json::to_vec_pretty(self)?).await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuditSinkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialisation error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl SinkError for AuditSinkError {}

// An interesting thing to note is that the methods is &mut self, which means that we are guaranteed
// to have exclusive access to the table files so long as we only write to them within the sink.
#[async_trait]
impl BatchSink for AuditSink {
    type Error = AuditSinkError;

    #[tracing::instrument(skip(self))]
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        self.state = match AuditState::read(&self.root).await? {
            Some(state) => state,
            None => {
                info!("No state found, starting from scratch");
                let state = AuditState::default();
                state.write(&self.root).await?;
                state
            }
        };

        Ok(PipelineResumptionState {
            copied_tables: self.state.tables.keys().copied().collect(),
            last_lsn: PgLsn::from(self.state.last_lsn),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        self.state.tables.extend(
            table_schemas
                .into_iter()
                .map(|(id, schema)| (id, TableDescription::new(id, schema))),
        );
        self.state.write(&self.root).await?;
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        table_rows: Vec<Result<TableRow, TableCopyStreamError>>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        debug!(
            ?table_rows,
            "Received {} table rows for table {}",
            table_rows.len(),
            table_id
        );
        let Some(table) = self.state.tables.get(&table_id) else {
            warn!(?table_id, "Received table rows for unknown table");
            return Ok(());
        };

        self.write_rows(table, table_rows.into_iter().filter_map(Result::ok))
            .await?;
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        debug!(?events, "Received {} CDC events", events.len());
        for event in events {
            match event {
                CdcEvent::Insert((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id) else {
                        warn!(?table_id, "Received insert for unknown table");
                        continue;
                    };

                    self.write_rows(table, iter::once(row)).await?;
                }
                CdcEvent::Update((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id) else {
                        warn!(?table_id, "Received update for unknown table");
                        continue;
                    };

                    self.write_rows(table, iter::once(row)).await?;
                }
                CdcEvent::Delete((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id) else {
                        warn!(?table_id, "Received delete for unknown table");
                        continue;
                    };

                    self.write_delete(table, row).await?;
                }
                CdcEvent::Relation(_) => { /* TODO */ }
                _ => { /* ignore */ }
            }
        }
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        debug!("Table {} copied", table_id);
        let Some(table) = self.state.tables.get(&table_id) else {
            warn!(?table_id, "Received table copied for unknown table");
            return Ok(());
        };

        self.write_rows(table, iter::empty()).await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        debug!("Table {} truncated", table_id);
        let Some(table) = self.state.tables.get(&table_id) else {
            warn!(?table_id, "Received table truncate for unknown table");
            return Ok(());
        };

        self.write_truncate(table).await?;
        Ok(())
    }
}
