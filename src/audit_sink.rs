use std::{
    collections::HashMap,
    fmt,
    fs::File,
    io::Write as _,
    iter,
    path::{Path, PathBuf},
    sync::Arc,
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
use serde_avro_fast::{
    object_container_file_encoding::{Compression, CompressionLevel, Writer, WriterBuilder},
    ser::{SerError, SerializerConfig},
};
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, instrument, warn};

use crate::snapshot::{Device, Event, Snapshot, Table, SCHEMA};

/// Timestamp in nanoseconds
///
/// This panics if the timestamp cannot fit in a u64, which will only happen past the year 2554.
fn ts() -> u64 {
    jiff::Timestamp::now().as_nanosecond().try_into().unwrap()
}

struct OpenFile {
    ts: jiff::Timestamp,
    inner: Writer<'static, 'static, File>,
}

impl fmt::Debug for OpenFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenFile")
            .field("ts", &self.ts)
            .field("inner", &"AvroWriter { .. }")
            .finish()
    }
}

impl OpenFile {
    fn new(root: &Path) -> Result<Self, AuditSinkError> {
        let ts = jiff::Timestamp::now();
        let path = root.join(format!("events-{}.avro", ts.as_nanosecond()));
        debug!(?ts, ?path, "opening file");

        let file = File::options().create_new(true).append(true).open(path)?;

        let mut config = SerializerConfig::new(&SCHEMA);
        config.allow_slow_sequence_to_bytes();
        let writer = WriterBuilder::with_owned_config(config)
            .compression(Compression::Zstandard {
                level: CompressionLevel::new(6),
            })
            .build(file)?;
        Ok(Self { ts, inner: writer })
    }
}

#[derive(Debug)]
pub struct AuditSink {
    root: PathBuf,
    state: AuditState,
    rotate_interval: jiff::Span,
    current: Option<OpenFile>,
}

impl AuditSink {
    #[instrument(level = "debug")]
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            state: Default::default(),
            rotate_interval: jiff::Span::new().hours(1),
            current: None,
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn should_rotate(&self) -> bool {
        if let Some(OpenFile { ts, .. }) = &self.current {
            ts.saturating_add(self.rotate_interval) < jiff::Timestamp::now()
        } else {
            true
        }
    }

    #[instrument(level = "debug", skip(self))]
    fn rotate(&mut self) -> Result<(), AuditSinkError> {
        if let Some(OpenFile { inner, ts }) = self.current.replace(OpenFile::new(&self.root)?) {
            debug!(?ts, "closing file");
            let file = inner.into_inner()?;
            file.sync_data()?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, table, rows))]
    fn write_rows(
        &mut self,
        table: Arc<TableDescription>,
        rows: impl Iterator<Item = TableRow>,
    ) -> Result<(), AuditSinkError> {
        if self.should_rotate() {
            self.rotate()?;
        }

        // UNWRAP: above rotation guarantees current is Some
        let writer = &mut self.current.as_mut().unwrap().inner;

        let device = self.state.device();
        writer.serialize_all(rows.map(|row| table.row_to_event(device, row)))?;
        writer.finish_block()?;

        Ok(())
    }

    fn write_delete(
        &mut self,
        _table: Arc<TableDescription>,
        _row: TableRow,
    ) -> Result<(), AuditSinkError> {
        // ignore deletes, as in the Tamanu sync model, no rows are ever deleted
        Ok(())
    }

    fn write_truncate(&mut self, _table: Arc<TableDescription>) -> Result<(), AuditSinkError> {
        // ignore truncates, as in the Tamanu sync model, no rows are ever deleted
        Ok(())
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

impl From<TableDescription> for Table {
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

    fn row_to_event(&self, device: Device, row: TableRow) -> Event {
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

        Event {
            table: self.clone().into(),
            device,
            snapshot: Snapshot {
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
                                    Cell::TimeStamp(v) => {
                                        jb(serde_json::Value::String(v.to_string()))
                                    }
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
            },
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditState {
    pub last_lsn: u64,
    pub device_id: uuid::Uuid,
    pub tables: HashMap<TableId, Arc<TableDescription>>,
}

impl AuditState {
    const fn filename() -> &'static str {
        "_state.json"
    }

    fn path(root: &Path) -> PathBuf {
        root.join(Self::filename())
    }

    fn device(&self) -> Device {
        Device {
            id: self.device_id.into_bytes(),
            ts: ts(),
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

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Avro error: {0}")]
    Avro(#[from] SerError),
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

                info!(fingerprint=?SCHEMA.rabin_fingerprint(), "Writing schema to file");
                let mut file = File::options()
                    .create_new(true)
                    .write(true)
                    .open(self.root.join("schema.avsc"))?;
                file.write_all(SCHEMA.json().as_bytes())?;

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
                .map(|(id, schema)| (id, Arc::new(TableDescription::new(id, schema)))),
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
        let Some(table) = self.state.tables.get(&table_id).cloned() else {
            warn!(?table_id, "Received table rows for unknown table");
            return Ok(());
        };

        self.write_rows(table, table_rows.into_iter().filter_map(Result::ok))?;
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        debug!(?events, "Received {} CDC events", events.len());
        for event in events {
            match event {
                CdcEvent::Insert((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id).cloned() else {
                        warn!(?table_id, "Received insert for unknown table");
                        continue;
                    };

                    self.write_rows(table, iter::once(row))?;
                }
                CdcEvent::Update((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id).cloned() else {
                        warn!(?table_id, "Received update for unknown table");
                        continue;
                    };

                    self.write_rows(table, iter::once(row))?;
                }
                CdcEvent::Delete((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id).cloned() else {
                        warn!(?table_id, "Received delete for unknown table");
                        continue;
                    };

                    self.write_delete(table, row)?;
                }
                CdcEvent::Relation(_) => { /* TODO */ }
                _ => { /* ignore */ }
            }
        }
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        debug!("Table {} copied", table_id);
        let Some(table) = self.state.tables.get(&table_id).cloned() else {
            warn!(?table_id, "Received table copied for unknown table");
            return Ok(());
        };

        self.write_rows(table, iter::empty())?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        debug!("Table {} truncated", table_id);
        let Some(table) = self.state.tables.get(&table_id).cloned() else {
            warn!(?table_id, "Received table truncate for unknown table");
            return Ok(());
        };

        self.write_truncate(table)?;
        Ok(())
    }
}
