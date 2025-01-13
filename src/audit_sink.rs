use std::{
    backtrace::Backtrace,
    collections::HashMap,
    fmt, iter,
    path::{Path, PathBuf},
    sync::Arc,
    u64,
};

use async_trait::async_trait;
use minicbor_io::AsyncWriter;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::{
        sinks::{BatchSink, SinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use tokio_postgres::types::PgLsn;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::event::{row_data::RowData, Device, Event, Snapshot, Table, VERSION};

#[derive(Debug)]
struct OpenFile {
    ts: jiff::Timestamp,
    inner: AsyncWriter<Compat<tokio::fs::File>>,
}

impl OpenFile {
    async fn new(root: &Path, device: Uuid, ts: Option<jiff::Timestamp>) -> Result<Self, AuditSinkError> {
        let ts = ts.unwrap_or_else(|| jiff::Timestamp::now());
        let path = root.join(format!(
            "events-{}-{}.cbor",
            device.as_simple(),
            ts.as_nanosecond()
        ));
        debug!(?ts, ?path, "opening file");

        let file = tokio::fs::File::create_new(path).await?;
        let inner = AsyncWriter::new(file.compat_write());

        Ok(Self { ts, inner })
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
    pub fn new(root: PathBuf, device_id: Uuid) -> Self {
        Self {
            root,
            state: AuditState::new(device_id),
            rotate_interval: jiff::Span::new().minutes(1),
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
    async fn rotate(&mut self, ts: Option<jiff::Timestamp>) -> Result<(), AuditSinkError> {
        if let Some(OpenFile { mut inner, ts, .. }) = self
            .current
            .replace(OpenFile::new(&self.root, self.state.device_id, ts).await?)
        {
            debug!(?ts, "closing file");
            inner.flush().await?;
            let (file, _) = inner.into_parts();
            file.into_inner().sync_data().await?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, table, rows))]
    async fn write_rows(
        &mut self,
        table: Arc<TableDescription>,
        mut rows: impl Iterator<Item = TableRow>,
    ) -> Result<(), AuditSinkError> {
        let Some(first_row) = rows.next() else {
            // if no rows, do nothing, don't even rotate needlessly
            return Ok(());
        };
        
        let device = self.state.device();
        
        let first_row = table.row_to_event(device, first_row)?;

        if self.should_rotate() {
            // use the ts of the first object to name the file
            self.rotate(Some(first_row.device.ts.0)).await?;
        }

        // UNWRAP: above rotation guarantees current is Some
        let writer = &mut self.current.as_mut().unwrap().inner;
        writer.write(first_row).await?;

        // process remaining rows
        for row in rows {
            writer.write(table.row_to_event(device, row)?).await?;
        }

        writer.flush().await?;
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
    pub type_oid: postgres_types::Oid,
    pub modifier: i32,
    pub nullable: bool,
    pub primary: bool,
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
    fn new(columns: &[ColumnDescription]) -> Option<Self> {
        let by_name = |name| {
            columns
                .iter()
                .enumerate()
                .find_map(|(index, col)| if col.name == name { Some(index) } else { None })
        };

        if let (Some(id), Some(created_at), Some(updated_at), Some(deleted_at), Some(sync_tick)) = (
            by_name("id"),
            by_name("created_at"),
            by_name("updated_at"),
            by_name("deleted_at"),
            by_name("updated_at_sync_tick"),
        ) {
            Some(Self {
                id,
                created_at,
                updated_at,
                deleted_at,
                sync_tick,
                updated_by: by_name("updated_by_user_id"),
            })
        } else {
            None
        }
    }
}

impl TableDescription {
    pub fn new(id: TableId, schema: TableSchema) -> Option<Self> {
        let columns: Vec<ColumnDescription> = schema
            .column_schemas
            .into_iter()
            .map(|col| ColumnDescription {
                name: col.name,
                type_oid: col.typ.oid(),
                modifier: col.modifier,
                nullable: col.nullable,
                primary: col.primary,
            })
            .collect();

        Offsets::new(&columns).map(|offsets| Self {
            id,
            schema: schema.table_name.schema,
            name: schema.table_name.name,
            offsets,
            columns,
        })
    }

    fn row_to_event(&self, device: Device, row: TableRow) -> Result<Event, AuditSinkError> {
        // TODO: convert panics to errors
        Ok(Event {
            version: VERSION,
            table: self.clone().into(),
            device,
            snapshot: Snapshot {
                id: match &row.values[self.offsets.id] {
                    Cell::String(s) => s.into(),
                    Cell::Uuid(u) => u.into(),
                    Cell::Bytes(b) => {
                        Uuid::from_bytes(b.clone().try_into().unwrap_or_default()).into()
                    }
                    cell => panic!("string or uuid expected but got {cell:?}"),
                },
                created_at: match &row.values[self.offsets.created_at] {
                    Cell::TimeStampTz(d) => d.try_into()?,
                    cell => panic!("timestamptz expected but got {cell:?}"),
                },
                updated_at: match &row.values[self.offsets.updated_at] {
                    Cell::TimeStampTz(d) => d.try_into()?,
                    cell => panic!("timestamptz expected but got {cell:?}"),
                },
                deleted_at: match &row.values[self.offsets.deleted_at] {
                    Cell::TimeStampTz(d) => Some(d.try_into()?),
                    Cell::Null => None,
                    cell => panic!("timestamptz expected but got {cell:?}"),
                },
                sync_tick: match &row.values[self.offsets.sync_tick] {
                    Cell::I64(t) => (*t).into(),
                    cell => panic!("i64 expected but got {cell:?}"),
                },
                updated_by: self
                    .offsets
                    .updated_by
                    .map(|index| &row.values[index])
                    .and_then(|cell| match cell {
                        Cell::String(s) => Some(s.into()),
                        Cell::Uuid(u) => Some(u.into()),
                        Cell::Null => None,
                        cell => panic!("string expected but got {cell:?}"),
                    }),
                data: RowData {
                    columns: self.columns.clone(),
                    cells: row.values,
                },
            },
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditState {
    pub last_lsn: u64,
    pub device_id: uuid::Uuid,
    pub tables: HashMap<TableId, Arc<TableDescription>>,
}

impl AuditState {
    fn new(device_id: uuid::Uuid) -> Self {
        Self {
            last_lsn: 0,
            device_id,
            tables: Default::default(),
        }
    }
    
    const fn filename() -> &'static str {
        "_state.json"
    }

    fn path(root: &Path) -> PathBuf {
        root.join(Self::filename())
    }

    fn device(&self) -> Device {
        Device {
            id: self.device_id,
            ts: jiff::Timestamp::now().into(),
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

#[derive(Debug)]
pub enum AuditSinkError {
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    Json {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    Cbor {
        source: minicbor_io::Error,
        backtrace: Backtrace,
    },

    Jiff {
        source: jiff::Error,
        backtrace: Backtrace,
    },
}

impl From<std::io::Error> for AuditSinkError {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<serde_json::Error> for AuditSinkError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<minicbor_io::Error> for AuditSinkError {
    fn from(source: minicbor_io::Error) -> Self {
        Self::Cbor {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<jiff::Error> for AuditSinkError {
    fn from(source: jiff::Error) -> Self {
        Self::Jiff {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl fmt::Display for AuditSinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { source, backtrace } => write!(f, "io: {source}\n{backtrace}"),
            Self::Json { source, backtrace } => write!(f, "json: {source}\n{backtrace}"),
            Self::Cbor { source, backtrace } => write!(f, "cbor: {source}\n{backtrace}"),
            Self::Jiff { source, backtrace } => write!(f, "jiff: {source}\n{backtrace}"),
        }
    }
}

impl std::error::Error for AuditSinkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json { source, .. } => Some(source),
            Self::Cbor { source, .. } => Some(source),
            Self::Jiff { source, .. } => Some(source),
        }
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
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
                let state = AuditState::new(self.state.device_id);
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
        self.state
            .tables
            .extend(table_schemas.into_iter().filter_map(|(id, schema)| {
                TableDescription::new(id, schema).map(|desc| (id, Arc::new(desc)))
            }));
        self.state.write(&self.root).await?;
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        table_rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        debug!(
            ?table_rows,
            "Received {} table rows for table {}",
            table_rows.len(),
            table_id
        );
        let Some(table) = self.state.tables.get(&table_id).cloned() else {
            warn!(
                ?table_id,
                "Received table rows for unknown/unsupported table"
            );
            return Ok(());
        };

        self.write_rows(table, table_rows.into_iter()).await?;
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

                    self.write_rows(table, iter::once(row)).await?;
                }
                CdcEvent::Update((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id).cloned() else {
                        warn!(?table_id, "Received update for unknown table");
                        continue;
                    };

                    self.write_rows(table, iter::once(row)).await?;
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

        self.write_rows(table, iter::empty()).await?;
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
