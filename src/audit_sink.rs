use std::{
    collections::HashMap,
    iter,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, SinkError},
        sources::postgres::TableCopyStreamError,
        PipelineResumptionState,
    },
    table::{TableId, TableName, TableSchema},
};
use tokio::io::AsyncWriteExt as _;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, warn};

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
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TableFile {
    pub schema: String,
    pub table: String,
}

impl From<TableName> for TableFile {
    fn from(name: TableName) -> Self {
        Self {
            schema: name.schema,
            table: name.name,
        }
    }
}

impl TableFile {
    pub fn path(&self, root: &Path) -> PathBuf {
        root.join(format!("{}.{}.json", self.schema, self.table))
    }

    pub async fn truncate(&self, root: &Path) -> Result<(), AuditSinkError> {
        tokio::fs::remove_file(self.path(root)).await?;
        Ok(())
    }

    pub async fn write_rows(
        &self,
        root: &Path,
        op: &str,
        rows: impl Iterator<Item = TableRow>,
    ) -> Result<(), AuditSinkError> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path(root))
            .await?;

        for row in rows {
            // TODO: we should probably store the columns in the state and write an object here
            let mut line = serde_json::to_vec(&Self::row_to_json(row, op))?;
            line.push(b'\n');
            file.write_all(&line).await?;
        }

        Ok(())
    }

    fn row_to_json(row: TableRow, op: &str) -> serde_json::Value {
        use pg_replicate::conversions::table_row::Cell::*;

        serde_json::Value::Array(
            iter::once(serde_json::Value::String(op.into())).chain(
            row.values
                .into_iter()
                .filter_map(|cell| {
                    Some(match cell {
                        Null => serde_json::Value::Null,
                        Bool(v) => serde_json::Value::Bool(v),
                        String(v) => serde_json::Value::String(v),
                        I16(v) => serde_json::Value::Number(serde_json::Number::from(v)),
                        I32(v) => serde_json::Value::Number(serde_json::Number::from(v)),
                        I64(v) => serde_json::Value::Number(serde_json::Number::from(v)),
                        TimeStamp(v) => serde_json::Value::String(v.to_string()),
                        TimeStampTz(v) => serde_json::Value::String(v.to_string()),
                        Bytes(v) => serde_json::json!({ "base64": unsafe {
                            // SAFETY: we know that the bytes are valid ASCII because they are base64
                            ::std::string::String::from_utf8_unchecked(subtle_encoding::base64::encode(v))
                        } }),
                        Json(v) => v,
                        _ => return None,
                    })
                }))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditState {
    pub last_lsn: u64,
    pub tables: HashMap<TableId, TableFile>,
}

impl AuditState {
    const fn filename() -> &'static str {
        "_state.json"
    }

    fn path(root: &Path) -> PathBuf {
        root.join(Self::filename())
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
        // update the state file with new table mappings, but discard the column schemas
        self.state.tables.extend(
            table_schemas
                .into_iter()
                .map(|(id, schema)| (id, schema.table_name.into())),
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

        table
            .write_rows(
                &self.root,
                "insert",
                table_rows.into_iter().filter_map(Result::ok),
            )
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

                    table
                        .write_rows(&self.root, "insert", iter::once(row))
                        .await?;
                }
                CdcEvent::Update((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id) else {
                        warn!(?table_id, "Received update for unknown table");
                        continue;
                    };

                    table
                        .write_rows(&self.root, "update", iter::once(row))
                        .await?;
                }
                CdcEvent::Delete((table_id, row)) => {
                    let Some(table) = self.state.tables.get(&table_id) else {
                        warn!(?table_id, "Received delete for unknown table");
                        continue;
                    };

                    table
                        .write_rows(&self.root, "delete", iter::once(row))
                        .await?;
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

        table
            .write_rows(&self.root, "copied", iter::empty())
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        debug!("Table {} truncated", table_id);
        if let Some(table) = self.state.tables.get(&table_id) {
            table.truncate(&self.root).await?;
        }
        Ok(())
    }
}
