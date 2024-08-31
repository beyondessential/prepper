use std::{collections::{HashMap, HashSet}, path::PathBuf};

use async_trait::async_trait;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError}, sources::postgres::TableCopyStreamError, PipelineResumptionState
    },
    table::{TableId, TableName, TableSchema},
};
use tokio_postgres::types::PgLsn;
use tracing::info;

#[derive(Debug)]
pub struct AuditSink {
    pub path: PathBuf,
    pub tables: HashSet<TableName>,
}

#[async_trait]
impl BatchSink for AuditSink {
    type Error = InfallibleSinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: Default::default(),
            last_lsn: PgLsn::from(0),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        info!(?table_schemas, "Received {} table schemas", table_schemas.len());
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        table_rows: Vec<Result<TableRow, TableCopyStreamError>>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        info!(?table_rows, "Received {} table rows for table {}", table_rows.len(), table_id);
        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        info!(?events, "Received {} CDC events", events.len());
        Ok(PgLsn::from(0))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("Table {} copied", table_id);
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("Table {} truncated", table_id);
        Ok(())
    }
}
