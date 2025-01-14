use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use pg_replicate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        sinks::{BatchSink, InfallibleSinkError},
        PipelineResumptionState,
    },
    table::{TableId, TableSchema},
};
use tokio_postgres::types::PgLsn;
use tracing::info;

#[derive(Debug)]
pub struct PostgresSink;

#[async_trait]
impl BatchSink for PostgresSink {
    type Error = InfallibleSinkError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        let _initial = PipelineResumptionState {
            copied_tables: HashSet::new(),
            last_lsn: PgLsn::from(0),
        };
        todo!()
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        info!("{table_schemas:?}");
        todo!()
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        _table_id: TableId,
    ) -> Result<(), Self::Error> {
        info!("{rows:?}");
        todo!()
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        info!("{events:?}");
        todo!()
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("table {table_id} copied");
        todo!()
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        info!("table {table_id} truncated");
        todo!()
    }
}
