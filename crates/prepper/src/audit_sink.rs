use std::{collections::HashMap, iter, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use pg_replicate::{
	conversions::{cdc_event::CdcEvent, table_row::TableRow},
	pipeline::{sinks::BatchSink, PipelineResumptionState},
	table::{TableId, TableSchema},
};
use prepper_event::Table;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
	error::AuditSinkError,
	event_dir::EventDir,
	state::{AuditState, TableDescription},
};

#[derive(Debug)]
pub struct AuditSink {
	state: AuditState,
	dir: EventDir,
}

impl AuditSink {
	#[instrument(level = "debug")]
	pub fn new(root: PathBuf, device_id: Uuid) -> Self {
		Self {
			dir: EventDir::new(root, device_id),
			state: AuditState::new(device_id),
		}
	}

	#[instrument(level = "debug", skip(self, table, rows))]
	async fn write_rows(
		&mut self,
		table: Arc<TableDescription>,
		rows: impl Iterator<Item = TableRow>,
	) -> Result<(), AuditSinkError> {
		self.dir
			.write_events(rows.map(|row| {
				table
					.row_to_snapshot(row)
					.map(|snapshot| (Table::from(table.as_ref()), snapshot))
			}))
			.await
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

// An interesting thing to note is that the methods is &mut self, which means that we are guaranteed
// to have exclusive access to the table files so long as we only write to them within the sink.
#[async_trait]
impl BatchSink for AuditSink {
	type Error = AuditSinkError;

	#[tracing::instrument(skip(self))]
	async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
		let root = self.dir.root();
		self.state = match AuditState::read(root).await? {
			Some(state) => state,
			None => {
				info!("No state found, starting from scratch");
				let state = AuditState::new(self.state.device_id);
				state.write(root).await?;

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
		self.state.write(self.dir.root()).await?;
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
