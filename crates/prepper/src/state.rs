use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::Arc,
};

use pg_replicate::{
	conversions::{table_row::TableRow, Cell},
	table::{TableId, TableSchema},
};
use postgres_replication::protocol::RelationBody;
use tracing::warn;
use uuid::Uuid;

use prepper_event::{row_data::RowData, Snapshot, Table};

use crate::error::AuditSinkError;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableDescription {
	pub id: TableId,
	pub schema: String,
	pub name: String,
	pub columns: Vec<ColumnDescription>,
	pub offsets: Offsets,
}

impl From<&TableDescription> for Table {
	fn from(value: &TableDescription) -> Self {
		Self {
			oid: value.id,
			schema: value.schema.clone(),
			name: value.name.clone(),
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
	pub fn from_schema(id: TableId, schema: TableSchema) -> Option<Self> {
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

	pub fn from_relation(relation: RelationBody) -> Option<Self> {
		let table_schema = relation
			.namespace()
			.inspect_err(|err| warn!(?err, "non-UTF-8 table schema"))
			.ok()?;
		let table_name = relation
			.name()
			.inspect_err(|err| warn!(?err, "non-UTF-8 table name"))
			.ok()?;

		let columns: Vec<ColumnDescription> = relation
			.columns()
			.into_iter()
			.map(|col| {
				ColumnDescription {
					name: col.name().expect("non-UTF-8 column name").into(),
					type_oid: col.type_id().try_into().expect("type oid too high"),
					modifier: col.type_modifier(),
					nullable: true, // TODO
					primary: col.flags() == 1,
				}
			})
			.collect();

		Offsets::new(&columns).map(|offsets| Self {
			id: relation.rel_id(),
			schema: table_schema.into(),
			name: table_name.into(),
			offsets,
			columns,
		})
	}

	pub fn row_to_snapshot(&self, row: TableRow) -> Result<Snapshot, AuditSinkError> {
		// TODO: convert panics to errors
		Ok(Snapshot {
			id: match &row.values[self.offsets.id] {
				Cell::String(s) => s.into(),
				Cell::Uuid(u) => u.into(),
				Cell::Bytes(b) => Uuid::from_bytes(b.clone().try_into().unwrap_or_default()).into(),
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
				columns: self.columns.iter().map(|c| &c.name).cloned().collect(),
				cells: row.values,
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
	pub fn new(device_id: uuid::Uuid) -> Self {
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
