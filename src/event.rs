use minicbor::{data::Int, Decode, Encode};
use pg_replicate::table::TableId;
use row_data::RowData;
pub use tamanu_id::TamanuId;
pub use timestamp::Timestamp;
use uuid::Uuid;

pub(crate) mod row_data;
mod tamanu_id;
mod timestamp;
mod uuid;

pub const VERSION: u8 = 1;

#[derive(Clone, Debug, Encode, Decode)]
pub struct Event {
    #[cbor(n(0))]
    pub version: u8,

    #[cbor(n(1))]
    pub table: Table,

    #[cbor(n(2))]
    pub device: Device,

    #[cbor(n(3))]
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct Table {
    #[cbor(n(1))]
    pub oid: TableId,

    #[cbor(n(2))]
    pub schema: String,

    #[cbor(n(3))]
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
#[cbor(map)]
pub struct Device {
    #[cbor(
        n(1),
        encode_with = "uuid::encode_uuid",
        decode_with = "uuid::decode_uuid"
    )]
    pub id: Uuid,

    #[cbor(n(2))]
    pub ts: Timestamp,
}

#[derive(Clone, Debug, Encode, Decode)]
#[cbor(map)]
pub struct Snapshot {
    #[cbor(n(1))]
    pub id: TamanuId,

    #[cbor(n(2))]
    pub data: RowData,

    #[cbor(n(10))]
    pub created_at: Timestamp,

    #[cbor(n(11))]
    pub updated_at: Timestamp,

    #[cbor(n(12))]
    pub deleted_at: Option<Timestamp>,

    #[cbor(n(20))]
    pub sync_tick: Int,

    #[cbor(n(21))]
    pub updated_by: Option<TamanuId>,
}
