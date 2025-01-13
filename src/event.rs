use std::time::Duration;

use minicbor::{
    data::{Int, Tag, Type},
    decode,
    encode::{self, Write},
    Decode, Decoder, Encode, Encoder,
};
use pg_replicate::table::TableId;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Encode, Decode)]
pub struct Event {
    #[cbor(n(1))]
    pub table: Table,

    #[cbor(n(2))]
    pub device: Device,

    #[cbor(n(3))]
    pub snapshot: Snapshot,
}

#[derive(Clone, Debug, PartialEq, Encode, Decode)]
#[cbor(map)]
pub struct Table {
    #[cbor(n(1))]
    pub oid: TableId,

    #[cbor(n(2))]
    pub schema: String,

    #[cbor(n(3))]
    pub name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Encode, Decode)]
#[cbor(map)]
pub struct Device {
    #[cbor(n(1), encode_with = "encode_uuid", decode_with = "decode_uuid")]
    pub id: Uuid,

    #[cbor(n(2))]
    pub ts: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Encode, Decode)]
#[cbor(map)]
pub struct Snapshot {
    #[cbor(n(1))]
    pub id: TamanuId,

    // #[cbor(n(2))]
    // pub data: HashMap<String, Vec<u8>>,
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

const TAG_UUID: Tag = Tag::new(37);

fn encode_uuid<Ctx, W: Write>(
    v: &Uuid,
    e: &mut Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), encode::Error<W::Error>> {
    e.tag(TAG_UUID)?.bytes(v.as_bytes())?;
    Ok(())
}

fn decode_uuid<'b, Ctx>(d: &mut Decoder<'b>, _ctx: &mut Ctx) -> Result<Uuid, decode::Error> {
    use decode::Error;

    let p = d.position();
    if d.tag()? != TAG_UUID {
        return Err(Error::tag_mismatch(TAG_UUID).at(p));
    }

    let bytes: Vec<u8> = d
        .bytes_iter()?
        .flat_map(|v| v.unwrap_or_default())
        .copied()
        .collect();

    Uuid::from_slice(&bytes)
        .map_err(|_| Error::message(format!("expected 16 bytes, got {}", bytes.len())).at(p))
}

#[derive(Clone, Debug, PartialEq)]
pub enum TamanuId {
    Uuid(Uuid),
    Free(String),
}

const TAG_TAMANUID: Tag = Tag::new(u64::from_be_bytes([
    b't', b'a', b'm', b'a', b'n', b'u', b'i', b'd',
]));

impl<C> Encode<C> for TamanuId {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        ctx: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_TAMANUID)?;
        match self {
            Self::Free(str) => Encode::encode(str, e, ctx),
            Self::Uuid(uuid) => encode_uuid(uuid, e, ctx),
        }
    }
}

impl<'b, C> Decode<'b, C> for TamanuId {
    fn decode(d: &mut Decoder<'b>, ctx: &mut C) -> Result<Self, decode::Error> {
        use decode::Error;

        let p = d.position();
        if d.tag()? != TAG_TAMANUID {
            return Err(Error::tag_mismatch(TAG_TAMANUID).at(p));
        }

        let p = d.position();
        match d.datatype()? {
            Type::String => {
                let s = d.str()?;
                Ok(Self::Free(s.into()))
            }
            Type::StringIndef => {
                let s = d.str_iter()?.collect::<Result<_, decode::Error>>()?;
                Ok(Self::Free(s))
            }
            Type::Tag => decode_uuid(d, ctx).map(Self::Uuid),
            unk => {
                Err(Error::message(format!("expected a string or byte string, got {unk}")).at(p))
            }
        }
    }
}

#[test]
fn round_trip_tamanuid_free() {
    let input = TamanuId::Free("facility-ExampleHospital".into());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: TamanuId = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}

#[test]
fn round_trip_tamanuid_uuid() {
    let input = TamanuId::Uuid(Uuid::new_v4());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: TamanuId = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}

const TAG_ETIME: Tag = Tag::new(1001);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Timestamp(pub jiff::Timestamp);

// Postel's law:
// - when we read it, we try to be as flexible as possible
//   within our schema limits (ints can be any ints, in any order)
// - when we write it, we always do so as a definite 2-element map
//   with i8 keys, i64 for the seconds and u32 for the nanos.

impl<C> Encode<C> for Timestamp {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_ETIME)?
            .map(2)?
            .i8(1)?
            .i64(self.0.as_second())?
            .i8(-9)?
            .u32(
                self.0
                    .subsec_nanosecond()
                    .try_into()
                    .expect("subsecond nanos will always fit in u32"),
            )?;
        Ok(())
    }
}

impl<'b, C> Decode<'b, C> for Timestamp {
    fn decode(d: &mut Decoder<'b>, ctx: &mut C) -> Result<Self, decode::Error> {
        use decode::Error;

        let p = d.position();
        if d.tag()? != TAG_ETIME {
            return Err(Error::tag_mismatch(TAG_ETIME).at(p));
        }

        let p = d.position();
        let mut s: Option<Int> = None;
        let mut ns: Option<Int> = None;
        match d.map()? {
            Some(n) => {
                for _ in 0..n {
                    // definite-length map (n items)
                    let p = d.position();
                    let key = d.int()?;
                    match i128::from(key) {
                        1 => {
                            // primary time field in seconds since the epoch
                            s = Some(Decode::decode(d, ctx)?);
                        }
                        -9 => {
                            // extended time field in nanoseconds (fractional)
                            ns = Some(Decode::decode(d, ctx)?);
                        }
                        unk => {
                            return Err(
                                Error::message(format!("unsupported etime field {unk}")).at(p)
                            )
                        }
                    }
                }
            }
            None => {
                // indefinite-length map
                // we read until a break
                while d.datatype()? != Type::Break {
                    let p = d.position();
                    let key = d.int()?;
                    match i128::from(key) {
                        1 => {
                            // primary time field in seconds since the epoch
                            s = Some(Decode::decode(d, ctx)?);
                        }
                        -9 => {
                            // extended time field in nanoseconds (fractional)
                            ns = Some(Decode::decode(d, ctx)?);
                        }
                        unk => {
                            return Err(
                                Error::message(format!("unsupported etime field {unk}")).at(p)
                            )
                        }
                    }
                }
                d.skip()?;
            }
        }

        let s = s.ok_or_else(|| {
            Error::end_of_input()
                .at(p)
                .with_message("etime value incomplete, missing primary field")
        })?;

        let s = i64::try_from(s).map_err(|err| {
            Error::type_mismatch(Type::I64)
                .at(p)
                .with_message("etime.time value must fit in i64")
                .with_message(err)
        })?;

        let mut ts = jiff::Timestamp::from_second(s).map_err(|err| {
            Error::type_mismatch(Type::I64)
                .at(p)
                .with_message("etime.time value must be valid epoch")
                .with_message(err)
        })?;

        if let Some(ns) = ns {
            let ns = u64::try_from(ns).map_err(|err| {
                Error::type_mismatch(Type::I64)
                    .at(p)
                    .with_message("etime.nanoseconds value must fit in u64")
                    .with_message(err)
            })?;

            let fractional = Duration::from_nanos(ns);
            if fractional >= Duration::from_secs(1) {
                return Err(
                    Error::message("etime.nanoseconds value must fit within a second").at(p),
                );
            }

            ts = ts.checked_add(Duration::from_nanos(ns)).map_err(|err| {
                Error::type_mismatch(Type::I64)
                    .at(p)
                    .with_message("etime.nanoseconds value must fit into timestamp")
                    .with_message(err)
            })?;
        }

        Ok(Self(ts))
    }
}

#[test]
fn round_trip_timestamp() {
    let input = Timestamp(jiff::Timestamp::now());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: Timestamp = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}
