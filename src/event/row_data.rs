use minicbor::{
    data::{IanaTag, Tag, Type},
    decode,
    encode::{self, Write},
    Decode, Decoder, Encode, Encoder,
};
use num_bigint::Sign;
use pg_replicate::conversions::{numeric::PgNumeric, Cell};

use crate::{audit_sink::ColumnDescription, event::Timestamp};

use super::uuid::encode_uuid;

const TAG_POSTGRES: Tag = Tag::new(u16::from_be_bytes([b'p', b'g']) as _);

const TAG_JSON: Tag = Tag::new(262);

#[derive(Clone, Debug)]
pub struct RowData {
    pub columns: Vec<ColumnDescription>,
    pub cells: Vec<Cell>,
}

impl<C> Encode<C> for RowData {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        ctx: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_POSTGRES)?;
        e.map(
            self.cells
                .len()
                .try_into()
                .expect("postgres table columns will always fit in a u64"),
        )?;
        for (index, cell) in self.cells.iter().enumerate() {
            let Some(col) = self.columns.get(index) else {
                // TODO: handle this somehow?
                continue;
            };

            e.str(&col.name)?;
            match cell {
                Cell::Null => e.null().map(drop),
                Cell::Bool(v) => e.bool(*v).map(drop),
                Cell::String(v) => e.str(v.as_str()).map(drop),
                Cell::I16(v) => e.i16(*v).map(drop),
                Cell::I32(v) => e.i32(*v).map(drop),
                Cell::I64(v) => e.i64(*v).map(drop),
                Cell::U32(v) => e.u32(*v).map(drop),
                Cell::F32(v) => e.f32(*v).map(drop),
                Cell::F64(v) => e.f64(*v).map(drop),
                Cell::Numeric(v) => {
                    match v {
                        PgNumeric::NaN => e.f32(f32::NAN).map(drop),
                        PgNumeric::PositiveInf => e.f32(f32::INFINITY).map(drop),
                        PgNumeric::NegativeInf => e.f32(f32::NEG_INFINITY).map(drop),
                        PgNumeric::Value(dec) => {
                            // https://www.rfc-editor.org/rfc/rfc8949.html#name-decimal-fractions-and-bigfl

                            let (mantissa, exponent) = dec.as_bigint_and_exponent();
                            e.tag(IanaTag::Decimal)?;
                            e.array(2)?;
                            e.int(exponent.into())?;

                            let (sign, bytes) = mantissa.to_bytes_be();
                            e.tag(match sign {
                                Sign::NoSign | Sign::Plus => IanaTag::PosBignum,
                                Sign::Minus => IanaTag::NegBignum,
                            })?;
                            e.bytes(&bytes)
                        }
                        .map(drop),
                    }
                }
                Cell::Date(v) => {
                    // https://www.rfc-editor.org/rfc/rfc8943.html
                    e.tag(Tag::new(1004))?;
                    e.str(&v.format("%Y-%m-%d").to_string())
                }
                .map(drop),
                Cell::Time(v) => {
                    // https://www.rfc-editor.org/rfc/rfc8943.html
                    e.tag(Tag::new(1005))?;
                    e.str(&v.format("%H:%M:%S").to_string())
                }
                .map(drop),
                Cell::Uuid(v) => encode_uuid(v, e, ctx),
                Cell::TimeStamp(v) => {
                    let ts = v.and_utc();
                    let ts = Timestamp::try_from(ts).map_err(|err| {
                        encode::Error::message(format!("chrono timestamp too large: {err}"))
                    })?;
                    ts.encode(e, ctx)
                }
                Cell::TimeStampTz(v) => {
                    let ts = Timestamp::try_from(v).map_err(|err| {
                        encode::Error::message(format!("chrono timestamp too large: {err}"))
                    })?;
                    ts.encode(e, ctx)
                }
                Cell::Bytes(v) => e.bytes(&v).map(drop),
                Cell::Json(v) => {
                    e.tag(TAG_JSON)?;
                    let json = serde_json::to_vec(v).map_err(|err| {
                        encode::Error::message(format!("json encode error: {err}"))
                    })?;
                    e.bytes(&json).map(drop)
                }
                Cell::Array(_arr) => todo!("postgres arrays"),
            }?;
        }

        Ok(())
    }
}

// We don't decode anything out, but we still need to skip over the structure correctly
impl<'b, C> Decode<'b, C> for RowData {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, decode::Error> {
        use decode::Error;

        let p = d.position();
        if d.tag()? != TAG_POSTGRES {
            return Err(Error::tag_mismatch(TAG_POSTGRES).at(p));
        }

        match d.map()? {
            Some(length) => {
                for _ in 0..length {
                    d.skip()?;
                    d.skip()?;
                }
            }
            None => {
                while d.datatype()? != Type::Break {
                    d.skip()?;
                    d.skip()?;
                }
                d.skip()?;
            }
        }

        Ok(Self {
            columns: Vec::new(),
            cells: Vec::new(),
        })
    }
}
