use std::{i64, time::Duration};

use minicbor::{
    data::{Int, Tag, Type},
    decode,
    encode::{self, Write},
    Decode, Decoder, Encode, Encoder,
};

const TAG_ETIME: Tag = Tag::new(1001);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Timestamp(pub jiff::Timestamp);

impl From<jiff::Timestamp> for Timestamp {
    fn from(value: jiff::Timestamp) -> Self {
        Self(value)
    }
}

impl TryFrom<chrono::DateTime<chrono::Utc>> for Timestamp {
    type Error = jiff::Error;
    fn try_from(value: chrono::DateTime<chrono::Utc>) -> Result<Self, Self::Error> {
        let nanos = value.timestamp_nanos_opt().unwrap_or(i64::MAX);
        jiff::Timestamp::from_nanosecond(nanos.into()).map(Self)
    }
}

impl TryFrom<&chrono::DateTime<chrono::Utc>> for Timestamp {
    type Error = jiff::Error;
    fn try_from(value: &chrono::DateTime<chrono::Utc>) -> Result<Self, Self::Error> {
        let nanos = value.timestamp_nanos_opt().unwrap_or(i64::MAX);
        jiff::Timestamp::from_nanosecond(nanos.into()).map(Self)
    }
}

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
pub(crate) fn round_trip_timestamp() {
    let input = Timestamp(jiff::Timestamp::now());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: Timestamp = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}
