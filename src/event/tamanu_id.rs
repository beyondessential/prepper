use super::uuid;

use minicbor::{
    data::{Tag, Type},
    decode,
    encode::{self, Write},
    Decode, Decoder, Encode, Encoder,
};

const TAG_TAMANUID: Tag = Tag::new(u64::from_be_bytes([
    b't', b'a', b'm', b'a', b'n', b'u', b'i', b'd',
]));

#[derive(Clone, Debug, PartialEq)]
pub enum TamanuId {
    Uuid(uuid::Uuid),
    Free(String),
}

impl From<String> for TamanuId {
    fn from(value: String) -> Self {
        Self::Free(value)
    }
}

impl From<&String> for TamanuId {
    fn from(value: &String) -> Self {
        Self::Free(value.into())
    }
}

impl From<&str> for TamanuId {
    fn from(value: &str) -> Self {
        Self::Free(value.into())
    }
}

impl From<uuid::Uuid> for TamanuId {
    fn from(value: uuid::Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<&uuid::Uuid> for TamanuId {
    fn from(value: &uuid::Uuid) -> Self {
        Self::Uuid(value.to_owned())
    }
}

impl<C> Encode<C> for TamanuId {
    fn encode<W: Write>(
        &self,
        e: &mut Encoder<W>,
        ctx: &mut C,
    ) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_TAMANUID)?;
        match self {
            Self::Free(str) => Encode::encode(str, e, ctx),
            Self::Uuid(uuid) => uuid::encode_uuid(uuid, e, ctx),
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
            Type::Tag => uuid::decode_uuid(d, ctx).map(Self::Uuid),
            unk => {
                Err(Error::message(format!("expected a string or byte string, got {unk}")).at(p))
            }
        }
    }
}

#[test]
pub(crate) fn round_trip_tamanuid_free() {
    let input = TamanuId::Free("facility-ExampleHospital".into());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: TamanuId = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}

#[test]
pub(crate) fn round_trip_tamanuid_uuid() {
    let input = TamanuId::Uuid(uuid::Uuid::new_v4());
    let mut buffer = [0u8; 128];
    minicbor::encode(&input, buffer.as_mut()).unwrap();
    let output: TamanuId = minicbor::decode(buffer.as_ref()).unwrap();
    assert_eq!(input, output);
}
