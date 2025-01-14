use minicbor::{
    data::Tag,
    decode,
    encode::{self, Write},
    Decoder, Encoder,
};
pub use uuid::Uuid;

const TAG_UUID: Tag = Tag::new(37);

pub(crate) fn encode_uuid<Ctx, W: Write>(
    v: &Uuid,
    e: &mut Encoder<W>,
    _ctx: &mut Ctx,
) -> Result<(), encode::Error<W::Error>> {
    e.tag(TAG_UUID)?.bytes(v.as_bytes())?;
    Ok(())
}

pub(crate) fn decode_uuid<'b, Ctx>(
    d: &mut Decoder<'b>,
    _ctx: &mut Ctx,
) -> Result<Uuid, decode::Error> {
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
