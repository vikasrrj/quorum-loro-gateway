use loro_protocol::ProtocolMessage;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_message_bytes: usize,
    pub max_room_id_bytes: usize,
    pub max_updates: usize,
    pub max_update_bytes: usize,
    pub max_updates_bytes: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: loro_protocol::MAX_MESSAGE_SIZE,
            max_room_id_bytes: 128,
            max_updates: 4096,
            max_update_bytes: loro_protocol::MAX_MESSAGE_SIZE,
            max_updates_bytes: loro_protocol::MAX_MESSAGE_SIZE,
        }
    }
}

pub fn decode_bounded(
    input: &[u8],
    limits: ProtocolLimits,
) -> Result<ProtocolMessage, ProtocolDecodeError> {
    precheck(input, limits)?;
    loro_protocol::decode(input).map_err(ProtocolDecodeError::OfficialCodec)
}

fn precheck(input: &[u8], limits: ProtocolLimits) -> Result<(), ProtocolDecodeError> {
    enforce_limit(
        "message bytes",
        input.len(),
        limits
            .max_message_bytes
            .min(loro_protocol::MAX_MESSAGE_SIZE),
    )?;
    let mut reader = Reader::new(input);
    reader.take(4)?;
    let room_len = reader.var_len()?;
    enforce_limit("room ID bytes", room_len, limits.max_room_id_bytes)?;
    reader.take(room_len)?;
    let message_type = reader.byte()?;
    match message_type {
        0x00 => {
            reader.var_bytes()?;
            reader.var_bytes()?;
        }
        0x01 => {
            reader.var_bytes()?;
            reader.var_bytes()?;
            reader.var_bytes()?;
        }
        0x02 => {
            let code = reader.byte()?;
            reader.var_bytes()?;
            if code == 0x01 && !reader.is_empty() {
                reader.var_bytes()?;
            }
            if code == 0x7f && !reader.is_empty() {
                reader.var_bytes()?;
            }
        }
        0x03 => {
            let count = reader.var_len()?;
            enforce_limit("update count", count, limits.max_updates)?;
            let mut updates_bytes = 0_usize;
            for _ in 0..count {
                let update_len = reader.var_len()?;
                enforce_limit(
                    "individual update bytes",
                    update_len,
                    limits.max_update_bytes,
                )?;
                updates_bytes = updates_bytes
                    .checked_add(update_len)
                    .ok_or(ProtocolDecodeError::LengthOverflow)?;
                enforce_limit(
                    "aggregate update bytes",
                    updates_bytes,
                    limits.max_updates_bytes,
                )?;
                reader.take(update_len)?;
            }
            reader.take(8)?;
        }
        0x04 => {
            reader.take(8)?;
            reader.uleb128()?;
            reader.uleb128()?;
        }
        0x05 => {
            reader.take(8)?;
            reader.uleb128()?;
            reader.var_bytes()?;
        }
        0x06 => {
            reader.byte()?;
            reader.var_bytes()?;
        }
        0x07 => {}
        0x08 => {
            reader.take(9)?;
        }
        _ => return Err(ProtocolDecodeError::Invalid("unknown message type")),
    }
    if !reader.is_empty() {
        return Err(ProtocolDecodeError::Invalid("trailing message bytes"));
    }
    Ok(())
}

fn enforce_limit(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), ProtocolDecodeError> {
    if actual > limit {
        return Err(ProtocolDecodeError::LimitExceeded {
            resource,
            actual,
            limit,
        });
    }
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, cursor: 0 }
    }

    fn is_empty(&self) -> bool {
        self.cursor == self.input.len()
    }

    fn byte(&mut self) -> Result<u8, ProtocolDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolDecodeError> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.input.len())
            .ok_or(ProtocolDecodeError::Truncated)?;
        let bytes = &self.input[self.cursor..end];
        self.cursor = end;
        Ok(bytes)
    }

    fn var_len(&mut self) -> Result<usize, ProtocolDecodeError> {
        usize::try_from(self.uleb128()?).map_err(|_| ProtocolDecodeError::LengthOverflow)
    }

    fn var_bytes(&mut self) -> Result<&'a [u8], ProtocolDecodeError> {
        let length = self.var_len()?;
        self.take(length)
    }

    fn uleb128(&mut self) -> Result<u64, ProtocolDecodeError> {
        let mut result = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.byte()?;
            if shift == 63 && byte & 0x7e != 0 {
                return Err(ProtocolDecodeError::LengthOverflow);
            }
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift = shift
                .checked_add(7)
                .ok_or(ProtocolDecodeError::LengthOverflow)?;
            if shift > 63 {
                return Err(ProtocolDecodeError::LengthOverflow);
            }
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolDecodeError {
    #[error("truncated protocol message")]
    Truncated,
    #[error("protocol length overflow")]
    LengthOverflow,
    #[error("{resource} exceeds limit {limit}: {actual}")]
    LimitExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("invalid protocol message: {0}")]
    Invalid(&'static str),
    #[error("official protocol codec rejected message: {0}")]
    OfficialCodec(String),
}

#[cfg(test)]
mod tests {
    use loro_protocol::BatchId;
    use loro_protocol::CrdtType;

    use super::*;

    #[test]
    fn official_message_round_trips_through_bounded_decoder() {
        let expected = ProtocolMessage::DocUpdate {
            crdt: CrdtType::Loro,
            room_id: "room".into(),
            updates: vec![b"first".to_vec(), b"second".to_vec()],
            batch_id: BatchId([7; 8]),
        };
        let encoded = loro_protocol::encode(&expected).expect("encode official message");
        assert_eq!(
            decode_bounded(&encoded, ProtocolLimits::default()).expect("decode bounded message"),
            expected
        );
    }

    #[test]
    fn hostile_update_count_is_rejected_before_official_decode() {
        let mut encoded = b"%LOR\x04room\x03".to_vec();
        encoded.extend_from_slice(&[0xff; 9]);
        encoded.push(1);
        assert_eq!(
            decode_bounded(&encoded, ProtocolLimits::default())
                .expect_err("hostile count must fail"),
            ProtocolDecodeError::LimitExceeded {
                resource: "update count",
                actual: usize::try_from(u64::MAX).unwrap_or(usize::MAX),
                limit: ProtocolLimits::default().max_updates,
            }
        );
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        for length in 0..512 {
            let bytes = (0..length)
                .map(|index| (index as u8).wrapping_mul(17).wrapping_add(length as u8))
                .collect::<Vec<_>>();
            assert!(
                std::panic::catch_unwind(|| { decode_bounded(&bytes, ProtocolLimits::default()) })
                    .is_ok(),
                "protocol decoder panicked for {length} bytes"
            );
        }
    }
}
