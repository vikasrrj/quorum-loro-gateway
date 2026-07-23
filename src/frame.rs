use crc32fast::Hasher as Crc32;
use loro_protocol::BatchId;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

const MAGIC: &[u8; 4] = b"QLGD";
const VERSION: u16 = 1;
const FIXED_PREFIX: usize = 4 + 2 + 2 + 8;
const FIXED_SUFFIX: usize = 32 + 4 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerTuple {
    pub id: String,
    pub epoch: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaFrame {
    pub producer: ProducerTuple,
    pub batch_id: BatchId,
    pub updates: Vec<Vec<u8>>,
    pub digest: [u8; 32],
}

impl DeltaFrame {
    pub fn new(producer: ProducerTuple, batch_id: BatchId, updates: Vec<Vec<u8>>) -> Self {
        let digest = update_digest(&updates);
        Self {
            producer,
            batch_id,
            updates,
            digest,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, FrameError> {
        if self.producer.id.is_empty() {
            return Err(FrameError::Invalid("producer ID is empty"));
        }
        let producer_len = u16::try_from(self.producer.id.len())
            .map_err(|_| FrameError::Invalid("producer ID is too long"))?;
        let update_count = u32::try_from(self.updates.len())
            .map_err(|_| FrameError::Invalid("too many updates"))?;
        let lengths = self
            .updates
            .iter()
            .map(|update| {
                u32::try_from(update.len()).map_err(|_| FrameError::Invalid("update is too large"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let updates_len = self.updates.iter().try_fold(0_usize, |sum, update| {
            sum.checked_add(update.len())
                .ok_or(FrameError::LengthOverflow)
        })?;
        let total_len = FIXED_PREFIX
            .checked_add(2)
            .and_then(|value| value.checked_add(usize::from(producer_len)))
            .and_then(|value| value.checked_add(8 + 8 + 8 + 4))
            .and_then(|value| value.checked_add(lengths.len() * 4))
            .and_then(|value| value.checked_add(updates_len))
            .and_then(|value| value.checked_add(FIXED_SUFFIX))
            .ok_or(FrameError::LengthOverflow)?;
        let total_len_u64 = u64::try_from(total_len).map_err(|_| FrameError::LengthOverflow)?;

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_be_bytes());
        output.extend_from_slice(&0_u16.to_be_bytes());
        output.extend_from_slice(&total_len_u64.to_be_bytes());
        output.extend_from_slice(&producer_len.to_be_bytes());
        output.extend_from_slice(self.producer.id.as_bytes());
        output.extend_from_slice(&self.producer.epoch.to_be_bytes());
        output.extend_from_slice(&self.producer.sequence.to_be_bytes());
        output.extend_from_slice(&self.batch_id.0);
        output.extend_from_slice(&update_count.to_be_bytes());
        for length in lengths {
            output.extend_from_slice(&length.to_be_bytes());
        }
        for update in &self.updates {
            output.extend_from_slice(update);
        }
        output.extend_from_slice(&self.digest);

        let mut crc = Crc32::new();
        crc.update(&output);
        output.extend_from_slice(&crc.finalize().to_be_bytes());
        output.extend_from_slice(&total_len_u64.to_be_bytes());
        debug_assert_eq!(output.len(), total_len);
        Ok(output)
    }

    pub fn decode_one(input: &[u8]) -> Result<(Self, usize), FrameError> {
        if input.len() < FIXED_PREFIX + FIXED_SUFFIX {
            return Err(FrameError::Truncated);
        }
        if input.get(..4) != Some(MAGIC.as_slice()) {
            return Err(FrameError::Invalid("bad frame magic"));
        }
        let mut cursor = 4;
        let version = read_u16(input, &mut cursor)?;
        if version != VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }
        let flags = read_u16(input, &mut cursor)?;
        if flags != 0 {
            return Err(FrameError::Invalid("unsupported frame flags"));
        }
        let total_len = usize::try_from(read_u64(input, &mut cursor)?)
            .map_err(|_| FrameError::LengthOverflow)?;
        if total_len < FIXED_PREFIX + FIXED_SUFFIX || input.len() < total_len {
            return Err(FrameError::Truncated);
        }
        let frame = input.get(..total_len).ok_or(FrameError::Truncated)?;
        let trailer_start = total_len.checked_sub(8).ok_or(FrameError::LengthOverflow)?;
        let trailer = read_u64_at(frame, trailer_start)?;
        if trailer != u64::try_from(total_len).map_err(|_| FrameError::LengthOverflow)? {
            return Err(FrameError::Invalid("frame length trailer mismatch"));
        }
        let crc_start = total_len
            .checked_sub(12)
            .ok_or(FrameError::LengthOverflow)?;
        let expected_crc = read_u32_at(frame, crc_start)?;
        let mut crc = Crc32::new();
        crc.update(frame.get(..crc_start).ok_or(FrameError::Truncated)?);
        if crc.finalize() != expected_crc {
            return Err(FrameError::ChecksumMismatch);
        }

        let producer_len = usize::from(read_u16(frame, &mut cursor)?);
        let producer_bytes = take(frame, &mut cursor, producer_len)?;
        let producer_id = std::str::from_utf8(producer_bytes)
            .map_err(|_| FrameError::Invalid("producer ID is not UTF-8"))?
            .to_owned();
        if producer_id.is_empty() {
            return Err(FrameError::Invalid("producer ID is empty"));
        }
        let epoch = read_u64(frame, &mut cursor)?;
        let sequence = read_u64(frame, &mut cursor)?;
        let batch_bytes = take(frame, &mut cursor, 8)?;
        let mut batch_id = [0_u8; 8];
        batch_id.copy_from_slice(batch_bytes);
        let count = usize::try_from(read_u32(frame, &mut cursor)?)
            .map_err(|_| FrameError::LengthOverflow)?;
        let mut lengths = Vec::with_capacity(count);
        for _ in 0..count {
            lengths.push(
                usize::try_from(read_u32(frame, &mut cursor)?)
                    .map_err(|_| FrameError::LengthOverflow)?,
            );
        }
        let payload_end = crc_start.checked_sub(32).ok_or(FrameError::Truncated)?;
        let mut updates = Vec::with_capacity(count);
        for length in lengths {
            let update = take_until(frame, &mut cursor, length, payload_end)?;
            updates.push(update.to_vec());
        }
        if cursor != payload_end {
            return Err(FrameError::Invalid("update lengths do not consume payload"));
        }
        let digest_bytes = take(frame, &mut cursor, 32)?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(digest_bytes);
        if digest != update_digest(&updates) {
            return Err(FrameError::DigestMismatch);
        }

        Ok((
            Self {
                producer: ProducerTuple {
                    id: producer_id,
                    epoch,
                    sequence,
                },
                batch_id: BatchId(batch_id),
                updates,
                digest,
            },
            total_len,
        ))
    }
}

pub fn decode_all(mut input: &[u8]) -> Result<Vec<DeltaFrame>, FrameError> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        let (frame, consumed) = DeltaFrame::decode_one(input)?;
        frames.push(frame);
        input = input.get(consumed..).ok_or(FrameError::Truncated)?;
    }
    Ok(frames)
}

fn update_digest(updates: &[Vec<u8>]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-updates-v1\0");
    hasher.update(
        u64::try_from(updates.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for update in updates {
        hasher.update(
            u64::try_from(update.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(update);
    }
    hasher.finalize().into()
}

fn take<'a>(input: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], FrameError> {
    take_until(input, cursor, length, input.len())
}

fn take_until<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    length: usize,
    limit: usize,
) -> Result<&'a [u8], FrameError> {
    let end = cursor
        .checked_add(length)
        .filter(|end| *end <= limit)
        .ok_or(FrameError::Truncated)?;
    let bytes = input.get(*cursor..end).ok_or(FrameError::Truncated)?;
    *cursor = end;
    Ok(bytes)
}

fn read_u16(input: &[u8], cursor: &mut usize) -> Result<u16, FrameError> {
    let bytes = take(input, cursor, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32, FrameError> {
    let bytes = take(input, cursor, 4)?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> Result<u64, FrameError> {
    let bytes = take(input, cursor, 8)?;
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(value))
}

fn read_u32_at(input: &[u8], offset: usize) -> Result<u32, FrameError> {
    let mut cursor = offset;
    read_u32(input, &mut cursor)
}

fn read_u64_at(input: &[u8], offset: usize) -> Result<u64, FrameError> {
    let mut cursor = offset;
    read_u64(input, &mut cursor)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("truncated frame")]
    Truncated,
    #[error("frame length overflow")]
    LengthOverflow,
    #[error("unsupported frame version {0}")]
    UnsupportedVersion(u16),
    #[error("frame checksum mismatch")]
    ChecksumMismatch,
    #[error("frame update digest mismatch")]
    DigestMismatch,
    #[error("invalid frame: {0}")]
    Invalid(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DeltaFrame {
        DeltaFrame::new(
            ProducerTuple {
                id: "producer-a".into(),
                epoch: 3,
                sequence: 9,
            },
            BatchId([1, 2, 3, 4, 5, 6, 7, 8]),
            vec![b"first".to_vec(), vec![0, 1, 2, 255]],
        )
    }

    #[test]
    fn exact_round_trip() {
        let expected = sample();
        let encoded = expected.encode().expect("encode frame");
        let (actual, consumed) = DeltaFrame::decode_one(&encoded).expect("decode frame");
        assert_eq!(actual, expected);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn rejects_every_truncated_prefix() {
        let encoded = sample().encode().expect("encode frame");
        for length in 0..encoded.len() {
            assert!(DeltaFrame::decode_one(&encoded[..length]).is_err());
        }
    }

    #[test]
    fn rejects_corruption() {
        let mut encoded = sample().encode().expect("encode frame");
        let middle = encoded.len() / 2;
        encoded[middle] ^= 0x80;
        assert_eq!(
            DeltaFrame::decode_one(&encoded).expect_err("corruption must fail"),
            FrameError::ChecksumMismatch
        );
    }
}
