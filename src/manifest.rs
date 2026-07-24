use crc32fast::Hasher as Crc32;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

const MAGIC: &[u8; 4] = b"QLGM";
const VERSION: u16 = 1;
pub const GENESIS_DIGEST: [u8; 32] = [0; 32];
const FIXED_RECORD_BYTES: usize = 4 + 2 + 2 + 8 + 32 + 8 + 32 + 8 + 8 + 32 + 8 + 32 + 4 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestLimits {
    pub max_record_bytes: usize,
    pub max_records: usize,
    pub max_stream_bytes: usize,
}

impl Default for ManifestLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: 4096,
            max_records: 65_536,
            max_stream_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRecord {
    pub room_hash: [u8; 32],
    pub revision: u64,
    pub previous_record_digest: [u8; 32],
    pub checkpoint_generation: u64,
    pub checkpoint_stream_end_offset: u64,
    pub checkpoint_record_digest: [u8; 32],
    pub active_delta_generation: u64,
    pub digest: [u8; 32],
}

impl ManifestRecord {
    pub fn new(
        room_id: &str,
        revision: u64,
        previous_record_digest: [u8; 32],
        checkpoint_generation: u64,
        checkpoint_record_bytes: &[u8],
        active_delta_generation: u64,
    ) -> Result<Self, ManifestError> {
        if checkpoint_record_bytes.is_empty() {
            return Err(ManifestError::Invalid("checkpoint record is empty"));
        }
        validate_generation_pair(checkpoint_generation, active_delta_generation)?;
        validate_previous_digest(revision, previous_record_digest)?;

        let checkpoint_stream_end_offset = u64::try_from(checkpoint_record_bytes.len())
            .map_err(|_| ManifestError::LengthOverflow)?;
        let checkpoint_record_digest = checkpoint_record_digest(checkpoint_record_bytes);
        let room_hash = room_hash(room_id);
        let digest = manifest_digest(
            &room_hash,
            revision,
            &previous_record_digest,
            checkpoint_generation,
            checkpoint_stream_end_offset,
            &checkpoint_record_digest,
            active_delta_generation,
        );

        Ok(Self {
            room_hash,
            revision,
            previous_record_digest,
            checkpoint_generation,
            checkpoint_stream_end_offset,
            checkpoint_record_digest,
            active_delta_generation,
            digest,
        })
    }

    pub fn belongs_to_room(&self, room_id: &str) -> bool {
        self.room_hash == room_hash(room_id)
    }

    pub fn verifies_checkpoint_bytes(&self, checkpoint_record_bytes: &[u8]) -> bool {
        self.checkpoint_stream_end_offset
            == u64::try_from(checkpoint_record_bytes.len()).unwrap_or(u64::MAX)
            && self.checkpoint_record_digest == checkpoint_record_digest(checkpoint_record_bytes)
    }

    pub fn encode(&self) -> Result<Vec<u8>, ManifestError> {
        self.encode_with_limits(ManifestLimits::default())
    }

    pub fn encode_with_limits(&self, limits: ManifestLimits) -> Result<Vec<u8>, ManifestError> {
        enforce_limit(
            "manifest record bytes",
            FIXED_RECORD_BYTES,
            limits.max_record_bytes,
        )?;
        validate_record_fields(self)?;

        let total_len =
            u64::try_from(FIXED_RECORD_BYTES).map_err(|_| ManifestError::LengthOverflow)?;
        let expected_digest = manifest_digest(
            &self.room_hash,
            self.revision,
            &self.previous_record_digest,
            self.checkpoint_generation,
            self.checkpoint_stream_end_offset,
            &self.checkpoint_record_digest,
            self.active_delta_generation,
        );
        if self.digest != expected_digest {
            return Err(ManifestError::DigestMismatch);
        }

        let mut output = Vec::with_capacity(FIXED_RECORD_BYTES);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_be_bytes());
        output.extend_from_slice(&0_u16.to_be_bytes());
        output.extend_from_slice(&total_len.to_be_bytes());
        output.extend_from_slice(&self.room_hash);
        output.extend_from_slice(&self.revision.to_be_bytes());
        output.extend_from_slice(&self.previous_record_digest);
        output.extend_from_slice(&self.checkpoint_generation.to_be_bytes());
        output.extend_from_slice(&self.checkpoint_stream_end_offset.to_be_bytes());
        output.extend_from_slice(&self.checkpoint_record_digest);
        output.extend_from_slice(&self.active_delta_generation.to_be_bytes());
        output.extend_from_slice(&self.digest);

        let mut crc = Crc32::new();
        crc.update(&output);
        output.extend_from_slice(&crc.finalize().to_be_bytes());
        output.extend_from_slice(&total_len.to_be_bytes());
        debug_assert_eq!(output.len(), FIXED_RECORD_BYTES);
        Ok(output)
    }

    pub fn decode_exact(input: &[u8], limits: ManifestLimits) -> Result<Self, ManifestError> {
        let (record, consumed) = Self::decode_one(input, limits)?;
        if consumed != input.len() {
            return Err(ManifestError::Invalid(
                "trailing bytes after manifest record",
            ));
        }
        Ok(record)
    }

    pub fn decode_one(
        input: &[u8],
        limits: ManifestLimits,
    ) -> Result<(Self, usize), ManifestError> {
        enforce_limit(
            "manifest record bytes",
            FIXED_RECORD_BYTES,
            limits.max_record_bytes,
        )?;
        if input.len() < FIXED_RECORD_BYTES {
            return Err(ManifestError::Truncated);
        }
        if input.get(..4) != Some(MAGIC.as_slice()) {
            return Err(ManifestError::Invalid("bad manifest magic"));
        }

        let mut cursor = 4;
        let version = read_u16(input, &mut cursor)?;
        if version != VERSION {
            return Err(ManifestError::UnsupportedVersion(version));
        }
        let flags = read_u16(input, &mut cursor)?;
        if flags != 0 {
            return Err(ManifestError::Invalid("unsupported manifest flags"));
        }

        let total_len = usize::try_from(read_u64(input, &mut cursor)?)
            .map_err(|_| ManifestError::LengthOverflow)?;
        if total_len != FIXED_RECORD_BYTES {
            return Err(ManifestError::Invalid("manifest record length mismatch"));
        }
        enforce_limit("manifest record bytes", total_len, limits.max_record_bytes)?;
        if input.len() < total_len {
            return Err(ManifestError::Truncated);
        }

        let frame = input.get(..total_len).ok_or(ManifestError::Truncated)?;
        let trailer_start = total_len
            .checked_sub(8)
            .ok_or(ManifestError::LengthOverflow)?;
        let trailer = read_u64_at(frame, trailer_start)?;
        if trailer != u64::try_from(total_len).map_err(|_| ManifestError::LengthOverflow)? {
            return Err(ManifestError::Invalid("manifest length trailer mismatch"));
        }

        let crc_start = total_len
            .checked_sub(12)
            .ok_or(ManifestError::LengthOverflow)?;
        let expected_crc = read_u32_at(frame, crc_start)?;
        let mut crc = Crc32::new();
        crc.update(frame.get(..crc_start).ok_or(ManifestError::Truncated)?);
        if crc.finalize() != expected_crc {
            return Err(ManifestError::ChecksumMismatch);
        }

        let room_hash_bytes = take(frame, &mut cursor, 32)?;
        let mut room_hash = [0_u8; 32];
        room_hash.copy_from_slice(room_hash_bytes);
        let revision = read_u64(frame, &mut cursor)?;

        let previous_digest_bytes = take(frame, &mut cursor, 32)?;
        let mut previous_record_digest = [0_u8; 32];
        previous_record_digest.copy_from_slice(previous_digest_bytes);

        let checkpoint_generation = read_u64(frame, &mut cursor)?;
        let checkpoint_stream_end_offset = read_u64(frame, &mut cursor)?;

        let checkpoint_digest_bytes = take(frame, &mut cursor, 32)?;
        let mut checkpoint_record_digest = [0_u8; 32];
        checkpoint_record_digest.copy_from_slice(checkpoint_digest_bytes);

        let active_delta_generation = read_u64(frame, &mut cursor)?;

        let digest_bytes = take(frame, &mut cursor, 32)?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(digest_bytes);

        if cursor != crc_start {
            return Err(ManifestError::Invalid("manifest fields are misplaced"));
        }

        let record = Self {
            room_hash,
            revision,
            previous_record_digest,
            checkpoint_generation,
            checkpoint_stream_end_offset,
            checkpoint_record_digest,
            active_delta_generation,
            digest,
        };
        validate_record_fields(&record)?;

        let expected_digest = manifest_digest(
            &record.room_hash,
            record.revision,
            &record.previous_record_digest,
            record.checkpoint_generation,
            record.checkpoint_stream_end_offset,
            &record.checkpoint_record_digest,
            record.active_delta_generation,
        );
        if record.digest != expected_digest {
            return Err(ManifestError::DigestMismatch);
        }

        Ok((record, total_len))
    }
}

pub fn decode_manifest_stream(
    mut input: &[u8],
    limits: ManifestLimits,
) -> Result<Vec<ManifestRecord>, ManifestError> {
    enforce_limit(
        "manifest stream bytes",
        input.len(),
        limits.max_stream_bytes,
    )?;
    let mut records = Vec::new();
    while !input.is_empty() {
        enforce_limit(
            "manifest record count",
            records.len().saturating_add(1),
            limits.max_records,
        )?;
        let (record, consumed) = ManifestRecord::decode_one(input, limits)?;
        records.push(record);
        input = input.get(consumed..).ok_or(ManifestError::Truncated)?;
    }
    Ok(records)
}

pub fn validate_manifest_chain<'a>(
    records: &'a [ManifestRecord],
    room_id: &str,
) -> Result<&'a ManifestRecord, ManifestError> {
    let Some(first) = records.first() else {
        return Err(ManifestError::EmptyStream);
    };
    if !first.belongs_to_room(room_id) {
        return Err(ManifestError::WrongRoom);
    }
    if first.revision != 0 {
        return Err(ManifestError::Invalid(
            "first manifest revision is not zero",
        ));
    }
    if first.previous_record_digest != GENESIS_DIGEST {
        return Err(ManifestError::Invalid(
            "first manifest record has a predecessor",
        ));
    }

    for window in records.windows(2) {
        let previous = &window[0];
        let current = &window[1];
        if !current.belongs_to_room(room_id) {
            return Err(ManifestError::WrongRoom);
        }
        let expected_revision = previous
            .revision
            .checked_add(1)
            .ok_or(ManifestError::LengthOverflow)?;
        if current.revision != expected_revision {
            return Err(ManifestError::Invalid("manifest revision gap"));
        }
        if current.previous_record_digest != previous.digest {
            return Err(ManifestError::Invalid(
                "manifest predecessor digest mismatch",
            ));
        }
        if current.checkpoint_generation <= previous.checkpoint_generation {
            return Err(ManifestError::Invalid(
                "checkpoint generation did not advance",
            ));
        }
        if current.active_delta_generation <= previous.active_delta_generation {
            return Err(ManifestError::Invalid(
                "active delta generation did not advance",
            ));
        }
    }

    records.last().ok_or(ManifestError::EmptyStream)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ManifestError {
    #[error("manifest record is truncated")]
    Truncated,
    #[error("manifest stream is empty")]
    EmptyStream,
    #[error("manifest record belongs to a different room")]
    WrongRoom,
    #[error("unsupported manifest version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid manifest record: {0}")]
    Invalid(&'static str),
    #[error("manifest checksum mismatch")]
    ChecksumMismatch,
    #[error("manifest digest mismatch")]
    DigestMismatch,
    #[error("manifest length overflow")]
    LengthOverflow,
    #[error("{resource} exceeds limit: {actual} > {limit}")]
    LimitExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
}

fn validate_record_fields(record: &ManifestRecord) -> Result<(), ManifestError> {
    validate_previous_digest(record.revision, record.previous_record_digest)?;
    validate_generation_pair(record.checkpoint_generation, record.active_delta_generation)?;
    if record.checkpoint_stream_end_offset == 0 {
        return Err(ManifestError::Invalid(
            "checkpoint stream end offset is zero",
        ));
    }
    Ok(())
}

fn validate_previous_digest(
    revision: u64,
    previous_record_digest: [u8; 32],
) -> Result<(), ManifestError> {
    if revision == 0 && previous_record_digest != GENESIS_DIGEST {
        return Err(ManifestError::Invalid(
            "first manifest record has a predecessor",
        ));
    }
    if revision > 0 && previous_record_digest == GENESIS_DIGEST {
        return Err(ManifestError::Invalid(
            "manifest predecessor digest is absent",
        ));
    }
    Ok(())
}

fn validate_generation_pair(
    checkpoint_generation: u64,
    active_delta_generation: u64,
) -> Result<(), ManifestError> {
    let expected_active = checkpoint_generation
        .checked_add(1)
        .ok_or(ManifestError::Invalid(
            "checkpoint generation cannot advance",
        ))?;
    if active_delta_generation != expected_active {
        return Err(ManifestError::Invalid(
            "active delta is not checkpoint generation plus one",
        ));
    }
    Ok(())
}

fn room_hash(room_id: &str) -> [u8; 32] {
    let digest = Sha256::digest(room_id.as_bytes());
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn checkpoint_record_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-checkpoint-record-v1\0");
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
    finalize_digest(hasher)
}

fn manifest_digest(
    room_hash: &[u8; 32],
    revision: u64,
    previous_record_digest: &[u8; 32],
    checkpoint_generation: u64,
    checkpoint_stream_end_offset: u64,
    checkpoint_record_digest: &[u8; 32],
    active_delta_generation: u64,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-manifest-v1\0");
    hasher.update(room_hash);
    hasher.update(revision.to_be_bytes());
    hasher.update(previous_record_digest);
    hasher.update(checkpoint_generation.to_be_bytes());
    hasher.update(checkpoint_stream_end_offset.to_be_bytes());
    hasher.update(checkpoint_record_digest);
    hasher.update(active_delta_generation.to_be_bytes());
    finalize_digest(hasher)
}

fn finalize_digest(hasher: Sha256) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn enforce_limit(resource: &'static str, actual: usize, limit: usize) -> Result<(), ManifestError> {
    if actual > limit {
        return Err(ManifestError::LimitExceeded {
            resource,
            actual,
            limit,
        });
    }
    Ok(())
}

fn read_u16(input: &[u8], cursor: &mut usize) -> Result<u16, ManifestError> {
    let bytes = take(input, cursor, 2)?;
    let mut output = [0_u8; 2];
    output.copy_from_slice(bytes);
    Ok(u16::from_be_bytes(output))
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32, ManifestError> {
    let bytes = take(input, cursor, 4)?;
    let mut output = [0_u8; 4];
    output.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(output))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> Result<u64, ManifestError> {
    let bytes = take(input, cursor, 8)?;
    let mut output = [0_u8; 8];
    output.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(output))
}

fn read_u32_at(input: &[u8], offset: usize) -> Result<u32, ManifestError> {
    let mut cursor = offset;
    read_u32(input, &mut cursor)
}

fn read_u64_at(input: &[u8], offset: usize) -> Result<u64, ManifestError> {
    let mut cursor = offset;
    read_u64(input, &mut cursor)
}

fn take<'a>(input: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], ManifestError> {
    let end = cursor
        .checked_add(length)
        .ok_or(ManifestError::LengthOverflow)?;
    let bytes = input.get(*cursor..end).ok_or(ManifestError::Truncated)?;
    *cursor = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_bytes(generation: u8) -> Vec<u8> {
        vec![generation; usize::from(generation) + 3]
    }

    fn first_record() -> ManifestRecord {
        ManifestRecord::new("room-a", 0, GENESIS_DIGEST, 7, &checkpoint_bytes(7), 8)
            .expect("create first manifest record")
    }

    #[test]
    fn manifest_record_round_trips_and_binds_checkpoint_bytes() {
        let checkpoint = checkpoint_bytes(7);
        let record = ManifestRecord::new("room-a", 0, GENESIS_DIGEST, 7, &checkpoint, 8)
            .expect("create manifest record");
        let encoded = record.encode().expect("encode manifest record");
        let decoded = ManifestRecord::decode_exact(&encoded, ManifestLimits::default())
            .expect("decode manifest record");

        assert_eq!(decoded, record);
        assert!(decoded.belongs_to_room("room-a"));
        assert!(!decoded.belongs_to_room("room-b"));
        assert!(decoded.verifies_checkpoint_bytes(&checkpoint));

        let mut changed = checkpoint;
        changed.push(9);
        assert!(!decoded.verifies_checkpoint_bytes(&changed));
    }

    #[test]
    fn manifest_stream_round_trips_and_validates_chain() {
        let first = first_record();
        let second = ManifestRecord::new("room-a", 1, first.digest, 8, &checkpoint_bytes(8), 9)
            .expect("create second manifest record");

        let mut encoded = first.encode().expect("encode first manifest record");
        encoded.extend_from_slice(&second.encode().expect("encode second manifest record"));

        let records = decode_manifest_stream(&encoded, ManifestLimits::default())
            .expect("decode manifest stream");
        let head = validate_manifest_chain(&records, "room-a").expect("validate manifest chain");

        assert_eq!(records, vec![first, second.clone()]);
        assert_eq!(head, &second);
    }

    #[test]
    fn manifest_corruption_fails_closed() {
        let record = first_record();
        let mut encoded = record.encode().expect("encode manifest record");
        encoded[64] ^= 1;

        assert_eq!(
            ManifestRecord::decode_exact(&encoded, ManifestLimits::default()),
            Err(ManifestError::ChecksumMismatch)
        );
    }

    #[test]
    fn manifest_chain_rejects_wrong_predecessor() {
        let first = first_record();
        let second = ManifestRecord::new("room-a", 1, [9; 32], 8, &checkpoint_bytes(8), 9)
            .expect("create second manifest record");

        assert_eq!(
            validate_manifest_chain(&[first, second], "room-a"),
            Err(ManifestError::Invalid(
                "manifest predecessor digest mismatch"
            ))
        );
    }

    #[test]
    fn manifest_requires_next_delta_generation() {
        assert_eq!(
            ManifestRecord::new("room-a", 0, GENESIS_DIGEST, 7, &checkpoint_bytes(7), 10),
            Err(ManifestError::Invalid(
                "active delta is not checkpoint generation plus one"
            ))
        );
    }

    #[test]
    fn manifest_limits_are_enforced() {
        let record = first_record();
        let limits = ManifestLimits {
            max_record_bytes: FIXED_RECORD_BYTES - 1,
            ..ManifestLimits::default()
        };

        assert_eq!(
            record.encode_with_limits(limits),
            Err(ManifestError::LimitExceeded {
                resource: "manifest record bytes",
                actual: FIXED_RECORD_BYTES,
                limit: FIXED_RECORD_BYTES - 1,
            })
        );
    }
}
