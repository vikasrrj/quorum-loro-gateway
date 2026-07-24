use crc32fast::Hasher as Crc32;
use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

const MAGIC: &[u8; 4] = b"QLGC";
const VERSION: u16 = 1;
const FIXED_HEADER: usize = 4 + 2 + 2 + 8 + 32 + 8 + 8 + 8 + 8 + 4;
const FIXED_SUFFIX: usize = 32 + 4 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointLimits {
    pub max_record_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_pending_updates: usize,
    pub max_pending_update_bytes: usize,
    pub max_pending_updates_bytes: usize,
}

impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: 128 * 1024 * 1024,
            max_snapshot_bytes: 64 * 1024 * 1024,
            max_pending_updates: 4096,
            max_pending_update_bytes: 32 * 1024 * 1024,
            max_pending_updates_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRecord {
    pub room_hash: [u8; 32],
    pub checkpoint_generation: u64,
    pub source_delta_generation: u64,
    pub source_delta_end_offset: u64,
    pub snapshot: Vec<u8>,
    pub pending_updates: Vec<Vec<u8>>,
    pub digest: [u8; 32],
}

impl CheckpointRecord {
    pub fn new(
        room_id: &str,
        checkpoint_generation: u64,
        source_delta_generation: u64,
        source_delta_end_offset: u64,
        snapshot: Vec<u8>,
        pending_updates: Vec<Vec<u8>>,
    ) -> Self {
        let digest = checkpoint_digest(&snapshot, &pending_updates);
        Self {
            room_hash: room_hash(room_id),
            checkpoint_generation,
            source_delta_generation,
            source_delta_end_offset,
            snapshot,
            pending_updates,
            digest,
        }
    }

    pub fn belongs_to_room(&self, room_id: &str) -> bool {
        self.room_hash == room_hash(room_id)
    }

    pub fn encode(&self) -> Result<Vec<u8>, CheckpointError> {
        self.encode_with_limits(CheckpointLimits::default())
    }

    pub fn encode_with_limits(&self, limits: CheckpointLimits) -> Result<Vec<u8>, CheckpointError> {
        if self.snapshot.is_empty() {
            return Err(CheckpointError::Invalid("checkpoint snapshot is empty"));
        }
        enforce_limit(
            "checkpoint snapshot bytes",
            self.snapshot.len(),
            limits.max_snapshot_bytes,
        )?;
        enforce_limit(
            "pending update count",
            self.pending_updates.len(),
            limits.max_pending_updates,
        )?;

        let snapshot_len =
            u64::try_from(self.snapshot.len()).map_err(|_| CheckpointError::LengthOverflow)?;
        let pending_count = u32::try_from(self.pending_updates.len())
            .map_err(|_| CheckpointError::Invalid("too many pending updates"))?;

        let lengths = self
            .pending_updates
            .iter()
            .map(|update| {
                if update.is_empty() {
                    return Err(CheckpointError::Invalid("pending update is empty"));
                }
                enforce_limit(
                    "individual pending update bytes",
                    update.len(),
                    limits.max_pending_update_bytes,
                )?;
                u32::try_from(update.len())
                    .map_err(|_| CheckpointError::Invalid("pending update is too large"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let pending_updates_len =
            self.pending_updates
                .iter()
                .try_fold(0_usize, |sum, update| {
                    let next = sum
                        .checked_add(update.len())
                        .ok_or(CheckpointError::LengthOverflow)?;
                    enforce_limit(
                        "aggregate pending update bytes",
                        next,
                        limits.max_pending_updates_bytes,
                    )?;
                    Ok(next)
                })?;

        let length_table_len = lengths
            .len()
            .checked_mul(4)
            .ok_or(CheckpointError::LengthOverflow)?;
        let total_len = FIXED_HEADER
            .checked_add(length_table_len)
            .and_then(|value| value.checked_add(self.snapshot.len()))
            .and_then(|value| value.checked_add(pending_updates_len))
            .and_then(|value| value.checked_add(FIXED_SUFFIX))
            .ok_or(CheckpointError::LengthOverflow)?;
        enforce_limit(
            "checkpoint record bytes",
            total_len,
            limits.max_record_bytes,
        )?;
        let total_len_u64 =
            u64::try_from(total_len).map_err(|_| CheckpointError::LengthOverflow)?;

        let expected_digest = checkpoint_digest(&self.snapshot, &self.pending_updates);
        if self.digest != expected_digest {
            return Err(CheckpointError::DigestMismatch);
        }

        let mut output = Vec::with_capacity(total_len);
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_be_bytes());
        output.extend_from_slice(&0_u16.to_be_bytes());
        output.extend_from_slice(&total_len_u64.to_be_bytes());
        output.extend_from_slice(&self.room_hash);
        output.extend_from_slice(&self.checkpoint_generation.to_be_bytes());
        output.extend_from_slice(&self.source_delta_generation.to_be_bytes());
        output.extend_from_slice(&self.source_delta_end_offset.to_be_bytes());
        output.extend_from_slice(&snapshot_len.to_be_bytes());
        output.extend_from_slice(&pending_count.to_be_bytes());
        for length in lengths {
            output.extend_from_slice(&length.to_be_bytes());
        }
        output.extend_from_slice(&self.snapshot);
        for update in &self.pending_updates {
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

    pub fn decode_exact(input: &[u8], limits: CheckpointLimits) -> Result<Self, CheckpointError> {
        if input.len() < FIXED_HEADER + FIXED_SUFFIX {
            return Err(CheckpointError::Truncated);
        }
        if input.get(..4) != Some(MAGIC.as_slice()) {
            return Err(CheckpointError::Invalid("bad checkpoint magic"));
        }

        let mut cursor = 4;
        let version = read_u16(input, &mut cursor)?;
        if version != VERSION {
            return Err(CheckpointError::UnsupportedVersion(version));
        }
        let flags = read_u16(input, &mut cursor)?;
        if flags != 0 {
            return Err(CheckpointError::Invalid("unsupported checkpoint flags"));
        }

        let total_len = usize::try_from(read_u64(input, &mut cursor)?)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        if total_len < FIXED_HEADER + FIXED_SUFFIX {
            return Err(CheckpointError::Truncated);
        }
        enforce_limit(
            "checkpoint record bytes",
            total_len,
            limits.max_record_bytes,
        )?;
        if input.len() < total_len {
            return Err(CheckpointError::Truncated);
        }
        if input.len() != total_len {
            return Err(CheckpointError::Invalid(
                "trailing bytes after checkpoint record",
            ));
        }

        let trailer_start = total_len
            .checked_sub(8)
            .ok_or(CheckpointError::LengthOverflow)?;
        let trailer = read_u64_at(input, trailer_start)?;
        if trailer != u64::try_from(total_len).map_err(|_| CheckpointError::LengthOverflow)? {
            return Err(CheckpointError::Invalid(
                "checkpoint length trailer mismatch",
            ));
        }

        let crc_start = total_len
            .checked_sub(12)
            .ok_or(CheckpointError::LengthOverflow)?;
        let expected_crc = read_u32_at(input, crc_start)?;
        let mut crc = Crc32::new();
        crc.update(input.get(..crc_start).ok_or(CheckpointError::Truncated)?);
        if crc.finalize() != expected_crc {
            return Err(CheckpointError::ChecksumMismatch);
        }

        let digest_start = crc_start
            .checked_sub(32)
            .ok_or(CheckpointError::Truncated)?;

        let room_hash_bytes = take(input, &mut cursor, 32)?;
        let mut room_hash = [0_u8; 32];
        room_hash.copy_from_slice(room_hash_bytes);

        let checkpoint_generation = read_u64(input, &mut cursor)?;
        let source_delta_generation = read_u64(input, &mut cursor)?;
        let source_delta_end_offset = read_u64(input, &mut cursor)?;
        let snapshot_len = usize::try_from(read_u64(input, &mut cursor)?)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        enforce_limit(
            "checkpoint snapshot bytes",
            snapshot_len,
            limits.max_snapshot_bytes,
        )?;
        if snapshot_len == 0 {
            return Err(CheckpointError::Invalid("checkpoint snapshot is empty"));
        }

        let pending_count = usize::try_from(read_u32(input, &mut cursor)?)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        enforce_limit(
            "pending update count",
            pending_count,
            limits.max_pending_updates,
        )?;

        let mut lengths = Vec::with_capacity(pending_count);
        let mut pending_updates_len = 0_usize;
        for _ in 0..pending_count {
            let length = usize::try_from(read_u32(input, &mut cursor)?)
                .map_err(|_| CheckpointError::LengthOverflow)?;
            if length == 0 {
                return Err(CheckpointError::Invalid("pending update is empty"));
            }
            enforce_limit(
                "individual pending update bytes",
                length,
                limits.max_pending_update_bytes,
            )?;
            pending_updates_len = pending_updates_len
                .checked_add(length)
                .ok_or(CheckpointError::LengthOverflow)?;
            enforce_limit(
                "aggregate pending update bytes",
                pending_updates_len,
                limits.max_pending_updates_bytes,
            )?;
            lengths.push(length);
        }

        let expected_payload_len = snapshot_len
            .checked_add(pending_updates_len)
            .ok_or(CheckpointError::LengthOverflow)?;
        let declared_payload_len = digest_start
            .checked_sub(cursor)
            .ok_or(CheckpointError::Truncated)?;
        if declared_payload_len != expected_payload_len {
            return Err(CheckpointError::Invalid(
                "checkpoint lengths do not consume payload",
            ));
        }

        let snapshot = take_until(input, &mut cursor, snapshot_len, digest_start)?.to_vec();
        let mut pending_updates = Vec::with_capacity(pending_count);
        for length in lengths {
            pending_updates.push(take_until(input, &mut cursor, length, digest_start)?.to_vec());
        }
        if cursor != digest_start {
            return Err(CheckpointError::Invalid(
                "checkpoint lengths do not consume payload",
            ));
        }

        let digest_bytes = take(input, &mut cursor, 32)?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(digest_bytes);
        if digest != checkpoint_digest(&snapshot, &pending_updates) {
            return Err(CheckpointError::DigestMismatch);
        }
        if cursor != crc_start {
            return Err(CheckpointError::Invalid("checkpoint digest is misplaced"));
        }

        Ok(Self {
            room_hash,
            checkpoint_generation,
            source_delta_generation,
            source_delta_end_offset,
            snapshot,
            pending_updates,
            digest,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CheckpointError {
    #[error("checkpoint record is truncated")]
    Truncated,
    #[error("unsupported checkpoint version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid checkpoint record: {0}")]
    Invalid(&'static str),
    #[error("checkpoint checksum mismatch")]
    ChecksumMismatch,
    #[error("checkpoint digest mismatch")]
    DigestMismatch,
    #[error("checkpoint length overflow")]
    LengthOverflow,
    #[error("{resource} exceeds limit: {actual} > {limit}")]
    LimitExceeded {
        resource: &'static str,
        actual: usize,
        limit: usize,
    },
}

fn room_hash(room_id: &str) -> [u8; 32] {
    let digest = Sha256::digest(room_id.as_bytes());
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn checkpoint_digest(snapshot: &[u8], pending_updates: &[Vec<u8>]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-checkpoint-v1\0");
    hasher.update(
        u64::try_from(snapshot.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(snapshot);
    hasher.update(
        u64::try_from(pending_updates.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for update in pending_updates {
        hasher.update(
            u64::try_from(update.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(update);
    }
    let digest = hasher.finalize();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn enforce_limit(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), CheckpointError> {
    if actual > limit {
        return Err(CheckpointError::LimitExceeded {
            resource,
            actual,
            limit,
        });
    }
    Ok(())
}

fn read_u16(input: &[u8], cursor: &mut usize) -> Result<u16, CheckpointError> {
    let bytes = take(input, cursor, 2)?;
    let mut output = [0_u8; 2];
    output.copy_from_slice(bytes);
    Ok(u16::from_be_bytes(output))
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32, CheckpointError> {
    let bytes = take(input, cursor, 4)?;
    let mut output = [0_u8; 4];
    output.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(output))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> Result<u64, CheckpointError> {
    let bytes = take(input, cursor, 8)?;
    let mut output = [0_u8; 8];
    output.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(output))
}

fn read_u32_at(input: &[u8], offset: usize) -> Result<u32, CheckpointError> {
    let mut cursor = offset;
    read_u32(input, &mut cursor)
}

fn read_u64_at(input: &[u8], offset: usize) -> Result<u64, CheckpointError> {
    let mut cursor = offset;
    read_u64(input, &mut cursor)
}

fn take<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], CheckpointError> {
    let end = cursor
        .checked_add(length)
        .ok_or(CheckpointError::LengthOverflow)?;
    let bytes = input.get(*cursor..end).ok_or(CheckpointError::Truncated)?;
    *cursor = end;
    Ok(bytes)
}

fn take_until<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    length: usize,
    end_limit: usize,
) -> Result<&'a [u8], CheckpointError> {
    let end = cursor
        .checked_add(length)
        .ok_or(CheckpointError::LengthOverflow)?;
    if end > end_limit {
        return Err(CheckpointError::Truncated);
    }
    take(input, cursor, length)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> CheckpointRecord {
        CheckpointRecord::new(
            "room-a",
            7,
            7,
            4096,
            vec![1, 2, 3],
            vec![vec![4, 5], vec![6, 7, 8]],
        )
    }

    #[test]
    fn checkpoint_record_round_trips() {
        let record = sample_record();
        let encoded = record.encode().expect("encode checkpoint record");
        let decoded = CheckpointRecord::decode_exact(&encoded, CheckpointLimits::default())
            .expect("decode checkpoint record");

        assert_eq!(decoded, record);
        assert!(decoded.belongs_to_room("room-a"));
        assert!(!decoded.belongs_to_room("room-b"));
    }

    #[test]
    fn checkpoint_corruption_fails_closed() {
        let record = sample_record();
        let mut encoded = record.encode().expect("encode checkpoint record");
        let payload_start = FIXED_HEADER + record.pending_updates.len() * 4;
        encoded[payload_start] ^= 0x01;

        assert_eq!(
            CheckpointRecord::decode_exact(&encoded, CheckpointLimits::default()),
            Err(CheckpointError::ChecksumMismatch)
        );
    }

    #[test]
    fn checkpoint_rejects_trailing_bytes() {
        let record = sample_record();
        let mut encoded = record.encode().expect("encode checkpoint record");
        encoded.push(0);

        assert_eq!(
            CheckpointRecord::decode_exact(&encoded, CheckpointLimits::default()),
            Err(CheckpointError::Invalid(
                "trailing bytes after checkpoint record"
            ))
        );
    }

    #[test]
    fn checkpoint_limits_are_enforced() {
        let record = sample_record();
        let limits = CheckpointLimits {
            max_snapshot_bytes: 2,
            ..CheckpointLimits::default()
        };

        assert_eq!(
            record.encode_with_limits(limits),
            Err(CheckpointError::LimitExceeded {
                resource: "checkpoint snapshot bytes",
                actual: 3,
                limit: 2,
            })
        );
    }

    #[test]
    fn checkpoint_encode_rejects_stale_digest() {
        let mut record = sample_record();
        record.snapshot.push(9);

        assert_eq!(record.encode(), Err(CheckpointError::DigestMismatch));
    }
}
