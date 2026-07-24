use sha2::Digest;
use sha2::Sha256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GenerationId(u64);

impl GenerationId {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamName {
    pub logical: String,
    pub physical: String,
}

pub fn document_hash(room_id: &str) -> String {
    hex(&Sha256::digest(room_id.as_bytes()))
}

pub fn manifest_stream(room_id: &str) -> StreamName {
    let hash = document_hash(room_id);

    StreamName {
        logical: format!("room/{hash}/manifest"),
        physical: format!("r-{hash}-m"),
    }
}

pub fn delta_stream(room_id: &str) -> StreamName {
    delta_stream_for_generation(room_id, GenerationId::ZERO)
}

pub fn delta_stream_for_generation(room_id: &str, generation: GenerationId) -> StreamName {
    let hash = document_hash(room_id);
    let generation = generation.value();

    StreamName {
        logical: format!("room/{hash}/delta/{generation}"),
        physical: format!("r-{hash}-d{generation}"),
    }
}

pub fn checkpoint_stream(room_id: &str, generation: GenerationId) -> StreamName {
    let hash = document_hash(room_id);
    let generation = generation.value();

    StreamName {
        logical: format!("room/{hash}/checkpoint/{generation}"),
        physical: format!("r-{hash}-c{generation}"),
    }
}

pub fn producer_id(boot_id: &[u8; 16], room_id: &str) -> String {
    producer_id_for_generation(boot_id, room_id, GenerationId::ZERO)
}

pub fn producer_id_for_generation(
    boot_id: &[u8; 16],
    room_id: &str,
    generation: GenerationId,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-producer-v1\0");
    hasher.update(boot_id);
    hasher.update(room_id.as_bytes());
    hasher.update(generation.value().to_be_bytes());

    format!("qlg-{}", hex(&hasher.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_names_are_stable_and_ursula_safe() {
        let names = [
            manifest_stream("document-a"),
            checkpoint_stream("document-a", GenerationId::new(17)),
            delta_stream_for_generation("document-a", GenerationId::new(18)),
        ];

        for name in names {
            assert!(!name.physical.contains('/'));
            assert!(name.physical.len() <= 117);

            let repeated = match name.logical.as_str() {
                logical if logical.ends_with("/manifest") => manifest_stream("document-a"),
                logical if logical.ends_with("/checkpoint/17") => {
                    checkpoint_stream("document-a", GenerationId::new(17))
                }
                _ => delta_stream_for_generation("document-a", GenerationId::new(18)),
            };

            assert_eq!(name, repeated);
        }
    }

    #[test]
    fn legacy_delta_stream_is_generation_zero() {
        assert_eq!(
            delta_stream("document-a"),
            delta_stream_for_generation("document-a", GenerationId::ZERO,)
        );
    }

    #[test]
    fn stream_kinds_and_generations_do_not_collide() {
        let manifest = manifest_stream("document-a");
        let checkpoint = checkpoint_stream("document-a", GenerationId::new(7));
        let delta_seven = delta_stream_for_generation("document-a", GenerationId::new(7));
        let delta_eight = delta_stream_for_generation("document-a", GenerationId::new(8));

        assert_ne!(manifest.logical, checkpoint.logical);
        assert_ne!(manifest.physical, checkpoint.physical);

        assert_ne!(checkpoint.logical, delta_seven.logical);
        assert_ne!(checkpoint.physical, delta_seven.physical);

        assert_ne!(delta_seven.logical, delta_eight.logical);
        assert_ne!(delta_seven.physical, delta_eight.physical);
    }

    #[test]
    fn producer_ids_are_generation_scoped() {
        let boot_id = [9; 16];

        let zero = producer_id(&boot_id, "document-a");
        let explicit_zero = producer_id_for_generation(&boot_id, "document-a", GenerationId::ZERO);
        let next = producer_id_for_generation(&boot_id, "document-a", GenerationId::new(1));

        assert_eq!(zero, explicit_zero);
        assert_ne!(zero, next);
    }

    #[test]
    fn generation_increment_fails_closed_at_u64_max() {
        assert_eq!(
            GenerationId::new(41).checked_next(),
            Some(GenerationId::new(42))
        );

        assert_eq!(GenerationId::new(u64::MAX).checked_next(), None);
    }
}
