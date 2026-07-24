use sha2::Digest;
use sha2::Sha256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamName {
    pub logical: String,
    pub physical: String,
}

pub fn document_hash(room_id: &str) -> String {
    hex(&Sha256::digest(room_id.as_bytes()))
}

pub fn delta_stream(room_id: &str) -> StreamName {
    let hash = document_hash(room_id);
    StreamName {
        logical: format!("room/{hash}/delta/0"),
        // Ursula stream IDs reject slashes, so the gateway uses a deterministic
        // physical encoding while preserving the approved logical name.
        physical: format!("r-{hash}-d0"),
    }
}

pub fn producer_id(boot_id: &[u8; 16], room_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quorum-loro-producer-v1\0");
    hasher.update(boot_id);
    hasher.update(room_id.as_bytes());
    hasher.update(0_u64.to_be_bytes());
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
        let first = delta_stream("document-a");
        let second = delta_stream("document-a");
        assert_eq!(first, second);
        assert_eq!(first.logical.matches('/').count(), 3);
        assert!(!first.physical.contains('/'));
        assert!(first.physical.len() <= 117);
    }
}
