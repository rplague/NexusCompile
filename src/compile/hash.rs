use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::compile_protocol::Hash32;

pub fn hash_bytes(bytes: &[u8]) -> Hash32 {
    Hash32(Sha256::digest(bytes).into())
}

pub fn hash_file(path: &Path) -> io::Result<(Hash32, u64)> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut size = 0u64;

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size.saturating_add(read as u64);
    }

    Ok((Hash32(hasher.finalize().into()), size))
}

pub fn hex_hash(hash: &Hash32) -> String {
    let mut result = String::with_capacity(64);
    for byte in hash.0 {
        let _ = write!(&mut result, "{byte:02x}");
    }
    result
}
