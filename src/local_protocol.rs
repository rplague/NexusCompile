//! Local wrapper ↔ node protocol over a Unix socket.

use std::io::{self, Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::compile_protocol::Hash32;

pub const LOCAL_PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME: usize = 16 << 20;
pub const MAX_DIAGNOSTIC: usize = 6 << 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalRequest {
    pub v: u16,
    pub cwd: String,
    pub rustc: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

impl LocalRequest {
    pub fn new(
        cwd: impl Into<String>,
        rustc: impl Into<String>,
        args: Vec<String>,
        env: Vec<(String, String)>,
    ) -> Self {
        Self {
            v: LOCAL_PROTOCOL_VERSION,
            cwd: cwd.into(),
            rustc: rustc.into(),
            args,
            env,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalResponse {
    pub v: u16,
    pub body: LocalBody,
}

impl LocalResponse {
    pub fn new(body: LocalBody) -> Self {
        Self {
            v: LOCAL_PROTOCOL_VERSION,
            body,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(LocalBody::Error {
            code: code.into(),
            message: message.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum LocalBody {
    Compiled {
        action_digest: Hash32,
        exit_code: i32,
        cache_hit: bool,
        #[serde(with = "serde_bytes")]
        stdout: Vec<u8>,
        #[serde(with = "serde_bytes")]
        stderr: Vec<u8>,
        duration_ms: u64,
    },
    Error {
        code: String,
        message: String,
    },
}

pub fn write_message<W: Write, T: Serialize>(writer: &mut W, value: &T) -> io::Result<()> {
    let mut body = Vec::new();
    ciborium::ser::into_writer(value, &mut body)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    if body.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame too large: {}", body.len()),
        ));
    }
    writer.write_all(&(body.len() as u32).to_be_bytes())?;
    writer.write_all(&body)?;
    writer.flush()
}

pub fn read_message<R: Read, T: DeserializeOwned>(reader: &mut R) -> io::Result<T> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len}"),
        ));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body)?;
    ciborium::de::from_reader(body.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

pub fn truncate_diagnostic(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.len() <= MAX_DIAGNOSTIC {
        return bytes;
    }
    bytes.truncate(MAX_DIAGNOSTIC);
    bytes.extend_from_slice(b"\n[nexus-compile: output truncated]\n");
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn request_roundtrip() {
        let request = LocalRequest::new(
            "/tmp/project",
            "/usr/bin/rustc",
            vec!["--crate-name".into(), "demo".into()],
            vec![("CARGO_INCREMENTAL".into(), "0".into())],
        );
        let mut bytes = Vec::new();
        write_message(&mut bytes, &request).unwrap();
        let decoded: LocalRequest = read_message(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn response_roundtrip_with_bytes() {
        let response = LocalResponse::new(LocalBody::Compiled {
            action_digest: Hash32([7; 32]),
            exit_code: 1,
            cache_hit: false,
            stdout: vec![1, 2],
            stderr: vec![3, 4],
            duration_ms: 5,
        });
        let mut bytes = Vec::new();
        write_message(&mut bytes, &response).unwrap();
        let decoded: LocalResponse = read_message(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn oversized_frame_is_rejected() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((MAX_FRAME + 1) as u32).to_be_bytes());
        let result: io::Result<LocalRequest> = read_message(&mut Cursor::new(bytes));
        assert!(result.is_err());
    }
}
