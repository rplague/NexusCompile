//! Local wrapper ↔ sidecar task protocol.
//!
//! The task protocol carries the same manifest/bundle/output units used by
//! remote providers, but over a local Unix socket.

use serde::{Deserialize, Serialize};

use crate::compile_protocol::{Hash32, OutputRef};

pub const LOCAL_TASK_PROTOCOL_VERSION: u16 = 1;
pub const TASK_CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalTaskRequest {
    pub v: u16,
    pub body: LocalTaskRequestBody,
}

impl LocalTaskRequest {
    pub fn new(body: LocalTaskRequestBody) -> Self {
        Self {
            v: LOCAL_TASK_PROTOCOL_VERSION,
            body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum LocalTaskRequestBody {
    Probe {
        task_id: Hash32,
    },
    Start {
        task_id: Hash32,
        bundle_hash: Hash32,
        bundle_len: u64,
    },
    Chunk {
        task_id: Hash32,
        offset: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Ready {
        task_id: Hash32,
    },
    Output {
        task_id: Hash32,
        hash: Hash32,
        offset: u64,
        len: u32,
    },
    Ack {
        task_id: Hash32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalTaskResponse {
    pub v: u16,
    pub body: LocalTaskResponseBody,
}

impl LocalTaskResponse {
    pub fn new(body: LocalTaskResponseBody) -> Self {
        Self {
            v: LOCAL_TASK_PROTOCOL_VERSION,
            body,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(LocalTaskResponseBody::Error {
            code: code.into(),
            message: message.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum LocalTaskResponseBody {
    Hit {
        outputs: Vec<OutputRef>,
    },
    Miss,
    Ack,
    Done {
        outputs: Vec<OutputRef>,
    },
    Failed {
        exit_code: i32,
        #[serde(with = "serde_bytes")]
        stdout: Vec<u8>,
        #[serde(with = "serde_bytes")]
        stderr: Vec<u8>,
    },
    Chunk {
        hash: Hash32,
        offset: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Error {
        code: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_protocol::{read_message, write_message};
    use std::io::Cursor;

    #[test]
    fn request_and_response_roundtrip() {
        let request = LocalTaskRequest::new(LocalTaskRequestBody::Start {
            task_id: Hash32([1; 32]),
            bundle_hash: Hash32([2; 32]),
            bundle_len: 3,
        });
        let mut bytes = Vec::new();
        write_message(&mut bytes, &request).unwrap();
        let decoded: LocalTaskRequest = read_message(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, request);

        let response = LocalTaskResponse::new(LocalTaskResponseBody::Miss);
        let mut bytes = Vec::new();
        write_message(&mut bytes, &response).unwrap();
        let decoded: LocalTaskResponse = read_message(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, response);
    }
}
