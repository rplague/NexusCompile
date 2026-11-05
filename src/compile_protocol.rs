//! Application protocol carried inside NexusNet sidecar service requests.

use serde::de::{Deserializer, Error as DeError};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Application protocol version.
pub const APP_PROTOCOL_VERSION: u16 = 2;

/// A NexusNet peer identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A 32-byte content/action hash. Encoded as CBOR bstr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash32(pub [u8; 32]);

impl Default for Hash32 {
    fn default() -> Self {
        Self([0; 32])
    }
}

impl Serialize for Hash32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?.into_vec();
        let hash: [u8; 32] = bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| D::Error::invalid_length(bytes.len(), &"32-byte hash"))?;
        Ok(Self(hash))
    }
}

/// Task identity: the action manifest hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub Hash32);

/// Reservation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseId(pub Uuid);

impl LeaseId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

/// One content-addressed output blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: Hash32,
    pub size: u64,
}

/// One output file in a remote task result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRef {
    pub path: String,
    pub hash: Hash32,
    pub size: u64,
    pub mode: u32,
}

/// Application-level error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    UnsupportedVersion,
    NotFound,
    Busy,
    Rejected,
    NotImplemented,
    InvalidState,
    Compile,
    Internal,
}

/// Task state returned by Status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Done {
        outputs: Vec<OutputRef>,
    },
    Failed {
        code: ErrorCode,
        message: String,
        #[serde(default, with = "serde_bytes")]
        stdout: Vec<u8>,
        #[serde(default, with = "serde_bytes")]
        stderr: Vec<u8>,
    },
    Cancelled,
}

/// Envelope shared by all application messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub v: u16,
    pub body: T,
}

impl<T> Envelope<T> {
    pub fn new(body: T) -> Self {
        Self {
            v: APP_PROTOCOL_VERSION,
            body,
        }
    }
}

/// Requests sent to a node's `rustc-pool` service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum AppRequest {
    Subscribe {
        consumer: PeerId,
        toolchain: String,
        target: String,
    },
    Unsubscribe {
        consumer: PeerId,
    },
    CapacityUpdate {
        provider: PeerId,
        seq: u64,
        free_slots: u32,
        queue_len: u32,
        accepting: bool,
    },
    Reserve {
        consumer: PeerId,
        task_id: TaskId,
        slots: u32,
        input_manifest: Hash32,
    },
    BlobPut {
        task_id: TaskId,
        hash: Hash32,
        offset: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    BlobGet {
        hash: Hash32,
        offset: u64,
        len: u32,
    },
    TaskReady {
        task_id: TaskId,
        lease_id: LeaseId,
    },
    Status {
        task_id: TaskId,
    },
    Cancel {
        task_id: TaskId,
    },
    ResultAck {
        lease_id: LeaseId,
    },
}

/// Replies returned by a node's `rustc-pool` service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum AppResponse {
    Subscribed {
        provider: PeerId,
        toolchain: String,
        target: String,
        max_slots: u32,
    },
    Ack,
    Cached {
        task_id: TaskId,
        outputs: Vec<OutputRef>,
        lease_id: LeaseId,
    },
    Granted {
        task_id: TaskId,
        lease_id: LeaseId,
    },
    Busy {
        retry_after_ms: u64,
    },
    Rejected {
        code: ErrorCode,
        message: String,
    },
    Chunk {
        hash: Hash32,
        offset: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    TaskStatus {
        state: TaskState,
    },
    Error {
        code: ErrorCode,
        message: String,
    },
}

impl AppResponse {
    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error {
            code,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(request: AppRequest) {
        let envelope = Envelope::new(request);
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&envelope, &mut bytes).unwrap();
        let decoded: Envelope<AppRequest> = ciborium::de::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded, envelope);
    }

    fn roundtrip_response(response: AppResponse) {
        let envelope = Envelope::new(response);
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&envelope, &mut bytes).unwrap();
        let decoded: Envelope<AppResponse> = ciborium::de::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn requests_roundtrip() {
        let consumer = PeerId::new("12D3KooWtest");
        let task_id = TaskId(Hash32([7; 32]));
        let lease_id = LeaseId::new();

        roundtrip_request(AppRequest::Subscribe {
            consumer: consumer.clone(),
            toolchain: "rustc-1".into(),
            target: "x86_64-unknown-linux-gnu".into(),
        });
        roundtrip_request(AppRequest::Unsubscribe {
            consumer: consumer.clone(),
        });
        roundtrip_request(AppRequest::CapacityUpdate {
            provider: consumer.clone(),
            seq: 1,
            free_slots: 3,
            queue_len: 0,
            accepting: true,
        });
        roundtrip_request(AppRequest::Reserve {
            consumer: consumer.clone(),
            task_id,
            slots: 1,
            input_manifest: Hash32([8; 32]),
        });
        roundtrip_request(AppRequest::BlobPut {
            task_id,
            hash: Hash32([9; 32]),
            offset: 0,
            data: vec![1, 2, 3],
        });
        roundtrip_request(AppRequest::BlobGet {
            hash: Hash32([9; 32]),
            offset: 0,
            len: 3,
        });
        roundtrip_request(AppRequest::TaskReady { task_id, lease_id });
        roundtrip_request(AppRequest::Status { task_id });
        roundtrip_request(AppRequest::Cancel { task_id });
        roundtrip_request(AppRequest::ResultAck { lease_id });
    }

    #[test]
    fn responses_roundtrip() {
        let provider = PeerId::new("12D3KooWprovider");
        let task_id = TaskId(Hash32([7; 32]));
        let lease_id = LeaseId::new();

        roundtrip_response(AppResponse::Subscribed {
            provider,
            toolchain: "rustc-1".into(),
            target: "x86_64-unknown-linux-gnu".into(),
            max_slots: 4,
        });
        roundtrip_response(AppResponse::Ack);
        roundtrip_response(AppResponse::Cached {
            task_id,
            outputs: vec![OutputRef {
                path: "libdemo.rlib".into(),
                hash: Hash32([2; 32]),
                size: 3,
                mode: 0o644,
            }],
            lease_id,
        });
        roundtrip_response(AppResponse::Granted { task_id, lease_id });
        roundtrip_response(AppResponse::Busy { retry_after_ms: 6 });
        roundtrip_response(AppResponse::Rejected {
            code: ErrorCode::Rejected,
            message: "no".into(),
        });
        roundtrip_response(AppResponse::Chunk {
            hash: Hash32([2; 32]),
            offset: 0,
            data: vec![1, 2, 3],
        });
        roundtrip_response(AppResponse::TaskStatus {
            state: TaskState::Queued,
        });
        roundtrip_response(AppResponse::Error {
            code: ErrorCode::NotFound,
            message: "missing".into(),
        });
    }

    #[test]
    fn hash32_uses_cbor_bytes() {
        let hash = Hash32([0xAB; 32]);
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&hash, &mut bytes).unwrap();
        // CBOR bstr(32) header is 0x58 0x20.
        assert_eq!(&bytes[..2], &[0x58, 0x20]);
        let decoded: Hash32 = ciborium::de::from_reader(bytes.as_slice()).unwrap();
        assert_eq!(decoded, hash);
    }
}
