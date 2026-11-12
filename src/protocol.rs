//! Node <-> sidecar protocol v2 contract.
//!
//! Wire format: `u32_be(len) || cbor(message)`, `len <= MAX_FRAME`.
//!
//! This module mirrors the NexusNet contract, including control operations
//! that this backend does not currently drive. Keep it in sync with
//! `NexusNet/src/sidecar_protocol.rs` and `nexusxray/src/protocol.rs`.

#![allow(dead_code)]

use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

/// Protocol version.
pub const PROTOCOL_VERSION: u32 = 2;
/// Maximum frame size (including the CBOR payload).
pub const MAX_FRAME: usize = 16 << 20;

/// Structured error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarError {
    pub code: String,
    pub message: String,
}

impl SidecarError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SidecarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for SidecarError {}

/// Serde default for optional boolean fields that default to `true`.
fn default_true() -> bool {
    true
}

/// Protocol messages are discriminated by a text field `t`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Message {
    /// Handshake (first frame in both directions).
    Hello {
        version: u32,
    },
    /// Node -> backend: forward an inbound service request.
    Request {
        id: Uuid,
        service: String,
        payload: Vec<u8>,
    },
    /// Correlated reply (service response / control result, both directions).
    Reply {
        id: Uuid,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Vec<u8>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<SidecarError>,
    },
    ListServices {
        id: Uuid,
    },
    DiscoverProviders {
        id: Uuid,
        service: String,
    },
    QueryPublicIp {
        id: Uuid,
    },
    Whoami {
        id: Uuid,
    },
    ReconnectBootstrap {
        id: Uuid,
    },
    ReannounceServices {
        id: Uuid,
    },
    ReloadConfig {
        id: Uuid,
    },
    /// Clear application caches and non-self local Kademlia replicas.
    RefreshCache {
        id: Uuid,
    },
    RelayStatus {
        id: Uuid,
    },
    PqStatus {
        id: Uuid,
    },
    AuthStatus {
        id: Uuid,
    },
    QueryKey {
        id: Uuid,
        key: String,
    },
    AddKey {
        id: Uuid,
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<Vec<u8>>,
        #[serde(default)]
        providing: bool,
    },
    /// Stop providing and/or remove the locally stored record for `key`.
    RemoveKey {
        id: Uuid,
        key: String,
        #[serde(default = "default_true")]
        providing: bool,
        #[serde(default)]
        record: bool,
    },
    ServiceRequest {
        id: Uuid,
        service: String,
        payload: Vec<u8>,
    },
    ServiceRequestTo {
        id: Uuid,
        service: String,
        peer: String,
        payload: Vec<u8>,
    },
}

impl Message {
    /// Get the correlation id; `hello` has none.
    pub fn id(&self) -> Option<Uuid> {
        match self {
            Message::Hello { .. } => None,
            Message::Request { id, .. }
            | Message::Reply { id, .. }
            | Message::ListServices { id }
            | Message::DiscoverProviders { id, .. }
            | Message::QueryPublicIp { id }
            | Message::Whoami { id }
            | Message::ReconnectBootstrap { id }
            | Message::ReannounceServices { id }
            | Message::ReloadConfig { id }
            | Message::RefreshCache { id }
            | Message::RelayStatus { id }
            | Message::PqStatus { id }
            | Message::AuthStatus { id }
            | Message::QueryKey { id, .. }
            | Message::AddKey { id, .. }
            | Message::RemoveKey { id, .. }
            | Message::ServiceRequest { id, .. }
            | Message::ServiceRequestTo { id, .. } => Some(*id),
        }
    }

    /// Human-readable discriminant for logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Message::Hello { .. } => "hello",
            Message::Request { .. } => "request",
            Message::Reply { .. } => "reply",
            Message::ListServices { .. } => "list_services",
            Message::DiscoverProviders { .. } => "discover_providers",
            Message::QueryPublicIp { .. } => "query_public_ip",
            Message::Whoami { .. } => "whoami",
            Message::ReconnectBootstrap { .. } => "reconnect_bootstrap",
            Message::ReannounceServices { .. } => "reannounce_services",
            Message::ReloadConfig { .. } => "reload_config",
            Message::RefreshCache { .. } => "refresh_cache",
            Message::RelayStatus { .. } => "relay_status",
            Message::PqStatus { .. } => "pq_status",
            Message::AuthStatus { .. } => "auth_status",
            Message::QueryKey { .. } => "query_key",
            Message::AddKey { .. } => "add_key",
            Message::RemoveKey { .. } => "remove_key",
            Message::ServiceRequest { .. } => "service_request",
            Message::ServiceRequestTo { .. } => "service_request_to",
        }
    }
}

/// `query_public_ip`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicIpInfo {
    #[serde(default)]
    pub ipv4: Option<String>,
    #[serde(default)]
    pub ipv6: Option<String>,
}

/// `whoami`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoamiInfo {
    pub peer_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
}

/// `query_key`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryKeyResult {
    pub key: String,
    #[serde(default)]
    pub value: Option<Vec<u8>>,
    #[serde(default)]
    pub providers: Vec<String>,
}

/// `relay_status`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayStatusResult {
    pub need_relay: bool,
    pub target: u64,
    #[serde(default)]
    pub active: Vec<String>,
    #[serde(default)]
    pub pending: Vec<String>,
}

/// `pq_status`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PqStatusResult {
    pub enabled: bool,
    pub transport: bool,
    pub identity: bool,
    pub required: bool,
}

/// `reconnect_bootstrap` / `reannounce_services` / `reload_config`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessResult {
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
}

/// `add_key`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddKeyResult {
    pub success: bool,
    pub key: String,
}

/// Decode `reply.result` CBOR bytes.
pub fn decode_result<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SidecarError> {
    ciborium::de::from_reader(bytes).map_err(|e| SidecarError::new("decode_error", e.to_string()))
}

/// Frame encode/decode error.
#[derive(Debug)]
pub enum FrameError {
    Io(std::io::Error),
    TooLarge(usize),
    Encode(String),
    Decode(String),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "io error: {e}"),
            FrameError::TooLarge(n) => write!(f, "frame too large: {n} bytes"),
            FrameError::Encode(e) => write!(f, "cbor encode error: {e}"),
            FrameError::Decode(e) => write!(f, "cbor decode error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Encode a message into a full frame (length prefix + CBOR).
pub fn encode_frame(msg: &Message) -> Result<Vec<u8>, FrameError> {
    let mut body = Vec::new();
    ciborium::ser::into_writer(msg, &mut body).map_err(|e| FrameError::Encode(e.to_string()))?;
    if body.len() > MAX_FRAME {
        return Err(FrameError::TooLarge(body.len()));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decode a frame body (without the length prefix).
pub fn decode_frame(body: &[u8]) -> Result<Message, FrameError> {
    if body.len() > MAX_FRAME {
        return Err(FrameError::TooLarge(body.len()));
    }
    ciborium::de::from_reader(body).map_err(|e| FrameError::Decode(e.to_string()))
}

/// Read one length-prefixed CBOR frame.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Message, FrameError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(FrameError::TooLarge(len));
    }
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).await?;
    decode_frame(&body)
}

/// Write one length-prefixed CBOR frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &Message,
) -> Result<(), FrameError> {
    let frame = encode_frame(msg)?;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: Message) {
        let frame = encode_frame(&msg).unwrap();
        let body = &frame[4..];
        assert_eq!(frame.len(), 4 + body.len());
        assert_eq!(
            u32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize,
            body.len()
        );
        assert_eq!(decode_frame(body).unwrap(), msg);
    }

    #[test]
    fn request_reply_round_trip() {
        let id = Uuid::new_v4();
        round_trip(Message::Request {
            id,
            service: "rustc-pool".into(),
            payload: vec![0, 1, 2, 255],
        });
        round_trip(Message::Reply {
            id,
            ok: false,
            result: None,
            error: Some(SidecarError::new("timeout", "too slow")),
        });
    }

    #[test]
    fn control_ops_round_trip() {
        let id = Uuid::new_v4();
        round_trip(Message::Hello {
            version: PROTOCOL_VERSION,
        });
        round_trip(Message::ListServices { id });
        round_trip(Message::Whoami { id });
        round_trip(Message::DiscoverProviders {
            id,
            service: "rustc-pool".into(),
        });
        round_trip(Message::QueryPublicIp { id });
        round_trip(Message::ReconnectBootstrap { id });
        round_trip(Message::ReannounceServices { id });
        round_trip(Message::ReloadConfig { id });
        round_trip(Message::RelayStatus { id });
        round_trip(Message::PqStatus { id });
        round_trip(Message::AuthStatus { id });
        round_trip(Message::QueryKey {
            id,
            key: "/oahd/service/rustc-pool".into(),
        });
        round_trip(Message::AddKey {
            id,
            key: "/k".into(),
            value: Some(vec![9, 8, 7]),
            providing: true,
        });
        round_trip(Message::RemoveKey {
            id,
            key: "/oahd/service/rustc-pool".into(),
            providing: true,
            record: false,
        });
        round_trip(Message::ServiceRequest {
            id,
            service: "rustc-pool".into(),
            payload: vec![],
        });
        round_trip(Message::ServiceRequestTo {
            id,
            service: "rustc-pool".into(),
            peer: "12D3KooW".into(),
            payload: vec![1, 2, 3],
        });
    }

    #[test]
    fn oversized_frame_rejected() {
        assert!(matches!(
            decode_frame(&vec![0u8; MAX_FRAME + 1]),
            Err(FrameError::TooLarge(_))
        ));
    }

    #[test]
    fn malformed_body_rejected() {
        assert!(matches!(decode_frame(&[0xff]), Err(FrameError::Decode(_))));
    }

    #[test]
    fn result_payloads_round_trip() {
        fn enc<T: Serialize>(v: &T) -> Vec<u8> {
            let mut buf = Vec::new();
            ciborium::ser::into_writer(v, &mut buf).unwrap();
            buf
        }

        let ip = PublicIpInfo {
            ipv4: Some("1.2.3.4".into()),
            ipv6: None,
        };
        let decoded: PublicIpInfo = decode_result(&enc(&ip)).unwrap();
        assert_eq!(decoded, ip);

        let who = WhoamiInfo {
            peer_id: "12D3KooW".into(),
            name: "n".into(),
            description: "d".into(),
        };
        let decoded: WhoamiInfo = decode_result(&enc(&who)).unwrap();
        assert_eq!(decoded, who);
    }
}
