//! Typed control-result payloads and CBOR decoding helpers.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::sidecar_protocol::SidecarError;

/// Decode `reply.result` CBOR bytes.
pub fn decode_result<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SidecarError> {
    ciborium::de::from_reader(bytes).map_err(|e| SidecarError::new("decode_error", e.to_string()))
}

/// `query_public_ip`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicIpInfo {
    #[serde(default)]
    pub ipv4: Option<String>,
    #[serde(default)]
    pub ipv6: Option<String>,
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
