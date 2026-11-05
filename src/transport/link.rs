use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::compile_protocol::{AppRequest, AppResponse, Envelope, PeerId};
use crate::pool::PoolOps;
use crate::sidecar_protocol::SidecarError;

use super::BackendClient;

pub struct SidecarOps {
    client: BackendClient,
}

impl SidecarOps {
    pub fn new(client: BackendClient) -> Self {
        Self { client }
    }
}

/// Shared handle to the current NexusNet sidecar connection.
#[derive(Clone, Default)]
pub struct NodeLink {
    client: Arc<RwLock<Option<BackendClient>>>,
}

impl NodeLink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, client: BackendClient) {
        if let Ok(mut slot) = self.client.write() {
            *slot = Some(client);
        }
    }

    pub fn clear(&self) {
        if let Ok(mut slot) = self.client.write() {
            *slot = None;
        }
    }

    fn get(&self) -> Option<BackendClient> {
        self.client.read().ok().and_then(|slot| slot.clone())
    }
}

#[async_trait]
impl PoolOps for NodeLink {
    async fn send(&self, peer: &PeerId, request: AppRequest) -> Result<AppResponse, SidecarError> {
        let client = self.get().ok_or_else(|| {
            SidecarError::new("not_connected", "NexusNet sidecar is not connected")
        })?;
        SidecarOps::new(client).send(peer, request).await
    }
}

#[async_trait]
impl PoolOps for SidecarOps {
    async fn send(&self, peer: &PeerId, request: AppRequest) -> Result<AppResponse, SidecarError> {
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&Envelope::new(request), &mut payload)
            .map_err(|e| SidecarError::new("encode_error", e.to_string()))?;
        let response = self
            .client
            .service_request_to("rustc-pool", peer.as_str(), payload)
            .await?;
        let envelope: Envelope<AppResponse> = ciborium::de::from_reader(response.as_slice())
            .map_err(|e| SidecarError::new("decode_error", e.to_string()))?;
        Ok(envelope.body)
    }
}
