use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;
use uuid::Uuid;

use crate::control::{
    AddKeyResult, PqStatusResult, PublicIpInfo, QueryKeyResult, RelayStatusResult, SuccessResult,
    decode_result,
};
use crate::sidecar_protocol::{Message, SidecarError, write_frame};

/// Pending table for one backend: id -> response waiter.
type PendingMap = Arc<Mutex<HashMap<Uuid, oneshot::Sender<Result<Vec<u8>, SidecarError>>>>>;

struct Shared {
    writer: Mutex<OwnedWriteHalf>,
    pending: PendingMap,
}

/// Control handle for the backend connection back to the node; cloneable across tasks.
#[derive(Clone)]
pub struct BackendClient {
    shared: Arc<Shared>,
    timeout: Duration,
}

impl BackendClient {
    /// Send a control command and wait for the node's `reply`.
    async fn call(&self, id: Uuid, msg: Message) -> Result<Vec<u8>, SidecarError> {
        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().await.insert(id, tx);

        if let Err(e) = write_msg(&self.shared.writer, &msg).await {
            self.shared.pending.lock().await.remove(&id);
            return Err(SidecarError::new("io_error", e));
        }

        match timeout(self.timeout, rx).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(SidecarError::new("node_gone", "response channel closed")),
            Err(_) => {
                self.shared.pending.lock().await.remove(&id);
                Err(SidecarError::new("timeout", "control command timed out"))
            }
        }
    }

    /// Send a control command and decode `reply.result` into `T`.
    async fn call_result<T: DeserializeOwned>(
        &self,
        id: Uuid,
        msg: Message,
    ) -> Result<T, SidecarError> {
        let bytes = self.call(id, msg).await?;
        decode_result(&bytes)
    }

    /// List global service types.
    pub async fn list_services(&self) -> Result<Vec<String>, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ListServices { id }).await
    }

    /// Query providers for a service.
    pub async fn discover_providers(&self, service: &str) -> Result<Vec<String>, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::DiscoverProviders {
                id,
                service: service.to_string(),
            },
        )
        .await
    }

    /// Query the node's public address.
    pub async fn query_public_ip(&self) -> Result<PublicIpInfo, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::QueryPublicIp { id }).await
    }

    /// Reconnect to bootstrap.
    pub async fn reconnect_bootstrap(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReconnectBootstrap { id })
            .await
    }

    /// Re-announce local services.
    pub async fn reannounce_services(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReannounceServices { id })
            .await
    }

    /// Reload node configuration.
    pub async fn reload_config(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReloadConfig { id }).await
    }

    /// Relay status.
    pub async fn relay_status(&self) -> Result<RelayStatusResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::RelayStatus { id }).await
    }

    /// Post-quantum status.
    pub async fn pq_status(&self) -> Result<PqStatusResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::PqStatus { id }).await
    }

    /// Authentication status (the structure may grow; returns the raw JSON value).
    pub async fn auth_status(&self) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::AuthStatus { id }).await
    }

    /// Query this node's identity (PeerId, name, service status, etc.).
    pub async fn whoami(&self) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::Whoami { id }).await
    }

    /// Clear application caches and non-self local Kademlia replicas.
    pub async fn refresh_cache(&self) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::RefreshCache { id }).await
    }

    /// Stop announcing as a provider and optionally remove the local record.
    pub async fn remove_key(
        &self,
        key: &str,
        providing: bool,
        record: bool,
    ) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::RemoveKey {
                id,
                key: key.to_string(),
                providing,
                record,
            },
        )
        .await
    }

    /// Read a DHT record.
    pub async fn query_key(&self, key: &str) -> Result<QueryKeyResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::QueryKey {
                id,
                key: key.to_string(),
            },
        )
        .await
    }

    /// Write a DHT record / announce providing.
    pub async fn add_key(
        &self,
        key: &str,
        value: Option<Vec<u8>>,
        providing: bool,
    ) -> Result<AddKeyResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::AddKey {
                id,
                key: key.to_string(),
                value,
                providing,
            },
        )
        .await
    }

    /// Start a P2P service request (the node selects the best provider).
    pub async fn service_request(
        &self,
        service: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        let id = Uuid::new_v4();
        self.call(
            id,
            Message::ServiceRequest {
                id,
                service: service.to_string(),
                payload,
            },
        )
        .await
    }

    /// Start a P2P service request to a specific peer.
    pub async fn service_request_to(
        &self,
        service: &str,
        peer: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        let id = Uuid::new_v4();
        self.call(
            id,
            Message::ServiceRequestTo {
                id,
                service: service.to_string(),
                peer: peer.to_string(),
                payload,
            },
        )
        .await
    }
}

pub(crate) type PendingSender = oneshot::Sender<Result<Vec<u8>, SidecarError>>;

impl BackendClient {
    pub(crate) fn new(writer: OwnedWriteHalf, timeout: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                writer: Mutex::new(writer),
                pending: Arc::new(Mutex::new(HashMap::new())),
            }),
            timeout,
        }
    }

    pub(crate) async fn write(&self, msg: &Message) -> Result<(), String> {
        write_msg(&self.shared.writer, msg).await
    }

    pub(crate) async fn take_pending(&self, id: Uuid) -> Option<PendingSender> {
        self.shared.pending.lock().await.remove(&id)
    }
}

async fn write_msg(writer: &Mutex<OwnedWriteHalf>, msg: &Message) -> Result<(), String> {
    let mut w = writer.lock().await;
    write_frame(&mut *w, msg).await.map_err(|e| e.to_string())
}
