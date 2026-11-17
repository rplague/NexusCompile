//! Node control plane abstraction.
//!
//! Business logic depends on [`ControlPlane`] instead of the concrete
//! [`crate::connection::BackendClient`], so tests can inject a mock and the
//! connection layer stays replaceable.

use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use crate::compile_protocol::{AppRequest, AppResponse, Envelope, PeerId};
use crate::protocol::{SidecarError, WhoamiInfo};

/// Domain-level operations this backend needs from the local NexusNet node.
#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Send one `rustc-pool` application request to a specific peer.
    async fn service_request_to(
        &self,
        service: &str,
        peer: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError>;

    /// Query this node's identity.
    async fn whoami(&self) -> Result<WhoamiInfo, SidecarError>;

    /// Discover providers for a service via NexusNet.
    async fn discover_providers(&self, service: &str) -> Result<Vec<String>, SidecarError>;
}

#[async_trait]
impl ControlPlane for crate::connection::BackendClient {
    async fn service_request_to(
        &self,
        service: &str,
        peer: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        crate::connection::BackendClient::service_request_to(self, service, peer, payload).await
    }

    async fn whoami(&self) -> Result<WhoamiInfo, SidecarError> {
        crate::connection::BackendClient::whoami(self).await
    }

    async fn discover_providers(&self, service: &str) -> Result<Vec<String>, SidecarError> {
        crate::connection::BackendClient::discover_providers(self, service).await
    }
}

/// Shared handle to the current node connection.
pub type ControlHandle = Arc<RwLock<Option<Arc<dyn ControlPlane>>>>;

pub fn new_control_handle() -> ControlHandle {
    Arc::new(RwLock::new(None))
}

pub fn current(handle: &ControlHandle) -> Option<Arc<dyn ControlPlane>> {
    handle.read().ok().and_then(|guard| guard.clone())
}

pub fn set(handle: &ControlHandle, control: Option<Arc<dyn ControlPlane>>) {
    if let Ok(mut guard) = handle.write() {
        *guard = control;
    }
}

/// Clear the handle only if it still points at `expected`.
pub fn clear_if(handle: &ControlHandle, expected: &Arc<dyn ControlPlane>) {
    if let Ok(mut guard) = handle.write()
        && guard
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
    {
        *guard = None;
    }
}

/// Discover providers for a service through the current node connection.
pub async fn discover(handle: &ControlHandle, service: &str) -> Result<Vec<String>, SidecarError> {
    let control = current(handle)
        .ok_or_else(|| SidecarError::new("not_connected", "NexusNet sidecar is not connected"))?;
    control.discover_providers(service).await
}

/// Send one `rustc-pool` request through the current node connection.
pub async fn send(
    handle: &ControlHandle,
    service_name: &str,
    peer: &PeerId,
    request: AppRequest,
) -> Result<AppResponse, SidecarError> {
    let control = current(handle)
        .ok_or_else(|| SidecarError::new("not_connected", "NexusNet sidecar is not connected"))?;

    let mut payload = Vec::new();
    ciborium::ser::into_writer(&Envelope::new(request), &mut payload)
        .map_err(|e| SidecarError::new("encode_error", e.to_string()))?;

    let response = control
        .service_request_to(service_name, peer.as_str(), payload)
        .await?;

    let envelope: Envelope<AppResponse> = ciborium::de::from_reader(response.as_slice())
        .map_err(|e| SidecarError::new("decode_error", e.to_string()))?;
    Ok(envelope.body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile_protocol::{AppRequest, AppResponse, Hash32, TaskId};
    use crate::test_support::MockControl;

    #[tokio::test]
    async fn send_and_connection_lifecycle() {
        let mock = Arc::new(MockControl::new(vec![Ok(AppResponse::Busy {
            retry_after_ms: 25,
        })]));
        let handle = new_control_handle();
        *handle.write().unwrap() = Some(mock.clone());

        let peer = PeerId::new("12D3KooWpeer");
        let response = send(
            &handle,
            "rustc-pool",
            &peer,
            AppRequest::Status {
                task_id: TaskId(Hash32([1; 32])),
            },
        )
        .await
        .unwrap();
        assert_eq!(response, AppResponse::Busy { retry_after_ms: 25 });
        let sent = mock.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, peer);

        let empty = new_control_handle();
        let error = send(
            &empty,
            "rustc-pool",
            &PeerId::new("peer"),
            AppRequest::Status {
                task_id: TaskId(Hash32([2; 32])),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "not_connected", "case: no connection");

        let first: Arc<dyn ControlPlane> = Arc::new(MockControl::new(vec![]));
        let second: Arc<dyn ControlPlane> = Arc::new(MockControl::new(vec![]));
        set(&handle, Some(first.clone()));
        set(&handle, Some(second.clone()));
        clear_if(&handle, &first);
        assert!(current(&handle).is_some(), "case: old disconnect");

        clear_if(&handle, &second);
        assert!(current(&handle).is_none(), "case: current disconnect");
    }
}
