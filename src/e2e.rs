//! End-to-end tests that do not require a live NexusNet node.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::compile_protocol::{AppRequest, AppResponse, Envelope, PeerId};
use crate::control::{self, ControlPlane, new_control_handle};
use crate::protocol::{SidecarError, WhoamiInfo};

struct MockControl {
    sent: Mutex<Vec<(PeerId, AppRequest)>>,
    responses: Mutex<VecDeque<Result<AppResponse, SidecarError>>>,
}

impl MockControl {
    fn new(responses: Vec<Result<AppResponse, SidecarError>>) -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        }
    }

    fn sent(&self) -> Vec<(PeerId, AppRequest)> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl ControlPlane for MockControl {
    async fn service_request_to(
        &self,
        _service: &str,
        peer: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        let envelope: Envelope<AppRequest> = ciborium::de::from_reader(payload.as_slice())
            .map_err(|e| SidecarError::new("decode_error", e.to_string()))?;
        self.sent
            .lock()
            .unwrap()
            .push((PeerId::new(peer), envelope.body));
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(AppResponse::Ack))?;
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&Envelope::new(response), &mut bytes)
            .map_err(|e| SidecarError::new("encode_error", e.to_string()))?;
        Ok(bytes)
    }

    async fn whoami(&self) -> Result<WhoamiInfo, SidecarError> {
        Ok(WhoamiInfo {
            peer_id: "mock".into(),
            name: String::new(),
            description: String::new(),
        })
    }
}

#[tokio::test]
async fn control_send_round_trip() {
    let mock = Arc::new(MockControl::new(vec![Ok(AppResponse::Busy {
        retry_after_ms: 25,
    })]));
    let handle = new_control_handle();
    *handle.write().unwrap() = Some(mock.clone());

    let peer = PeerId::new("12D3KooWpeer");
    let response = control::send(
        &handle,
        &peer,
        AppRequest::Status {
            task_id: crate::compile_protocol::TaskId(crate::compile_protocol::Hash32([1; 32])),
        },
    )
    .await
    .unwrap();

    assert_eq!(response, AppResponse::Busy { retry_after_ms: 25 });
    let sent = mock.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, peer);
}

#[tokio::test]
async fn control_send_without_connection_is_not_connected() {
    let handle = new_control_handle();
    let error = control::send(
        &handle,
        &PeerId::new("peer"),
        AppRequest::Status {
            task_id: crate::compile_protocol::TaskId(crate::compile_protocol::Hash32([2; 32])),
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.code, "not_connected");
}
