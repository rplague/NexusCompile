//! Shared helpers for unit tests.

use std::collections::VecDeque;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::compile_protocol::{AppRequest, AppResponse, Envelope, PeerId};
use crate::control::ControlPlane;
use crate::protocol::{SidecarError, WhoamiInfo};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nexuscompile-{tag}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct MockControl {
    sent: Mutex<Vec<(PeerId, AppRequest)>>,
    responses: Mutex<VecDeque<Result<AppResponse, SidecarError>>>,
}

impl MockControl {
    pub fn new(responses: Vec<Result<AppResponse, SidecarError>>) -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            responses: Mutex::new(responses.into()),
        }
    }

    pub fn sent(&self) -> Vec<(PeerId, AppRequest)> {
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

    async fn discover_providers(&self, _service: &str) -> Result<Vec<String>, SidecarError> {
        Ok(Vec::new())
    }
}
