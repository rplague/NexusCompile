use async_trait::async_trait;

use crate::compile_protocol::{AppRequest, AppResponse, PeerId};
use crate::sidecar_protocol::SidecarError;

#[async_trait]
pub trait PoolOps: Send + Sync {
    async fn send(&self, peer: &PeerId, request: AppRequest) -> Result<AppResponse, SidecarError>;
}
