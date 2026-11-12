//! Shared application state.

use std::sync::Arc;

use crate::compile::cache::ResultCache;
use crate::compile_protocol::PeerId;
use crate::config::{ConfigHandle, ServiceConfig};
use crate::control::{self, ControlHandle, ControlPlane};
use crate::pool::{Consumer, Provider};
use crate::workspace::Workspace;

/// Shared state handed to the connection, pool runtime, and local compiler.
#[derive(Clone)]
pub struct AppState {
    pub provider: Arc<Provider>,
    pub consumer: Arc<Consumer>,
    pub cache: Arc<ResultCache>,
    pub workspace: Arc<Workspace>,
    pub config: ConfigHandle,
    pub control: ControlHandle,
}

impl AppState {
    pub fn new(
        provider: Arc<Provider>,
        consumer: Arc<Consumer>,
        cache: Arc<ResultCache>,
        workspace: Arc<Workspace>,
        config: ConfigHandle,
        control: ControlHandle,
    ) -> Self {
        Self {
            provider,
            consumer,
            cache,
            workspace,
            config,
            control,
        }
    }

    pub fn config_snapshot(&self) -> ServiceConfig {
        self.config.snapshot()
    }

    pub fn set_control(&self, control: Option<Arc<dyn ControlPlane>>) {
        control::set(&self.control, control);
    }

    pub fn current_control(&self) -> Option<Arc<dyn ControlPlane>> {
        control::current(&self.control)
    }

    pub fn set_provider_id(&self, peer_id: PeerId) {
        self.provider.set_provider_id(peer_id);
    }
}
