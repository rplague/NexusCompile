use std::sync::{Arc, Mutex};

use crate::compile::cache::ResultCache;
use crate::config::{ConfigHandle, ServiceConfig};
use crate::state::NodeState;
use crate::transport::NodeLink;

#[derive(Clone)]
pub struct AppContext {
    pub state: Arc<Mutex<NodeState>>,
    pub cache: Arc<ResultCache>,
    pub config: ConfigHandle,
    pub link: Arc<NodeLink>,
}

impl AppContext {
    pub fn new(
        state: Arc<Mutex<NodeState>>,
        cache: Arc<ResultCache>,
        config: ConfigHandle,
        link: Arc<NodeLink>,
    ) -> Self {
        Self {
            state,
            cache,
            config,
            link,
        }
    }

    pub fn config_snapshot(&self) -> ServiceConfig {
        self.config.snapshot()
    }
}
