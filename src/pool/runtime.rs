//! Background capacity sampling, provider publishing, and consumer targeting.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use tokio::time::{Duration, interval};

use crate::capacity::{CapacitySnapshot, CapacitySource, calculate_capacity};
use crate::compile_protocol::{AppRequest, AppResponse, PeerId};
use crate::config::{NodeConfig, ServiceConfig, SubscriptionConfig};
use crate::control::{self, ControlHandle};
use crate::log::{LogLevel, LogStruct};
use crate::state::NodeState;
use crate::util::time::now_ms;

pub async fn publish_due(
    control: &ControlHandle,
    state: &Arc<Mutex<NodeState>>,
    snapshot: CapacitySnapshot,
    interval_ms: u64,
) -> usize {
    let due = {
        let Ok(mut state) = state.lock() else {
            return 0;
        };
        state.due_capacity_updates(now_ms(), snapshot, interval_ms)
    };

    let mut sent = 0;
    for (peer, request) in due {
        match control::send(control, &peer, request).await {
            Ok(AppResponse::Ack) => sent += 1,
            Ok(_) | Err(_) => {
                if let Ok(mut state) = state.lock() {
                    state.unsubscribe(&peer);
                }
            }
        }
    }
    sent
}

pub async fn ensure_target(
    control: &ControlHandle,
    state: &Arc<Mutex<NodeState>>,
    node: &NodeConfig,
    subscription: &SubscriptionConfig,
) -> usize {
    if subscription.target_slots == 0 {
        return 0;
    }

    let (provider_id, mut visible_capacity, mut known) = {
        let Ok(state) = state.lock() else {
            return 0;
        };
        let visible = state.max_slots
            + state
                .providers
                .values()
                .map(|view| view.max_slots)
                .sum::<u32>();
        (
            state.provider_id.clone(),
            visible,
            state.providers.keys().cloned().collect::<HashSet<_>>(),
        )
    };

    let mut subscribed = 0;
    for raw_peer in &subscription.static_peers {
        if visible_capacity >= subscription.target_slots {
            break;
        }

        let peer = PeerId::new(raw_peer.clone());
        if peer == provider_id || known.contains(&peer) {
            continue;
        }

        if let Ok(mut state) = state.lock() {
            state.record_pending(peer.clone(), node.toolchain.clone(), node.target.clone());
        }

        let request = AppRequest::Subscribe {
            consumer: provider_id.clone(),
            toolchain: node.toolchain.clone(),
            target: node.target.clone(),
        };

        match control::send(control, &peer, request).await {
            Ok(AppResponse::Subscribed { max_slots, .. }) => {
                if let Ok(mut state) = state.lock() {
                    state.record_subscribed(
                        peer.clone(),
                        node.toolchain.clone(),
                        node.target.clone(),
                        max_slots,
                    );
                }
                visible_capacity = visible_capacity.saturating_add(max_slots);
                known.insert(peer);
                subscribed += 1;
            }
            Ok(_) | Err(_) => {
                if let Ok(mut state) = state.lock() {
                    state.remove_provider(&peer);
                }
            }
        }
    }
    subscribed
}

pub fn expire_providers(state: &Arc<Mutex<NodeState>>, ttl_ms: u64) -> Vec<PeerId> {
    let Ok(mut state) = state.lock() else {
        return Vec::new();
    };
    state.expire_providers(now_ms(), ttl_ms)
}

enum SampleError {
    Source(std::io::Error),
    State,
}

fn sampled_snapshot(
    source: &mut (dyn CapacitySource + Send),
    state: &Arc<Mutex<NodeState>>,
    node: &NodeConfig,
) -> Result<CapacitySnapshot, SampleError> {
    let sample = source.sample().map_err(SampleError::Source)?;
    let (active_slots, queue_len) = {
        let state = state.lock().map_err(|_| SampleError::State)?;
        (state.active_slots(), state.queue_len())
    };
    Ok(calculate_capacity(&sample, active_slots, queue_len, node))
}

fn apply_capacity(state: &Arc<Mutex<NodeState>>, snapshot: CapacitySnapshot) {
    if let Ok(mut state) = state.lock() {
        state.set_capacity(snapshot.max_slots, snapshot.free_slots, snapshot.accepting);
    }
}

async fn publish_capacity_tick(
    control: &ControlHandle,
    state: &Arc<Mutex<NodeState>>,
    subscription: &SubscriptionConfig,
) {
    let snapshot = {
        let Ok(state) = state.lock() else {
            return;
        };
        state.capacity_snapshot()
    };
    let _ = publish_due(
        control,
        state,
        snapshot,
        subscription
            .update_interval_secs
            .max(1)
            .saturating_mul(1_000),
    )
    .await;
}

async fn subscribe_capacity_tick(
    control: &ControlHandle,
    state: &Arc<Mutex<NodeState>>,
    node: &NodeConfig,
    subscription: &SubscriptionConfig,
) {
    let _ = ensure_target(control, state, node, subscription).await;
}

fn expire_capacity_tick(state: &Arc<Mutex<NodeState>>, subscription: &SubscriptionConfig) {
    for peer in expire_providers(state, subscription.ttl_secs.max(1).saturating_mul(1_000)) {
        LogStruct::new(LogLevel::Debug, "订阅过期", peer.to_string()).emit();
    }
}

pub async fn run_pool(
    control: ControlHandle,
    state: Arc<Mutex<NodeState>>,
    config: ServiceConfig,
    mut shutdown: watch::Receiver<bool>,
    mut source: Box<dyn CapacitySource + Send>,
) {
    let node = config.node;
    let subscription = config.subscription;

    // Seed capacity before the first Subscribe/Subscribed exchange so `max_slots`
    // does not start at the placeholder value.
    match sampled_snapshot(source.as_mut(), &state, &node) {
        Ok(snapshot) => apply_capacity(&state, snapshot),
        Err(SampleError::State) => return,
        Err(SampleError::Source(_)) => {}
    }

    let mut capacity_tick = interval(Duration::from_secs(node.sample_interval_secs.max(1)));
    let mut provider_tick = interval(Duration::from_secs(1));
    let mut consumer_tick = interval(Duration::from_secs(
        subscription.target_reevaluate_secs.max(1),
    ));
    let mut ttl_tick = interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = capacity_tick.tick() => {
                match sampled_snapshot(source.as_mut(), &state, &node) {
                    Ok(snapshot) => apply_capacity(&state, snapshot),
                    Err(SampleError::State) => continue,
                    Err(SampleError::Source(e)) => {
                        LogStruct::new(LogLevel::Warning, "容量采样失败", e.to_string()).emit();
                    }
                }
            }
            _ = provider_tick.tick() => {
                publish_capacity_tick(&control, &state, &subscription).await;
            }
            _ = consumer_tick.tick() => {
                subscribe_capacity_tick(&control, &state, &node, &subscription).await;
            }
            _ = ttl_tick.tick() => {
                expire_capacity_tick(&state, &subscription);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    use async_trait::async_trait;

    use crate::capacity::CapacitySnapshot;
    use crate::compile_protocol::{AppRequest, AppResponse, Envelope};
    use crate::control::new_control_handle;
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
    impl crate::control::ControlPlane for MockControl {
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

    fn control_with(control: Arc<MockControl>) -> ControlHandle {
        let handle = new_control_handle();
        *handle.write().unwrap() = Some(control);
        handle
    }

    fn snapshot(free_slots: u32) -> CapacitySnapshot {
        CapacitySnapshot {
            max_slots: 4,
            free_slots,
            queue_len: 0,
            accepting: free_slots > 0,
        }
    }

    #[tokio::test]
    async fn publishes_initial_and_removes_on_failure() {
        let state = Arc::new(Mutex::new(NodeState::placeholder()));
        {
            let mut state = state.lock().unwrap();
            state.subscribe(PeerId::new("consumer"), String::new(), String::new());
        }
        let control = Arc::new(MockControl::new(vec![Ok(AppResponse::Ack)]));
        let handle = control_with(control.clone());
        let sent = publish_due(&handle, &state, snapshot(1), 30_000).await;
        assert_eq!(sent, 1);
        assert_eq!(control.sent().len(), 1);
        assert_eq!(state.lock().unwrap().subscriptions.len(), 1);

        {
            let mut state = state.lock().unwrap();
            state.subscribe(PeerId::new("consumer-2"), String::new(), String::new());
        }
        let control = Arc::new(MockControl::new(vec![Err(SidecarError::new("io", "down"))]));
        let handle = control_with(control);
        let sent = publish_due(&handle, &state, snapshot(1), 30_000).await;
        assert_eq!(sent, 0);
        assert!(
            !state
                .lock()
                .unwrap()
                .subscriptions
                .contains_key(&PeerId::new("consumer-2"))
        );
    }

    #[tokio::test]
    async fn subscribes_until_target_capacity_is_visible() {
        let state = Arc::new(Mutex::new(NodeState::new(
            PeerId::new("self"),
            1,
            "rustc",
            "x86_64",
        )));
        let node = NodeConfig {
            toolchain: "rustc".into(),
            target: "x86_64".into(),
            ..NodeConfig::default()
        };
        let subscription = SubscriptionConfig {
            target_slots: 4,
            static_peers: vec!["a".into(), "b".into(), "c".into()],
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::new(vec![
            Ok(AppResponse::Subscribed {
                provider: PeerId::new("a"),
                toolchain: "rustc".into(),
                target: "x86_64".into(),
                max_slots: 2,
            }),
            Ok(AppResponse::Subscribed {
                provider: PeerId::new("b"),
                toolchain: "rustc".into(),
                target: "x86_64".into(),
                max_slots: 2,
            }),
        ]));
        let handle = control_with(control.clone());

        let subscribed = ensure_target(&handle, &state, &node, &subscription).await;
        assert_eq!(subscribed, 2);
        assert_eq!(control.sent().len(), 2);
        assert_eq!(state.lock().unwrap().providers.len(), 2);
    }

    #[tokio::test]
    async fn failed_subscribe_removes_pending_provider() {
        let state = Arc::new(Mutex::new(NodeState::new(PeerId::new("self"), 0, "", "")));
        let node = NodeConfig::default();
        let subscription = SubscriptionConfig {
            target_slots: 1,
            static_peers: vec!["a".into()],
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::new(vec![Err(SidecarError::new("io", "down"))]));
        let handle = control_with(control);

        assert_eq!(
            ensure_target(&handle, &state, &node, &subscription).await,
            0
        );
        assert!(state.lock().unwrap().providers.is_empty());
    }
}
