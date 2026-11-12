//! Background capacity sampling, provider publishing, and consumer targeting.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::watch;
use tokio::time::{Duration, interval};

use super::{Consumer, Provider};
use crate::capacity::{CapacitySnapshot, CapacitySource, calculate_capacity};
use crate::compile::cache::ResultCache;
use crate::compile_protocol::{AppRequest, AppResponse, PeerId};
use crate::config::{NodeConfig, ServiceConfig, SubscriptionConfig};
use crate::control::{self, ControlHandle};
use crate::log::{LogLevel, LogStruct};
use crate::util::time::now_ms;

pub async fn publish_due(
    control: &ControlHandle,
    provider: &Arc<Provider>,
    interval_ms: u64,
) -> usize {
    let snapshot = provider.capacity_snapshot();
    let due = provider.due_capacity_updates(now_ms(), snapshot, interval_ms);

    let mut sent = 0;
    for (peer, request) in due {
        match control::send(control, &peer, request).await {
            Ok(AppResponse::Ack) => sent += 1,
            Ok(_) | Err(_) => {
                provider.unsubscribe(&peer);
            }
        }
    }
    sent
}

pub async fn ensure_target(
    control: &ControlHandle,
    provider: &Arc<Provider>,
    consumer: &Arc<Consumer>,
    node: &NodeConfig,
    subscription: &SubscriptionConfig,
) -> usize {
    if subscription.target_slots == 0 {
        return 0;
    }

    let provider_id = provider.provider_id();
    let mut visible_capacity = provider.max_slots() + consumer.visible_max_slots();
    let mut known: HashSet<PeerId> = consumer
        .providers()
        .into_iter()
        .map(|view| view.provider)
        .collect();

    let mut subscribed = 0;
    for raw_peer in &subscription.static_peers {
        if visible_capacity >= subscription.target_slots {
            break;
        }

        let peer = PeerId::new(raw_peer.clone());
        if peer == provider_id || known.contains(&peer) {
            continue;
        }

        consumer.record_pending(peer.clone(), node.toolchain.clone(), node.target.clone());

        let request = AppRequest::Subscribe {
            consumer: provider_id.clone(),
            toolchain: node.toolchain.clone(),
            target: node.target.clone(),
        };

        match control::send(control, &peer, request).await {
            Ok(AppResponse::Subscribed {
                provider: _,
                toolchain,
                target,
                max_slots,
            }) => {
                consumer.record_subscribed(peer.clone(), toolchain, target, max_slots);
                visible_capacity = visible_capacity.saturating_add(max_slots);
                known.insert(peer);
                subscribed += 1;
            }
            Ok(_) | Err(_) => consumer.remove_provider(&peer),
        }
    }
    subscribed
}

pub fn expire_providers(consumer: &Arc<Consumer>, ttl_ms: u64) -> Vec<PeerId> {
    consumer.expire_providers(now_ms(), ttl_ms)
}

enum SampleError {
    Source(std::io::Error),
}

fn sampled_snapshot(
    source: &mut (dyn CapacitySource + Send),
    provider: &Arc<Provider>,
    node: &NodeConfig,
) -> Result<CapacitySnapshot, SampleError> {
    let sample = source.sample().map_err(SampleError::Source)?;
    Ok(calculate_capacity(&sample, provider.queue_len(), node))
}

fn apply_capacity(provider: &Arc<Provider>, snapshot: CapacitySnapshot) {
    provider.set_capacity(snapshot.max_slots, snapshot.free_slots);
}

async fn publish_capacity_tick(
    control: &ControlHandle,
    provider: &Arc<Provider>,
    subscription: &SubscriptionConfig,
) {
    let _ = publish_due(
        control,
        provider,
        subscription
            .update_interval_secs
            .max(1)
            .saturating_mul(1_000),
    )
    .await;
}

async fn subscribe_capacity_tick(
    control: &ControlHandle,
    provider: &Arc<Provider>,
    consumer: &Arc<Consumer>,
    node: &NodeConfig,
    subscription: &SubscriptionConfig,
) {
    let _ = ensure_target(control, provider, consumer, node, subscription).await;
}

fn expire_capacity_tick(consumer: &Arc<Consumer>, subscription: &SubscriptionConfig) {
    for peer in expire_providers(consumer, subscription.ttl_secs.max(1).saturating_mul(1_000)) {
        LogStruct::new(LogLevel::Debug, "订阅过期", peer.to_string()).emit();
    }
}

fn expire_leases(provider: &Arc<Provider>, cache: &Arc<ResultCache>) {
    for (task_id, count) in provider.expire_leases(now_ms()) {
        for _ in 0..count {
            cache.release(&task_id.0);
        }
    }
}

pub async fn run_pool(
    control: ControlHandle,
    provider: Arc<Provider>,
    consumer: Arc<Consumer>,
    cache: Arc<ResultCache>,
    config: ServiceConfig,
    mut shutdown: watch::Receiver<bool>,
    mut source: Box<dyn CapacitySource + Send>,
) {
    let node = config.node;
    let subscription = config.subscription;

    // Seed capacity before the first Subscribe/Subscribed exchange so `max_slots`
    // does not start at the placeholder value.
    match sampled_snapshot(source.as_mut(), &provider, &node) {
        Ok(snapshot) => apply_capacity(&provider, snapshot),
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
                match sampled_snapshot(source.as_mut(), &provider, &node) {
                    Ok(snapshot) => apply_capacity(&provider, snapshot),
                    Err(SampleError::Source(e)) => {
                        LogStruct::new(LogLevel::Warning, "容量采样失败", e.to_string()).emit();
                    }
                }
            }
            _ = provider_tick.tick() => {
                publish_capacity_tick(&control, &provider, &subscription).await;
            }
            _ = consumer_tick.tick() => {
                subscribe_capacity_tick(&control, &provider, &consumer, &node, &subscription).await;
            }
            _ = ttl_tick.tick() => {
                expire_capacity_tick(&consumer, &subscription);
                expire_leases(&provider, &cache);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;

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

    #[tokio::test]
    async fn publishes_initial_and_removes_on_failure() {
        let provider = Arc::new(Provider::placeholder());
        provider.set_capacity(4, 1);
        provider.subscribe(PeerId::new("consumer"), String::new(), String::new());

        let control = Arc::new(MockControl::new(vec![Ok(AppResponse::Ack)]));
        let handle = control_with(control.clone());
        let sent = publish_due(&handle, &provider, 30_000).await;
        assert_eq!(sent, 1);
        assert_eq!(control.sent().len(), 1);
        assert_eq!(provider.subscription_count(), 1);

        provider.subscribe(PeerId::new("consumer-2"), String::new(), String::new());
        let control = Arc::new(MockControl::new(vec![Err(SidecarError::new("io", "down"))]));
        let handle = control_with(control);
        let sent = publish_due(&handle, &provider, 30_000).await;
        assert_eq!(sent, 0);
        assert!(!provider.has_subscription(&PeerId::new("consumer-2")));
    }

    #[tokio::test]
    async fn subscribes_until_target_capacity_is_visible() {
        let provider = Arc::new(Provider::new(PeerId::new("self"), 1, "rustc", "x86_64"));
        let consumer = Arc::new(Consumer::new());
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
                toolchain: "rustc-provider".into(),
                target: "aarch64".into(),
                max_slots: 2,
            }),
            Ok(AppResponse::Subscribed {
                provider: PeerId::new("b"),
                toolchain: "rustc-provider".into(),
                target: "aarch64".into(),
                max_slots: 2,
            }),
        ]));
        let handle = control_with(control.clone());

        let subscribed = ensure_target(&handle, &provider, &consumer, &node, &subscription).await;
        assert_eq!(subscribed, 2);
        assert_eq!(control.sent().len(), 2);
        let views = consumer.providers();
        assert_eq!(views.len(), 2);
        assert!(views.iter().all(|view| view.toolchain == "rustc-provider"));
    }

    #[tokio::test]
    async fn failed_subscribe_removes_pending_provider() {
        let provider = Arc::new(Provider::new(PeerId::new("self"), 0, "", ""));
        let consumer = Arc::new(Consumer::new());
        let node = NodeConfig::default();
        let subscription = SubscriptionConfig {
            target_slots: 1,
            static_peers: vec!["a".into()],
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::new(vec![Err(SidecarError::new("io", "down"))]));
        let handle = control_with(control);

        assert_eq!(
            ensure_target(&handle, &provider, &consumer, &node, &subscription).await,
            0
        );
        assert!(consumer.providers().is_empty());
    }
}
