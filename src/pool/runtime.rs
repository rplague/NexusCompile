//! Background capacity sampling, provider publishing, and consumer targeting.

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

const DISCOVERY_NORMAL_INTERVAL_MS: u64 = 60 * 60 * 1000;
const DISCOVERY_FAULT_INTERVAL_MS: u64 = 30 * 1000;

#[derive(Debug, Default)]
pub struct DiscoveryState {
    last_attempt_ms: Option<u64>,
    last_abnormal: bool,
}

fn discovery_due(state: &DiscoveryState, now: u64, visible: u32) -> bool {
    let interval = if visible == 0 || state.last_abnormal {
        DISCOVERY_FAULT_INTERVAL_MS
    } else {
        DISCOVERY_NORMAL_INTERVAL_MS
    };
    match state.last_attempt_ms {
        None => true,
        Some(last) => now.saturating_sub(last) >= interval,
    }
}

pub async fn publish_due(
    control: &ControlHandle,
    service_name: &str,
    provider: &Arc<Provider>,
    interval_ms: u64,
) -> usize {
    let snapshot = provider.capacity_snapshot();
    let due = provider.due_capacity_updates(now_ms(), snapshot, interval_ms);

    let mut sent = 0;
    for (peer, request) in due {
        match control::send(control, service_name, &peer, request).await {
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
    _node: &NodeConfig,
    subscription: &SubscriptionConfig,
    service_name: &str,
    discovery: &mut DiscoveryState,
) -> usize {
    if subscription.target_slots == 0 {
        return 0;
    }

    let provider_id = provider.provider_id();
    let mut visible_capacity = provider.max_slots() + consumer.visible_max_slots();
    if visible_capacity >= subscription.target_slots {
        return 0;
    }

    let now = now_ms();
    if !discovery_due(discovery, now, visible_capacity) {
        return 0;
    }
    discovery.last_attempt_ms = Some(now);

    let peers = match control::discover(control, service_name).await {
        Ok(peers) if !peers.is_empty() => {
            LogStruct::new(
                LogLevel::Important,
                "自动发现结果",
                format!("service={service_name} peers={}", peers.join(",")),
            )
            .emit();
            peers
        }
        Ok(_) => {
            discovery.last_abnormal = true;
            LogStruct::new(
                LogLevel::Warning,
                "自动发现异常",
                format!("service={service_name}: 空 provider 列表"),
            )
            .emit();
            clear_providers(control, consumer, service_name, &provider_id).await;
            return 0;
        }
        Err(error) => {
            discovery.last_abnormal = true;
            LogStruct::new(
                LogLevel::Warning,
                "自动发现异常",
                format!("service={service_name}: {error}"),
            )
            .emit();
            clear_providers(control, consumer, service_name, &provider_id).await;
            return 0;
        }
    };

    let mut subscribed = 0;
    for raw_peer in peers {
        if visible_capacity >= subscription.target_slots {
            break;
        }

        let peer = PeerId::new(raw_peer);
        if peer == provider_id || consumer.has_provider(&peer) {
            continue;
        }

        consumer.record_pending(peer.clone());

        let request = AppRequest::Subscribe {
            consumer: provider_id.clone(),
            toolchain: String::new(),
            target: String::new(),
        };

        match control::send(control, service_name, &peer, request).await {
            Ok(AppResponse::Subscribed {
                max_slots,
                capabilities,
                ..
            }) => {
                consumer.record_subscribed(peer.clone(), max_slots);
                consumer.record_capabilities(peer.clone(), capabilities);
                visible_capacity = visible_capacity.saturating_add(max_slots);
                subscribed += 1;
            }
            Ok(other) => {
                LogStruct::new(
                    LogLevel::Warning,
                    "订阅失败",
                    format!("provider={peer} response={other:?}"),
                )
                .emit();
                consumer.remove_provider(&peer);
            }
            Err(error) => {
                LogStruct::new(
                    LogLevel::Warning,
                    "订阅失败",
                    format!("provider={peer} error={error}"),
                )
                .emit();
                consumer.remove_provider(&peer);
            }
        }
    }
    if visible_capacity >= subscription.target_slots {
        discovery.last_abnormal = false;
    } else {
        discovery.last_abnormal = true;
        LogStruct::new(
            LogLevel::Warning,
            "自动发现不足",
            format!(
                "service={service_name} visible_slots={} target_slots={} subscribed={subscribed}",
                visible_capacity, subscription.target_slots
            ),
        )
        .emit();
    }
    subscribed
}

async fn clear_providers(
    control: &ControlHandle,
    consumer: &Arc<Consumer>,
    service_name: &str,
    provider_id: &PeerId,
) {
    let cleared = consumer.clear();
    for peer in cleared {
        let _ = control::send(
            control,
            service_name,
            &peer,
            AppRequest::Unsubscribe {
                consumer: provider_id.clone(),
            },
        )
        .await;
    }
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
    Ok(calculate_capacity(
        &sample,
        provider.queue_len(),
        node.max_usage_percent,
    ))
}

fn apply_capacity(provider: &Arc<Provider>, snapshot: CapacitySnapshot) {
    provider.set_capacity(snapshot.max_slots, snapshot.free_slots);
}

async fn publish_capacity_tick(
    control: &ControlHandle,
    service_name: &str,
    provider: &Arc<Provider>,
    subscription: &SubscriptionConfig,
) {
    let _ = publish_due(
        control,
        service_name,
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
    service_name: &str,
    discovery: &mut DiscoveryState,
) {
    let _ = ensure_target(
        control,
        provider,
        consumer,
        node,
        subscription,
        service_name,
        discovery,
    )
    .await;
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
    let service_name = config.service_name;
    let node = config.node;
    let subscription = config.subscription;
    let mut discovery = DiscoveryState::default();

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
                publish_capacity_tick(&control, &service_name, &provider, &subscription).await;
            }
            _ = consumer_tick.tick() => {
                subscribe_capacity_tick(
                    &control,
                    &provider,
                    &consumer,
                    &node,
                    &subscription,
                    &service_name,
                    &mut discovery,
                )
                .await;
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
        service_names: Mutex<Vec<String>>,
        responses: Mutex<VecDeque<Result<AppResponse, SidecarError>>>,
        discoveries: Mutex<VecDeque<Result<Vec<String>, SidecarError>>>,
    }

    impl MockControl {
        fn new(responses: Vec<Result<AppResponse, SidecarError>>) -> Self {
            Self::with_discoveries(responses, Vec::new())
        }

        fn with_discoveries(
            responses: Vec<Result<AppResponse, SidecarError>>,
            discoveries: Vec<Result<Vec<String>, SidecarError>>,
        ) -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
                service_names: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into()),
                discoveries: Mutex::new(discoveries.into()),
            }
        }

        fn sent(&self) -> Vec<(PeerId, AppRequest)> {
            self.sent.lock().unwrap().clone()
        }

        fn discovered_services(&self) -> Vec<String> {
            self.service_names.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl crate::control::ControlPlane for MockControl {
        async fn service_request_to(
            &self,
            service: &str,
            peer: &str,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, SidecarError> {
            let envelope: Envelope<AppRequest> = ciborium::de::from_reader(payload.as_slice())
                .map_err(|e| SidecarError::new("decode_error", e.to_string()))?;
            self.service_names.lock().unwrap().push(service.to_string());
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

        async fn discover_providers(&self, service: &str) -> Result<Vec<String>, SidecarError> {
            self.service_names.lock().unwrap().push(service.to_string());
            self.discoveries
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
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
        provider.subscribe(PeerId::new("consumer"));

        let control = Arc::new(MockControl::new(vec![Ok(AppResponse::Ack)]));
        let handle = control_with(control.clone());
        let sent = publish_due(&handle, "rustc-pool", &provider, 30_000).await;
        assert_eq!(sent, 1);
        assert_eq!(control.sent().len(), 1);
        assert_eq!(provider.subscription_count(), 1);

        provider.subscribe(PeerId::new("consumer-2"));
        let control = Arc::new(MockControl::new(vec![Err(SidecarError::new("io", "down"))]));
        let handle = control_with(control);
        let sent = publish_due(&handle, "rustc-pool", &provider, 30_000).await;
        assert_eq!(sent, 0);
        assert!(!provider.has_subscription(&PeerId::new("consumer-2")));
    }

    #[tokio::test]
    async fn subscribes_until_target_and_removes_failed_provider() {
        let provider = Arc::new(Provider::new(PeerId::new("self"), 1));
        let consumer = Arc::new(Consumer::new());
        let node = NodeConfig::default();
        let subscription = SubscriptionConfig {
            target_slots: 4,
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::with_discoveries(
            vec![
                Ok(AppResponse::Subscribed {
                    provider: PeerId::new("a"),
                    toolchain: "rustc".into(),
                    target: "x86_64".into(),
                    max_slots: 2,
                    capabilities: Vec::new(),
                }),
                Ok(AppResponse::Subscribed {
                    provider: PeerId::new("b"),
                    toolchain: "rustc".into(),
                    target: "x86_64".into(),
                    max_slots: 2,
                    capabilities: Vec::new(),
                }),
            ],
            vec![Ok(vec!["a".into(), "b".into(), "c".into()])],
        ));
        let handle = control_with(control.clone());
        let mut discovery = DiscoveryState::default();

        let subscribed = ensure_target(
            &handle,
            &provider,
            &consumer,
            &node,
            &subscription,
            "rustc-pool",
            &mut discovery,
        )
        .await;
        assert_eq!(subscribed, 2);
        assert_eq!(control.sent().len(), 2);
        assert_eq!(control.discovered_services().len(), 3);
        assert!(
            control
                .discovered_services()
                .iter()
                .all(|service| service == "rustc-pool")
        );
        let views = consumer.providers();
        assert_eq!(views.len(), 2);

        let provider = Arc::new(Provider::new(PeerId::new("self"), 0));
        let consumer = Arc::new(Consumer::new());
        let subscription = SubscriptionConfig {
            target_slots: 1,
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::with_discoveries(
            vec![Err(SidecarError::new("io", "down"))],
            vec![Ok(vec!["a".into()])],
        ));
        let handle = control_with(control);
        let mut discovery = DiscoveryState::default();

        assert_eq!(
            ensure_target(
                &handle,
                &provider,
                &consumer,
                &node,
                &subscription,
                "rustc-pool",
                &mut discovery
            )
            .await,
            0
        );
        assert!(consumer.providers().is_empty());
    }

    #[tokio::test]
    async fn empty_discovery_clears_views_and_retries_after_30s_and_intervals() {
        let provider = Arc::new(Provider::new(PeerId::new("self"), 1));
        let consumer = Arc::new(Consumer::new());
        consumer.record_subscribed(PeerId::new("old"), 2);
        let node = NodeConfig::default();
        let subscription = SubscriptionConfig {
            target_slots: 4,
            ..SubscriptionConfig::default()
        };
        let control = Arc::new(MockControl::with_discoveries(vec![], vec![Ok(Vec::new())]));
        let handle = control_with(control);
        let mut discovery = DiscoveryState::default();

        let subscribed = ensure_target(
            &handle,
            &provider,
            &consumer,
            &node,
            &subscription,
            "rustc-pool",
            &mut discovery,
        )
        .await;
        assert_eq!(subscribed, 0);
        assert!(consumer.providers().is_empty());
        assert!(discovery.last_abnormal);
        let last = discovery.last_attempt_ms.unwrap();
        assert!(!discovery_due(&discovery, last + 29_999, 1));
        assert!(discovery_due(&discovery, last + 30_000, 1));
        assert!(discovery_due(&discovery, last + 59 * 60_000, 1));

        let mut discovery = DiscoveryState::default();
        assert!(discovery_due(&discovery, 1_000, 1));

        discovery.last_attempt_ms = Some(1_000);
        discovery.last_abnormal = false;
        assert!(!discovery_due(&discovery, 1_000 + 59 * 60_000, 1));
        assert!(discovery_due(&discovery, 1_000 + 60 * 60_000, 1));

        discovery.last_abnormal = true;
        assert!(!discovery_due(&discovery, 1_000 + 29_999, 1));
        assert!(discovery_due(&discovery, 1_000 + 30_000, 1));

        discovery.last_abnormal = false;
        assert!(!discovery_due(&discovery, 1_000 + 29_999, 0));
        assert!(discovery_due(&discovery, 1_000 + 30_000, 0));
    }
}
