//! Consumer-side provider views and scheduling state.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::compile_protocol::{AppResponse, ErrorCode, Hash32, PeerId, ToolchainCapability};
use crate::util::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderView {
    pub provider: PeerId,
    pub max_slots: u32,
    pub free_slots: u32,
    pub queue_len: u32,
    pub accepting: bool,
    pub capabilities: Vec<ToolchainCapability>,
    pub last_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Default)]
struct Inner {
    providers: HashMap<PeerId, ProviderView>,
    remote_cache: HashMap<Hash32, Vec<PeerId>>,
}

/// Consumer role: tracks subscribed providers and their last capacity update.
#[derive(Debug, Default)]
pub struct Consumer {
    inner: RwLock<Inner>,
}

impl Consumer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a pending subscription before sending `Subscribe`.
    pub fn record_pending(&self, provider: PeerId) {
        self.write().providers.insert(
            provider.clone(),
            ProviderView {
                provider,
                max_slots: 0,
                free_slots: 0,
                queue_len: 0,
                accepting: false,
                capabilities: Vec::new(),
                last_seq: 0,
                last_seen_ms: now_ms(),
            },
        );
    }

    /// Record the provider's advertised slot count from `Subscribed`.
    pub fn record_subscribed(&self, provider: PeerId, max_slots: u32) {
        let mut inner = self.write();
        let view = inner
            .providers
            .entry(provider.clone())
            .or_insert_with(|| ProviderView {
                provider,
                max_slots,
                free_slots: 0,
                queue_len: 0,
                accepting: false,
                capabilities: Vec::new(),
                last_seq: 0,
                last_seen_ms: now_ms(),
            });
        view.max_slots = max_slots;
        view.last_seen_ms = now_ms();
    }

    pub fn record_capabilities(&self, provider: PeerId, capabilities: Vec<ToolchainCapability>) {
        if let Some(view) = self.write().providers.get_mut(&provider) {
            view.capabilities = capabilities;
        }
    }

    pub fn apply_capacity(
        &self,
        provider: PeerId,
        seq: u64,
        free_slots: u32,
        queue_len: u32,
        accepting: bool,
    ) -> AppResponse {
        let mut inner = self.write();
        let Some(view) = inner.providers.get_mut(&provider) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown capacity provider");
        };

        if seq <= view.last_seq {
            return AppResponse::Ack;
        }

        view.last_seq = seq;
        view.free_slots = free_slots;
        view.queue_len = queue_len;
        view.accepting = accepting;
        view.last_seen_ms = now_ms();
        AppResponse::Ack
    }

    pub fn remove_provider(&self, provider: &PeerId) {
        let mut inner = self.write();
        inner.providers.remove(provider);
        forget_remote_provider(&mut inner, provider);
    }

    pub fn expire_providers(&self, now: u64, ttl_ms: u64) -> Vec<PeerId> {
        let mut inner = self.write();
        let expired: Vec<PeerId> = inner
            .providers
            .iter()
            .filter(|(_, view)| now.saturating_sub(view.last_seen_ms) > ttl_ms)
            .map(|(peer, _)| peer.clone())
            .collect();

        for peer in &expired {
            inner.providers.remove(peer);
            forget_remote_provider(&mut inner, peer);
        }
        expired
    }

    pub fn providers(&self) -> Vec<ProviderView> {
        self.read().providers.values().cloned().collect()
    }

    pub fn record_remote_cache(&self, task_id: Hash32, provider: &PeerId) {
        let mut inner = self.write();
        let providers = inner.remote_cache.entry(task_id).or_default();
        if !providers.iter().any(|p| p == provider) {
            providers.push(provider.clone());
        }
    }

    pub fn remote_cache_providers(&self, task_id: &Hash32) -> Vec<PeerId> {
        self.read()
            .remote_cache
            .get(task_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn remove_remote_cache(&self, task_id: &Hash32, provider: &PeerId) {
        let mut inner = self.write();
        let empty = {
            let Some(providers) = inner.remote_cache.get_mut(task_id) else {
                return;
            };
            providers.retain(|p| p != provider);
            providers.is_empty()
        };
        if empty {
            inner.remote_cache.remove(task_id);
        }
    }

    pub fn has_provider(&self, provider: &PeerId) -> bool {
        self.read().providers.contains_key(provider)
    }

    pub fn visible_max_slots(&self) -> u32 {
        self.read().providers.values().map(|view| view.max_slots).sum()
    }

    /// Remove all views and return the peers that were dropped.
    pub fn clear(&self) -> Vec<PeerId> {
        let mut inner = self.write();
        let peers: Vec<PeerId> = inner.providers.keys().cloned().collect();
        inner.providers.clear();
        inner.remote_cache.clear();
        peers
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().expect("Consumer RwLock 被污染")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().expect("Consumer RwLock 被污染")
    }
}

fn forget_remote_provider(inner: &mut Inner, provider: &PeerId) {
    inner.remote_cache.retain(|_, providers| {
        providers.retain(|p| p != provider);
        !providers.is_empty()
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(value: &str) -> PeerId {
        PeerId::new(value)
    }

    #[test]
    fn capacity_updates_and_subscription() {
        let consumer = Consumer::new();
        consumer.record_subscribed(peer("provider"), 4);
        assert_eq!(consumer.providers()[0].max_slots, 4);

        assert_eq!(
            consumer.apply_capacity(peer("provider"), 2, 3, 0, true),
            AppResponse::Ack
        );
        let view = &consumer.providers()[0];
        assert_eq!(view.free_slots, 3);
        assert!(view.accepting);

        assert_eq!(
            consumer.apply_capacity(peer("provider"), 1, 0, 0, false),
            AppResponse::Ack
        );
        assert_eq!(consumer.providers()[0].free_slots, 3);

        assert!(matches!(
            consumer.apply_capacity(peer("missing"), 1, 0, 0, false),
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }

    #[test]
    fn capabilities_are_recorded() {
        use crate::compile_protocol::ToolchainCapability;

        let consumer = Consumer::new();
        consumer.record_subscribed(peer("provider"), 2);
        consumer.record_capabilities(
            peer("provider"),
            vec![ToolchainCapability {
                id: "rustc-1".into(),
                target: "x86_64-unknown-linux-gnu".into(),
            }],
        );
        let view = &consumer.providers()[0];
        assert_eq!(view.capabilities.len(), 1);
        assert_eq!(view.capabilities[0].id, "rustc-1");
    }

    #[test]
    fn remote_cache_hints_are_recorded_and_removed() {
        let consumer = Consumer::new();
        let task_id = Hash32([7; 32]);
        let first = peer("provider-a");
        let second = peer("provider-b");

        consumer.record_remote_cache(task_id, &first);
        consumer.record_remote_cache(task_id, &first);
        consumer.record_remote_cache(task_id, &second);
        assert_eq!(
            consumer.remote_cache_providers(&task_id),
            vec![first.clone(), second.clone()]
        );

        consumer.remove_remote_cache(&task_id, &first);
        assert_eq!(
            consumer.remote_cache_providers(&task_id),
            vec![second.clone()]
        );

        consumer.remove_remote_cache(&task_id, &second);
        assert!(consumer.remote_cache_providers(&task_id).is_empty());
    }

    #[test]
    fn removing_or_expiring_provider_drops_remote_cache() {
        let consumer = Consumer::new();
        let task_id = Hash32([8; 32]);
        let provider = peer("provider");
        consumer.record_subscribed(provider.clone(), 1);
        consumer.record_remote_cache(task_id, &provider);

        consumer.remove_provider(&provider);
        assert!(consumer.remote_cache_providers(&task_id).is_empty());

        let consumer = Consumer::new();
        let provider = peer("provider");
        consumer.record_pending(provider.clone());
        consumer.record_remote_cache(task_id, &provider);
        consumer.expire_providers(now_ms().saturating_add(61_000), 60_000);
        assert!(consumer.remote_cache_providers(&task_id).is_empty());
    }

    #[test]
    fn expiry_removes_views() {
        let consumer = Consumer::new();
        consumer.record_pending(peer("provider"));
        let expired = consumer.expire_providers(now_ms().saturating_add(61_000), 60_000);
        assert_eq!(expired, vec![peer("provider")]);
        assert!(consumer.providers().is_empty());

        let consumer = Consumer::new();
        consumer.record_subscribed(peer("provider"), 4);
        assert_eq!(consumer.providers().len(), 1);
        assert_eq!(consumer.visible_max_slots(), 4);
    }
}
