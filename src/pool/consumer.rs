//! Consumer-side provider views and scheduling state.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::compile_protocol::{AppResponse, ErrorCode, PeerId};
use crate::util::time::now_ms;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderView {
    pub provider: PeerId,
    pub max_slots: u32,
    pub free_slots: u32,
    pub queue_len: u32,
    pub accepting: bool,
    pub last_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Default)]
struct Inner {
    providers: HashMap<PeerId, ProviderView>,
    cooldowns: HashMap<PeerId, u64>,
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
                last_seq: 0,
                last_seen_ms: now_ms(),
            });
        view.max_slots = max_slots;
        view.last_seen_ms = now_ms();
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

    pub fn cool_down_provider(&self, provider: &PeerId, duration_ms: u64) {
        let until = now_ms().saturating_add(duration_ms.max(1));
        self.write().cooldowns.insert(provider.clone(), until);
    }

    pub fn clear_cooldown(&self, provider: &PeerId) {
        self.write().cooldowns.remove(provider);
    }

    pub fn remove_provider(&self, provider: &PeerId) {
        self.write().providers.remove(provider);
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
        }
        expired
    }

    pub fn providers(&self) -> Vec<ProviderView> {
        let now = now_ms();
        let mut inner = self.write();
        inner.cooldowns.retain(|_, until| *until > now);
        inner
            .providers
            .iter()
            .filter(|(provider, _)| !inner.cooldowns.contains_key(*provider))
            .map(|(_, view)| view.clone())
            .collect()
    }

    pub fn has_provider(&self, provider: &PeerId) -> bool {
        self.read().providers.contains_key(provider)
    }

    pub fn visible_max_slots(&self) -> u32 {
        let now = now_ms();
        let mut inner = self.write();
        inner.cooldowns.retain(|_, until| *until > now);
        inner
            .providers
            .iter()
            .filter(|(provider, _)| !inner.cooldowns.contains_key(*provider))
            .map(|(_, view)| view.max_slots)
            .sum()
    }

    /// Remove all views and return the peers that were dropped.
    pub fn clear(&self) -> Vec<PeerId> {
        let mut inner = self.write();
        let peers: Vec<PeerId> = inner.providers.keys().cloned().collect();
        inner.providers.clear();
        peers
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().expect("Consumer RwLock 被污染")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().expect("Consumer RwLock 被污染")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(value: &str) -> PeerId {
        PeerId::new(value)
    }

    #[test]
    fn record_subscribed_sets_slots() {
        let consumer = Consumer::new();
        consumer.record_subscribed(peer("p"), 4);
        let view = &consumer.providers()[0];
        assert_eq!(view.max_slots, 4);
    }

    #[test]
    fn capacity_updates_respect_sequence() {
        let consumer = Consumer::new();
        consumer.record_subscribed(peer("provider"), 4);

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
    }

    #[test]
    fn providers_expire_after_ttl() {
        let consumer = Consumer::new();
        consumer.record_pending(peer("provider"));
        let expired = consumer.expire_providers(now_ms().saturating_add(61_000), 60_000);
        assert_eq!(expired, vec![peer("provider")]);
        assert!(consumer.providers().is_empty());
    }

    #[test]
    fn cooldown_hides_provider_and_is_not_reset_by_subscribe() {
        let consumer = Consumer::new();
        consumer.record_subscribed(peer("provider"), 4);
        consumer.cool_down_provider(&peer("provider"), 60_000);

        assert!(consumer.providers().is_empty());
        assert_eq!(consumer.visible_max_slots(), 0);

        consumer.record_subscribed(peer("provider"), 4);
        assert!(consumer.providers().is_empty());

        consumer.clear_cooldown(&peer("provider"));
        assert_eq!(consumer.providers().len(), 1);
        assert_eq!(consumer.visible_max_slots(), 4);
    }

    #[test]
    fn unknown_provider_returns_not_found() {
        let consumer = Consumer::new();
        assert!(matches!(
            consumer.apply_capacity(peer("missing"), 1, 0, 0, false),
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }
}
