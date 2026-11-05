use crate::capacity::CapacitySnapshot;
use crate::compile_protocol::{AppRequest, AppResponse, ErrorCode, PeerId};
use crate::util::time::now_ms;

use super::{NodeState, ProviderView, Subscription};

impl NodeState {
    pub fn subscribe(
        &mut self,
        consumer: PeerId,
        toolchain: String,
        target: String,
    ) -> AppResponse {
        self.subscriptions.insert(
            consumer.clone(),
            Subscription {
                consumer,
                toolchain,
                target,
                seq: 0,
                last_sent_ms: 0,
                last_sent_free: 0,
                last_sent_queue: 0,
                last_sent_accepting: false,
            },
        );
        AppResponse::Subscribed {
            provider: self.provider_id.clone(),
            toolchain: self.toolchain.clone(),
            target: self.target.clone(),
            max_slots: self.max_slots,
        }
    }

    pub fn unsubscribe(&mut self, consumer: &PeerId) -> AppResponse {
        self.subscriptions.remove(consumer);
        AppResponse::Ack
    }

    /// Record a pending subscription before sending Subscribe. This prevents an
    /// initial CapacityUpdate from arriving before the Subscribed reply.
    pub fn record_pending(&mut self, provider: PeerId, toolchain: String, target: String) {
        self.providers.insert(
            provider.clone(),
            ProviderView {
                provider,
                toolchain,
                target,
                max_slots: 0,
                free_slots: 0,
                queue_len: 0,
                accepting: false,
                last_seq: 0,
                last_seen_ms: now_ms(),
            },
        );
    }

    pub fn record_subscribed(
        &mut self,
        provider: PeerId,
        toolchain: String,
        target: String,
        max_slots: u32,
    ) {
        let view = self
            .providers
            .entry(provider.clone())
            .or_insert_with(|| ProviderView {
                provider,
                toolchain: toolchain.clone(),
                target: target.clone(),
                max_slots,
                free_slots: 0,
                queue_len: 0,
                accepting: false,
                last_seq: 0,
                last_seen_ms: now_ms(),
            });
        view.toolchain = toolchain;
        view.target = target;
        view.max_slots = max_slots;
        view.last_seen_ms = now_ms();
    }

    pub fn apply_capacity(
        &mut self,
        provider: PeerId,
        seq: u64,
        free_slots: u32,
        queue_len: u32,
        accepting: bool,
    ) -> AppResponse {
        let Some(view) = self.providers.get_mut(&provider) else {
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

    pub fn remove_provider(&mut self, provider: &PeerId) {
        self.providers.remove(provider);
    }

    pub fn expire_providers(&mut self, now: u64, ttl_ms: u64) -> Vec<PeerId> {
        let expired: Vec<PeerId> = self
            .providers
            .iter()
            .filter(|(_, view)| now.saturating_sub(view.last_seen_ms) > ttl_ms)
            .map(|(peer, _)| peer.clone())
            .collect();

        for peer in &expired {
            self.providers.remove(peer);
        }
        expired
    }

    pub fn due_capacity_updates(
        &mut self,
        now: u64,
        snapshot: CapacitySnapshot,
        interval_ms: u64,
    ) -> Vec<(PeerId, AppRequest)> {
        let provider = self.provider_id.clone();
        let mut updates = Vec::new();

        for subscription in self.subscriptions.values_mut() {
            let changed = subscription.last_sent_ms == 0
                || subscription.last_sent_free != snapshot.free_slots
                || subscription.last_sent_queue != snapshot.queue_len
                || subscription.last_sent_accepting != snapshot.accepting;
            let keepalive = now.saturating_sub(subscription.last_sent_ms) >= interval_ms;

            if !changed && !keepalive {
                continue;
            }

            subscription.seq = subscription.seq.saturating_add(1);
            subscription.last_sent_ms = now;
            subscription.last_sent_free = snapshot.free_slots;
            subscription.last_sent_queue = snapshot.queue_len;
            subscription.last_sent_accepting = snapshot.accepting;

            updates.push((
                subscription.consumer.clone(),
                AppRequest::CapacityUpdate {
                    provider: provider.clone(),
                    seq: subscription.seq,
                    free_slots: snapshot.free_slots,
                    queue_len: snapshot.queue_len,
                    accepting: snapshot.accepting,
                },
            ));
        }

        updates
    }
}
