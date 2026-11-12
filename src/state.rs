//! In-memory provider/consumer state for the compile pool.

use std::sync::{Arc, Mutex};

use crate::compile::cache::ResultCache;
use crate::compile_protocol::PeerId;
use crate::config::{ConfigHandle, ServiceConfig};
use crate::control::{self, ControlHandle, ControlPlane};

mod model;
mod provider;
mod task;

pub use model::{NodeState, ProviderView, Subscription, TaskEntry};

/// Shared application state handed to the connection and local services.
#[derive(Clone)]
pub struct AppState {
    pub state: Arc<Mutex<NodeState>>,
    pub cache: Arc<ResultCache>,
    pub config: ConfigHandle,
    pub control: ControlHandle,
}

impl AppState {
    pub fn new(
        state: Arc<Mutex<NodeState>>,
        cache: Arc<ResultCache>,
        config: ConfigHandle,
        control: ControlHandle,
    ) -> Self {
        Self {
            state,
            cache,
            config,
            control,
        }
    }

    pub fn config_snapshot(&self) -> ServiceConfig {
        self.config.snapshot()
    }

    pub fn set_control(&self, control: Option<Arc<dyn ControlPlane>>) {
        crate::control::set(&self.control, control);
    }

    pub fn current_control(&self) -> Option<Arc<dyn ControlPlane>> {
        control::current(&self.control)
    }

    pub fn set_provider_id(&self, peer_id: PeerId) {
        if let Ok(mut state) = self.state.lock() {
            state.set_provider_id(peer_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::CapacitySnapshot;
    use crate::compile_protocol::{
        AppRequest, AppResponse, ErrorCode, Hash32, OutputRef, PeerId, TaskId, TaskState,
    };
    use crate::util::time::now_ms;

    fn peer(value: &str) -> PeerId {
        PeerId::new(value)
    }

    fn task(value: u8) -> TaskId {
        TaskId(Hash32([value; 32]))
    }

    fn manifest(value: u8) -> Hash32 {
        Hash32([value; 32])
    }

    fn snapshot(free_slots: u32, queue_len: u32, accepting: bool) -> CapacitySnapshot {
        CapacitySnapshot {
            max_slots: 4,
            free_slots,
            queue_len,
            accepting,
        }
    }

    fn update_seq(request: &AppRequest) -> u64 {
        match request {
            AppRequest::CapacityUpdate { seq, .. } => *seq,
            other => panic!("expected capacity update, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_is_keyed_by_peer_id_and_replaces() {
        let mut state = NodeState::new(peer("provider"), 2, "rustc", "x86_64");
        let first = state.subscribe(peer("consumer"), "rustc".into(), "x86_64".into());
        let second = state.subscribe(peer("consumer"), "rustc-2".into(), "aarch64".into());

        assert_eq!(
            first,
            AppResponse::Subscribed {
                provider: peer("provider"),
                toolchain: "rustc".into(),
                target: "x86_64".into(),
                max_slots: 2,
            }
        );
        assert_eq!(
            second,
            AppResponse::Subscribed {
                provider: peer("provider"),
                toolchain: "rustc".into(),
                target: "x86_64".into(),
                max_slots: 2,
            }
        );
        assert_eq!(state.subscriptions.len(), 1);
        assert_eq!(state.subscriptions[&peer("consumer")].toolchain, "rustc-2");
        assert_eq!(state.subscriptions[&peer("consumer")].seq, 0);
    }

    #[test]
    fn capacity_updates_respect_sequence() {
        let mut state = NodeState::placeholder();
        state.record_subscribed(peer("provider"), "rustc".into(), "x86_64".into(), 4);

        assert_eq!(
            state.apply_capacity(peer("provider"), 2, 3, 0, true),
            AppResponse::Ack
        );
        let view = &state.providers[&peer("provider")];
        assert_eq!(view.free_slots, 3);
        assert!(view.accepting);

        assert_eq!(
            state.apply_capacity(peer("provider"), 1, 0, 0, false),
            AppResponse::Ack
        );
        assert_eq!(state.providers[&peer("provider")].free_slots, 3);
    }

    #[test]
    fn due_updates_include_initial_change_and_keepalive() {
        let mut state = NodeState::placeholder();
        state.subscribe(peer("consumer"), String::new(), String::new());

        let initial = state.due_capacity_updates(1_000, snapshot(2, 0, true), 30_000);
        assert_eq!(initial.len(), 1);
        assert_eq!(update_seq(&initial[0].1), 1);

        let no_change = state.due_capacity_updates(2_000, snapshot(2, 0, true), 30_000);
        assert!(no_change.is_empty());

        let changed = state.due_capacity_updates(3_000, snapshot(1, 1, true), 30_000);
        assert_eq!(changed.len(), 1);
        assert_eq!(update_seq(&changed[0].1), 2);

        let keepalive = state.due_capacity_updates(33_000, snapshot(1, 1, true), 30_000);
        assert_eq!(keepalive.len(), 1);
        assert_eq!(update_seq(&keepalive[0].1), 3);
    }

    #[test]
    fn providers_expire_after_ttl() {
        let mut state = NodeState::placeholder();
        state.record_pending(peer("provider"), String::new(), String::new());
        let expired = state.expire_providers(now_ms().saturating_add(61_000), 60_000);
        assert_eq!(expired, vec![peer("provider")]);
        assert!(state.providers.is_empty());
    }

    #[test]
    fn reserve_is_idempotent_and_busy_when_full() {
        let mut state = NodeState::placeholder();
        let id = task(1);

        let first = state.reserve(peer("consumer"), id, 1, manifest(2));
        assert!(matches!(first, AppResponse::Granted { .. }));
        assert_eq!(state.free_slots, 0);
        assert!(!state.accepting);

        let second = state.reserve(peer("consumer"), id, 1, manifest(2));
        assert!(matches!(second, AppResponse::Granted { .. }));

        let other = state.reserve(peer("consumer"), task(3), 1, manifest(4));
        assert_eq!(
            other,
            AppResponse::Busy {
                retry_after_ms: 1_000
            }
        );
    }

    #[test]
    fn one_task_requires_exactly_one_slot() {
        let mut state = NodeState::placeholder();
        let response = state.reserve(peer("consumer"), task(1), 2, manifest(2));
        assert!(matches!(
            response,
            AppResponse::Rejected {
                code: ErrorCode::BadRequest,
                ..
            }
        ));
    }

    #[test]
    fn task_lifecycle_and_cache_hit() {
        let mut state = NodeState::placeholder();
        let id = task(1);

        let granted = state.reserve(peer("consumer"), id, 1, manifest(2));
        let lease = match granted {
            AppResponse::Granted { lease_id, .. } => lease_id,
            other => panic!("expected granted, got {other:?}"),
        };

        assert_eq!(state.task_ready(id, lease), AppResponse::Ack);
        assert_eq!(
            state.status(id),
            AppResponse::TaskStatus {
                state: TaskState::Running
            }
        );

        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: manifest(9),
            size: 10,
            mode: 0o644,
        };
        assert_eq!(
            state.complete_task(id, vec![output.clone()]),
            AppResponse::Ack
        );
        assert_eq!(state.free_slots, 1);
        assert_eq!(
            state.status(id),
            AppResponse::TaskStatus {
                state: TaskState::Done {
                    outputs: vec![output],
                }
            }
        );

        let cached = state.reserve(peer("consumer"), id, 1, manifest(2));
        assert!(matches!(cached, AppResponse::Cached { .. }));
    }

    #[test]
    fn cancel_and_fail_free_the_slot() {
        let mut state = NodeState::placeholder();
        let id = task(1);
        assert!(matches!(
            state.reserve(peer("consumer"), id, 1, manifest(2)),
            AppResponse::Granted { .. }
        ));
        assert_eq!(state.cancel(id), AppResponse::Ack);
        assert_eq!(state.free_slots, 1);
        assert_eq!(
            state.status(id),
            AppResponse::TaskStatus {
                state: TaskState::Cancelled
            }
        );

        let id2 = task(3);
        assert!(matches!(
            state.reserve(peer("consumer"), id2, 1, manifest(4)),
            AppResponse::Granted { .. }
        ));
        assert_eq!(
            state.fail_task(id2, ErrorCode::Internal, "boom"),
            AppResponse::Ack
        );
        assert_eq!(state.free_slots, 1);
    }

    #[test]
    fn unknown_task_and_provider_return_not_found() {
        let mut state = NodeState::placeholder();
        assert!(matches!(
            state.status(task(1)),
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
        assert!(matches!(
            state.apply_capacity(peer("missing"), 1, 0, 0, false),
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }
}
