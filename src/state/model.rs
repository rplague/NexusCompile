use std::collections::HashMap;

use crate::capacity::CapacitySnapshot;
use crate::compile_protocol::{Hash32, LeaseId, PeerId, TaskId, TaskState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub consumer: PeerId,
    pub toolchain: String,
    pub target: String,
    pub seq: u64,
    pub last_sent_ms: u64,
    pub last_sent_free: u32,
    pub last_sent_queue: u32,
    pub last_sent_accepting: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderView {
    pub provider: PeerId,
    pub toolchain: String,
    pub target: String,
    pub max_slots: u32,
    pub free_slots: u32,
    pub queue_len: u32,
    pub accepting: bool,
    pub last_seq: u64,
    pub last_seen_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEntry {
    pub consumer: PeerId,
    pub lease_id: LeaseId,
    pub input_manifest: Hash32,
    pub state: TaskState,
}

#[derive(Debug)]
pub struct NodeState {
    pub provider_id: PeerId,
    pub toolchain: String,
    pub target: String,
    pub max_slots: u32,
    pub free_slots: u32,
    pub accepting: bool,
    pub subscriptions: HashMap<PeerId, Subscription>,
    pub tasks: HashMap<TaskId, TaskEntry>,
    pub providers: HashMap<PeerId, ProviderView>,
}

impl NodeState {
    pub fn new(
        provider_id: PeerId,
        max_slots: u32,
        toolchain: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            provider_id,
            toolchain: toolchain.into(),
            target: target.into(),
            max_slots,
            free_slots: max_slots,
            accepting: max_slots > 0,
            subscriptions: HashMap::new(),
            tasks: HashMap::new(),
            providers: HashMap::new(),
        }
    }

    pub fn placeholder() -> Self {
        Self::new(PeerId::new("local"), 1, "", "")
    }

    pub fn set_provider_id(&mut self, provider_id: PeerId) {
        self.provider_id = provider_id;
    }
}

impl NodeState {
    pub fn active_slots(&self) -> u32 {
        self.tasks
            .values()
            .filter(|entry| matches!(entry.state, TaskState::Queued | TaskState::Running))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    pub fn queue_len(&self) -> u32 {
        self.tasks
            .values()
            .filter(|entry| matches!(entry.state, TaskState::Queued))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    pub fn capacity_snapshot(&self) -> CapacitySnapshot {
        CapacitySnapshot {
            max_slots: self.max_slots,
            free_slots: self.free_slots,
            queue_len: self.queue_len(),
            accepting: self.accepting,
        }
    }

    pub fn set_capacity(&mut self, max_slots: u32, free_slots: u32, accepting: bool) {
        self.max_slots = max_slots;
        self.free_slots = free_slots.min(max_slots);
        self.accepting = accepting && self.free_slots > 0;
    }
}
