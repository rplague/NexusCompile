use crate::compile_protocol::{
    AppResponse, ErrorCode, Hash32, LeaseId, OutputRef, PeerId, TaskId, TaskState,
};

use super::{NodeState, TaskEntry};

impl NodeState {
    pub fn reserve(
        &mut self,
        consumer: PeerId,
        task_id: TaskId,
        slots: u32,
        input_manifest: Hash32,
    ) -> AppResponse {
        if slots != 1 {
            return AppResponse::Rejected {
                code: ErrorCode::BadRequest,
                message: "exactly one slot must be requested".into(),
            };
        }

        if let Some(existing) = self.tasks.get(&task_id).cloned() {
            match &existing.state {
                TaskState::Done { outputs } => {
                    return AppResponse::Cached {
                        task_id,
                        outputs: outputs.clone(),
                        lease_id: existing.lease_id,
                    };
                }
                TaskState::Queued | TaskState::Running => {
                    return AppResponse::Granted {
                        task_id,
                        lease_id: existing.lease_id,
                    };
                }
                TaskState::Failed { .. } | TaskState::Cancelled => {
                    self.tasks.remove(&task_id);
                }
            }
        }

        if self.free_slots == 0 {
            return AppResponse::Busy {
                retry_after_ms: 1_000,
            };
        }

        let lease_id = LeaseId::new();
        self.tasks.insert(
            task_id,
            TaskEntry {
                consumer,
                lease_id,
                input_manifest,
                state: TaskState::Queued,
            },
        );
        self.free_slots -= 1;
        self.accepting = self.free_slots > 0;

        AppResponse::Granted { task_id, lease_id }
    }

    pub fn task_ready(&mut self, task_id: TaskId, lease_id: LeaseId) -> AppResponse {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };
        if entry.lease_id != lease_id {
            return AppResponse::error(ErrorCode::NotFound, "lease does not match task");
        }

        match entry.state {
            TaskState::Queued => {
                entry.state = TaskState::Running;
                AppResponse::Ack
            }
            TaskState::Running => AppResponse::Ack,
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not queued"),
        }
    }

    pub fn task_entry(&self, task_id: &TaskId) -> Option<TaskEntry> {
        self.tasks.get(task_id).cloned()
    }

    pub fn task_by_lease(&self, lease_id: LeaseId) -> Option<(TaskId, TaskEntry)> {
        self.tasks
            .iter()
            .find(|(_, entry)| entry.lease_id == lease_id)
            .map(|(task_id, entry)| (*task_id, entry.clone()))
    }

    pub fn status(&self, task_id: TaskId) -> AppResponse {
        match self.tasks.get(&task_id) {
            Some(entry) => AppResponse::TaskStatus {
                state: entry.state.clone(),
            },
            None => AppResponse::error(ErrorCode::NotFound, "unknown task"),
        }
    }

    pub fn cancel(&mut self, task_id: TaskId) -> AppResponse {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        match entry.state {
            TaskState::Queued | TaskState::Running => {
                entry.state = TaskState::Cancelled;
                self.free_slot();
            }
            _ => {}
        }
        AppResponse::Ack
    }

    pub fn result_ack(&mut self, lease_id: LeaseId) -> Option<TaskId> {
        let task_id = self
            .tasks
            .iter()
            .find(|(_, entry)| entry.lease_id == lease_id)
            .map(|(task_id, _)| *task_id)?;
        let remove = self
            .tasks
            .get(&task_id)
            .is_some_and(|entry| matches!(entry.state, TaskState::Done { .. }));
        if remove {
            self.tasks.remove(&task_id);
        }
        Some(task_id)
    }

    pub fn complete_task(&mut self, task_id: TaskId, outputs: Vec<OutputRef>) -> AppResponse {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        match entry.state {
            TaskState::Queued | TaskState::Running => {
                entry.state = TaskState::Done { outputs };
                self.free_slot();
                AppResponse::Ack
            }
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not running"),
        }
    }

    pub fn fail_task(
        &mut self,
        task_id: TaskId,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> AppResponse {
        self.fail_task_with_logs(task_id, code, message, Vec::new(), Vec::new())
    }

    pub fn fail_task_with_logs(
        &mut self,
        task_id: TaskId,
        code: ErrorCode,
        message: impl Into<String>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    ) -> AppResponse {
        let Some(entry) = self.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        match entry.state {
            TaskState::Queued | TaskState::Running => {
                entry.state = TaskState::Failed {
                    code,
                    message: message.into(),
                    stdout,
                    stderr,
                };
                self.free_slot();
                AppResponse::Ack
            }
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not running"),
        }
    }

    fn free_slot(&mut self) {
        self.free_slots = self.free_slots.saturating_add(1).min(self.max_slots);
        self.accepting = self.free_slots > 0;
    }
}
