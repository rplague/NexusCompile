//! Inbound sidecar service dispatch (protocol v2 business entry point).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::compile::hash::hex_hash;
use crate::compile_protocol::{
    APP_PROTOCOL_VERSION, AppRequest, AppResponse, Envelope, ErrorCode, PeerId, TaskState,
};
use crate::control::ControlPlane;
use crate::log::{LogLevel, LogStruct, log_task_error};
use crate::pool;
use crate::protocol::{SidecarError, WhoamiInfo};
use crate::state::AppState;
use crate::task_blob::TaskBlobStore;

/// Boxed future so async business logic can be spawned from the connection loop.
pub type ServiceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, SidecarError>> + Send + 'a>>;

/// Service dispatcher. The connection layer only depends on this abstraction,
/// which makes it trivial to inject an echo/mock dispatcher in tests.
pub trait Dispatch: Clone + Send + Sync + 'static {
    fn handle<'a>(
        &'a self,
        service: &'a str,
        payload: &'a [u8],
        control: &'a dyn ControlPlane,
    ) -> ServiceFuture<'a>;

    fn on_connected(&self, _control: Arc<dyn ControlPlane>) {}

    fn on_whoami(&self, _info: &WhoamiInfo) {}

    fn on_disconnected(&self, _control: &Arc<dyn ControlPlane>) {}
}

/// Handle one inbound sidecar service request.
pub async fn handle_service_request(
    service: &str,
    payload: &[u8],
    app: &AppState,
) -> Result<Vec<u8>, SidecarError> {
    LogStruct::new(
        LogLevel::Debug,
        "服务请求",
        format!("service={}, payload_len={}", service, payload.len()),
    )
    .emit();

    let app = app.clone();
    let payload = payload.to_vec();
    tokio::task::spawn_blocking(move || handle_app_payload(&payload, &app))
        .await
        .map_err(|error| SidecarError::new("internal", format!("service task failed: {error}")))?
}

/// Decode, dispatch, and encode one application request.
pub(crate) fn handle_app_payload(payload: &[u8], app: &AppState) -> Result<Vec<u8>, SidecarError> {
    let envelope: Envelope<AppRequest> = ciborium::de::from_reader(payload)
        .map_err(|e| SidecarError::new("invalid_request", e.to_string()))?;

    let response = if envelope.v != APP_PROTOCOL_VERSION {
        AppResponse::error(
            ErrorCode::UnsupportedVersion,
            format!("unsupported version {}", envelope.v),
        )
    } else {
        dispatch(envelope.body, app)
    };

    let mut result = Vec::new();
    ciborium::ser::into_writer(&Envelope::new(response), &mut result)
        .map_err(|e| SidecarError::new("encode_error", e.to_string()))?;
    Ok(result)
}

fn dispatch(request: AppRequest, app: &AppState) -> AppResponse {
    match request {
        AppRequest::Subscribe { consumer, .. } => app.provider.subscribe(consumer),
        AppRequest::Unsubscribe { consumer } => app.provider.unsubscribe(&consumer),
        AppRequest::CapacityUpdate {
            provider,
            seq,
            free_slots,
            queue_len,
            accepting,
        } => app
            .consumer
            .apply_capacity(provider, seq, free_slots, queue_len, accepting),
        AppRequest::Reserve {
            consumer,
            task_id,
            slots,
            input_manifest,
            toolchain_id,
            target,
        } => {
            if app.provider.task_entry(&task_id).is_none()
                && let Some(outputs) = app.cache.cached_outputs(&task_id.0)
                && app.cache.pin(&task_id.0)
            {
                return AppResponse::Cached {
                    task_id,
                    outputs,
                    lease_id: app.provider.create_lease(task_id),
                };
            }
            if let Err(message) = validate_reserve(app, &toolchain_id, &target) {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "reserve_rejected",
                    &message,
                );
                return AppResponse::rejected(message);
            }
            let response = app
                .provider
                .reserve(consumer, task_id, slots, input_manifest);
            if matches!(response, AppResponse::Cached { .. }) {
                app.cache.pin(&task_id.0);
            }
            response
        }
        AppRequest::TaskReady { task_id, lease_id } => {
            let Some(entry) = app.provider.task_entry(&task_id) else {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "task_ready_unknown",
                    "unknown task",
                );
                return AppResponse::error(ErrorCode::NotFound, "unknown task");
            };
            if entry.lease_id != lease_id {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "task_ready_lease_mismatch",
                    "lease does not match task",
                );
                return AppResponse::error(ErrorCode::NotFound, "lease does not match task");
            }
            if !matches!(entry.state, TaskState::Queued) {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "task_ready_state",
                    "task is not queued",
                );
                return AppResponse::error(ErrorCode::InvalidState, "task is not queued");
            }
            let response = app.provider.task_ready(task_id, lease_id);
            if matches!(response, AppResponse::Ack) {
                let bundle_hash = entry.input_manifest;
                let cache = app.cache.clone();
                let config = app.config.snapshot();
                pool::spawn_task(
                    app.provider.clone(),
                    cache,
                    app.workspace.clone(),
                    config,
                    task_id,
                    bundle_hash,
                );
            }
            response
        }
        AppRequest::Status { task_id } => {
            let response = app.provider.status(task_id);
            if let AppResponse::TaskStatus { .. } = &response {
                return response;
            }
            match app.cache.cached_outputs(&task_id.0) {
                Some(outputs) => AppResponse::TaskStatus {
                    state: TaskState::Done { outputs },
                },
                None => response,
            }
        }
        AppRequest::Cancel { task_id } => {
            let leases = app.provider.lease_count_for(task_id);
            let response = app.provider.cancel(task_id);
            for _ in 0..leases {
                app.cache.release(&task_id.0);
            }
            TaskBlobStore::new(&app.workspace).remove_task_dir(&task_id.0);
            response
        }
        AppRequest::ResultAck { lease_id } => {
            let Some(task_id) = app.provider.result_ack(lease_id) else {
                return AppResponse::error(ErrorCode::NotFound, "unknown lease");
            };
            app.cache.release(&task_id.0);
            TaskBlobStore::new(&app.workspace).remove_task_dir(&task_id.0);
            AppResponse::Ack
        }
        AppRequest::BlobPut {
            task_id,
            hash,
            offset,
            data,
        } => {
            let Some(entry) = app.provider.task_entry(&task_id) else {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "blob_put_unknown",
                    "unknown task",
                );
                return AppResponse::error(ErrorCode::NotFound, "unknown task");
            };
            if entry.input_manifest != hash {
                log_task_error(
                    "remote-provider",
                    &hex_hash(&task_id.0),
                    "blob_put_hash_mismatch",
                    "bundle hash mismatch",
                );
                return AppResponse::rejected("bundle hash mismatch");
            }

            match TaskBlobStore::new(&app.workspace).put_chunk(task_id, offset, &data) {
                Ok(()) => AppResponse::Ack,
                Err(error) => AppResponse::error(ErrorCode::Internal, error.to_string()),
            }
        }
        AppRequest::BlobGet { hash, offset, len } => {
            match app.cache.read_chunk(&hash, offset, len as usize) {
                Ok(data) => AppResponse::Chunk { hash, offset, data },
                Err(error) => AppResponse::error(ErrorCode::NotFound, error.to_string()),
            }
        }
    }
}

impl Dispatch for AppState {
    fn handle<'a>(
        &'a self,
        service: &'a str,
        payload: &'a [u8],
        _control: &'a dyn ControlPlane,
    ) -> ServiceFuture<'a> {
        Box::pin(async move { handle_service_request(service, payload, self).await })
    }

    fn on_connected(&self, control: Arc<dyn ControlPlane>) {
        self.set_control(Some(control));
    }

    fn on_whoami(&self, info: &WhoamiInfo) {
        self.set_provider_id(PeerId::new(info.peer_id.clone()));
    }

    fn on_disconnected(&self, control: &Arc<dyn ControlPlane>) {
        crate::control::clear_if(&self.control, control);
    }
}

fn validate_reserve(app: &AppState, toolchain_id: &str, target: &str) -> Result<(), String> {
    let config = app.config_snapshot();
    crate::toolchain::resolve_rustc(&config, toolchain_id, target)
        .map(|_| ())
        .map_err(|error| format!("toolchain check failed: {error}"))
}

impl AppResponse {
    fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected {
            code: ErrorCode::Rejected,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile_protocol::{Hash32, OutputRef, PeerId, TaskId};
    use crate::config::ConfigHandle;
    use crate::control::new_control_handle;
    use crate::pool::{Consumer, Provider};
    use crate::test_support::TempDir;
    use std::path::Path;

    fn context(dir: &Path) -> AppState {
        AppState::new(
            Arc::new(Provider::placeholder()),
            Arc::new(Consumer::new()),
            Arc::new(crate::compile::cache::ResultCache::open(dir, 1024 * 1024).unwrap()),
            Arc::new(crate::workspace::Workspace::new(dir)),
            ConfigHandle::new(crate::config::ServiceConfig::default()),
            new_control_handle(),
        )
    }

    fn encode_request(request: AppRequest) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&Envelope::new(request), &mut bytes).unwrap();
        bytes
    }

    fn decode_response(bytes: &[u8]) -> Envelope<AppResponse> {
        ciborium::de::from_reader(bytes).unwrap()
    }

    #[test]
    fn rejects_malformed_and_unsupported_requests() {
        let dir = TempDir::new("service");
        let context = context(&dir);

        let error = handle_app_payload(&[0xff], &context).unwrap_err();
        assert_eq!(error.code, "invalid_request", "case: malformed cbor");

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(
            &Envelope {
                v: APP_PROTOCOL_VERSION + 1,
                body: AppRequest::Unsubscribe {
                    consumer: PeerId::new("consumer"),
                },
            },
            &mut bytes,
        )
        .unwrap();

        let response = decode_response(&handle_app_payload(&bytes, &context).unwrap());
        assert!(matches!(
            response.body,
            AppResponse::Error {
                code: ErrorCode::UnsupportedVersion,
                ..
            }
        ));
    }

    #[test]
    fn reserve_checks_toolchain_and_target() {
        let dir = TempDir::new("service");
        let context = context(&dir);
        let toolchain_id =
            crate::compile::manifest::toolchain_id(std::path::Path::new("rustc"), "").unwrap();

        let request = encode_request(AppRequest::Reserve {
            consumer: PeerId::new("consumer"),
            task_id: TaskId(Hash32([14; 32])),
            slots: 1,
            input_manifest: Hash32([15; 32]),
            toolchain_id,
            target: String::new(),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert!(
            matches!(response.body, AppResponse::Granted { .. }),
            "case: matching"
        );

        let request = encode_request(AppRequest::Reserve {
            consumer: PeerId::new("consumer"),
            task_id: TaskId(Hash32([10; 32])),
            slots: 1,
            input_manifest: Hash32([11; 32]),
            toolchain_id: "not-the-local-toolchain".into(),
            target: String::new(),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert!(
            matches!(
                response.body,
                AppResponse::Rejected {
                    code: ErrorCode::Rejected,
                    ..
                }
            ),
            "case: toolchain mismatch"
        );

        let request = encode_request(AppRequest::Reserve {
            consumer: PeerId::new("consumer"),
            task_id: TaskId(Hash32([12; 32])),
            slots: 1,
            input_manifest: Hash32([13; 32]),
            toolchain_id: String::new(),
            target: "definitely-not-a-target".into(),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert!(
            matches!(
                response.body,
                AppResponse::Rejected {
                    code: ErrorCode::Rejected,
                    ..
                }
            ),
            "case: uninstalled target"
        );
    }

    #[test]
    fn blob_put_and_get_round_trip() {
        let dir = TempDir::new("service");
        let context = context(&dir);
        let task_id = TaskId(Hash32([1; 32]));
        let bundle = context.cache.cas().put_bytes(b"bundle").unwrap();
        assert!(matches!(
            context
                .provider
                .reserve(PeerId::new("consumer"), task_id, 1, bundle.hash),
            AppResponse::Granted { .. }
        ));

        let request = encode_request(AppRequest::BlobPut {
            task_id,
            hash: bundle.hash,
            offset: 0,
            data: b"bundle".to_vec(),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert_eq!(response.body, AppResponse::Ack);

        let request = encode_request(AppRequest::BlobGet {
            hash: bundle.hash,
            offset: 0,
            len: 64,
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        match response.body {
            AppResponse::Chunk { data, .. } => assert_eq!(data, b"bundle"),
            other => panic!("expected chunk, got {other:?}"),
        }
    }

    fn seed_output(context: &AppState, task_id: TaskId, bytes: &[u8]) -> OutputRef {
        let blob = context.cache.cas().put_bytes(bytes).unwrap();
        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: blob.hash,
            size: blob.size,
            mode: 0o644,
        };
        assert!(
            context
                .cache
                .store(task_id.0, vec![output.clone()])
                .unwrap()
        );
        output
    }

    #[test]
    fn cache_and_result_ack_flow() {
        let dir = TempDir::new("service");
        let context = context(&dir);

        let cached_task = TaskId(Hash32([4; 32]));
        let cached_output = seed_output(&context, cached_task, b"cached");
        let request = encode_request(AppRequest::Reserve {
            consumer: PeerId::new("consumer"),
            task_id: cached_task,
            slots: 1,
            input_manifest: Hash32([5; 32]),
            toolchain_id: String::new(),
            target: String::new(),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        match response.body {
            AppResponse::Cached {
                task_id, outputs, ..
            } => {
                assert_eq!(task_id, cached_task, "case: cached task");
                assert_eq!(outputs, vec![cached_output], "case: cached outputs");
            }
            other => panic!("expected cached, got {other:?}"),
        }
        assert!(context.provider.task_entry(&cached_task).is_none());

        let ack_task = TaskId(Hash32([6; 32]));
        let ack_output = seed_output(&context, ack_task, b"done");
        let lease_id =
            match context
                .provider
                .reserve(PeerId::new("consumer"), ack_task, 1, Hash32([7; 32]))
            {
                AppResponse::Granted { lease_id, .. } => lease_id,
                other => panic!("expected granted, got {other:?}"),
            };
        assert_eq!(
            context
                .provider
                .complete_task(ack_task, vec![ack_output.clone()]),
            AppResponse::Ack
        );

        let request = encode_request(AppRequest::ResultAck { lease_id });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert_eq!(response.body, AppResponse::Ack);
        assert!(context.provider.task_entry(&ack_task).is_none());

        let request = encode_request(AppRequest::Status { task_id: ack_task });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        match response.body {
            AppResponse::TaskStatus {
                state: TaskState::Done { outputs },
            } => assert_eq!(outputs, vec![ack_output], "case: cached done status"),
            other => panic!("expected cached done status, got {other:?}"),
        }
    }
}
