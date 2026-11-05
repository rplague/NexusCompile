use std::sync::{Arc, Mutex};

use crate::compile::cache::ResultCache;
use crate::compile_protocol::{
    APP_PROTOCOL_VERSION, AppRequest, AppResponse, Envelope, ErrorCode, LeaseId, TaskState,
};
use crate::config::ServiceConfig;
use crate::log::{LogLevel, LogStruct};
use crate::pool;
use crate::sidecar_protocol::SidecarError;
use crate::state::NodeState;
use crate::task_blob::TaskBlobStore;

#[derive(Clone)]
pub struct ServiceContext {
    pub state: Arc<Mutex<NodeState>>,
    pub cache: Arc<ResultCache>,
    pub config: ServiceConfig,
}

/// Handle one inbound sidecar service request.
pub async fn handle_service_request(
    service: &str,
    payload: &[u8],
    context: ServiceContext,
) -> Result<Vec<u8>, SidecarError> {
    LogStruct::new(
        LogLevel::Debug,
        "服务请求",
        format!("service={}, payload_len={}", service, payload.len()),
    )
    .emit();

    handle_app_payload(payload, &context)
}

/// Decode, dispatch, and encode one application request.
pub(crate) fn handle_app_payload(
    payload: &[u8],
    context: &ServiceContext,
) -> Result<Vec<u8>, SidecarError> {
    let envelope: Envelope<AppRequest> = ciborium::de::from_reader(payload)
        .map_err(|e| SidecarError::new("invalid_request", e.to_string()))?;

    let response = if envelope.v != APP_PROTOCOL_VERSION {
        AppResponse::error(
            ErrorCode::UnsupportedVersion,
            format!("unsupported version {}", envelope.v),
        )
    } else {
        dispatch(envelope.body, context)
    };

    let mut result = Vec::new();
    ciborium::ser::into_writer(&Envelope::new(response), &mut result)
        .map_err(|e| SidecarError::new("encode_error", e.to_string()))?;
    Ok(result)
}

fn dispatch(request: AppRequest, context: &ServiceContext) -> AppResponse {
    let Ok(mut state) = context.state.lock() else {
        return AppResponse::error(ErrorCode::Internal, "state lock poisoned");
    };

    match request {
        AppRequest::Subscribe {
            consumer,
            toolchain,
            target,
        } => state.subscribe(consumer, toolchain, target),
        AppRequest::Unsubscribe { consumer } => state.unsubscribe(&consumer),
        AppRequest::CapacityUpdate {
            provider,
            seq,
            free_slots,
            queue_len,
            accepting,
        } => state.apply_capacity(provider, seq, free_slots, queue_len, accepting),
        AppRequest::Reserve {
            consumer,
            task_id,
            slots,
            input_manifest,
        } => {
            if state.task_entry(&task_id).is_none()
                && let Some(outputs) = context.cache.cached_outputs(&task_id.0)
            {
                return AppResponse::Cached {
                    task_id,
                    outputs,
                    lease_id: LeaseId::new(),
                };
            }
            state.reserve(consumer, task_id, slots, input_manifest)
        }
        AppRequest::TaskReady { task_id, lease_id } => {
            let Some(entry) = state.task_entry(&task_id) else {
                return AppResponse::error(ErrorCode::NotFound, "unknown task");
            };
            if entry.lease_id != lease_id {
                return AppResponse::error(ErrorCode::NotFound, "lease does not match task");
            }
            if !matches!(entry.state, TaskState::Queued) {
                return AppResponse::error(ErrorCode::InvalidState, "task is not queued");
            }
            let response = state.task_ready(task_id, lease_id);
            if matches!(response, AppResponse::Ack) {
                let bundle_hash = entry.input_manifest;
                let state = context.state.clone();
                let cache = context.cache.clone();
                let config = context.config.clone();
                pool::spawn_task(state, cache, config, task_id, bundle_hash);
            }
            response
        }
        AppRequest::Status { task_id } => {
            let response = state.status(task_id);
            if let AppResponse::TaskStatus { .. } = &response {
                return response;
            }
            match context.cache.cached_outputs(&task_id.0) {
                Some(outputs) => AppResponse::TaskStatus {
                    state: TaskState::Done { outputs },
                },
                None => response,
            }
        }
        AppRequest::Cancel { task_id } => {
            let response = state.cancel(task_id);
            context.cache.release(&task_id.0);
            TaskBlobStore::new(&context.cache).remove_task_dir(&task_id.0);
            response
        }
        AppRequest::ResultAck { lease_id } => {
            let Some(task_id) = state.result_ack(lease_id) else {
                return AppResponse::error(ErrorCode::NotFound, "unknown lease");
            };
            context.cache.release(&task_id.0);
            TaskBlobStore::new(&context.cache).remove_task_dir(&task_id.0);
            AppResponse::Ack
        }
        AppRequest::BlobPut {
            task_id,
            hash,
            offset,
            data,
        } => {
            let Some(entry) = state.task_entry(&task_id) else {
                return AppResponse::error(ErrorCode::NotFound, "unknown task");
            };
            if entry.input_manifest != hash {
                return AppResponse::rejected("bundle hash mismatch");
            }
            drop(state);

            match TaskBlobStore::new(&context.cache).put_chunk(task_id, offset, &data) {
                Ok(()) => AppResponse::Ack,
                Err(error) => AppResponse::error(ErrorCode::Internal, error.to_string()),
            }
        }
        AppRequest::BlobGet { hash, offset, len } => {
            match context.cache.read_chunk(&hash, offset, len as usize) {
                Ok(data) => AppResponse::Chunk { hash, offset, data },
                Err(error) => AppResponse::error(ErrorCode::NotFound, error.to_string()),
            }
        }
    }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn context() -> ServiceContext {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let cache_dir = std::env::temp_dir().join(format!("nexuscompile-service-{unique}"));
        ServiceContext {
            state: Arc::new(Mutex::new(NodeState::placeholder())),
            cache: Arc::new(ResultCache::open(&cache_dir, 1024 * 1024).unwrap()),
            config: ServiceConfig::default(),
        }
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
    fn subscribe_returns_provider_limits() {
        let context = context();
        let request = encode_request(AppRequest::Subscribe {
            consumer: PeerId::new("consumer"),
            toolchain: "rustc".into(),
            target: "x86_64".into(),
        });

        let response = handle_app_payload(&request, &context).unwrap();
        let response = decode_response(&response);

        assert_eq!(
            response.body,
            AppResponse::Subscribed {
                provider: PeerId::new("local"),
                toolchain: String::new(),
                target: String::new(),
                max_slots: 1,
            }
        );
    }

    #[test]
    fn invalid_cbor_is_transport_error() {
        let context = context();
        let error = handle_app_payload(&[0xff], &context).unwrap_err();
        assert_eq!(error.code, "invalid_request");
    }

    #[test]
    fn unsupported_version_is_application_error() {
        let context = context();
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
    fn blob_put_and_get_round_trip() {
        let context = context();
        let task_id = TaskId(Hash32([1; 32]));
        let bundle = context.cache.cas().put_bytes(b"bundle").unwrap();
        {
            let mut state = context.state.lock().unwrap();
            assert!(matches!(
                state.reserve(PeerId::new("consumer"), task_id, 1, bundle.hash),
                AppResponse::Granted { .. }
            ));
        }

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

    #[test]
    fn status_of_unknown_task_is_not_found() {
        let context = context();
        let request = encode_request(AppRequest::Status {
            task_id: TaskId(Hash32([3; 32])),
        });

        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert!(matches!(
            response.body,
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }

    fn seed_output(context: &ServiceContext, task_id: TaskId, bytes: &[u8]) -> OutputRef {
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
    fn reserve_uses_cache_when_task_entry_is_missing() {
        let context = context();
        let task_id = TaskId(Hash32([4; 32]));
        let output = seed_output(&context, task_id, b"cached");

        let request = encode_request(AppRequest::Reserve {
            consumer: PeerId::new("consumer"),
            task_id,
            slots: 1,
            input_manifest: Hash32([5; 32]),
        });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());

        match response.body {
            AppResponse::Cached {
                task_id: cached_id,
                outputs,
                ..
            } => {
                assert_eq!(cached_id, task_id);
                assert_eq!(outputs, vec![output]);
            }
            other => panic!("expected cached, got {other:?}"),
        }
        assert!(context.state.lock().unwrap().task_entry(&task_id).is_none());
    }

    #[test]
    fn result_ack_removes_done_entry_and_status_uses_cache() {
        let context = context();
        let task_id = TaskId(Hash32([6; 32]));
        let output = seed_output(&context, task_id, b"done");

        let lease_id = {
            let mut state = context.state.lock().unwrap();
            let lease_id = match state.reserve(PeerId::new("consumer"), task_id, 1, Hash32([7; 32]))
            {
                AppResponse::Granted { lease_id, .. } => lease_id,
                other => panic!("expected granted, got {other:?}"),
            };
            assert_eq!(
                state.complete_task(task_id, vec![output.clone()]),
                AppResponse::Ack
            );
            lease_id
        };

        let request = encode_request(AppRequest::ResultAck { lease_id });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        assert_eq!(response.body, AppResponse::Ack);
        assert!(context.state.lock().unwrap().task_entry(&task_id).is_none());

        let request = encode_request(AppRequest::Status { task_id });
        let response = decode_response(&handle_app_payload(&request, &context).unwrap());
        match response.body {
            AppResponse::TaskStatus {
                state: TaskState::Done { outputs },
            } => assert_eq!(outputs, vec![output]),
            other => panic!("expected cached done status, got {other:?}"),
        }
    }
}
