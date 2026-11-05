use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;

use crate::app::AppContext;
use crate::capacity::{CapacitySample, CapacitySource, FixedSource, LinuxProcSource};
use crate::compile_protocol::PeerId;
use crate::log::{LogLevel, LogStruct};
use crate::pool;
use crate::service::{self, ServiceContext};
use crate::sidecar_protocol::{Message, PROTOCOL_VERSION, SidecarError, message_kind, read_frame};

use super::{BackendClient, SidecarOps};

/// Default timeout for control commands; must be at least the node-side
/// `dispatcher.query_timeout_secs` (60s by default).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);

async fn handshake(read_half: &mut OwnedReadHalf, client: &BackendClient) -> Result<(), String> {
    match read_frame(read_half).await {
        Ok(Message::Hello { version }) if version == PROTOCOL_VERSION => {
            client
                .write(&Message::Hello {
                    version: PROTOCOL_VERSION,
                })
                .await?;
            Ok(())
        }
        Ok(Message::Hello { version }) => Err(format!(
            "协议版本不兼容: 对端 {version}, 本端 {PROTOCOL_VERSION}"
        )),
        Ok(other) => Err(format!("期望 hello 首帧, 收到 {}", message_kind(&other))),
        Err(e) => Err(e.to_string()),
    }
}

/// Handle one node connection: handshake, then enter the read/write loop.
pub async fn handle_connection(stream: TcpStream, app: Arc<AppContext>) -> std::io::Result<()> {
    let state = app.state.clone();
    let cache = app.cache.clone();
    let config = app.config.clone();
    let link = app.link.clone();
    let (mut read_half, write_half) = stream.into_split();
    let client = BackendClient::new(write_half, CONTROL_TIMEOUT);

    if let Err(e) = handshake(&mut read_half, &client).await {
        LogStruct::new(LogLevel::Warning, "后端握手失败", e).emit();
        return Ok(());
    }
    LogStruct::new(
        LogLevel::Preset,
        "握手完成",
        format!("协议 v{PROTOCOL_VERSION}"),
    )
    .emit();
    link.set(client.clone());

    let read_client = client.clone();
    let read_context = ServiceContext {
        state: state.clone(),
        cache: cache.clone(),
        config: config.snapshot(),
    };
    let read_task = tokio::spawn(async move {
        read_loop(read_half, read_client, read_context).await;
    });

    match client.whoami().await {
        Ok(info) => {
            if let Some(peer_id) = info.get("peer_id").and_then(|value| value.as_str())
                && let Ok(mut state) = state.lock()
            {
                state.set_provider_id(PeerId::new(peer_id));
            }
            LogStruct::new(LogLevel::Important, "节点身份", info.to_string()).emit();
        }
        Err(e) => LogStruct::new(
            LogLevel::Warning,
            "节点身份查询失败",
            format!("{}: {}", e.code, e.message),
        )
        .emit(),
    }

    let source: Box<dyn CapacitySource + Send> = match LinuxProcSource::new() {
        Ok(source) => Box::new(source),
        Err(error) => {
            LogStruct::new(
                LogLevel::Warning,
                "容量采样初始化失败",
                format!("{error}; 使用固定回退容量"),
            )
            .emit();
            Box::new(FixedSource::new(CapacitySample {
                total_cores: 1.0,
                used_cores: 0.0,
                mem_total_mb: 1024,
                mem_available_mb: 512,
            }))
        }
    };
    let ops = Arc::new(SidecarOps::new(client.clone()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let runtime_handle = tokio::spawn(pool::run_pool(
        ops,
        state.clone(),
        config.snapshot(),
        shutdown_rx,
        source,
    ));

    let _ = read_task.await;
    let _ = shutdown_tx.send(true);
    let _ = runtime_handle.await;
    link.clear();
    Ok(())
}

/// Read node messages: dispatch service requests and wake control waiters on replies.
async fn read_loop(mut read_half: OwnedReadHalf, client: BackendClient, context: ServiceContext) {
    loop {
        match read_frame(&mut read_half).await {
            Ok(Message::Request {
                id,
                service,
                payload,
            }) => {
                let client = client.clone();
                let context = context.clone();
                tokio::spawn(async move {
                    let reply =
                        match service::handle_service_request(&service, &payload, context).await {
                            Ok(result) => Message::Reply {
                                id,
                                ok: true,
                                result: Some(result),
                                error: None,
                            },
                            Err(e) => Message::Reply {
                                id,
                                ok: false,
                                result: None,
                                error: Some(e),
                            },
                        };
                    if let Err(e) = client.write(&reply).await {
                        LogStruct::new(LogLevel::Critical, "回复写入失败", e).emit();
                    }
                });
            }
            Ok(Message::Reply {
                id,
                ok,
                result,
                error,
            }) => {
                let sender = client.take_pending(id).await;
                match sender {
                    Some(tx) => {
                        let outcome = if ok {
                            Ok(result.unwrap_or_default())
                        } else {
                            Err(error.unwrap_or_else(|| {
                                SidecarError::new("backend_error", "unknown error")
                            }))
                        };
                        let _ = tx.send(outcome);
                    }
                    None => {
                        LogStruct::new(LogLevel::Warning, "未知响应", format!("未匹配的 id {id}"))
                            .emit();
                    }
                }
            }
            Ok(other) => {
                LogStruct::new(
                    LogLevel::Warning,
                    "非预期消息",
                    format!("节点不应发送 {}", message_kind(&other)),
                )
                .emit();
            }
            Err(e) => {
                LogStruct::new(LogLevel::Warning, "连接断开", e.to_string()).emit();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::cache::ResultCache;
    use crate::compile_protocol::{AppRequest, AppResponse, Envelope};
    use crate::config::ConfigHandle;
    use crate::control::PublicIpInfo;
    use crate::sidecar_protocol::write_frame;
    use crate::state::NodeState;
    use crate::transport::NodeLink;
    use std::sync::Mutex as StdMutex;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    fn test_cache() -> Arc<ResultCache> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("nexuscompile-connection-{unique}"));
        Arc::new(ResultCache::open(&dir, 1024 * 1024).unwrap())
    }

    async fn spawn_server() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let state = Arc::new(StdMutex::new(NodeState::new(
                PeerId::new("test-peer"),
                1,
                "",
                "",
            )));
            let config = ConfigHandle::new(crate::config::ServiceConfig::default());
            let cache = test_cache();
            let link = Arc::new(NodeLink::new());
            let app = Arc::new(AppContext::new(state, cache, config, link));
            let _ = handle_connection(stream, app).await;
        });
        (port, handle)
    }

    async fn connect(port: u16) -> TcpStream {
        TcpStream::connect(("127.0.0.1", port)).await.unwrap()
    }

    fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(v, &mut buf).unwrap();
        buf
    }

    #[tokio::test]
    async fn handshake_ok() {
        let (port, _srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        write_frame(
            &mut w,
            &Message::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();

        match read_frame(&mut r).await.unwrap() {
            Message::Hello { version } => assert_eq!(version, PROTOCOL_VERSION),
            other => panic!("expected hello, got {}", message_kind(&other)),
        }
    }

    #[tokio::test]
    async fn handshake_version_mismatch_closes() {
        let (port, srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        write_frame(&mut w, &Message::Hello { version: 999 })
            .await
            .unwrap();

        assert!(read_frame(&mut r).await.is_err());
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_non_hello_closes() {
        let (port, srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        write_frame(&mut w, &Message::ListServices { id: Uuid::new_v4() })
            .await
            .unwrap();

        assert!(read_frame(&mut r).await.is_err());
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn service_request_round_trip() {
        let (port, _srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        write_frame(
            &mut w,
            &Message::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();
        read_frame(&mut r).await.unwrap();

        match read_frame(&mut r).await.unwrap() {
            Message::Whoami { id } => {
                write_frame(
                    &mut w,
                    &Message::Reply {
                        id,
                        ok: true,
                        result: Some(enc(&serde_json::json!({"peer_id": "test-peer"}))),
                        error: None,
                    },
                )
                .await
                .unwrap();
            }
            other => panic!("expected whoami, got {}", message_kind(&other)),
        }

        let id = Uuid::new_v4();
        let payload = enc(&Envelope::new(AppRequest::Subscribe {
            consumer: PeerId::new("consumer"),
            toolchain: String::new(),
            target: String::new(),
        }));
        write_frame(
            &mut w,
            &Message::Request {
                id,
                service: "rustc-pool".into(),
                payload,
            },
        )
        .await
        .unwrap();

        match read_frame(&mut r).await.unwrap() {
            Message::Reply {
                id: rid,
                ok,
                result,
                error,
            } => {
                assert_eq!(rid, id);
                assert!(ok);
                assert!(error.is_none());

                let response: Envelope<AppResponse> =
                    ciborium::de::from_reader(result.unwrap().as_slice()).unwrap();
                match response.body {
                    AppResponse::Subscribed {
                        provider,
                        toolchain,
                        target,
                        max_slots,
                    } => {
                        assert_eq!(provider, PeerId::new("test-peer"));
                        assert!(toolchain.is_empty());
                        assert!(target.is_empty());
                        assert!(max_slots >= 1);
                    }
                    other => panic!("expected subscribed, got {other:?}"),
                }
            }
            other => panic!("expected reply, got {}", message_kind(&other)),
        }
    }

    #[tokio::test]
    async fn control_call_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let node = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut r, mut w) = stream.into_split();
            match read_frame(&mut r).await.unwrap() {
                Message::QueryPublicIp { id } => {
                    let body = enc(&PublicIpInfo {
                        ipv4: Some("1.2.3.4".into()),
                        ipv6: None,
                    });
                    write_frame(
                        &mut w,
                        &Message::Reply {
                            id,
                            ok: true,
                            result: Some(body),
                            error: None,
                        },
                    )
                    .await
                    .unwrap();
                }
                other => panic!("unexpected {}", message_kind(&other)),
            }
        });

        let stream = connect(addr.port()).await;
        let (r, w) = stream.into_split();
        let client = BackendClient::new(w, Duration::from_secs(5));

        let state = Arc::new(StdMutex::new(NodeState::placeholder()));
        let context = ServiceContext {
            state,
            cache: test_cache(),
            config: crate::config::ServiceConfig::default(),
        };
        let read_task = tokio::spawn(read_loop(r, client.clone(), context));
        let info = client.query_public_ip().await.unwrap();
        assert_eq!(info.ipv4.as_deref(), Some("1.2.3.4"));
        assert!(info.ipv6.is_none());

        node.await.unwrap();
        read_task.abort();
    }
}
