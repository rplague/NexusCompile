//! Connection to the NexusNet node: handshake, read/write loop, control handle.
//!
//! The node dials in and completes the `hello` handshake. Afterwards the node
//! forwards `request { id, service, payload }` frames and this backend can send
//! control commands back through [`BackendClient`].

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, oneshot};
use tokio::time::timeout;
use uuid::Uuid;

use crate::control::ControlPlane;
use crate::log::{LogLevel, LogStruct};
use crate::protocol::{
    self, AddKeyResult, Message, PROTOCOL_VERSION, PqStatusResult, PublicIpInfo, QueryKeyResult,
    RelayStatusResult, SidecarError, SuccessResult, WhoamiInfo,
};
use crate::service::Dispatch;

/// Default timeout for control commands; must be at least the node-side
/// `dispatcher.query_timeout_secs` (60s by default).
const CONTROL_TIMEOUT: Duration = Duration::from_secs(60);

type PendingMap = Arc<Mutex<HashMap<Uuid, oneshot::Sender<Result<Vec<u8>, SidecarError>>>>>;

struct Shared {
    writer: Mutex<OwnedWriteHalf>,
    pending: PendingMap,
}

/// Control handle back to the local node; cloneable across tasks.
#[derive(Clone)]
pub struct BackendClient {
    shared: Arc<Shared>,
    timeout: Duration,
}

impl BackendClient {
    async fn call(&self, id: Uuid, msg: Message) -> Result<Vec<u8>, SidecarError> {
        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().await.insert(id, tx);

        if let Err(e) = write_msg(&self.shared.writer, &msg).await {
            self.shared.pending.lock().await.remove(&id);
            return Err(SidecarError::new("io_error", e));
        }

        match timeout(self.timeout, rx).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(SidecarError::new("node_gone", "response channel closed")),
            Err(_) => {
                self.shared.pending.lock().await.remove(&id);
                Err(SidecarError::new("timeout", "control command timed out"))
            }
        }
    }

    async fn call_result<T: DeserializeOwned>(
        &self,
        id: Uuid,
        msg: Message,
    ) -> Result<T, SidecarError> {
        let bytes = self.call(id, msg).await?;
        protocol::decode_result(&bytes)
    }

    pub async fn list_services(&self) -> Result<Vec<String>, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ListServices { id }).await
    }

    pub async fn discover_providers(&self, service: &str) -> Result<Vec<String>, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::DiscoverProviders {
                id,
                service: service.to_string(),
            },
        )
        .await
    }

    pub async fn query_public_ip(&self) -> Result<PublicIpInfo, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::QueryPublicIp { id }).await
    }

    pub async fn whoami(&self) -> Result<WhoamiInfo, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::Whoami { id }).await
    }

    pub async fn reconnect_bootstrap(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReconnectBootstrap { id })
            .await
    }

    pub async fn reannounce_services(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReannounceServices { id })
            .await
    }

    pub async fn reload_config(&self) -> Result<SuccessResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::ReloadConfig { id }).await
    }

    pub async fn relay_status(&self) -> Result<RelayStatusResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::RelayStatus { id }).await
    }

    pub async fn pq_status(&self) -> Result<PqStatusResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::PqStatus { id }).await
    }

    pub async fn auth_status(&self) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::AuthStatus { id }).await
    }

    pub async fn refresh_cache(&self) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(id, Message::RefreshCache { id }).await
    }

    pub async fn remove_key(
        &self,
        key: &str,
        providing: bool,
        record: bool,
    ) -> Result<serde_json::Value, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::RemoveKey {
                id,
                key: key.to_string(),
                providing,
                record,
            },
        )
        .await
    }

    pub async fn query_key(&self, key: &str) -> Result<QueryKeyResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::QueryKey {
                id,
                key: key.to_string(),
            },
        )
        .await
    }

    pub async fn add_key(
        &self,
        key: &str,
        value: Option<Vec<u8>>,
        providing: bool,
    ) -> Result<AddKeyResult, SidecarError> {
        let id = Uuid::new_v4();
        self.call_result(
            id,
            Message::AddKey {
                id,
                key: key.to_string(),
                value,
                providing,
            },
        )
        .await
    }

    pub async fn service_request(
        &self,
        service: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        let id = Uuid::new_v4();
        self.call(
            id,
            Message::ServiceRequest {
                id,
                service: service.to_string(),
                payload,
            },
        )
        .await
    }

    pub async fn service_request_to(
        &self,
        service: &str,
        peer: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, SidecarError> {
        let id = Uuid::new_v4();
        self.call(
            id,
            Message::ServiceRequestTo {
                id,
                service: service.to_string(),
                peer: peer.to_string(),
                payload,
            },
        )
        .await
    }
}

pub(crate) type PendingSender = oneshot::Sender<Result<Vec<u8>, SidecarError>>;

impl BackendClient {
    pub(crate) fn new(writer: OwnedWriteHalf, timeout: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                writer: Mutex::new(writer),
                pending: Arc::new(Mutex::new(HashMap::new())),
            }),
            timeout,
        }
    }

    pub(crate) async fn write(&self, msg: &Message) -> Result<(), String> {
        write_msg(&self.shared.writer, msg).await
    }

    pub(crate) async fn take_pending(&self, id: Uuid) -> Option<PendingSender> {
        self.shared.pending.lock().await.remove(&id)
    }
}

async fn write_msg(writer: &Mutex<OwnedWriteHalf>, msg: &Message) -> Result<(), String> {
    let mut w = writer.lock().await;
    protocol::write_frame(&mut *w, msg)
        .await
        .map_err(|e| e.to_string())
}

async fn handshake(read_half: &mut OwnedReadHalf, client: &BackendClient) -> Result<(), String> {
    match protocol::read_frame(read_half).await {
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
        Ok(other) => Err(format!("期望 hello 首帧, 收到 {}", other.kind())),
        Err(e) => Err(e.to_string()),
    }
}

/// Handle one node connection: handshake, notify dispatch, then read loop.
pub async fn handle_connection<D: Dispatch>(stream: TcpStream, dispatch: D) -> std::io::Result<()> {
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

    let control: Arc<dyn ControlPlane> = Arc::new(client.clone());
    dispatch.on_connected(control.clone());

    // Start the read loop before issuing control calls: replies can only be
    // correlated while the loop is draining the socket.
    let read_client = client.clone();
    let read_dispatch = dispatch.clone();
    let read_control = control.clone();
    let read_task = tokio::spawn(async move {
        read_loop(read_half, read_client, read_dispatch, read_control).await;
    });

    match client.whoami().await {
        Ok(info) => {
            LogStruct::new(
                LogLevel::Important,
                "节点身份",
                format!("{} ({})", info.peer_id, info.name),
            )
            .emit();
            dispatch.on_whoami(&info);
        }
        Err(e) => LogStruct::new(
            LogLevel::Warning,
            "节点身份查询失败",
            format!("{}: {}", e.code, e.message),
        )
        .emit(),
    }

    let _ = read_task.await;
    dispatch.on_disconnected(&control);
    Ok(())
}

/// Read node messages: dispatch service requests and wake control waiters.
async fn read_loop<D: Dispatch>(
    mut read_half: OwnedReadHalf,
    client: BackendClient,
    dispatch: D,
    control: Arc<dyn ControlPlane>,
) {
    loop {
        match protocol::read_frame(&mut read_half).await {
            Ok(Message::Request {
                id,
                service,
                payload,
            }) => {
                let client = client.clone();
                let dispatch = dispatch.clone();
                let control = control.clone();
                tokio::spawn(async move {
                    let reply = match dispatch.handle(&service, &payload, control.as_ref()).await {
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
                    format!("节点不应发送 {}", other.kind()),
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
    use crate::service::ServiceFuture;
    use tokio::net::TcpListener;

    /// Echo dispatcher for connection tests.
    #[derive(Clone)]
    struct Echo;

    impl Dispatch for Echo {
        fn handle<'a>(
            &'a self,
            _service: &'a str,
            payload: &'a [u8],
            _control: &'a dyn ControlPlane,
        ) -> ServiceFuture<'a> {
            Box::pin(async move { Ok(payload.to_vec()) })
        }
    }

    async fn spawn_server() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(stream, Echo).await;
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

        protocol::write_frame(
            &mut w,
            &Message::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();

        match protocol::read_frame(&mut r).await.unwrap() {
            Message::Hello { version } => assert_eq!(version, PROTOCOL_VERSION),
            other => panic!("expected hello, got {}", other.kind()),
        }
    }

    #[tokio::test]
    async fn handshake_version_mismatch_closes() {
        let (port, srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        protocol::write_frame(&mut w, &Message::Hello { version: 999 })
            .await
            .unwrap();

        assert!(protocol::read_frame(&mut r).await.is_err());
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn handshake_non_hello_closes() {
        let (port, srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        protocol::write_frame(&mut w, &Message::ListServices { id: Uuid::new_v4() })
            .await
            .unwrap();

        assert!(protocol::read_frame(&mut r).await.is_err());
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn service_request_round_trip() {
        let (port, _srv) = spawn_server().await;
        let mut stream = connect(port).await;
        let (mut r, mut w) = stream.split();

        protocol::write_frame(
            &mut w,
            &Message::Hello {
                version: PROTOCOL_VERSION,
            },
        )
        .await
        .unwrap();
        protocol::read_frame(&mut r).await.unwrap();

        // whoami is sent by the server right after handshake.
        match protocol::read_frame(&mut r).await.unwrap() {
            Message::Whoami { id } => {
                protocol::write_frame(
                    &mut w,
                    &Message::Reply {
                        id,
                        ok: true,
                        result: Some(enc(&WhoamiInfo {
                            peer_id: "test-peer".into(),
                            name: String::new(),
                            description: String::new(),
                        })),
                        error: None,
                    },
                )
                .await
                .unwrap();
            }
            other => panic!("expected whoami, got {}", other.kind()),
        }

        let id = Uuid::new_v4();
        protocol::write_frame(
            &mut w,
            &Message::Request {
                id,
                service: "rustc-pool".into(),
                payload: b"ping".to_vec(),
            },
        )
        .await
        .unwrap();

        match protocol::read_frame(&mut r).await.unwrap() {
            Message::Reply {
                id: rid,
                ok,
                result,
                error,
            } => {
                assert_eq!(rid, id);
                assert!(ok);
                assert_eq!(result.unwrap(), b"ping");
                assert!(error.is_none());
            }
            other => panic!("expected reply, got {}", other.kind()),
        }
    }

    #[tokio::test]
    async fn control_call_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let node = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut r, mut w) = stream.into_split();
            match protocol::read_frame(&mut r).await.unwrap() {
                Message::QueryPublicIp { id } => {
                    let body = enc(&PublicIpInfo {
                        ipv4: Some("1.2.3.4".into()),
                        ipv6: None,
                    });
                    protocol::write_frame(
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
                other => panic!("unexpected {}", other.kind()),
            }
        });

        let stream = connect(addr.port()).await;
        let (r, w) = stream.into_split();
        let client = BackendClient::new(w, Duration::from_secs(5));
        let context = crate::state::AppState::new(
            Arc::new(crate::pool::Provider::placeholder()),
            Arc::new(crate::pool::Consumer::new()),
            Arc::new(crate::compile::cache::ResultCache::disabled(
                std::env::temp_dir(),
            )),
            crate::config::ConfigHandle::new(crate::config::ServiceConfig::default()),
            crate::control::new_control_handle(),
        );
        let read_task = tokio::spawn(read_loop(
            r,
            client.clone(),
            context,
            Arc::new(client.clone()),
        ));
        let info = client.query_public_ip().await.unwrap();
        assert_eq!(info.ipv4.as_deref(), Some("1.2.3.4"));
        assert!(info.ipv6.is_none());

        node.await.unwrap();
        read_task.abort();
    }
}
