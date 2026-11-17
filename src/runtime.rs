//! Process runtime: startup, server loop and graceful shutdown.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::capacity::{
    CapacitySample, CapacitySource, FixedSource, LinuxProcSource, calculate_capacity,
};
use crate::compile::cache::ResultCache;
use crate::compile_protocol::PeerId;
use crate::config::{ConfigError, ConfigHandle, ServiceConfig};
use crate::connection;
use crate::control::{self, ControlHandle};
use crate::local_task;
use crate::log::{LogLevel, LogStruct};
use crate::paths;
use crate::pool;
use crate::pool::{Consumer, Provider};
use crate::remote::Coordinator;
use crate::state::AppState;
use crate::workspace::Workspace;

/// Runtime startup options.
#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub config_path: PathBuf,
    pub task_socket: PathBuf,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            config_path: paths::config_path(),
            task_socket: paths::task_socket_path(),
        }
    }
}

/// Runtime startup failure.
#[derive(Debug)]
pub enum RuntimeError {
    Config(ConfigError),
    Bind { address: String, source: io::Error },
    Io(io::Error),
    Server(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(f, "配置加载失败: {error}"),
            Self::Bind { address, source } => write!(f, "绑定 {address} 失败: {source}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Server(message) => write!(f, "服务运行失败: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<ConfigError> for RuntimeError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<io::Error> for RuntimeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

struct RuntimeInner {
    shutdown_tx: watch::Sender<bool>,
    server_done_rx: watch::Receiver<Option<Result<(), String>>>,
    server_task: Mutex<Option<JoinHandle<()>>>,
    config: ConfigHandle,
    local_addr: SocketAddr,
}

/// A running NexusCompile node. Cheap to clone; all clones share one runtime.
#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    pub fn config(&self) -> ConfigHandle {
        self.inner.config.clone()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Wait until the server loop exits on its own.
    pub async fn wait(&self) -> Result<(), RuntimeError> {
        let mut done = self.inner.server_done_rx.clone();
        loop {
            if let Some(result) = done.borrow().clone() {
                return result.map_err(RuntimeError::Server);
            }
            if done.changed().await.is_err() {
                return Err(RuntimeError::Server(
                    "server task ended without a result".into(),
                ));
            }
        }
    }

    /// Stop the server and wait for its tasks to finish.
    pub async fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.send(true);
        let task = self.inner.server_task.lock().await.take();
        if let Some(mut task) = task
            && tokio::time::timeout(Duration::from_secs(8), &mut task)
                .await
                .is_err()
        {
            task.abort();
            LogStruct::new(
                LogLevel::Warning,
                "服务关闭超时",
                "server task did not stop within 8s; aborted",
            )
            .emit();
        }
    }
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.server_task.get_mut().take() {
            task.abort();
        }
    }
}

/// Load configuration, bind the listener and start all runtime tasks.
pub async fn start(options: RuntimeOptions) -> Result<RuntimeHandle, RuntimeError> {
    let config_handle = ConfigHandle::try_from_toml_file(&options.config_path, true)?;
    let config = config_handle.snapshot();
    for warning in config.warnings() {
        LogStruct::new(LogLevel::Warning, "配置警告", warning.to_string()).emit();
    }

    let address = format!("127.0.0.1:{}", config_handle.port());
    let listener = TcpListener::bind(&address)
        .await
        .map_err(|source| RuntimeError::Bind {
            address: address.clone(),
            source,
        })?;
    let local_addr = listener.local_addr()?;

    let mut source = capacity_source();
    let initial = match source.sample() {
        Ok(sample) => calculate_capacity(&sample, 0, config.node.max_usage_percent),
        Err(error) => {
            LogStruct::new(
                LogLevel::Warning,
                "初始容量采样失败",
                format!("{error}; 使用回退容量"),
            )
            .emit();
            calculate_capacity(&fallback_sample(), 0, config.node.max_usage_percent)
        }
    };

    let provider = Arc::new(Provider::new(PeerId::new("local"), initial.max_slots));
    provider.set_capacity(initial.max_slots, initial.free_slots);
    provider.set_capabilities(crate::toolchain::capabilities(&config));
    let consumer = Arc::new(Consumer::new());
    LogStruct::new(
        LogLevel::Important,
        "服务启动",
        format!(
            "{address}，service={}，max_slots={}，free_slots={}，percent={}%",
            config.service_name,
            initial.max_slots,
            initial.free_slots,
            config.node.max_usage_percent
        ),
    )
    .emit();

    let cache = open_cache(&config);
    let workspace = Arc::new(Workspace::new(cache.root().to_path_buf()));
    if let Err(error) = workspace.cleanup_staging() {
        LogStruct::new(LogLevel::Warning, "清理 staging 失败", error.to_string()).emit();
    }
    let control = new_control();
    let app = AppState::new(
        provider.clone(),
        consumer.clone(),
        cache.clone(),
        workspace.clone(),
        config_handle.clone(),
        control.clone(),
    );
    let coordinator = Arc::new(Coordinator::new(
        provider.clone(),
        consumer.clone(),
        cache.clone(),
        workspace.clone(),
        control.clone(),
        config.clone(),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(None);
    let services = RuntimeServices {
        app,
        coordinator,
        cache,
        workspace,
        control,
        provider,
        consumer,
        config,
        source,
        task_socket: options.task_socket,
    };
    let server_task = tokio::spawn(async move {
        let result = run_server(listener, services, shutdown_rx).await;
        let _ = done_tx.send(Some(result));
    });
    Ok(RuntimeHandle {
        inner: Arc::new(RuntimeInner {
            shutdown_tx,
            server_done_rx: done_rx,
            server_task: Mutex::new(Some(server_task)),
            config: config_handle,
            local_addr,
        }),
    })
}

struct RuntimeServices {
    app: AppState,
    coordinator: Arc<Coordinator>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    control: ControlHandle,
    provider: Arc<Provider>,
    consumer: Arc<Consumer>,
    config: ServiceConfig,
    source: Box<dyn CapacitySource + Send>,
    task_socket: PathBuf,
}

struct ServerTasks {
    cache_sweeper: Option<JoinHandle<()>>,
    pool: Option<JoinHandle<()>>,
    task_service: Option<JoinHandle<()>>,
    task_socket: PathBuf,
}

impl ServerTasks {
    async fn shutdown(&mut self) {
        if let Some(task) = self.cache_sweeper.take() {
            task.abort();
        }
        if let Some(mut task) = self.pool.take()
            && tokio::time::timeout(Duration::from_secs(3), &mut task)
                .await
                .is_err()
        {
            task.abort();
        }
        if let Some(task) = self.task_service.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.task_socket);
    }
}

impl Drop for ServerTasks {
    fn drop(&mut self) {
        if let Some(task) = self.cache_sweeper.take() {
            task.abort();
        }
        if let Some(task) = self.pool.take() {
            task.abort();
        }
        if let Some(task) = self.task_service.take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.task_socket);
    }
}

async fn run_server(
    listener: TcpListener,
    services: RuntimeServices,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), String> {
    let RuntimeServices {
        app,
        coordinator,
        cache,
        workspace,
        control,
        provider,
        consumer,
        config,
        source,
        task_socket,
    } = services;
    let cache_sweep_task = spawn_cache_sweeper(cache.clone());
    let pool_task = tokio::spawn(pool::run_pool(
        control,
        provider,
        consumer,
        cache.clone(),
        config.clone(),
        shutdown_rx.clone(),
        source,
    ));
    let task_service = bind_task_service(&task_socket, coordinator, cache.clone(), workspace);

    let mut tasks = ServerTasks {
        cache_sweeper: Some(cache_sweep_task),
        pool: Some(pool_task),
        task_service,
        task_socket: task_socket.clone(),
    };
    let mut connections = JoinSet::new();
    let result: Result<(), String> = loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        LogStruct::new(LogLevel::Preset, "节点接入", addr.to_string()).emit();
                        let app = app.clone();
                        connections.spawn(async move {
                            if let Err(error) = connection::handle_connection(stream, app).await {
                                LogStruct::new(
                                    LogLevel::Critical,
                                    "连接错误",
                                    format!("位于 {addr}: {error}"),
                                )
                                .emit();
                            }
                        });
                    }
                    Err(error) if is_transient_accept_error(&error) => {
                        LogStruct::new(LogLevel::Warning, "接受连接失败", error.to_string()).emit();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        LogStruct::new(LogLevel::Critical, "监听失败", error.to_string()).emit();
                        break Err(format!("listener accept failed: {error}"));
                    }
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    LogStruct::new(LogLevel::Warning, "连接任务异常", error.to_string()).emit();
                }
            }
            _ = shutdown_rx.changed() => break Ok(()),
        }
    };

    drop(listener);
    tasks.shutdown().await;
    connections.shutdown().await;
    result
}

fn spawn_cache_sweeper(cache: Arc<ResultCache>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            cache.sweep();
        }
    })
}

fn bind_task_service(
    socket: &std::path::Path,
    coordinator: Arc<Coordinator>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
) -> Option<JoinHandle<()>> {
    match local_task::bind(socket) {
        Ok(listener) => {
            LogStruct::new(
                LogLevel::Important,
                "本地任务 socket",
                socket.display().to_string(),
            )
            .emit();
            Some(tokio::spawn(local_task::run(
                listener,
                coordinator,
                cache,
                workspace,
                tokio::runtime::Handle::current(),
            )))
        }
        Err(error) => {
            LogStruct::new(
                LogLevel::Warning,
                "本地任务 socket 失败",
                format!("{}: {error}", socket.display()),
            )
            .emit();
            None
        }
    }
}

fn capacity_source() -> Box<dyn CapacitySource + Send> {
    match LinuxProcSource::new() {
        Ok(source) => Box::new(source),
        Err(error) => {
            LogStruct::new(
                LogLevel::Warning,
                "容量采样初始化失败",
                format!("{error}; 使用固定回退容量"),
            )
            .emit();
            Box::new(FixedSource::new(fallback_sample()))
        }
    }
}

fn fallback_sample() -> CapacitySample {
    CapacitySample {
        total_cores: 2.0,
        used_cores: 0.0,
        mem_total_mb: 4096,
        mem_available_mb: 4096,
    }
}

fn open_cache(config: &ServiceConfig) -> Arc<ResultCache> {
    let cache_dir = config.cache_dir();
    match ResultCache::open(
        &cache_dir,
        config.cache.result_cache_mb.saturating_mul(1024 * 1024),
    ) {
        Ok(cache) => {
            LogStruct::new(
                LogLevel::Important,
                "结果缓存",
                format!(
                    "{}，{} MB",
                    cache_dir.display(),
                    config.cache.result_cache_mb
                ),
            )
            .emit();
            Arc::new(cache)
        }
        Err(error) => {
            LogStruct::new(
                LogLevel::Warning,
                "结果缓存不可用",
                format!("{}: {error}", cache_dir.display()),
            )
            .emit();
            Arc::new(ResultCache::disabled(cache_dir))
        }
    }
}

fn new_control() -> ControlHandle {
    control::new_control_handle()
}

fn is_transient_accept_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted | io::ErrorKind::WouldBlock
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CacheConfig, ServiceConfig, SubscriptionConfig};
    use crate::test_support::TempDir;

    #[tokio::test]
    async fn starts_and_shuts_down() {
        let dir = TempDir::new("runtime");
        let config = ServiceConfig {
            port: 0,
            subscription: SubscriptionConfig {
                target_slots: 0,
                ..Default::default()
            },
            cache: CacheConfig {
                dir: dir.join("cache").display().to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let config_path = dir.join("config.toml");
        config.save_to_file(&config_path).unwrap();

        let socket = dir.join("task.sock");
        let handle = start(RuntimeOptions {
            config_path,
            task_socket: socket.clone(),
        })
        .await
        .unwrap();

        assert!(handle.local_addr().ip().is_loopback());
        assert_eq!(handle.config().port(), 0);

        handle.shutdown().await;
        handle.shutdown().await;
        assert!(!socket.exists(), "shutdown removes the task socket");
        assert!(handle.wait().await.is_ok(), "wait observes clean shutdown");
    }

    #[test]
    fn fallback_capacity_has_one_slot() {
        let snapshot = calculate_capacity(&fallback_sample(), 0, 80);
        assert_eq!(snapshot.max_slots, 1);
        assert_eq!(snapshot.free_slots, 1);
        assert!(snapshot.accepting);
    }
}
