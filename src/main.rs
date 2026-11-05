use std::sync::{Arc, Mutex};

use nexuscompile::app::AppContext;
use nexuscompile::compile::cache::ResultCache;
use nexuscompile::compile_protocol::PeerId;
use nexuscompile::config::{ConfigHandle, ServiceConfig};
use nexuscompile::local_server;
use nexuscompile::log::{LogLevel, LogStruct};
use nexuscompile::paths;
use nexuscompile::pool::PoolOps;
use nexuscompile::remote::Coordinator;
use nexuscompile::state::NodeState;
use nexuscompile::transport::{self, NodeLink};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config_handle = ConfigHandle::load_or_create_default();
    let config_snapshot = config_handle.snapshot();
    let address = format!("127.0.0.1:{}", config_handle.port());
    let listener = TcpListener::bind(&address).await?;
    let state = Arc::new(Mutex::new(NodeState::new(
        PeerId::new("local"),
        1,
        config_snapshot.node.toolchain.clone(),
        config_snapshot.node.target.clone(),
    )));
    LogStruct::new(LogLevel::Important, "服务启动", &address).emit();

    let cache = open_cache(&config_snapshot);
    let cache_sweep_task = {
        let cache = cache.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                cache.sweep();
            }
        })
    };

    let link = Arc::new(NodeLink::new());
    let ops: Arc<dyn PoolOps> = link.clone();
    let app = Arc::new(AppContext::new(
        state.clone(),
        cache.clone(),
        config_handle.clone(),
        link.clone(),
    ));
    let coordinator = Arc::new(Coordinator::new(
        state.clone(),
        cache.clone(),
        ops,
        config_snapshot.clone(),
    ));

    let (local_task, local_socket): (
        Option<tokio::task::JoinHandle<()>>,
        Option<std::path::PathBuf>,
    ) = {
        let socket = paths::socket_path();
        match local_server::bind(&socket) {
            Ok(listener) => {
                LogStruct::new(
                    LogLevel::Important,
                    "本地编译 socket",
                    socket.display().to_string(),
                )
                .emit();
                (
                    Some(tokio::spawn(local_server::run(
                        listener,
                        coordinator,
                        tokio::runtime::Handle::current(),
                    ))),
                    Some(socket),
                )
            }
            Err(error) => {
                LogStruct::new(
                    LogLevel::Warning,
                    "本地编译 socket 失败",
                    format!("{}: {error}", socket.display()),
                )
                .emit();
                (None, None)
            }
        }
    };

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                LogStruct::new(LogLevel::Preset, "节点接入", addr.to_string()).emit();
                let app = app.clone();
                tokio::spawn(async move {
                    if let Err(e) = transport::handle_connection(stream, app).await {
                        LogStruct::new(
                            LogLevel::Critical,
                            "连接错误",
                            format!("位于 {}: {}", addr, e),
                        ).emit();
                    }
                });
            }
            _ = shutdown_signal() => {
                LogStruct::new(LogLevel::Important, "关闭", "收到关闭信号").emit();
                break;
            }
        }
    }

    drop(listener);
    cache_sweep_task.abort();

    if let Some(task) = local_task {
        task.abort();
    }
    if let Some(path) = local_socket {
        let _ = std::fs::remove_file(path);
    }

    Ok(())
}

fn open_cache(config: &ServiceConfig) -> Arc<ResultCache> {
    let cache_dir = if config.cache.dir.trim().is_empty() {
        paths::cache_path()
    } else {
        std::path::PathBuf::from(&config.cache.dir)
    };
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

/// Wait for a termination signal: listen for both SIGTERM and Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("安装 Ctrl-C 处理器失败");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("安装 SIGTERM 处理器失败")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
