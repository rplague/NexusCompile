use std::sync::Arc;

use nexuscompile::capacity::{
    CapacitySample, CapacitySnapshot, CapacitySource, FixedSource, LinuxProcSource,
    calculate_capacity,
};
use nexuscompile::compile::cache::ResultCache;
use nexuscompile::compile_protocol::PeerId;
use nexuscompile::config::{ConfigHandle, ServiceConfig};
use nexuscompile::connection;
use nexuscompile::control::new_control_handle;
use nexuscompile::local;
use nexuscompile::log::{LogLevel, LogStruct};
use nexuscompile::paths;
use nexuscompile::pool;
use nexuscompile::pool::{Consumer, Provider};
use nexuscompile::remote::Coordinator;
use nexuscompile::state::AppState;
use nexuscompile::workspace::Workspace;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config_handle = ConfigHandle::load_or_create_default();
    let config_snapshot = config_handle.snapshot();
    let address = format!("127.0.0.1:{}", config_handle.port());
    let listener = TcpListener::bind(&address).await?;

    let mut source: Box<dyn CapacitySource + Send> = match LinuxProcSource::new() {
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
    let initial = source
        .sample()
        .map(|sample| calculate_capacity(&sample, 0, config_snapshot.node.max_usage_percent))
        .unwrap_or(CapacitySnapshot {
            max_slots: 0,
            free_slots: 0,
            queue_len: 0,
            accepting: false,
        });

    let provider = Arc::new(Provider::new(
        PeerId::new("local"),
        initial.max_slots,
        config_snapshot.node.toolchain.clone(),
        config_snapshot.node.target.clone(),
    ));
    provider.set_capacity(initial.max_slots, initial.free_slots);
    let consumer = Arc::new(Consumer::new());
    LogStruct::new(
        LogLevel::Important,
        "服务启动",
        format!(
            "{address}，service={}，max_slots={}，free_slots={}，percent={}%",
            config_snapshot.service_name,
            initial.max_slots,
            initial.free_slots,
            config_snapshot.node.max_usage_percent
        ),
    )
    .emit();

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

    let workspace = Arc::new(Workspace::new(cache.root().to_path_buf()));
    if let Err(error) = workspace.cleanup_staging() {
        LogStruct::new(LogLevel::Warning, "清理 staging 失败", error.to_string()).emit();
    }
    let control = new_control_handle();
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
        config_snapshot.clone(),
    ));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let pool_task = tokio::spawn(pool::run_pool(
        control.clone(),
        provider,
        consumer,
        cache.clone(),
        config_snapshot.clone(),
        shutdown_rx,
        source,
    ));

    let (local_task, local_socket): (
        Option<tokio::task::JoinHandle<()>>,
        Option<std::path::PathBuf>,
    ) = {
        let socket = paths::socket_path();
        match local::bind(&socket) {
            Ok(listener) => {
                LogStruct::new(
                    LogLevel::Important,
                    "本地编译 socket",
                    socket.display().to_string(),
                )
                .emit();
                (
                    Some(tokio::spawn(local::run(
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
                    if let Err(e) = connection::handle_connection(stream, app).await {
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
    let _ = shutdown_tx.send(true);
    cache_sweep_task.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), pool_task).await;

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
