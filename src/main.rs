use std::error::Error;

use nexuscompile::log::{LogLevel, LogStruct};
use nexuscompile::runtime::{RuntimeHandle, RuntimeOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(code) = nexuscompile::cli::run_from_args(&args) {
        std::process::exit(code);
    }

    let handle = match nexuscompile::runtime::start(RuntimeOptions::default()).await {
        Ok(handle) => handle,
        Err(error) => {
            LogStruct::new(LogLevel::Critical, "启动失败", error.to_string()).emit();
            return Err(error.into());
        }
    };

    wait_for_exit(&handle).await;
    handle.shutdown().await;
    Ok(())
}

async fn wait_for_exit(_handle: &RuntimeHandle) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(error) => {
                LogStruct::new(
                    LogLevel::Critical,
                    "安装 SIGTERM 处理器失败",
                    error.to_string(),
                )
                .emit();
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    LogStruct::new(LogLevel::Critical, "监听 Ctrl-C 失败", error.to_string()).emit();
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    LogStruct::new(LogLevel::Important, "关闭", "收到关闭信号").emit();
}
