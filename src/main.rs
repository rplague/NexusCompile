use std::error::Error;

use nexuscompile::log::{LogLevel, LogStruct};
use nexuscompile::runtime::RuntimeOptions;

fn run_from_args(args: &[String]) -> Option<i32> {
    nexuscompile::installer::run_cli(args).or_else(|| nexuscompile::toolchain::run_cli(args))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(code) = run_from_args(&args) {
        std::process::exit(code);
    }

    let handle = match nexuscompile::runtime::start(RuntimeOptions::default()).await {
        Ok(handle) => handle,
        Err(error) => {
            LogStruct::new(LogLevel::Critical, "启动失败", error.to_string()).emit();
            return Err(error.into());
        }
    };

    let server = handle.wait();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            if let Err(error) = &result {
                LogStruct::new(LogLevel::Critical, "服务退出", error.to_string()).emit();
            }
            handle.shutdown().await;
            nexuscompile::log::flush();
            result?;
        }
        _ = wait_for_signal() => {
            handle.shutdown().await;
            nexuscompile::log::flush();
        }
    }
    Ok(())
}

async fn wait_for_signal() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_priority_and_unknown_args() {
        assert_eq!(run_from_args(&[]), None);
        assert_eq!(run_from_args(&["unknown".into()]), None);
        assert_eq!(run_from_args(&["setup-user".into()]), Some(2));
        assert_eq!(run_from_args(&["unsetup-user".into()]), Some(2));
    }
}
