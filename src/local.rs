//! Unix-socket server for local `nexus-rustc` requests.

use std::fs;
use std::io;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;

use tokio::net::{UnixListener, UnixStream};

use crate::local_protocol::{self, LocalRequest, LocalResponse};
use crate::log::{LogLevel, LogStruct};
use crate::remote::Coordinator;

const SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(3600);
const SOCKET_MODE: u32 = 0o660;

pub fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path)?;
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a socket", path.display()),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(SOCKET_MODE))?;
    Ok(listener)
}

pub async fn run(listener: UnixListener, coordinator: Arc<Coordinator>, handle: Handle) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let coordinator = coordinator.clone();
                let handle = handle.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) = handle_stream(stream, coordinator, handle) {
                        LogStruct::new(LogLevel::Warning, "本地编译请求失败", error.to_string())
                            .emit();
                    }
                });
            }
            Err(error) => {
                LogStruct::new(LogLevel::Warning, "本地 socket 接受失败", error.to_string()).emit();
                break;
            }
        }
    }
}

fn prepare_stream(stream: UnixStream) -> io::Result<std::os::unix::net::UnixStream> {
    let stream = stream.into_std()?;
    // Tokio sockets are nonblocking. The blocking request handler must switch
    // back explicitly, otherwise read_exact/write_all return EAGAIN.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(SOCKET_READ_TIMEOUT))?;
    stream.set_write_timeout(Some(SOCKET_READ_TIMEOUT))?;
    Ok(stream)
}

fn handle_stream(
    stream: UnixStream,
    coordinator: Arc<Coordinator>,
    handle: Handle,
) -> io::Result<()> {
    let mut stream = prepare_stream(stream)?;

    let request: LocalRequest = local_protocol::read_message(&mut stream)?;
    let response: LocalResponse = handle.block_on(coordinator.execute(&request));
    local_protocol::write_message(&mut stream, &response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn socket_is_group_read_write() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "nexuscompile-socket-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nexus.sock");
        let _listener = bind(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, SOCKET_MODE);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn prepared_stream_blocks_for_request() {
        let (server, client) = UnixStream::pair().unwrap();
        let mut server = prepare_stream(server).unwrap();
        let mut client = client.into_std().unwrap();
        client.set_nonblocking(false).unwrap();

        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            client.write_all(b"x").unwrap();
        });

        let mut byte = [0u8; 1];
        server.read_exact(&mut byte).unwrap();
        assert_eq!(byte[0], b'x');
        writer.join().unwrap();
    }
}
