use std::collections::HashMap;
use std::env;
use std::io::{self, Write};
use std::process::Command;

use nexuscompile::compile::executor;
use nexuscompile::local_protocol::{LocalBody, LocalRequest, LocalResponse};
use nexuscompile::paths;

fn main() {
    let mut raw_args = env::args_os().skip(1);
    let rustc = raw_args.next().unwrap_or_else(|| "rustc".into());
    let args: Vec<String> = raw_args
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let env_map: HashMap<String, String> = env::vars().collect();

    if executor::is_supported(&args, &env_map).is_ok()
        && let Some(response) = try_local(&rustc, &args, &env_map)
    {
        match response.body {
            LocalBody::Compiled {
                exit_code,
                stdout,
                stderr,
                ..
            } => {
                let _ = io::stdout().write_all(&stdout);
                let _ = io::stdout().flush();
                let _ = io::stderr().write_all(&stderr);
                let _ = io::stderr().flush();
                std::process::exit(exit_code);
            }
            LocalBody::Error { .. } => {}
        }
    }

    exec_rustc(&rustc, &args);
}

fn try_local(
    rustc: &std::ffi::OsStr,
    args: &[String],
    env_map: &HashMap<String, String>,
) -> Option<LocalResponse> {
    use std::os::unix::net::UnixStream;

    let socket = paths::socket_path();
    let mut stream = UnixStream::connect(socket).ok()?;
    let cwd = env::current_dir().ok()?;
    let request = LocalRequest::new(
        cwd.to_string_lossy(),
        rustc.to_string_lossy(),
        args.to_vec(),
        env_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );

    nexuscompile::local_protocol::write_message(&mut stream, &request).ok()?;
    nexuscompile::local_protocol::read_message(&mut stream).ok()
}

fn exec_rustc(rustc: &std::ffi::OsStr, args: &[String]) -> ! {
    use std::os::unix::process::CommandExt;

    let error = Command::new(rustc).args(args).exec();
    eprintln!("nexus-rustc: failed to exec rustc: {error}");
    std::process::exit(127);
}
