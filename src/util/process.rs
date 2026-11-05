use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;

pub fn exit_code(status: ExitStatus) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }
    128 + status.signal().unwrap_or(1)
}
