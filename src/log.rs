mod file;
mod format;

use std::collections::HashMap;
use std::io::IsTerminal;
use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

use chrono::Local;
use colored::{ColoredString, Colorize};

use crate::compile::manifest::ActionManifest;
use crate::compile_protocol::Hash32;
use file::{append_file, maybe_check_and_roll};
use format::{render_journald, render_stream, write_stream};

/// Whether to emit ANSI colors on the terminal.
///
/// Emits plain text when stdout is not a TTY or `NO_COLOR` is set, avoiding
/// escape sequences in logs.
fn use_color() -> bool {
    if std::env::var("NO_COLOR").is_ok() {
        return false;
    }
    std::io::stdout().is_terminal()
}

/// Log output mode.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LogMode {
    Journald,
    File,
}

/// Determine the log mode.
///
/// The presence of `JOURNAL_STREAM` means stdout/stderr are captured by
/// journald, so log management is fully delegated to journald.
fn log_mode() -> LogMode {
    static MODE: OnceLock<LogMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        if std::env::var_os("JOURNAL_STREAM").is_some_and(|v| !v.is_empty()) {
            LogMode::Journald
        } else {
            LogMode::File
        }
    })
}

pub enum LogLevel {
    Important,
    Debug,
    Preset,
    Warning,
    Error,
    Critical,
}

impl LogLevel {
    fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Important => "[IMPORTANT]",
            LogLevel::Debug => "[+]",
            LogLevel::Preset => "[-]",
            LogLevel::Warning => "[*]",
            LogLevel::Error => "[!]",
            LogLevel::Critical => "[CRITICAL]",
        }
    }

    fn color(&self) -> ColoredString {
        match self {
            LogLevel::Important => "[IMPORTANT]".on_green().bold(),
            LogLevel::Debug => "[+]".cyan(),
            LogLevel::Preset => "[-]".normal(),
            LogLevel::Warning => "[*]".yellow(),
            LogLevel::Error => "[!]".red(),
            LogLevel::Critical => "[CRITICAL]".on_red().bold().blink(),
        }
    }

    /// Map to syslog priority (journald parses and strips the leading `<N>`).
    fn syslog_priority(&self) -> u8 {
        match self {
            LogLevel::Critical => 2,
            LogLevel::Error => 3,
            LogLevel::Warning => 4,
            LogLevel::Important => 5,
            LogLevel::Preset => 6,
            LogLevel::Debug => 7,
        }
    }
}

pub struct LogStruct {
    level: LogLevel,
    topic: String,
    content: String,
}

impl LogStruct {
    pub fn new(level: LogLevel, topic: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            level,
            topic: topic.into(),
            content: content.into(),
        }
    }

    pub fn emit(&self) {
        if log_mode() == LogMode::File {
            maybe_check_and_roll();
        }
        log(self)
    }
}

/// Log one failed compile invocation without recording successful compiles.
///
/// Each line is a small entry. Long input lines are deliberately kept intact.
#[allow(clippy::too_many_arguments)]
pub fn log_compile_failure(
    source: &str,
    task: &str,
    crate_name: &str,
    rustc: &str,
    cwd: &str,
    exit_code: i32,
    duration_ms: u64,
    args: &[String],
    env: &HashMap<String, String>,
    stdout: &[u8],
    stderr: &[u8],
    manifest: Option<&ActionManifest>,
    bundle_hash: Option<&Hash32>,
    bundle_size: Option<u64>,
    bundle_file_count: Option<usize>,
) {
    let out_dir = crate::util::args::value(args, "--out-dir").unwrap_or_default();
    LogStruct::new(
        LogLevel::Error,
        "编译失败",
        format!(
            "source={source} task={task} crate={crate_name} exit={exit_code} duration_ms={duration_ms} rustc={rustc} cwd={cwd} out_dir={out_dir} stdout_bytes={} stderr_bytes={}",
            stdout.len(),
            stderr.len()
        ),
    )
    .emit();

    log_rustc_version(source, task, rustc);
    log_env(source, task, env);
    log_args(source, task, args);
    log_bundle(
        source,
        task,
        manifest,
        bundle_hash,
        bundle_size,
        bundle_file_count,
    );
    log_externs(source, task, args, manifest);
    log_search_paths(source, task, args);
    log_compile_stream(source, task, "stdout", stdout);
    log_compile_stream(source, task, "stderr", stderr);
}

fn log_rustc_version(source: &str, task: &str, rustc: &str) {
    let text = match Command::new(rustc).arg("-vV").output() {
        Ok(output) => String::from_utf8_lossy(&output.stdout).into_owned(),
        Err(error) => format!("error:{error}"),
    };
    for line in text.lines() {
        LogStruct::new(
            LogLevel::Error,
            "编译工具链",
            format!("source={source} task={task} rustc={rustc} {line}"),
        )
        .emit();
    }
}

fn log_env(source: &str, task: &str, env: &HashMap<String, String>) {
    const KEYS: [&str; 9] = [
        "CARGO_MANIFEST_DIR",
        "OUT_DIR",
        "CARGO_TARGET_DIR",
        "RUSTFLAGS",
        "RUSTC",
        "TARGET",
        "PROFILE",
        "RUSTUP_TOOLCHAIN",
        "LD_LIBRARY_PATH",
    ];
    for key in KEYS {
        if let Some(value) = env.get(key) {
            LogStruct::new(
                LogLevel::Error,
                "编译环境",
                format!("source={source} task={task} {key}={value}"),
            )
            .emit();
        }
    }
}

fn log_args(source: &str, task: &str, args: &[String]) {
    for (index, arg) in args.iter().enumerate() {
        LogStruct::new(
            LogLevel::Error,
            "编译参数",
            format!("source={source} task={task} index={index} arg={arg}"),
        )
        .emit();
    }
}

fn log_bundle(
    source: &str,
    task: &str,
    manifest: Option<&ActionManifest>,
    bundle_hash: Option<&Hash32>,
    bundle_size: Option<u64>,
    bundle_file_count: Option<usize>,
) {
    let manifest_digest = manifest
        .and_then(|manifest| manifest.digest().ok())
        .map(|digest| crate::compile::hash::hex_hash(&digest))
        .unwrap_or_else(|| "not_available".to_string());
    let manifest_inputs = manifest
        .map(|manifest| manifest.inputs.len().to_string())
        .unwrap_or_else(|| "not_available".to_string());
    let bundle_hash = bundle_hash
        .map(crate::compile::hash::hex_hash)
        .unwrap_or_else(|| "not_available".to_string());
    LogStruct::new(
        LogLevel::Error,
        "编译包",
        format!(
            "source={source} task={task} manifest_digest={manifest_digest} bundle_hash={bundle_hash} bundle_size={} bundle_files={} manifest_inputs={manifest_inputs}",
            bundle_size
                .map(|value| value.to_string())
                .unwrap_or_else(|| "not_available".to_string()),
            bundle_file_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "not_available".to_string())
        ),
    )
    .emit();
}

fn log_externs(source: &str, task: &str, args: &[String], manifest: Option<&ActionManifest>) {
    for (name, path) in extern_values(args) {
        let (size, sha256): (String, String) =
            match crate::compile::hash::hash_file(Path::new(&path)) {
                Ok((hash, size)) => (size.to_string(), crate::compile::hash::hex_hash(&hash)),
                Err(error) => ("error".to_string(), format!("error:{error}")),
            };
        let expected = manifest
            .and_then(|manifest| {
                manifest
                    .inputs
                    .iter()
                    .find(|input| input.path == format!("extern/{name}"))
            })
            .map(|input| crate::compile::hash::hex_hash(&input.hash))
            .unwrap_or_else(|| "not_available".to_string());
        LogStruct::new(
            LogLevel::Error,
            "编译外部依赖",
            format!(
                "source={source} task={task} extern={name} path={path} size={size} sha256={sha256} expected={expected} matches={}",
                expected == sha256
            ),
        )
        .emit();
    }
}

fn log_search_paths(source: &str, task: &str, args: &[String]) {
    for (kind, path) in search_paths(args) {
        let metadata = std::fs::metadata(Path::new(&path));
        let exists = metadata.is_ok();
        let is_dir = metadata
            .as_ref()
            .map(|value| value.is_dir())
            .unwrap_or(false);
        let readable = std::fs::read_dir(Path::new(&path)).is_ok();
        LogStruct::new(
            LogLevel::Error,
            "编译搜索路径",
            format!(
                "source={source} task={task} kind={kind} path={path} exists={exists} is_dir={is_dir} readable={readable}"
            ),
        )
        .emit();
    }
}

/// Log a local/remote task-level error that is not a rustc invocation.
pub fn log_task_error(source: &str, task: &str, code: &str, message: &str) {
    LogStruct::new(
        LogLevel::Error,
        "任务失败",
        format!("source={source} task={task} code={code} message={message}"),
    )
    .emit();
}

fn log_compile_stream(source: &str, task: &str, stream: &str, bytes: &[u8]) {
    let text = String::from_utf8_lossy(bytes);
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        LogStruct::new(
            LogLevel::Error,
            "编译输出",
            format!("source={source} task={task} stream={stream} {line}"),
        )
        .emit();
    }
}

fn extern_values(args: &[String]) -> Vec<(String, String)> {
    let mut values = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "--extern" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("--extern=")
        };
        if let Some(value) = value
            && let Some((name, path)) = value.split_once('=')
        {
            values.push((name.to_string(), path.to_string()));
        }
        index += 1;
    }
    values
}

fn search_paths(args: &[String]) -> Vec<(String, String)> {
    let mut values = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "-L" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("-L")
        };
        if let Some(value) = value {
            let (kind, path) = value.split_once('=').unwrap_or(("unknown", value));
            values.push((kind.to_string(), path.to_string()));
        }
        index += 1;
    }
    values
}

fn error_entry(topic: &str, content: &str) {
    let entry = LogStruct::new(LogLevel::Error, topic, content);
    log_onlycli(&entry);
}

fn log(info: &LogStruct) {
    match log_mode() {
        LogMode::Journald => {
            write_stream(&info.level, &render_journald(info));
        }
        LogMode::File => {
            let time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            write_stream(&info.level, &render_stream(info, Some(&time)));

            let file_text =
                format::format_entry(info.level.as_str(), Some(&time), &info.topic, &info.content);
            append_file(&file_text);
        }
    }
}

fn log_onlycli(info: &LogStruct) {
    match log_mode() {
        LogMode::Journald => {
            write_stream(&info.level, &render_journald(info));
        }
        LogMode::File => {
            let time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            write_stream(&info.level, &render_stream(info, Some(&time)));
        }
    }
}
