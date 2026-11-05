use std::fs;
use std::io::{Read, Write};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::spawn;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use flate2::Compression;
use flate2::write::GzEncoder;
use scopeguard::defer;

use super::{LogLevel, LogStruct, error_entry, log_onlycli};
use crate::paths;

static LOG_MUTEX: Mutex<()> = Mutex::new(());
static ROLLING: AtomicBool = AtomicBool::new(false);
static LAST_ROLL_CHECK: Mutex<Option<Instant>> = Mutex::new(None);

const MAX_SIZE_BYTES: u64 = 10 * 1024 * 1024;
const ROLL_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// System log file path.
///
/// Resolved lazily once from `paths::log_path()`; the rotation temp path appends `.tmp`.
fn log_filepath() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| format!("{}/log", paths::log_path().to_string_lossy()))
}

/// Rotation temp path.
fn logtmp_filepath() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| format!("{}.tmp", log_filepath()))
}

/// Directory for archived `.gz` files.
fn gz_path() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| paths::log_path().to_string_lossy().into_owned())
}

fn archive_and_cleanup(tmp_file: String, output_file_name: String) {
    let mut buffer = Vec::new();
    match fs::File::open(&tmp_file) {
        Ok(mut input_file) => {
            if let Err(e) = input_file.read_to_end(&mut buffer) {
                error_entry("无法读取log.tmp文件", &e.to_string());
                return;
            }
        }
        Err(e) => {
            error_entry("无法读取log.tmp文件", &e.to_string());
            return;
        }
    }

    match fs::File::create(&output_file_name) {
        Ok(output_file) => {
            let mut encoder = GzEncoder::new(output_file, Compression::best());
            if let Err(e) = encoder.write_all(&buffer) {
                error_entry("无法压缩log.tmp数据", &e.to_string());
                return;
            }
            if let Err(e) = encoder.finish() {
                error_entry("无法压缩log.tmp数据", &e.to_string());
                return;
            }

            if let Err(e) = fs::remove_file(&tmp_file) {
                error_entry("无法删除log.tmp文件", &e.to_string());
            }
        }
        Err(e) => {
            error_entry("无法创建日志轮转文件", &e.to_string());
        }
    }
}

fn repair_tmp_file() {
    let tmp_metadata = match fs::metadata(logtmp_filepath()) {
        Ok(meta) => meta,
        Err(_) => return,
    };

    let tmp_time = tmp_metadata
        .created()
        .map(|t| {
            let dt: DateTime<Local> = DateTime::from(t);
            dt.format("%m%d_%H%M").to_string()
        })
        .unwrap_or_else(|_| "XXXX_XXXX".to_string());

    let fine_now = Local::now().timestamp_nanos_opt().unwrap_or(0);
    let output_filepath = format!("{}/REPAIR-{}-{}.gz", gz_path(), tmp_time, fine_now);
    archive_and_cleanup(logtmp_filepath().to_owned(), output_filepath);
}

fn perform_roll(file_metadata: fs::Metadata) {
    {
        let _guard = LOG_MUTEX.lock().unwrap();
        match fs::rename(log_filepath(), logtmp_filepath()) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                drop(_guard);
                let warning = LogStruct::new(LogLevel::Warning, "修复错误tmp文件", "");
                log_onlycli(&warning);
                repair_tmp_file();
                let _guard = LOG_MUTEX.lock().unwrap();
                if let Err(e) = fs::rename(log_filepath(), logtmp_filepath()) {
                    let critical =
                        LogStruct::new(LogLevel::Critical, "无法重命名log文件", e.to_string());
                    log_onlycli(&critical);
                    return;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                let critical =
                    LogStruct::new(LogLevel::Critical, "无法重命名log文件", e.to_string());
                log_onlycli(&critical);
                return;
            }
            _ => {}
        }
    }
    let timestamp = file_metadata
        .created()
        .map(|t| {
            let dt: DateTime<Local> = DateTime::from(t);
            dt.format("%m%d_%H%M").to_string()
        })
        .unwrap_or_else(|_| "XXXX_XXXX".to_string());
    let current_time = Local::now().format("%m%d_%H%M").to_string();
    let fine_now = Local::now().timestamp_nanos_opt().unwrap_or(0);
    let output_filepath = format!(
        "{}/{}-{}-{}.gz",
        gz_path(),
        timestamp,
        current_time,
        fine_now
    );

    archive_and_cleanup(logtmp_filepath().to_owned(), output_filepath);
}

/// Throttled rotation check: in File mode, stat at most once per `ROLL_CHECK_INTERVAL`.
pub(super) fn maybe_check_and_roll() {
    {
        let mut last = LAST_ROLL_CHECK.lock().unwrap();
        let now = Instant::now();
        if last.is_some_and(|prev| now.duration_since(prev) < ROLL_CHECK_INTERVAL) {
            return;
        }
        *last = Some(now);
    }
    if !ROLLING.swap(true, Ordering::AcqRel) {
        check_and_roll();
    }
}

fn check_and_roll() {
    let metadata = match fs::metadata(log_filepath()) {
        Ok(m) => m,
        Err(_) => {
            ROLLING.store(false, Ordering::Release);
            return;
        }
    };

    if metadata.len() <= MAX_SIZE_BYTES {
        ROLLING.store(false, Ordering::Release);
        return;
    }

    spawn(|| {
        defer! { ROLLING.store(false, Ordering::Release); }
        let metadata = match fs::metadata(log_filepath()) {
            Ok(m) => m,
            Err(_) => return,
        };
        perform_roll(metadata);
    });
}

/// Append a line to the log file (File mode).
pub(super) fn append_file(text: &str) {
    let _guard = LOG_MUTEX.lock().unwrap();
    let mut log_file = match fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(log_filepath())
    {
        Ok(file) => file,
        Err(_) => {
            let err = LogStruct::new(LogLevel::Error, "无法录入日志", "log文件无法被追加写入");
            log_onlycli(&err);
            return;
        }
    };

    if writeln!(log_file, "{}", text).is_err() {
        let err = LogStruct::new(LogLevel::Error, "无法录入日志", "log文件无法被追加写入");
        log_onlycli(&err);
    }
}
