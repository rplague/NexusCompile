mod file;
mod format;

use std::io::IsTerminal;
use std::sync::OnceLock;

use chrono::Local;
use colored::{ColoredString, Colorize};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syslog_priority_mapping() {
        assert_eq!(LogLevel::Critical.syslog_priority(), 2);
        assert_eq!(LogLevel::Error.syslog_priority(), 3);
        assert_eq!(LogLevel::Warning.syslog_priority(), 4);
        assert_eq!(LogLevel::Important.syslog_priority(), 5);
        assert_eq!(LogLevel::Preset.syslog_priority(), 6);
        assert_eq!(LogLevel::Debug.syslog_priority(), 7);
    }
}
