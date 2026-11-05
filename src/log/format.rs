use super::{LogLevel, LogStruct, use_color};

pub(super) fn format_entry(
    prefix: impl std::fmt::Display,
    time: Option<&str>,
    topic: &str,
    content: &str,
) -> String {
    match (time, content.is_empty()) {
        (Some(t), true) => format!("{} {}\n    {}", prefix, t, topic),
        (Some(t), false) => format!("{} {}\n    {}\n    {}", prefix, t, topic, content),
        (None, true) => format!("{} {}", prefix, topic),
        (None, false) => format!("{} {}\n    {}", prefix, topic, content),
    }
}

/// Render stream output with optional color (File mode, multi-line).
pub(super) fn render_stream(info: &LogStruct, time: Option<&str>) -> String {
    if use_color() {
        format_entry(info.level.color(), time, &info.topic, &info.content)
    } else {
        format_entry(info.level.as_str(), time, &info.topic, &info.content)
    }
}

/// journald mode: single line with an `<N>` priority prefix (journald parses and strips it).
pub(super) fn render_journald(info: &LogStruct) -> String {
    let content = info
        .content
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let body = if content.is_empty() {
        format!("{} {}", info.level.as_str(), info.topic)
    } else {
        format!("{} {}: {}", info.level.as_str(), info.topic, content)
    };
    format!("<{}>{}", info.level.syslog_priority(), body)
}

/// Write one line to the stream (errors go to stderr, everything else to stdout).
pub(super) fn write_stream(level: &LogLevel, text: &str) {
    match level {
        LogLevel::Error | LogLevel::Critical | LogLevel::Warning => eprintln!("{}", text),
        _ => println!("{}", text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journald_single_line_with_prefix_and_collapsed_newlines() {
        let entry = LogStruct::new(LogLevel::Error, "topic", "line1\nline2");
        let line = render_journald(&entry);
        assert_eq!(line, "<3>[!] topic: line1 line2");
        assert!(!line.contains('\n'));
    }

    #[test]
    fn journald_empty_content() {
        let entry = LogStruct::new(LogLevel::Debug, "topic", "");
        assert_eq!(render_journald(&entry), "<7>[+] topic");
    }
}
