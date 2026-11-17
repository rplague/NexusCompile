pub fn value(args: &[String], flag: &str) -> Option<String> {
    let prefix = format!("{flag}=");
    for (index, arg) in args.iter().enumerate() {
        if arg == flag {
            return args.get(index + 1).cloned();
        }
        if let Some(value) = arg.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_parsing() {
        let args = vec![
            "--crate-name=demo".into(),
            "--out-dir".into(),
            "/tmp/out".into(),
            "--emit".into(),
            "link".into(),
        ];
        assert_eq!(value(&args, "--crate-name").as_deref(), Some("demo"));
        assert_eq!(value(&args, "--out-dir").as_deref(), Some("/tmp/out"));
        assert_eq!(value(&args, "--emit").as_deref(), Some("link"));
    }
}
