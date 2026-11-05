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
