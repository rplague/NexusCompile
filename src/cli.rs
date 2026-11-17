//! Installer-facing command dispatch.

/// Run a recognized installer command, returning its exit code.
pub fn run_from_args(args: &[String]) -> Option<i32> {
    crate::config_cli::run_cli(args)
        .or_else(|| crate::setup::run_cli(args))
        .or_else(|| crate::toolchain::run_cli(args))
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
