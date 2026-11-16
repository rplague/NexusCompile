//! Installer-facing configuration commands.

use std::path::PathBuf;

use crate::config::{ConfigErrorKind, ServiceConfig};
use crate::paths;

const EXIT_USAGE: i32 = 2;
const EXIT_IO: i32 = 4;

/// Handle `config-defaults` / `config-check`.
///
/// Returns `None` when the arguments are not a configuration command so
/// `main` can continue starting the server.
pub fn run_cli(args: &[String]) -> Option<i32> {
    let command = args.first()?.as_str();
    let rest = &args[1..];
    match command {
        "config-defaults" => Some(run_defaults(rest)),
        "config-check" => Some(run_check(rest)),
        _ => None,
    }
}

fn run_defaults(args: &[String]) -> i32 {
    let path = match parse_config_arg(args) {
        Ok(path) => path,
        Err(code) => return code,
    };

    if path.exists() {
        println!("config: keeping existing {}", path.display());
        return 0;
    }

    match ServiceConfig::default().save_to_file(&path) {
        Ok(()) => {
            println!("config: wrote {}", path.display());
            0
        }
        Err(error) => {
            eprintln!("config: {error}");
            EXIT_IO
        }
    }
}

fn run_check(args: &[String]) -> i32 {
    let path = match parse_config_arg(args) {
        Ok(path) => path,
        Err(code) => return code,
    };

    if !path.exists() {
        eprintln!("config: no config at {}", path.display());
        return EXIT_IO;
    }

    match ServiceConfig::try_from_toml_file(&path, false) {
        Ok(config) => {
            for warning in config.warnings() {
                println!("config: warning: {warning}");
            }
            println!("config: ok {}", path.display());
            0
        }
        Err(error) => {
            eprintln!("config: {error}");
            if error.kind == ConfigErrorKind::Io {
                EXIT_IO
            } else {
                EXIT_USAGE
            }
        }
    }
}

fn parse_config_arg(args: &[String]) -> Result<PathBuf, i32> {
    let mut config = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                let Some(value) = args.get(index) else {
                    eprintln!("config: --config requires a path");
                    return Err(EXIT_USAGE);
                };
                config = Some(PathBuf::from(value));
            }
            other => {
                eprintln!("config: unknown argument: {other}");
                return Err(EXIT_USAGE);
            }
        }
        index += 1;
    }

    Ok(config.unwrap_or_else(paths::config_path))
}
