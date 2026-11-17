//! Per-user Cargo configuration used by the package installer.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use toml_edit::{DocumentMut, Item, Table, value};

pub const WRAPPER_PATH: &str = "/usr/bin/nexus-rustc";
pub const DEFAULT_SOCKET: &str = "/run/nexuscompile/nexus.sock";

const MANAGED_SOCKET_PREFIX: &str = "/run/nexuscompile/";
const BACKUP_SUFFIX: &str = ".nexuscompile.bak";

const BUILD_TABLE: &str = "build";
const ENV_TABLE: &str = "env";
const RUSTC_WRAPPER_KEY: &str = "rustc-wrapper";
const INCREMENTAL_KEY: &str = "incremental";
const CARGO_INCREMENTAL_KEY: &str = "CARGO_INCREMENTAL";
const NEXUSCOMPILE_SOCKET_KEY: &str = "NEXUSCOMPILE_SOCKET";

/// One managed Cargo config command.
#[derive(Debug)]
enum CliCommand {
    SetupUser { config: PathBuf, socket: String },
    UnsetupUser { config: PathBuf },
}

impl CliCommand {
    fn name(&self) -> &'static str {
        match self {
            Self::SetupUser { .. } => "setup-user",
            Self::UnsetupUser { .. } => "unsetup-user",
        }
    }

    fn execute(&self) -> io::Result<()> {
        match self {
            Self::SetupUser { config, socket } => setup_user(config, socket),
            Self::UnsetupUser { config } => unsetup_user(config),
        }
    }
}

/// Result of parsing the first CLI argument.
#[derive(Debug)]
enum CliParse {
    NotSetup,
    MissingConfig(&'static str),
    Command(CliCommand),
}

/// Handle `NexusCompile setup-user ...` / `unsetup-user ...`.
///
/// Returns `None` when the arguments are not a setup command so `main` can
/// continue starting the server.
pub fn run_cli(args: &[String]) -> Option<i32> {
    let command = match parse_cli(args) {
        CliParse::NotSetup => return None,
        CliParse::MissingConfig(name) => {
            eprintln!("{name}: missing --config <path>");
            return Some(2);
        }
        CliParse::Command(command) => command,
    };

    Some(match command.execute() {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{}: {error}", command.name());
            1
        }
    })
}

fn parse_cli(args: &[String]) -> CliParse {
    match args.first().map(String::as_str) {
        Some("setup-user") => {
            let Some(config) = argument_value(args, "--config") else {
                return CliParse::MissingConfig("setup-user");
            };
            let socket =
                argument_value(args, "--socket").unwrap_or_else(|| DEFAULT_SOCKET.to_owned());
            CliParse::Command(CliCommand::SetupUser {
                config: PathBuf::from(config),
                socket,
            })
        }
        Some("unsetup-user") => {
            let Some(config) = argument_value(args, "--config") else {
                return CliParse::MissingConfig("unsetup-user");
            };
            CliParse::Command(CliCommand::UnsetupUser {
                config: PathBuf::from(config),
            })
        }
        _ => CliParse::NotSetup,
    }
}

/// Return the first value that follows an exact flag match.
fn argument_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|argument| argument == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}

/// A parsed Cargo config together with its origin path and permissions.
struct CargoConfigFile {
    path: PathBuf,
    document: DocumentMut,
    permissions: Option<fs::Permissions>,
}

impl CargoConfigFile {
    fn load(path: &Path) -> io::Result<Self> {
        let document = match fs::read_to_string(path) {
            Ok(content) => content
                .parse::<DocumentMut>()
                .map_err(|error| invalid_data(error.to_string()))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => DocumentMut::new(),
            Err(error) => return Err(error),
        };
        let permissions = fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions());

        Ok(Self {
            path: path.to_path_buf(),
            document,
            permissions,
        })
    }

    /// Create the one-time backup without overwriting an existing one.
    fn ensure_backup(&self) -> io::Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        let backup = backup_path(&self.path);
        if backup.exists() {
            return Ok(());
        }

        fs::copy(&self.path, backup)?;
        Ok(())
    }

    fn persist(&self) -> io::Result<()> {
        crate::util::fs::write_atomic(
            &self.path,
            self.document.to_string().as_bytes(),
            self.permissions.clone(),
        )
    }
}

/// Install the managed wrapper settings into a user's Cargo config.
///
/// `socket` is stored as `NEXUSCOMPILE_SOCKET`; it is the base for the task
/// socket, not the wrapper socket itself.
///
/// # Errors
///
/// Returns an error if the file cannot be read, parsed, backed up, or written,
/// or if an existing `[build]` / `[env]` key is not a table.
pub fn setup_user(path: &Path, socket: &str) -> io::Result<()> {
    let mut config = CargoConfigFile::load(path)?;
    apply_managed_settings(&mut config.document, socket)?;
    config.ensure_backup()?;
    config.persist()
}

/// Remove only the settings installed by [`setup_user`].
///
/// Values are removed only when they still match the managed defaults, so user
/// customizations are preserved.
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read, parsed, or written.
pub fn unsetup_user(path: &Path) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut config = CargoConfigFile::load(path)?;
    if clear_managed_settings(&mut config.document) {
        config.persist()?;
    }
    Ok(())
}

fn apply_managed_settings(document: &mut DocumentMut, socket: &str) -> io::Result<()> {
    let build = managed_table_mut(document, BUILD_TABLE)?;
    build[RUSTC_WRAPPER_KEY] = value(WRAPPER_PATH);
    build[INCREMENTAL_KEY] = value(false);

    let env = managed_table_mut(document, ENV_TABLE)?;
    env[CARGO_INCREMENTAL_KEY] = value("0");
    env[NEXUSCOMPILE_SOCKET_KEY] = value(socket);
    Ok(())
}

fn clear_managed_settings(document: &mut DocumentMut) -> bool {
    let mut changed = false;

    if let Some(build) = document.get_mut(BUILD_TABLE).and_then(Item::as_table_mut) {
        changed |= remove_managed_value(build, RUSTC_WRAPPER_KEY, |item| {
            item.as_str() == Some(WRAPPER_PATH)
        });
        changed |=
            remove_managed_value(build, INCREMENTAL_KEY, |item| item.as_bool() == Some(false));
        if build.is_empty() {
            document.remove(BUILD_TABLE);
        }
    }

    if let Some(env) = document.get_mut(ENV_TABLE).and_then(Item::as_table_mut) {
        changed |= remove_managed_value(env, CARGO_INCREMENTAL_KEY, |item| {
            item.as_str() == Some("0")
        });
        changed |= remove_managed_value(env, NEXUSCOMPILE_SOCKET_KEY, |item| {
            item.as_str()
                .is_some_and(|value| value.starts_with(MANAGED_SOCKET_PREFIX))
        });
        if env.is_empty() {
            document.remove(ENV_TABLE);
        }
    }

    changed
}

fn remove_managed_value(table: &mut Table, key: &str, matches: impl FnOnce(&Item) -> bool) -> bool {
    if table.get(key).is_some_and(matches) {
        table.remove(key);
        true
    } else {
        false
    }
}

fn managed_table_mut<'a>(document: &'a mut DocumentMut, name: &str) -> io::Result<&'a mut Table> {
    if !document.contains_key(name) {
        document.insert(name, Item::Table(Table::new()));
    }

    document[name]
        .as_table_mut()
        .ok_or_else(|| invalid_data(format!("managed Cargo config key {name:?} must be a table")))
}

fn backup_path(path: &Path) -> PathBuf {
    path.with_file_name(format!("{}{BACKUP_SUFFIX}", file_name(path)))
}

fn file_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml")
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn setup_writes_managed_keys_backup_and_permissions() {
        let dir = TempDir::new("setup");
        let path = dir.join("config.toml");
        let original = "# keep me
[net]
retry = 2
";
        fs::write(&path, original).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        setup_user(&path, DEFAULT_SOCKET).unwrap();
        let first = fs::read_to_string(&path).unwrap();
        assert!(first.contains("# keep me"));
        assert_eq!(first.matches("rustc-wrapper").count(), 1);
        assert_eq!(first.matches("CARGO_INCREMENTAL").count(), 1);
        assert_eq!(fs::read_to_string(backup_path(&path)).unwrap(), original);

        setup_user(&path, DEFAULT_SOCKET).unwrap();
        let second = fs::read_to_string(&path).unwrap();
        assert_eq!(second.matches("[build]").count(), 1);
        assert_eq!(second.matches("[env]").count(), 1);
        assert_eq!(second.matches("rustc-wrapper").count(), 1);
        assert_eq!(second.matches("NEXUSCOMPILE_SOCKET").count(), 1);

        fs::write(
            &path,
            "# changed
",
        )
        .unwrap();
        setup_user(&path, DEFAULT_SOCKET).unwrap();
        assert_eq!(
            fs::read_to_string(backup_path(&path)).unwrap(),
            original,
            "case: backup once"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "case: permissions");
        }
    }

    #[test]
    fn unsetup_removes_managed_keys_and_preserves_custom_values() {
        let dir = TempDir::new("setup-managed");
        let path = dir.join("config.toml");
        fs::write(
            &path,
            r#"[build]
rustc-wrapper = "/usr/bin/nexus-rustc"
incremental = false
jobs = 4

[env]
CARGO_INCREMENTAL = "0"
NEXUSCOMPILE_SOCKET = "/run/nexuscompile/nexus.sock"
RUST_BACKTRACE = "1"
"#,
        )
        .unwrap();

        unsetup_user(&path).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(!text.contains("rustc-wrapper"), "case: managed wrapper");
        assert!(!text.contains("incremental"), "case: managed incremental");
        assert!(!text.contains("CARGO_INCREMENTAL"), "case: managed env");
        assert!(
            !text.contains("NEXUSCOMPILE_SOCKET"),
            "case: managed socket"
        );
        assert!(text.contains("jobs = 4"));
        assert!(text.contains("RUST_BACKTRACE"));

        let dir = TempDir::new("setup-custom");
        let path = dir.join("config.toml");
        fs::write(
            &path,
            r#"[build]
rustc-wrapper = "/usr/local/bin/other"
incremental = true
jobs = 4

[env]
CARGO_INCREMENTAL = "1"
NEXUSCOMPILE_SOCKET = "/tmp/custom.sock"
RUST_BACKTRACE = "1"
"#,
        )
        .unwrap();

        unsetup_user(&path).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("rustc-wrapper = \"/usr/local/bin/other\""));
        assert!(text.contains("incremental = true"));
        assert!(text.contains("CARGO_INCREMENTAL = \"1\""));
        assert!(text.contains("NEXUSCOMPILE_SOCKET = \"/tmp/custom.sock\""));
        assert!(text.contains("jobs = 4"));
        assert!(text.contains("RUST_BACKTRACE"));
    }

    #[test]
    fn setup_rejects_invalid_config_without_modifying() {
        for (name, original) in [("non_table", "build = 1\n"), ("invalid_toml", "[build\n")] {
            let dir = TempDir::new("setup-reject");
            let path = dir.join("config.toml");
            fs::write(&path, original).unwrap();

            let error = setup_user(&path, DEFAULT_SOCKET).unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "case: {name}");
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                original,
                "case: {name} content"
            );
            assert!(!backup_path(&path).exists(), "case: {name} backup");
        }
    }

    #[test]
    fn cli_contract() {
        assert!(matches!(parse_cli(&[]), CliParse::NotSetup));
        assert!(matches!(parse_cli(&["doctor".into()]), CliParse::NotSetup));
        assert!(matches!(
            parse_cli(&["setup-user".into()]),
            CliParse::MissingConfig("setup-user")
        ));
        assert!(matches!(
            parse_cli(&["unsetup-user".into()]),
            CliParse::MissingConfig("unsetup-user")
        ));
        assert_eq!(run_cli(&["setup-user".into()]), Some(2));
        assert_eq!(run_cli(&["unsetup-user".into()]), Some(2));

        match parse_cli(&[
            "setup-user".into(),
            "--config".into(),
            "/tmp/config.toml".into(),
        ]) {
            CliParse::Command(CliCommand::SetupUser { socket, .. }) => {
                assert_eq!(socket, DEFAULT_SOCKET);
            }
            other => panic!("unexpected parse result: {other:?}"),
        }

        assert!(matches!(
            parse_cli(&[
                "unsetup-user".into(),
                "--config".into(),
                "/tmp/config.toml".into(),
            ]),
            CliParse::Command(CliCommand::UnsetupUser { .. })
        ));
    }
}
