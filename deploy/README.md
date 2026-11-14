# Deployment and deb installation guide

NexusCompile is distributed as a native Debian package (`.deb`); `dpkg`/`apt` manage its install, upgrade, and removal lifecycle.
The service runs as NexusNet's dedicated `nexusnet` user; paths and the systemd unit are declared by the package.

Package and binary names are **derived automatically from `Cargo.toml`'s `[package]` section** at packaging time:

| Source | Default | Purpose |
|---|---|---|
| `Cargo.toml` `name` | `NexusCompile` | Binary name |
| Normalized (lowercase, `_` -> `-`) | `nexuscompile` | Debian package / systemd unit name |

> Below, `<pkg>` refers to the normalized package name and `<bin>` to the original binary name. After changing
> `Cargo.toml`'s `name`, `build-deb.sh` follows automatically; no script changes are needed.

## Directory layout

| File | Path | Owner/permissions | Description |
|---|---|---|---|
| Configuration | `/etc/<pkg>/config.toml` | `nexusnet:nexusnet 0644` | **Generated automatically on first start** (not shipped in the package) |
| Data/logs | `/var/lib/<pkg>/log` | `nexusnet:nexusnet 0700` | Rotated files (`.gz`) live in the same directory; nothing is written in journald mode |
| Cache | `/var/lib/<pkg>/cache` | `nexusnet:nexusnet` | Result cache, created on first run |
| Logs | journald | - | Collected, rotated, compressed, and retained by systemd |
| Binary | `/usr/bin/<bin>` | `root:root 0755` | Main program |
| Wrapper | `/usr/bin/nexus-rustc` | `root:root 0755` | `RUSTC_WRAPPER` entry point |
| Unit | `/lib/systemd/system/<pkg>.service` | `root:root 0644` | systemd unit |

Paths are anchored by the `NEXUSCOMPILE_HOME` / `NEXUSCOMPILE_CONFIG` / `NEXUSCOMPILE_LOG_PATH` /
`NEXUSCOMPILE_CACHE` / `NEXUSCOMPILE_SOCKET` environment variables; see `src/paths.rs`.
A local `cargo run` with no environment variables still falls back to `./config.toml` / `./log` / `./cache`.

## Installation

Run as root on Debian/Ubuntu:

```bash
apt install -y ./<pkg>_<version>_amd64.deb
```

The installation automatically ensures the dedicated `nexusnet` user exists, creates data directories, makes
journald persistent, runs `daemon-reload`, enables the unit, and starts it.

```bash
systemctl status <pkg>        # active (running)
journalctl -u <pkg> -f        # follow logs (single-line, leveled, ANSI colors removed)
```

Logs are written to journald with syslog priorities; use `journalctl -u <pkg> -p err` to filter errors and above.

## Upgrade

```bash
apt install -y ./<pkg>_<new-version>.deb
```

During an upgrade, prerm stops the service and postinst starts it again; configuration and data are preserved.

## Removal

```bash
apt remove <pkg>             # remove the package and stop the service
apt purge <pkg>              # remove package configuration as well
```

Neither `remove` nor `purge` deletes runtime data under `/var/lib/<pkg>/`, and neither removes the `nexusnet` user.

## Environment variables

| Variable | Purpose | systemd default |
|---|---|---|
| `NEXUSCOMPILE_HOME` | Data root | `/var/lib/<pkg>` |
| `NEXUSCOMPILE_CONFIG` | Configuration file | `/etc/<pkg>/config.toml` |
| `NEXUSCOMPILE_LOG_PATH` | Log directory | `/var/lib/<pkg>` |
| `NEXUSCOMPILE_CACHE` | Result cache directory | `$NEXUSCOMPILE_HOME/cache` when unset |
| `NEXUSCOMPILE_SOCKET` | Local wrapper socket | `/run/<pkg>/nexus.sock` under systemd |
| `NEXUSCOMPILE_TASK_SOCKET` | Local bundle/task socket | `/run/<pkg>/task.sock` under systemd |
| `RUSTUP_HOME` | Private rustup toolchain root | `/var/lib/<pkg>/toolchains/rustup` |
| `CARGO_HOME` | Private rustup proxy/cargo root | `/var/lib/<pkg>/toolchains/cargo` |

`deploy/service.service` sets `NEXUSCOMPILE_SOCKET=/run/<pkg>/nexus.sock` and uses
`RuntimeDirectory=<pkg>` with mode `0770`. The socket itself is `0660` and owned by
`nexusnet:nexusnet`. Users that should use cargo acceleration must be members of the
`nexusnet` group. `PrivateTmp=true` is kept; the socket is not placed in `/tmp`.

The postinst runs a debconf multiselect for the users to accelerate. Selected users are
added to `nexusnet` and receive a managed `~/.cargo/config.toml`. The question is shown
again on upgrade and reinstall, and managed Cargo keys are cleared before being rewritten.
A duplicate config invocation within one dpkg run is suppressed with
`/var/lib/<pkg>/onboarding-in-progress`; postinst removes the marker after a successful
configuration, and `dpkg-reconfigure` always asks.
Noninteractive installs do not modify users unless preseeded:

```bash
nexuscompile nexuscompile/cargo_users multiselect alice, bob
```

Reconfigure later with `sudo dpkg-reconfigure nexuscompile`. Uninstall clears the managed
Cargo keys but never removes users from the shared `nexusnet` group.

When `JOURNAL_STREAM` is present, logging switches to journald mode automatically (set by systemd);
`NO_COLOR` or a non-TTY disables colors.

## Private Rust toolchain

The package installs its own Rust toolchains; it does not require a system rustc.
The default channel is `stable`. `postinst` downloads `rustup-init` as `nexusnet`
and stores the result under:

```text
/var/lib/<pkg>/toolchains/
  rustup/
  cargo/
  rust/
    current -> selected base toolchain
    state   -> installed toolchains
```

For `stable`, the installer auto-detects the current stable version and installs
the previous two minor releases as well. Exact version selections install only
that version. The provider resolves tasks by `toolchain_id` across the registry.
If a historical toolchain fails to install, the install keeps the successful
ones. The service unit sets `RUSTUP_HOME` and `CARGO_HOME` and runs
`NexusCompile doctor --skip-socket` before starting. `postrm` never deletes the
toolchains; it prints `/var/lib/<pkg>/toolchains` and a manual `rm -rf` command.

## Directory contents

| File | Purpose |
|---|---|
| `build-deb.sh` | Assembles the `.deb` with `dpkg-deb` (no cargo-deb needed) and derives names automatically |
| `install-toolchain` | Downloads and installs the private Rust toolchain as `nexusnet` |
| `service.service` | systemd unit (packaging asset with `@PKG@`/`@BIN@`/`@USER@` placeholders) |
| `service.tmpfiles.conf` | Pre-creates directories and sets ownership (packaging asset) |
| `../deb/{postinst,prerm,postrm}` | Debian maintainer scripts (packaging assets) |
| `../deb/{config,templates}` | debconf user onboarding and Rust toolchain selection |
| `publish-release.sh` | Idempotent manual release helper: upload files from a directory to a Gitea Release |

## Building the deb manually

```bash
cargo build --release
./deploy/build-deb.sh
# Output: target/packaging/<pkg>_<version>_amd64.deb
```

## Common commands

```bash
systemctl restart <pkg>         # restart
systemctl stop <pkg>            # stop (graceful SIGTERM)
systemctl cat <pkg>             # show the current unit definition
systemctl show <pkg>            # show detailed runtime status
```

## Notes

- The service runs as NexusNet's dedicated user `nexusnet`.
  Sharing the account means this backend can read the node's `/var/lib/nexusnet/keypair.bin` and
  `/etc/nexusnet/config.toml`; this is only suitable for trusted backends. Use a dedicated account for isolation.
- The unit is hardened with `NoNewPrivileges`, `ProtectSystem=strict`, and similar settings; `ReadWritePaths`
  grants write access to the configuration and data directories. Logs are sent to journald via stdout/stderr.
- On `SIGTERM`, the program shuts down gracefully; `TimeoutStopSec=15` prevents a stuck process from being killed too late.
- This service only listens on `127.0.0.1:<port>`; it must match the `host`/`port` registered in NexusNet's
  `local_services`. `local_services.name` should be set to `rustc-pool`.
