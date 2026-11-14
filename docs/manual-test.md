# NexusCompile manual test checklist

Run from the repository root unless noted.

## 0. Build

```bash
cargo fmt -- --check
cargo test --verbose
cargo build --release
```

## 1. First start

```bash
cargo run
```

Expected:

- `config.toml` is created next to the binary (or at `NEXUSCOMPILE_CONFIG`).
- The local socket is created (`NEXUSCOMPILE_SOCKET`, `$XDG_RUNTIME_DIR/nexus-compile.sock`, or `/tmp/nexus-compile-<user>.sock`).
- The cache directory is created.
- The process listens on `127.0.0.1:<port>`.

## 1b. Debian package install and user onboarding

```bash
sudo apt install -y ./target/packaging/nexuscompile_<version>_amd64.deb
```

Expected:

- A debconf multiselect asks which users should be accelerated.
- Initial selection is the previous NexusCompile selection, or the current
  `nexusnet` group members on first install.
- Noninteractive installs do not modify users unless preseeded.
- Selected users are added to `nexusnet`.
- Their `~/.cargo/config.toml` contains exactly one managed `[build]` block and
  one managed `[env]` block.
- Re-running the installer or `dpkg-reconfigure nexuscompile` shows the wizard
  again and does not duplicate keys.

Reconfigure later:

```bash
sudo dpkg-reconfigure nexuscompile
```

Preseed example:

```bash
nexuscompile nexuscompile/cargo_users multiselect alice, bob
```

## 1c. Systemd socket

```bash
ls -ld /run/nexuscompile
ls -l /run/nexuscompile/nexus.sock
```

Expected:

```text
drwxrwx--- nexusnet nexusnet /run/nexuscompile
srw-rw---- nexusnet nexusnet /run/nexuscompile/nexus.sock
```

The current session needs the group:

```bash
newgrp nexusnet
# or log out and back in
```

## 2. Local wrapper and cache

After the Debian package has configured the user, `~/.cargo/config.toml`
sets the wrapper and socket automatically:

```bash
cd /tmp
cargo new --lib nexus-smoke
cd nexus-smoke
cargo build
cargo clean
cargo build   # second build should hit the result cache
```

If you are running from a local build instead of the package:

```bash
export NEXUSCOMPILE_SOCKET=/run/nexuscompile/nexus.sock
export CARGO_INCREMENTAL=0
export RUSTC_WRAPPER=/path/to/NexusCompile/target/release/nexus-rustc
cargo build
```

Expected: both builds succeed; the second build logs a cache hit for the action digest.

Check for duplicate managed settings:

```bash
grep -c '^\[build\]' ~/.cargo/config.toml
grep -c '^\[env\]' ~/.cargo/config.toml
grep -c 'rustc-wrapper' ~/.cargo/config.toml
```

Each should be `1`.

## 3. Unsupported invocations

```bash
cargo test
cargo build --bin something
```

Expected: test/build-script/bin invocations bypass the wrapper and use the real rustc.

## 4. Backend offline fallback

Stop the NexusCompile process, then:

```bash
cargo clean
cargo build
```

Expected: `nexus-rustc` falls back to `exec rustc` and the build succeeds.
The first failure writes a 30-second backoff marker; subsequent invocations
should skip the sidecar without waiting on the socket.

## 5. Remote cached result

With a mock or second node registered as `rustc-pool`:

1. Reserve returns `granted` or `cached`.
2. `blob_put` chunks are accepted.
3. `task_ready` starts rustc.
4. `status` reaches `done` with output refs.
5. `blob_get` chunks verify by SHA-256.
6. `result_ack` releases the lease.

Expected: the consumer writes `lib<crate>.rlib`, `.rmeta`, and `.d` into `--out-dir`.

## 6. Provider failure fallback

Kill or cancel the provider in the middle of a task.

Expected: the consumer retries another provider or falls back to local execution; cargo still completes.

## 7. Shared result

Reserve the same task from two consumers before either sends `result_ack`.

Expected:

- Both consumers can download the result.
- The task entry is removed only after the last lease is acknowledged.

## 8. Cache quota and pinning

Set `result_cache_mb` to a small value, compile a few crates, then run a cached remote task.

Expected:

- Pinned results are not evicted before `result_ack`.
- After `result_ack`, old unpinned entries are reclaimed.

## 9b. Automatic discovery

With two nodes using the same `service_name`:

1. Make the local provider busy or raise `target_slots`.
2. The consumer calls NexusNet `discover_providers`.
3. Compatible providers are subscribed automatically.
4. Toolchain/target mismatch is rejected at `Reserve`, not by a manual label.
5. Empty/failed discovery clears provider views and retries after 30 seconds.
6. A normal but insufficient list retries after 1 hour.

Expected: no `static_peers` or toolchain/target labels in `config.toml`.

## 9. Reconnect

Stop and restart the NexusNet connection.

Expected:

- Capacity sampling continues.
- The new connection replaces the control handle.
- A disconnect from an older connection does not clear the newer handle.
- Subscriptions are not duplicated.

## 10. Logs

```bash
journalctl -u <pkg> -f
```

Expected key events:

- local compile / cache hit with an 8-character action digest;
- remote provider selection and completion;
- provider task start, failure, and completion;
- subscription expiry.

## 11. Packaging

```bash
cargo build --release
./deploy/build-deb.sh
```

Expected: `target/packaging/<pkg>_<version>_amd64.deb` is produced and contains both binaries.
