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

## 2. Local wrapper and cache

```bash
cd /tmp
cargo new --lib nexus-smoke
cd nexus-smoke
export CARGO_INCREMENTAL=0
export RUSTC_WRAPPER=/path/to/NexusCompile/target/release/nexus-rustc
cargo build
cargo clean
cargo build   # second build should hit the result cache
```

Expected: both builds succeed; the second build logs a cache hit for the action digest.

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
