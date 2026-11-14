# NexusCompile

**A distributed Rust compile node for NexusNet** — intercepts cargo's rustc invocations through the local `nexus-rustc` wrapper and caches results by action digest. When local capacity is busy, it can forward compile tasks to other nodes over NexusNet.

This project is more than a NexusNet backend adapter: it implements the local wrapper, result cache, remote provider/consumer roles, and capacity-aware scheduling.

## Features

- **Local compile interception**: `nexus-rustc` acts as `RUSTC_WRAPPER`, taking over supported lib-crate builds and falling back to the real rustc for everything else.
- **Result cache**: caches outputs by action digest (toolchain, target, normalized arguments, input file hashes); outputs are stored in a SHA-256 CAS.
- **Remote compile pool**: a consumer uploads a bundle, a provider runs rustc and returns the outputs; tasks are coordinated through the CBOR `rustc-pool` v2 application protocol.
- **Capacity awareness**: samples CPU and memory on Linux, computes available slots, and publishes them through `Subscribe`/`CapacityUpdate`.
- **Deployment**: Debian `.deb` + systemd; paths are anchored by environment variables.

## Roles and terminology

| Term | Description |
|---|---|
| **node / backend** | This process. Listens on `127.0.0.1:<port>` and is dialed by the NexusNet node, which completes the hello handshake. |
| **wrapper** | `nexus-rustc`. cargo invokes it through `RUSTC_WRAPPER`; it forwards requests to this process over a Unix socket. |
| **provider** | The side that accepts `rustc-pool` tasks, runs rustc, and returns outputs. |
| **consumer** | The side that starts remote compiles when the local cache misses and local capacity is busy. |
| **task / action digest** | The normalized fingerprint of one rustc invocation (32 bytes), also used as the `TaskId`. |
| **bundle** | The manifest, sources, externs, and OUT_DIR inputs required by one remote task. |
| **slot** | One unit of compile concurrency, derived from CPU and memory. |

A single process can act as both provider and consumer.

## Architecture

```mermaid
graph LR
    C["cargo"] --> W["nexus-rustc"]
    W -->|"Unix socket"| L["local"]
    L --> K["Coordinator"]
    K -->|"cache hit"| M["materialize outputs"]
    K -->|"local slot free"| E["executor"]
    E --> R1["rustc"]
    K -->|"local busy"| S["NexusNet sidecar"]
    S -->|"rustc-pool"| P["remote provider"]
    P --> R2["rustc"]
    K --- CA["ResultCache / CAS"]
    P --- CA2["ResultCache / CAS"]
```

- **Local path**: cargo -> `nexus-rustc` -> task socket -> bundle materialization -> `Coordinator::execute` -> cache, local rustc, or remote provider.
- **Legacy local path**: cargo -> `nexus-rustc` -> local socket -> `Coordinator::execute` -> cache or local rustc (used as fallback).
- **Remote path**: consumer -> NexusNet -> provider's `rustc-pool` service -> provider rustc -> outputs transferred back by SHA-256.
- **Scheduling path**: `pool/runtime.rs` samples capacity; providers broadcast `CapacityUpdate`; consumers maintain provider views through `Subscribe`/`Subscribed`.

## Quick start

### 1. Start the backend

```bash
cargo run
```

The first run creates `./config.toml` automatically and listens on `127.0.0.1:{port}` (default port `5100`; it must match the `host`/`port` registered in NexusNet `local_services`).

Complete default configuration:

```toml
port = 5100
service_name = "rustc-pool"

[node]
max_usage_percent = 80
sample_interval_secs = 1
rustc = "rustc"

[subscription]
target_slots = 8
update_interval_secs = 30
ttl_secs = 60
target_reevaluate_secs = 5

[cache]
dir = ""
result_cache_mb = 512
```

### 2. Enable the local wrapper

```bash
cargo build
export CARGO_INCREMENTAL=0
export RUSTC_WRAPPER="$PWD/target/debug/nexus-rustc"
cargo build
```

`CARGO_INCREMENTAL=0` is required for the wrapper to take over compilation; calls that do not meet the requirements execute the real rustc directly. For release builds, use `target/release/nexus-rustc`.

### 3. Two-node example (optional)

Run this backend on both machines and register the local service name as `rustc-pool` in NexusNet:

```toml
[[services.dispatcher.local_services]]
name = "rustc-pool"
host = "127.0.0.1"
port = 5100
require_auth = false
```

Provider discovery is automatic through NexusNet. Both nodes must use the same
`service_name`, and NexusNet's `local_services.name` must match it. The consumer
only needs a target for remote capacity:

```toml
[subscription]
target_slots = 16
```

When the consumer exhausts its local slots, `Coordinator` tries to start a remote compile on a subscribed provider and falls back to local execution if the remote path is unavailable.

> `require_auth = false` is only suitable for localhost or trusted networks. Remote compilation executes rustc arguments and externs supplied by the submitter, so production deployments should enable NexusNet authentication and restrict peers that can reach this service.

## Configuration reference

### Top level

| Field | Default | Description |
|---|---|---|
| `port` | `5100` | Listen port; the address is always `127.0.0.1`. |
| `service_name` | `"rustc-pool"` | NexusNet service name used for discovery and peer requests. Must match `local_services.name`. |

### `[node]`

| Field | Default | Description |
|---|---|---|
| `max_usage_percent` | `80` | Upper bound on CPU and memory used by compile slots, as a percentage of total resources. |
| `sample_interval_secs` | `1` | Capacity sampling interval (seconds). |
| `rustc` | `"rustc"` | Fallback/default rustc executable used by the provider side. The Debian package points this at the base private toolchain. |
| `toolchains` | `[]` | Optional list of `{ channel, rustc, target }` entries. When non-empty, the provider selects the matching entry by `toolchain_id`. |

`cores_per_slot` and `mem_per_slot_mb` are fixed constants: `1.0` core and
`2048` MB per slot. Toolchain/target are selected automatically from the action
manifest; there are no manual labels.

### `[subscription]`

| Field | Default | Description |
|---|---|---|
| `target_slots` | `8` | Desired visible total capacity (local `max_slots` plus subscribed providers' `max_slots`). `0` disables active subscription. |
| `update_interval_secs` | `30` | `CapacityUpdate` keepalive interval; updates are also sent on this period when capacity is unchanged. |
| `ttl_secs` | `60` | Expiration time for a provider capacity view. |
| `target_reevaluate_secs` | `5` | How often visible capacity is compared with `target_slots`. |

> `target_slots` includes local capacity. If it is not greater than the local `max_slots`, no remote provider will be subscribed.

### `[cache]`

| Field | Default | Description |
|---|---|---|
| `dir` | `""` | Cache root. When empty, resolves via `NEXUSCOMPILE_CACHE` -> `$NEXUSCOMPILE_HOME/cache` -> `./cache`. |
| `result_cache_mb` | `512` | Result cache quota (MB). `0` disables the result cache while still allowing compilation. |

## Local compile wrapper

`nexus-rustc` only takes over invocations that meet all of the following conditions; everything else is passed through to the real rustc:

- `CARGO_INCREMENTAL=0`;
- crate name is not `build_script_build`;
- `--crate-type` contains only `lib` or `rlib`;
- `--emit` includes `link`;
- `--out-dir` is present;
- no `--test` or `--print`;
- no incremental compilation flags (`-C incremental`).

### Execution flow

1. Compute the action digest (toolchain, target, normalized args/env, input file hashes).
2. On a local result-cache hit, materialize the outputs into `--out-dir` and return success.
3. On a miss, rewrite `--out-dir` to a staging directory and run rustc.
4. On success, rewrite staging paths in `.d` files, store outputs in the CAS, then cache and materialize them into the real `--out-dir`.
5. On compile failure, return the exit code, stdout, and stderr unchanged.

The wrapper replays the local result to cargo as stdout/stderr and an exit code. If the node is offline, the invocation is unsupported, or infrastructure fails, the wrapper `exec`s the real rustc.

### Socket path

Priority:

1. `NEXUSCOMPILE_SOCKET`;
2. `$XDG_RUNTIME_DIR/nexus-compile.sock`;
3. `/tmp/nexus-compile-<user>.sock`.

The socket permission is `0660` (group read/write). Under systemd the socket is
placed at `/run/<pkg>/nexus.sock` with group `nexusnet`, so users in that group
can connect without sharing the service account.

### Cache behavior

- The cache is partitioned by action digest; outputs are stored by SHA-256 content address in the CAS.
- Result TTL is fixed at 24 hours; hits renew the TTL; the TTL resets after a restart.
- The cache is local only and is not shared across nodes. A remote provider's own cache can still hit on a later `Reserve` and return `Cached`.
- If the cache cannot be opened, it degrades to disabled; if local execution fails, the wrapper falls back to the real rustc.

## Distributed compile pool

### Task lifecycle

```text
Reserve
  -> BlobPut (upload the bundle in 1 MiB chunks)
  -> TaskReady
  -> provider runs rustc
  -> consumer polls Status
  -> BlobGet (download outputs in 1 MiB chunks)
  -> verify SHA-256 and write to the local cache
  -> ResultAck
```

- `TaskId` is the action digest (32 bytes); the same digest maps to the same task.
- `Reserve` accepts only `slots = 1` and returns:
  - `Granted`: a new task, or an already queued/running task with its lease;
  - `Cached`: existing outputs are available from the task entry or the provider result cache (blobs may have been evicted by the cache quota);
  - `Busy`: no local slot is available;
  - `Rejected`: the request is invalid.
- By default, the consumer polls `Status` once per second with an overall timeout of 30 minutes.
- `Done` returns the `OutputRef` list; the consumer downloads each chunk, verifies hashes, and writes to its local CAS/cache.
- Every `Reserve` that returns a result creates a lease. `ResultAck` releases that lease's pin, removes the task directory when the last lease is gone, and deletes the `TaskEntry`. The provider result cache remains available for later hits.
- `Reserve` and `Status` fall back to the provider result cache when no `TaskEntry` exists; a cache-only `Cached` reply carries a lease and pins the cache entry until `ResultAck` or lease expiry.
- `Cancel` marks the task `Cancelled` and frees the slot.

The provider side:

1. verifies that the bundle file hash equals `Reserve.input_manifest`;
2. recomputes the manifest digest and compares it with `TaskId`;
3. materializes bundle files into the task work directory;
4. substitutes `<MANIFEST>`, `<OUT>`, and `<EXTERN>` placeholders;
5. runs `[node].rustc` and collects outputs under `OUT_DIR`;
6. writes the local cache and marks the task `Done`.

### Capacity and subscriptions

- On Linux, reads `/proc/stat` and `/proc/meminfo`; CPU usage uses an EWMA (0.3 new value + 0.7 previous value).
- `max_slots = floor(total * max_usage_percent)` for CPU and memory, using fixed 1.0 core / 2048 MB slots.
- `free_slots` is the remaining headroom under the percentage cap; it is not reduced by active tasks because `/proc` usage already includes them.
- Providers send `CapacityUpdate` to subscribers; `seq` is monotonically increasing and older sequence numbers are ignored.
- Consumers build provider views through `Subscribe`/`Subscribed` and drop a provider if no update arrives within `ttl_secs`.
- Providers are discovered automatically through NexusNet `discover_providers`. Discovery runs only when capacity is insufficient: every 30 seconds when visible capacity is zero or the last discovery failed, otherwise every 1 hour.

### Cache and CAS

- `ResultCache` lives at `<cache>/actions/<digest>/manifest.cbor` and records the output list.
- Output blobs live at `<cache>/blobs/<sha256>`; writes use a temporary file plus rename, and materialization verifies the hash again.
- Compile staging lives at `<cache>/staging/`; provider task bundles and work dirs live at `<cache>/work/<digest>/`.
- The quota is controlled by `result_cache_mb`; unpinned entries are reclaimed oldest-expiry first.
- Providers pin results while a lease is outstanding, so shared consumers can download before `ResultAck`. Expired leases release their pins.

## NexusNet integration

Register this backend's service name as `rustc-pool` (or the configured `service_name`) in NexusNet; remote calls use that name:

```toml
[[services.dispatcher.local_services]]
name = "rustc-pool"
host = "127.0.0.1"
port = 5100
require_auth = false
```

- Both nodes must run this backend and use the same `service_name`.
- Providers are discovered automatically; there is no static peer list.
- Consumers send the action manifest's `toolchain_id` and `target` in `Reserve`; providers reject mismatches and missing target standard libraries.
- Production deployments should enable NexusNet authentication and allow only trusted peers to reach this service.

## Protocol

### sidecar v2

#### Framing

```text
u32_be(len) || cbor(message)      # len <= 16 MiB
```

The first frame in both directions must be `hello`; incompatible versions are disconnected. The current `PROTOCOL_VERSION = 2`.

#### Messages

Messages are discriminated by the text field `t`:

| `t` | Direction | Description |
|---|---|---|
| `hello` | both | Handshake and version negotiation |
| `request` | node -> backend | Forward an inbound service request `{ id, service, payload }` |
| `reply` | both | Correlated reply `{ id, ok, result?, error? }` |
| `list_services` | backend -> node | List global service types |
| `discover_providers` | backend -> node | Query providers for a service |
| `query_public_ip` | backend -> node | Query the node's public address |
| `whoami` | backend -> node | Query this node's identity |
| `reconnect_bootstrap` | backend -> node | Reconnect to bootstrap |
| `reannounce_services` | backend -> node | Re-announce local services |
| `reload_config` | backend -> node | Reload node configuration |
| `refresh_cache` | backend -> node | Clear application caches and non-self local Kademlia replicas |
| `relay_status` | backend -> node | Relay status |
| `pq_status` | backend -> node | Post-quantum status |
| `auth_status` | backend -> node | Authentication status |
| `query_key` | backend -> node | Read a DHT record |
| `add_key` | backend -> node | Write a DHT record / announce providing |
| `remove_key` | backend -> node | Stop announcing as a provider / remove the local record |
| `service_request` | backend -> node | Start a P2P service request (automatic provider selection) |
| `service_request_to` | backend -> node | Start a P2P service request to a specific peer |

- Correlation ids are UUIDs (encoded on the wire as a 16-byte CBOR `bstr`).
- `reply.result` is **CBOR** bytes defined per op; `add_key`'s `value` is a `bstr` and is binary-safe.
- Control commands time out after 60 seconds by default and reply with `error{code:"timeout"}`.

#### `reply.result` payloads

| op | result |
|---|---|
| `list_services` | `[* tstr]` |
| `discover_providers` | `[* tstr]` (PeerId strings) |
| `query_public_ip` | `{ ? ipv4: tstr, ? ipv6: tstr }` |
| `whoami` | CBOR object; contains at least `peer_id` |
| `reconnect_bootstrap` | `{ success: bool }` |
| `reannounce_services` / `reload_config` | `{ success: bool, ? error: tstr }` |
| `refresh_cache` | Node-defined CBOR object |
| `relay_status` | `{ need_relay: bool, target: uint, active: [* tstr], pending: [* tstr] }` |
| `pq_status` | `{ enabled: bool, transport: bool, identity: bool, required: bool }` |
| `auth_status` | `{ network: tstr, state: tstr, local_required: [* tstr], ... }` |
| `query_key` | `{ key: tstr, ? value: bstr, ? providers: [* tstr] }` |
| `add_key` | `{ success: bool, key: tstr }` |
| `remove_key` | Node-defined CBOR object |
| `service_request*` | Raw service response bytes |

#### Control helpers

`BackendClient` provides typed helpers for all control operations:

```rust
let ip: PublicIpInfo = client.query_public_ip().await?;
let providers: Vec<String> = client.discover_providers("rustc-pool").await?;
let record: QueryKeyResult = client.query_key("/oahd/service/rustc-pool").await?;

client.add_key("/oahd/service/rustc-pool", Some(b"meta".to_vec()), true).await?;
client.remove_key("/oahd/service/rustc-pool", true, false).await?;

let raw: Vec<u8> = client.service_request("rustc-pool", payload).await?;
let raw: Vec<u8> = client.service_request_to("rustc-pool", "12D3KooW...", payload).await?;

client.reconnect_bootstrap().await?;
client.reannounce_services().await?;
client.reload_config().await?;
client.refresh_cache().await?;
client.relay_status().await?;
client.pq_status().await?;
client.auth_status().await?;
client.whoami().await?;
```

Result structs are defined in `src/protocol.rs` (`PublicIpInfo`, `WhoamiInfo`, `QueryKeyResult`, `RelayStatusResult`, `PqStatusResult`, `SuccessResult`, `AddKeyResult`); `refresh_cache` and `remove_key` return `serde_json::Value`.

### rustc-pool application protocol v2

Application messages are carried as CBOR bytes inside sidecar `request.payload` / `reply.result`.

```text
Envelope { v: u16, body }
```

The current `APP_PROTOCOL_VERSION = 2`. `body` is discriminated by the text field `t`.

#### Requests

| `t` | Direction | Fields | Description |
|---|---|---|---|
| `subscribe` | consumer -> provider | `consumer`, `toolchain`, `target` | Subscribe to capacity updates |
| `unsubscribe` | consumer -> provider | `consumer` | Cancel a subscription |
| `capacity_update` | provider -> consumer | `provider`, `seq`, `free_slots`, `queue_len`, `accepting` | Capacity broadcast |
| `reserve` | consumer -> provider | `consumer`, `task_id`, `slots`, `input_manifest` | Request a slot |
| `blob_put` | consumer -> provider | `task_id`, `hash`, `offset`, `data` | Upload a bundle chunk |
| `blob_get` | consumer -> provider | `hash`, `offset`, `len` | Download an output chunk |
| `task_ready` | consumer -> provider | `task_id`, `lease_id` | Notify that the bundle upload is complete |
| `status` | consumer -> provider | `task_id` | Query task status |
| `cancel` | consumer -> provider | `task_id` | Cancel a task |
| `result_ack` | consumer -> provider | `lease_id` | Acknowledge that the result was retrieved |

#### Responses

| `t` | Fields | Description |
|---|---|---|
| `subscribed` | `provider`, `toolchain`, `target`, `max_slots` | Subscription accepted |
| `ack` | - | Generic acknowledgement |
| `cached` | `task_id`, `outputs`, `lease_id` | Result is available from the task entry or the provider cache |
| `granted` | `task_id`, `lease_id` | Slot granted |
| `busy` | `retry_after_ms` | No free slot |
| `rejected` | `code`, `message` | Request rejected |
| `chunk` | `hash`, `offset`, `data` | Blob chunk |
| `task_status` | `state` | Task status |
| `error` | `code`, `message` | Application error |

#### Types and limits

- `PeerId` -> CBOR `tstr`; `Hash32` -> 32-byte `bstr`; `LeaseId` -> UUID.
- `OutputRef { path, hash, size, mode }`.
- `TaskState`: `queued`, `running`, `done`, `failed`, `cancelled`.
- `ErrorCode`: `bad_request`, `unsupported_version`, `not_found`, `busy`, `rejected`, `not_implemented`, `invalid_state`, `compile`, `internal`.
- Maximum frame size is 16 MiB; upload and download chunks default to 1 MiB.
- The consumer polls every second with an overall timeout of 30 minutes; provider result-cache entries have a 24-hour TTL.

### bundle / manifest / action digest

- `BUNDLE_SCHEMA = 1`.
- Bundle structure: `{ schema, manifest, files: [{ path, mode, data }] }`.
- Bundle input path prefixes:
  - `pkg/`: files under `CARGO_MANIFEST_DIR`;
  - `out/`: files under `OUT_DIR`;
  - `extern/`: rlibs referenced by `--extern`;
  - `source/`: the crate root file.
- Key manifest fields: `toolchain_id`, `target`, `profile`, `cwd`, `crate_name`, `crate_type`, `edition`, `args`, `env`, `inputs`, `outputs`, `incremental`.
- The action digest is the SHA-256 of the CBOR-serialized manifest. The manifest includes:
  - the `toolchain_id` derived from `rustc -vV` and the target;
  - normalized args/env (paths replaced with `<MANIFEST>`, `<OUT>`, `<EXTERN>`, `<TARGET>`);
  - SHA-256 and size of every input file.
- The provider verifies the bundle file hash and manifest digest before running rustc.
- `<TARGET>` is currently used only for digest normalization; the provider substitutes only `<MANIFEST>`, `<OUT>`, and `<EXTERN>` before execution.

## Enabling cargo acceleration on Debian/Ubuntu

The `.deb` postinst uses debconf to ask for a Rust toolchain and which users
should be accelerated:

- the Rust channel defaults to `stable` and is downloaded as `nexusnet`;
- selected users are added to the `nexusnet` group;
- their `~/.cargo/config.toml` is merged with `rustc-wrapper`, `incremental = false`,
  `CARGO_INCREMENTAL = "0"`, and `NEXUSCOMPILE_SOCKET=/run/<pkg>/nexus.sock`;
- the question is shown again on upgrade and reinstall, and old managed keys are
  cleared before the new selection is written;
- a duplicate `config` invocation inside the same dpkg run is detected through
  `/var/lib/<pkg>/onboarding-in-progress`, so the wizard appears only once;
- postinst removes that marker only after configuration succeeds, so a failed
  install can be retried without re-asking; `dpkg-reconfigure` always asks;
- noninteractive installs do not modify users unless preseeded; the toolchain
  channel defaults to `stable` unless preseeded.

Preseed example:

```bash
nexuscompile nexuscompile/cargo_users multiselect alice, bob
nexuscompile nexuscompile/rust_toolchain string stable
```

Reconfigure later with:

```bash
sudo dpkg-reconfigure nexuscompile
```

### Private Rust toolchain

The installer asks for a Rust channel or exact version and defaults to `stable`.
It does not look for a system rustc. During `postinst`, the selected toolchain is
downloaded and installed as the `nexusnet` user under:

```text
/var/lib/<pkg>/toolchains/
  rustup/       RUSTUP_HOME
  cargo/        CARGO_HOME
  rust/current  symlink to the selected base toolchain
  rust/state    installed toolchain registry
```

When the selection is `stable`, the installer detects the actual stable version
and also installs the previous two minor releases, for example:

```text
stable (1.100.0)
1.99.0
1.98.0
```

Those three entries are written to `[node].toolchains`; the provider selects the
matching rustc from the registry for each remote task. An exact version selection
installs only that version. If a historical version cannot be downloaded, the
install continues with the versions that succeeded.

The directory is inside the existing `ReadWritePaths=/var/lib/<pkg>` sandbox.
No user home or user project access is required.

On removal or purge the toolchain is deliberately retained. The postrm output
prints the directory and asks the user to remove it manually with
`sudo rm -rf /var/lib/<pkg>/toolchains`.

When the sidecar is unavailable, `nexus-rustc` writes a 30-second backoff marker
and falls back to the real rustc. Builds still complete; incremental compilation
remains disabled while the wrapper config is active.

## Debian deployment

```bash
cargo build --release
./deploy/build-deb.sh
apt install -y ./target/packaging/nexuscompile_<version>_amd64.deb
```

Package, unit, and binary names are derived automatically from `Cargo.toml`'s `name` (`NexusCompile` -> `nexuscompile`). The package installs:

| File | Path |
|---|---|
| Main binary | `/usr/bin/NexusCompile` |
| Wrapper | `/usr/bin/nexus-rustc` |
| Toolchain installer | `/usr/lib/<pkg>/install-toolchain` |
| Configuration | `/etc/<pkg>/config.toml` (generated on first start) |
| Data/logs | `/var/lib/<pkg>/log` |
| Cache | Defaults to `/var/lib/<pkg>/cache` |
| Rust toolchains | `/var/lib/<pkg>/toolchains` (retained on removal) |
| systemd unit | `/lib/systemd/system/<pkg>.service` |

The service runs as NexusNet's dedicated `nexusnet` user; logs go to journald. Paths are anchored by environment variables:

| Variable | Purpose | systemd default |
|---|---|---|
| `NEXUSCOMPILE_HOME` | Data root | `/var/lib/<pkg>` |
| `NEXUSCOMPILE_CONFIG` | Configuration file | `/etc/<pkg>/config.toml` |
| `NEXUSCOMPILE_LOG_PATH` | Log directory | `/var/lib/<pkg>` |
| `NEXUSCOMPILE_CACHE` | Cache directory | `$NEXUSCOMPILE_HOME/cache` when unset |
| `NEXUSCOMPILE_SOCKET` | Local wrapper socket | `/run/<pkg>/nexus.sock` under systemd; otherwise `$XDG_RUNTIME_DIR/nexus-compile.sock` or `/tmp/nexus-compile-<user>.sock` |
| `NEXUSCOMPILE_TASK_SOCKET` | Local bundle/task socket | `/run/<pkg>/task.sock` under systemd; otherwise derived from `NEXUSCOMPILE_SOCKET` |
| `RUSTUP_HOME` | Private rustup toolchain root | `/var/lib/<pkg>/toolchains/rustup` |
| `CARGO_HOME` | Private rustup proxy/cargo root | `/var/lib/<pkg>/toolchains/cargo` |

`deploy/service.service` sets the first three variables plus `NEXUSCOMPILE_SOCKET=/run/<pkg>/nexus.sock`. The cache defaults to `$NEXUSCOMPILE_HOME/cache`. The service creates the socket with mode `0660` in a `RuntimeDirectory`; users that need local compile acceleration must be members of the `nexusnet` group. `PrivateTmp=true` is kept, so the socket is never placed in `/tmp` for the systemd deployment.

```bash
systemctl status <pkg>
journalctl -u <pkg> -f
```

Install a new `.deb` to upgrade. `apt remove` / `apt purge` do not delete `/var/lib/<pkg>/` and do not remove the `nexusnet` user.

> This backend shares the `nexusnet` account with NexusNet and can therefore read the node's keypair and configuration. It is intended only for trusted backends; use a dedicated account if isolation is required.

## Module map

| Module | Responsibility |
|---|---|
| **main** | Entry point: configuration, listener, accept loop, result-cache setup, graceful shutdown |
| **state** | Shared `AppState` (provider, consumer, cache, workspace, config, control handle) |
| **config** | Load/generate `config.toml` and expose `ConfigHandle` |
| **paths** | Path resolution anchored by environment variables, with current-directory fallback for local runs |
| **log** | Leveled logging split into `log/format.rs` and `log/file.rs`: terminal output, file rotation (gz at 10 MB), journald mode |
| **protocol** | sidecar v2 contract: `Message`, control result payloads, frame encoding/decoding |
| **control** | `ControlPlane` abstraction over the local node connection |
| **connection** | Handshake, read loop, pending table, and `BackendClient` control helpers |
| **compile_protocol** | `rustc-pool` application v2: `AppRequest`, `AppResponse`, task states |
| **service** | Decode and dispatch application requests: subscribe, reserve, blob, status, cancel, result ack |
| **task_blob** | Task bundle chunk I/O and task-directory cleanup |
| **local_protocol** | Local wrapper <-> node protocol |
| **local** | Unix socket server and local/remote compile orchestration |
| **nexus-rustc** (bin) | cargo `RUSTC_WRAPPER` entry point |
| **compile** | Compile primitives: `executor` (local staging), `manifest` (digest), `bundle`, `cache`, `cas`, and `hash` |
| **remote** | Consumer `Coordinator` over `remote/transfer.rs` (bundle upload, task polling, result import) |
| **pool** | `Provider` (capacity/subscriptions/tasks/leases), `Consumer` (provider views), and `runtime` scheduling |
| **workspace** | Staging and provider task directories |
| **capacity** | `/proc` sampling and slot calculation |
| **util** | Shared argument, path, filesystem, process, and time helpers |

## Manual test checklist

See [docs/manual-test.md](docs/manual-test.md) for the two-node/local smoke checklist used before release.

## Tests

```bash
cargo fmt -- --check
cargo test --verbose
cargo build --release
./deploy/build-deb.sh
```

## Current limitations and security boundary

- **Shared task lifetime**: multiple consumers can share a task lease; the first `ResultAck` removes the `TaskEntry`. Other consumers recover the result through the provider cache via `Status`; if the cache entry is evicted first, they retry or fall back locally.
- **Unacked tasks**: task entries whose consumers never send `ResultAck` remain in memory until the same task id is reserved again or the process restarts.
- **Local builds do not consume slots**: `nexus-rustc` local execution does not go through `Provider::reserve`; concurrency is mainly controlled by cargo/the wrapper. Capacity sampling only reflects load after the fact.
- **Single-connection assumption**: the server accepts multiple NexusNet connections, but `AppState` holds one control handle; the newest connection wins, and an older disconnect cannot clear it.
- **Toolchain selection**: consumers pass the action manifest's `toolchain_id` and `target` in `Reserve`. Providers reject mismatches and check that the target standard library is installed. This is a local compatibility check, not a security boundary.
- **Trust boundary**: remote compilation executes the submitter's rustc arguments and externs (including proc-macros), which is equivalent to running code on the provider; the consumer only verifies output hashes and does not independently validate the compile result. Deploy only on trusted networks with NexusNet authentication enabled.
- **Discovery**: providers are found automatically through NexusNet `discover_providers`; there is no static peer list. An empty/failed discovery result clears the current provider views and retries after 30 seconds; a normal but insufficient result retries after 1 hour.
- **Cache granularity**: the action digest hashes the entire `CARGO_MANIFEST_DIR` (excluding `target`, `.git`, and `node_modules`), so changes to non-build files also invalidate the cache; full-tree hashing and bundle construction are expensive for large packages.

## Migration notes

- The old UUID frame protocol (`[uuid_len][uuid][payload_len][payload]`) has been fully replaced by the CBOR v2 contract.
- The application protocol is now `rustc-pool` v2; NexusNet's `local_services.name` should be set to `rustc-pool` (the old `example` name no longer applies).
- The local wrapper requires `CARGO_INCREMENTAL=0`; incremental compilation is not supported.
- Remote compilation requires both nodes to run this backend and use the same `service_name`; provider discovery is automatic.
