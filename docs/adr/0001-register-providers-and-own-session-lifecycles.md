---
status: accepted
date: 2026-09-07
---

# ADR-0001: Register providers and give executors ownership of session lifecycles

## Decision

Use one explicit built-in provider registry for CLI checks, resource discovery, configuration dispatch and TUI execution. Each provider exposes descriptors and creates a configured executor. The executor owns native clients, operations, connection-status reporting and bounded shutdown. Use `#[async_trait]` for asynchronous methods invoked through trait objects.

This is the accepted **desired architecture**, not a claim of implemented behavior. Kafka, NATS and DynamoDB implementations, live following and executable plugins are outside the initial provider refactor.

## Current evidence

Inspected source: `main` at `6607e3f`, before this documentation change.

| Deciding surface | Current behavior |
| --- | --- |
| [Root CLI](../../src/main.rs), `run` and `Command::Schema` | Explicit PostgreSQL/Qdrant check dispatch and a hardcoded datasource filter list |
| [Core config](../../crates/core/src/config.rs), `Connection` and `ResolvedConnection` | Backend-specific configuration enums and secret resolution in core |
| [TUI worker](../../crates/tui/src/ui.rs), `run` | Direct PostgreSQL fetch call; other backends cannot browse |
| [Navigation](../../crates/tui/src/app.rs), `descriptor` and `act` | PostgreSQL-specific entry resource, descriptor lookup and column-navigation targets |
| [Schema dump](../../src/schema.rs), `dump` | Explicit assembly of both connectors' capabilities |
| [PostgreSQL descriptors](../../crates/postgres/src/lib.rs) and [Qdrant capabilities](../../crates/qdrant/src/lib.rs) | Existing connector-owned data to reuse; Qdrant currently advertises checks only |
| [PostgreSQL requests](../../crates/postgres/src/browse.rs), `fetch` | Each request owns and discards its client/driver; cancellation cleanup has a one-second budget |

Package isolation already exists. Runtime registration does not. Adding a datasource currently requires changing several shared modules. Persistent transport reuse is not current PostgreSQL browsing behavior.

## Provider and executor interfaces

Keep shared contracts in `onetui-core`, without native database SDK dependencies. Each connector crate owns its provider, strict typed config, executor, resource/action implementations and tests. Root registers built-ins and passes the registry to CLI/TUI/catalog consumers. TUI production code must not import concrete connectors.

The following Rust fragments illustrate the intended interface; they are not standalone programs or shipped APIs. Supporting contract names describe responsibilities below, not additional frameworks to scaffold now.

```rust
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

pub trait Provider: Send + Sync {
    fn descriptor(&self) -> &ProviderDescriptor;

    fn validate_config(&self, options: &toml::Table) -> Result<()>;

    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Box<dyn Executor>>;
}

#[async_trait]
pub trait Executor: Send + Sync {
    fn status(&self) -> watch::Receiver<ConnectionStatus>;

    async fn check(&self, context: RequestContext) -> Result<CheckResult>;

    async fn fetch_page(
        &self,
        request: PageRequest,
        context: RequestContext,
    ) -> Result<Page>;

    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()>;
}
```

- `ProviderDescriptor` contains kind, configuration documentation, resource/action descriptors and an optional entry resource. Read modes and contextual actions belong to each resource. Reuse existing descriptors and extend only where dispatch needs more information.
- `validate_config` parses provider-owned Rust types without network access or secret lookup. Validate all configured entries so unknown fields in an unselected entry do not become silently accepted. `configure` resolves only the selected alias's secret references and constructs its executor; establishing transport is lazy and asynchronous.
- `RequestContext` carries an absolute monotonic deadline and cancellation signal. Connection establishment, queueing and execution share the active request budget; retries must not reset it. The application attaches immutable alias/resource scope and a generation ID to completion events.
- `PageRequest` identifies the resource and a provider-owned page position/continuation. Do not require every provider to understand a numeric offset. Adapt existing PostgreSQL offsets inside its connector.
- `Page` retains bounded display rows/columns, continuation and notices. Reuse the existing display model; do not turn every native value into JSON or discard native identity. Tokens are opaque to the shell, scoped to alias/resource/query and counted in memory budgets.
- `CheckResult` describes the access actually tested, such as collection listing. Success is not a promise that all resources are readable. All error/status text must remain credential-safe and terminal-safe.
- `ConnectionStatus` reports configured/connecting/connected/disconnected/reconnecting/closing/closed state where observable. It describes transport lifecycle, not data freshness or authorization. A watch channel coalesces status updates; it is not a lossless record of streamed-message gaps.
- `ShutdownContext` supplies a separate finite cleanup deadline. Shutdown rejects new work, cancels active work and releases owned tasks/transports. The application coordinates outstanding borrows/tasks before calling the exclusive shutdown method.

`fetch_page` is capability-gated. A check-only provider returns an explicit unsupported-operation error if called anyway; it must not manufacture an empty page. Do not add a `follow` method until the first live-message implementation establishes its concrete stream/handle contract.

### Registration and caller examples

Register once at binary startup. Registration rejects duplicate kinds and invalid/duplicate descriptor IDs. Registry enumeration and offline output must be deterministic.

```rust
let mut providers = ProviderRegistry::new();
providers.register(PostgresProvider)?;
providers.register(QdrantProvider)?;
```

The registry stores providers behind trait objects; the illustrative `register` accepts a concrete provider and boxes it internally. Adding a datasource requires a connector crate with its own tests, a Cargo dependency and one registration. It must not require new core config variants, CLI value lists, schema branches or TUI backend conditions.

Headless validation uses the configured executor too:

```rust
let provider = providers.get(connection_config.kind())?;
let mut executor = provider.configure(connection_config.options(), &env)?;

let checked = executor.check(request_context).await;
let closed = executor.shutdown(shutdown_context).await;

// Shutdown runs even when validation fails. Preserve the primary error.
let result = checked?;
closed?;
println!("{}", result.summary);
```

The application also routes Ctrl-C into cancellation and cleanup; it must not simply drop the executor future and assume native work stopped. If both checking and shutdown fail, retain the primary error and report the cleanup failure safely as secondary context.

The TUI worker calls `executor.fetch_page(request, context).await`; it never calls `onetui_postgres::fetch` directly. Offline `schema` enumerates provider descriptors without loading config, constructing executors, resolving secrets or connecting. Contextual help and navigation use those same descriptors and provider-owned action targets. The PostgreSQL columns action must no longer construct `postgres.columns` paths in the shell. Connector-local functions and resource `match` statements remain valid implementation details.

## Read semantics across datasources

| Datasource/resource | Contract and limitation |
| --- | --- |
| PostgreSQL rows | Independent bounded read-only page queries; native keyset or explicit best-effort offset fallback, without a cross-page snapshot |
| Qdrant points | Native scroll continuation; lazy payload/vector detail; currently planned, not advertised as implemented |
| DynamoDB items | Distinguish key query from scan. Preserve `LastEvaluatedKey`; an empty filtered page may still continue. Limits bound evaluated items/response size, not guaranteed returned matches. Do not silently loop through scans to fill the screen. [AWS Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html), [Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html) |
| Kafka records | Positions are per partition; there is no global topic offset/order. Historical reads and live following are distinct. Observe with independent partition assignment and no offset commits by default, not an application's consumer group. [Kafka consumer semantics](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) |
| NATS Core subjects | Live observation, not historical paging or guaranteed resume. Use an independent subscription, not an application queue group. Core delivery is at-most-once. [NATS Core](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/core-nats/README.md) |
| NATS JetStream messages | Stored-message inspection differs from stateful consumer delivery. Prefer direct stored-message reads where supported; consumer creation, acknowledgement and deletion require explicit semantics and authorization. Acknowledgement can affect retention. [Consumers](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/consumers.md), [Streams](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md) |
| RabbitMQ | Metadata/metrics first. Stateful message inspection needs its own consuming/requeue/acknowledgement decision; do not label it passive browsing |

Future following requires explicit start/stop, finite buffers, backpressure where supported and visible gaps/drop counts where it is not. A quiet stream is not end-of-data or an expired page. Define stream-start/read deadlines separately from the deliberate lifetime of a live view. Do not force an infinite subscription into repeated page calls.

Resource descriptors declare which read modes exist. Kafka topic metadata may page while topic records follow; NATS Core and JetStream expose different resources. Supported actions are not permission guarantees. Data-changing or consuming actions must never hide behind `check` or ordinary browsing.

## Connection lifetime and cancellation

| Owned object | Lifetime |
| --- | --- |
| Provider registration | Application process; no live connections |
| Configured executor and reusable native clients | Selected alias session; not necessarily one physical socket |
| Query/transaction | One bounded page/check/detail operation |
| Live reader/subscription, when implemented | Explicit follow operation until stopped |
| Displayed pages | UI memory/history budget, independent of connection health |

Open an alias lazily; do not preconnect every configured alias. Keep its executor while navigating its resources. Retain suitable native clients across successful requests: PostgreSQL client plus driver, Qdrant gRPC channel, DynamoDB SDK transport, Kafka client or NATS client. Initially retain only one selected executor, not an unbounded pool/cache of inactive aliases.

```text
select alias -> configure executor -> first operation establishes transport
             -> fetch page -> finish transaction -> display cached page
             -> next operation reuses healthy client

switch alias / disconnect / quit
             -> invalidate old generation -> cancel work -> bounded shutdown
             -> release old client/tasks before opening replacement
```

One worker owns the active executor. Preserve one active foreground operation and one pending replacement request; replace superseded pending work rather than queueing navigation indefinitely. Native driver/heartbeat tasks run independently of terminal input, not through the foreground request queue.

For PostgreSQL, persistent socket reuse must never introduce a cursor or transaction spanning user think time. Finish commit/rollback before delivering a page. An idle connection differs from an idle transaction, which can retain locks and obstruct vacuum. [PostgreSQL session settings](https://www.postgresql.org/docs/current/runtime-config-client.html), [tokio-postgres client/driver ownership](https://docs.rs/tokio-postgres/latest/tokio_postgres/)

On PostgreSQL cancellation/timeout, initially retire the affected physical connection after bounded cancellation cleanup. Keep the logical executor available for a later explicit read, which establishes a new connection. Never let a delayed cancellation target the next query on a reused socket. Other errors allow reuse only after the provider establishes clean protocol/transaction state; uncertain cleanup means discard.

Transport reconnection is provider-owned. PostgreSQL reconnects for an explicit new read; native NATS/Kafka reconnection may maintain an active selected session. There must be no second generic retry loop competing with the SDK. Stop retries on shutdown, keep foreground attempts inside their deadline and never silently replay state-changing operations. Reconnection does not prove cursor validity or recover missed live messages: preserve explicit refresh/resume/gap behavior.

Normal shutdown is asynchronous and bounded. Current PostgreSQL cleanup allows one second and the TUI waits up to two seconds for a quitting worker; preserve those existing bounds in the first refactor. Persistent-client implementation must additionally prove owned driver/reconnect tasks and sockets are released on the forced-close path. Dropping a Tokio task handle alone does not stop it. Drop is fallback local resource release, not proof of graceful protocol completion. No automatic acknowledgements/offset commits are permitted as a shutdown side effect.

## Heartbeats and connection status

Keep native liveness machinery running for a selected live session even when the user is idle. Do not implement a universal `heartbeat()` method or a UI timer that repeatedly calls `check()`.

| Provider | Desired policy |
| --- | --- |
| PostgreSQL | Native TCP keepalive, not recurring `SELECT 1`. `tokio-postgres` exposes keepalive controls; TCP controls do not apply to Unix sockets. Keepalive does not prevent a server-enforced idle-session close. [Configuration](https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html) |
| Qdrant | Native gRPC/HTTP2 keepalive where server/proxy policy permits. Do not enable aggressive pings without active calls by default; servers can reject them with `too_many_pings`. [gRPC keepalive](https://grpc.io/docs/guides/keepalive/) |
| NATS | Native PING/PONG and SDK reconnect events; no duplicate application heartbeat task. [async-nats options](https://docs.rs/async-nats/latest/async_nats/struct.ConnectOptions.html) |
| Kafka | Native broker-connection handling. Consumer-group heartbeats maintain membership, not generic health; do not join a group just to obtain heartbeats. Group heartbeat settings also depend on the selected protocol. [Consumer configuration](https://docs.confluent.io/cloud/current/client-apps/consumer.html) |
| DynamoDB | Reuse the SDK client; no periodic application-level validation requests by default |

Cancellation ends an operation, not necessarily all client liveness work. Stopping follow removes its reader/subscription. Disconnect, alias switch and quit stop the executor's heartbeat/reconnect machinery. Native failures update connection status without deleting cached pages. Coalesce status updates and do not render on every successful ping.

Transport liveness is not query success, resource permission or data freshness. A status indicator must not imply continuous verification when the SDK exposes only last-observed state. Active requests still need deadlines. Keepalive cannot guarantee an immortal socket or bypass server lifecycle policies.

## Alternatives rejected or deferred

| Alternative | Verdict and reason |
| --- | --- |
| Backend `if`/`match` dispatch scattered through CLI, core, TUI and schema | Reject. Registration and provider-owned behavior replace these branches; connector-local resource matching remains valid |
| Public standalone connector `check` functions called directly by CLI | Reject as the shared interface. `check` is an executor operation; private native helpers remain fine |
| Handwritten `ProviderFuture = Pin<Box<dyn Future<...>>>` everywhere | Valid Rust, not technically unusable; reject as our author-facing convention because it adds avoidable implementation boilerplate. Choose `async-trait`, which generates essentially the same boxed futures; no allocation/performance advantage is claimed. [Macro behavior](https://docs.rs/async-trait/latest/async_trait/) |
| Native `async fn` in a trait used directly as `dyn Trait` | Not supported by the stable dyn-compatible interface discussed here. Native async traits alone do not solve the mixed-provider registry. [Rust Reference](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility) |
| Copy SQLx's full `Executor` model | Reject for the common interface. SQLx uses SQL/database-associated rows/statements and generic methods; its `Executor` is not dyn-compatible. `AnyConnection` is a separate runtime-selection abstraction. Borrow the execution responsibility/name, not a SQL model for brokers/vector stores. [Executor](https://docs.rs/sqlx/latest/sqlx/trait.Executor.html), [AnyConnection](https://docs.rs/sqlx/latest/sqlx/struct.AnyConnection.html) |
| Tower-style associated future types | Valid alternative, but not needed for this small heterogeneous registry. Do not add associated-type/type-erasure plumbing without a measured need. [Service](https://docs.rs/tower-service/latest/tower_service/trait.Service.html) |
| One mandatory SQL-like query or page model for every resource | Reject. Preserve native queries, tokens and distinct follow semantics; unsupported operations must be explicit |
| Persistent PostgreSQL cursors/transactions, `WITH HOLD`, or eager whole-result materialization | Reject for interactive browsing. Transport reuse must not resurrect the withdrawn cursor design |
| Idle expiry of cached results, reading deadlines or forced refresh after a session timer | Reject. Memory budgets govern cached data; active-operation deadlines govern network work |
| Close every transport after every successful operation forever | Retain current behavior during registry refactor, but reject as a universal final policy. Persistent clients/subscriptions belong to the selected session |
| Generic heartbeat queries, one heartbeat interval for every provider, automatic calls to `check` | Reject. Protocol-native liveness and explicit validation have different jobs |
| Automatic commits/acks, shared production consumers, or invisible retry/resume | Reject for observation. They can change application state or hide data gaps |
| Dynamic plugin loading, plugin ABI, SDK stubs for later backends, global connection pools or additional runtimes | Defer. Built-in registration and existing Tokio runtime cover the concrete scope |

Apache DataFusion's `#[async_trait]` table providers are a relevant precedent for an extensible async datasource interface, not a reason to import its query planner or execution layers. [Provider example](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)

## Configuration and compatibility

This ADR adds **no implemented TOML keys, CLI flags or dependencies**. `async-trait` is a dependency choice for the implementation, not added by this document. Preserve existing configuration shape and strict validation while moving backend-specific types into connector crates. Core retains alias/kind routing, file discovery and reusable safe validation helpers; raw TOML is confined to provider configuration, not query execution.

Existing usage remains:

```sh
onetui --config "$HOME/onetui.toml" --check --connection local_pg
onetui schema --datasource postgres
```

Current valid config fragment:

```toml
[connections.local_pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
```

`url_env` names a required nonblank environment value; it is not a built-in variable. Explicit `--config` selects exactly that file (relative paths use the working directory). Otherwise use absolute `$XDG_CONFIG_HOME/onetui/config.toml`, then absolute `$HOME/.config/onetui/config.toml`. No merge, project search, creation or path/URL environment expansion. `~/onetui.toml` is explicit only. Full field values, TLS restrictions and defaults remain in the [README](../../README.md#configuration).

Preserve `--timeout`: default five seconds, integer range 1–300, active request only. There is no heartbeat/session-TTL setting in TOML. Start with native client policies and record actual defaults/platform support during implementation. Concrete keepalive overrides, reconnect backoff and future stream buffer sizes require fixture/proxy evidence before selecting and documenting values; no universal numeric default is decided here.
