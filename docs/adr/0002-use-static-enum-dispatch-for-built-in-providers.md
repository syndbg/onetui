---
status: accepted
date: 2026-09-07
---

# ADR-0002: Use static enum dispatch for built-in providers

## Decision

Know the complete set of datasource implementations at compile time. Use shared `Provider` and `Executor` traits with concrete associated executor types, native async methods and explicit built-in enums. Do not use `Box<dyn Provider>`, `Box<dyn Executor>` or `async-trait` for this dispatch layer, and do not box the futures returned by its operations.

Users still select connection aliases and datasource kinds at runtime. That selects an already compiled enum variant; it does not load new executable code. Runtime dynamic plugins, plugin discovery/ABIs and a mutable plugin registry are not requirements.

This decision supersedes the **dispatch choice** in [ADR-0001](0001-register-providers-and-own-session-lifecycles.md): boxed provider/executor trait objects, `#[async_trait]`, and the claim that a new datasource needs only one registration after adding its crate. It carries forward ADR-0001's provider responsibilities, per-resource capabilities, strict configuration, independent paging, persistent-client ownership, cancellation, shutdown and native-heartbeat policies unchanged.

This is an accepted target, not implemented functionality. This ADR records the decision and examples.

## Context and rationale

The project owner explicitly chose built-in datasources known at compile time. PostgreSQL and Qdrant are the initial implementations; DynamoDB, Kafka, NATS and RabbitMQ require future builds that include their connectors. There is no need to accept an arbitrary provider implementation inside an already running binary.

ADR-0001 selected trait objects for convenient heterogeneous registration, not because OneTUI needed dynamic loading. Trait objects remain valid for built-ins too, but that convenience is not worth requiring future boxing here. The enum approach retains the provider interface while making the supported set explicit and compiler-checked.

Current source at `20d2a7d` still has backend dispatch in the [CLI](../../src/main.rs), [TUI worker](../../crates/tui/src/ui.rs) and [core config](../../crates/core/src/config.rs). Those scattered decisions are not the desired enum design: only the composition layer should know the complete list of concrete providers. Connector packages retain their behavior and tests; core and TUI remain independent of concrete SDKs.

## Shared interfaces without boxed futures

These Rust fragments illustrate the proposed contracts and wiring, not standalone programs or APIs already present in the repository. `RequestContext`, `PageRequest`, `ShutdownContext`, status and descriptor types retain the meanings established in ADR-0001.

In `onetui-core`:

```rust
use std::future::Future;
use anyhow::Result;
use tokio::sync::watch;

pub trait Provider: Send + Sync {
    type Executor: Executor;

    fn descriptor(&self) -> &ProviderDescriptor;

    fn validate_config(&self, options: &toml::Table) -> Result<()>;

    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self::Executor>;
}

pub trait Executor: Send + Sync {
    fn status(&self) -> watch::Receiver<ConnectionStatus>;

    fn check(
        &self,
        context: RequestContext,
    ) -> impl Future<Output = Result<CheckResult>> + Send;

    fn fetch_page(
        &self,
        request: PageRequest,
        context: RequestContext,
    ) -> impl Future<Output = Result<Page>> + Send;

    fn shutdown(
        &mut self,
        context: ShutdownContext,
    ) -> impl Future<Output = Result<()>> + Send;
}
```

`impl Future` denotes a concrete compiler-known return type, not a trait object or heap allocation. Explicit `Send` bounds let generic workers use these futures on Tokio's multithreaded runtime. Implementations may use ordinary `async fn` while satisfying these signatures; no proc macro is needed. These traits are used as generic bounds, not as `dyn Executor`. [Rust async-trait guidance](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/)

The borrowed `&dyn Fn` secret-lookup callback does not box either the callback or a future. The decision concerns provider/executor dispatch, not a ban on every borrowed trait object or allocation in the application. Provider configuration remains synchronous and network-free; raw TOML is parsed into strict connector-owned types before execution, and only selected secrets are resolved.

## Centralized enum delegation

The binary's composition module owns the concrete enum. Neither core nor TUI imports it or the connector implementations.

```rust
enum BuiltinExecutor {
    Postgres(PostgresExecutor),
    Qdrant(QdrantExecutor),
}

impl Executor for BuiltinExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        match self {
            Self::Postgres(pg) => pg.status(),
            Self::Qdrant(qdrant) => qdrant.status(),
        }
    }

    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        match self {
            Self::Postgres(pg) => pg.check(context).await,
            Self::Qdrant(qdrant) => qdrant.check(context).await,
        }
    }

    async fn fetch_page(
        &self,
        request: PageRequest,
        context: RequestContext,
    ) -> Result<Page> {
        match self {
            Self::Postgres(pg) => pg.fetch_page(request, context).await,
            Self::Qdrant(qdrant) => qdrant.fetch_page(request, context).await,
        }
    }

    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        match self {
            Self::Postgres(pg) => pg.shutdown(context).await,
            Self::Qdrant(qdrant) => qdrant.shutdown(context).await,
        }
    }
}
```

Each outer async method creates one concrete future type that can represent either branch. Awaiting the native call inside each arm unifies the method's result without erasing its future behind `dyn Future`. The compiler still generates runtime variant selection; static composition does not mean there is no branch or guarantee that every call will be inlined.

Use exhaustive matches without a catch-all arm for built-in delegation. A new enum variant then exposes missing forwarding arms at compile time. These matches are mechanical forwarding only: no SQL, resource-path construction, capability exceptions, secret handling or connection-lifetime decisions belong in them.

### Provider construction and catalog

Use the same pattern for providers:

```rust
enum BuiltinProvider {
    Postgres(PostgresProvider),
    Qdrant(QdrantProvider),
}

const BUILTINS: &[BuiltinProvider] = &[
    BuiltinProvider::Postgres(PostgresProvider),
    BuiltinProvider::Qdrant(QdrantProvider),
];
```

The example uses stateless unit provider structs. `BuiltinProvider` implements `Provider` with `type Executor = BuiltinExecutor`. It forwards descriptor/config-validation calls and wraps each configured concrete executor in the matching variant. The corresponding `configure` method body is:

```rust
match self {
    Self::Postgres(pg) => pg
        .configure(options, env)
        .map(BuiltinExecutor::Postgres),
    Self::Qdrant(qdrant) => qdrant
        .configure(options, env)
        .map(BuiltinExecutor::Qdrant),
}
```

Pass the immutable built-in catalog to generic CLI/config/schema/TUI orchestration. Kind lookup scans or indexes provider descriptors; it does not repeat a hardcoded kind list in each caller. For this small set, a slice and lookup are sufficient. Validate duplicate kinds and descriptor IDs when assembling the catalog; enum exhaustiveness alone cannot detect duplicate descriptor strings or an omitted catalog entry.

Offline `schema` enumerates that same catalog without constructing executors, loading configuration, resolving secrets or connecting. Runtime selection of `kind = "postgres"` chooses `BuiltinProvider::Postgres`; its factory returns `BuiltinExecutor::Postgres`. Unknown kinds fail safely. There is no fallback plugin search.

### Generic worker and isolated tests

```rust
async fn load_page<E: Executor>(
    executor: &E,
    request: PageRequest,
    context: RequestContext,
) -> Result<Page> {
    executor.fetch_page(request, context).await
}
```

Production instantiates the worker with `BuiltinExecutor`; shared tests supply a `FakeExecutor`. The executor-owning worker and catalog consumers are generic where needed. Rendering and cached `Page` values do not need to become generic over database types. Spawned workers own their executor for the task lifetime rather than borrowing a temporary executor.

```text
core: Provider / Executor / descriptors / Page
  ^                  ^
  |                  |
connector crates     generic TUI worker + shared rendering
  ^                  ^
  +-- binary composition: built-in enums and catalog --+
```

Each connector implements the core contracts without importing siblings. Core has no concrete-provider enum. TUI does not match provider kinds or resource IDs to choose backend behavior. Provider-specific validation, navigation targets, paging and native clients stay in their connector package. Package-specific tests remain separate; no PostgreSQL/Qdrant parameterized test loop is introduced.

## Consequences and alternatives

| Choice | Consequence |
| --- | --- |
| Native traits plus built-in enums — chosen | No required provider/executor trait-object allocation or per-operation future boxing at this seam; explicit supported set and exhaustive forwarding |
| `dyn Provider` / `dyn Executor` plus `async-trait` | Valid, including for built-ins, but returns boxed futures and hides concrete execution types. Its open-ended registration convenience is not needed. [Macro expansion](https://docs.rs/async-trait/latest/async_trait/#explanation) |
| Handwritten `BoxFuture` / `Pin<Box<dyn Future>>` | The same future-allocation choice with more explicit plumbing; not selected for the shared interface |
| Independent backend matches in CLI, TUI, config and schema | Rejected. An enum is useful only when the full implementation list is confined to composition and shared callers use the contracts |
| Handwritten polling/state-machine interfaces | Can avoid future boxing, but add complexity not justified by this built-in set |
| Dynamic plugin loading or generated dispatch framework | Not required. Use ordinary Rust enums and traits; do not add a macro dependency merely to conceal a small set of delegation arms |

Adding a datasource now entails its connector crate/tests, Cargo dependency, provider/executor enum variants, forwarding arms and a catalog entry, followed by rebuilding the binary. This is intentional compile-time extensibility, not runtime extensibility. It replaces ADR-0001's one-registration promise. Compiler coverage of the variant list is useful, but does not prove descriptor correctness or behavior.

Enum storage must accommodate its largest variant, and generated futures must accommodate the state needed by their branches; neither has a promised stable layout. This can increase value/future size. Generic instantiations can increase compile time and code size. Avoid claiming a speedup without measurements. [Rust type layout](https://doc.rust-lang.org/reference/type-layout.html)

The decision is not an allocation-free application promise. Native SDKs, result buffers, strings, channels and spawned tasks may allocate internally. It does not require rewriting native clients to remove their own boxing. If large executor/future state later becomes a measured problem, reconsider storage narrowly and record a material dispatch reversal in a new ADR.

## Unchanged contracts

Static dispatch changes how calls reach providers, not what calls mean. `check` remains an executor operation with scoped permission results. Resource-level capabilities still distinguish finite pages from future live following; unsupported operations remain explicit. No generic SQL translation, global offset, silent acknowledgement/offset commit or automatic replay is introduced.

The selected session still owns reusable native clients and their driver/heartbeat tasks. Query cancellation, transport reuse, PostgreSQL's no-idle-transaction rule, stale-result rejection, bounded shutdown and provider-native keepalive policies remain as specified in ADR-0001. Replacing an enum value is not a substitute for asynchronous shutdown. Concrete persistent-client work remains separate from changing dispatch.

There are no configuration or runtime changes in this documentation update. Existing TOML, CLI flags, deadlines and offline-catalog behavior remain as documented in the [README](../../README.md#configuration). Do not add `async-trait` for the planned provider interface.
