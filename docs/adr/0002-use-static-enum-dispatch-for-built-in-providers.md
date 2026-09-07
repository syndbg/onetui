---
status: accepted
date: 2026-09-07
---

# ADR-0002: Use static enum dispatch for built-in providers

## Decision

Use a fixed set of providers compiled into the binary. `Provider` and `Executor` traits define the interface; built-in enums select concrete implementations. Use native async methods without `Box<dyn Provider>`, `Box<dyn Executor>`, `async-trait` or boxed operation futures.

Selecting a connection chooses a compiled enum variant. OneTUI does not load executable plugins or need a plugin ABI, discovery mechanism or mutable registry.

This replaces [ADR-0001](0001-register-providers-and-own-session-lifecycles.md)'s boxed dispatch and one-registration extension model. Its resource capabilities, strict configuration, paging, client ownership, cancellation, shutdown and heartbeat contracts still apply.

Implemented.

## Context and rationale

ADR-0001 used trait objects to register different provider types through one interface. Enums retain that interface and let the compiler check dispatch across a fixed provider set without boxing its futures.

At `20d2a7d`, the [CLI](../../src/main.rs), [TUI worker](../../crates/tui/src/ui.rs) and [core config](../../crates/core/src/config.rs) each select backends. Move this selection into the binary's composition module. Keep connector behavior and tests in their packages, with core and TUI independent of SDKs.

## Shared interfaces without boxed futures

Interface sketches follow. Supporting types have the meanings defined in ADR-0001.

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

`impl Future` returns a concrete future without boxing it. `Send` lets a generic worker use it on Tokio's multithreaded runtime. Implementations can use ordinary `async fn`; callers use generic bounds rather than `dyn Executor`. [Rust async-trait guidance](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits/)

The borrowed `&dyn Fn` callback needs no box. Borrowed trait objects and allocations elsewhere remain allowed. Configuration runs synchronously without network access, parses TOML into strict connector-owned types and resolves only the selected connection's secrets.

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

Each async method returns one future type that handles either branch. Awaiting inside the arms avoids a `dyn Future` wrapper. Variant selection still happens at runtime, and the compiler may or may not inline the calls.

Use exhaustive matches without catch-all arms so a new variant causes compile errors at missing dispatch arms. Keep these arms limited to forwarding calls. SQL, resource paths, capabilities, secrets and connection lifetimes belong to the connector.

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

The example uses stateless unit structs. `BuiltinProvider` implements `Provider` with `type Executor = BuiltinExecutor`, forwards descriptor and validation calls, and wraps configured executors. Its `configure` body:

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

Pass the catalog to generic CLI, config, schema and TUI code. Look up kinds in that slice instead of copying kind lists into callers. Validate duplicate kinds and descriptor IDs; exhaustive matches cannot catch duplicate strings or missing catalog entries.

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

Use `BuiltinExecutor` in production and `FakeExecutor` in shared tests. Only workers and catalog consumers need generic provider types; rendering and cached `Page` values do not. Spawned workers own their executor for the task's lifetime.

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
| Native traits plus built-in enums (chosen) | No required provider/executor trait-object allocation or per-operation future boxing at this seam; explicit supported set and exhaustive forwarding |
| `dyn Provider` / `dyn Executor` plus `async-trait` | Valid, including for built-ins, but returns boxed futures and hides concrete execution types. Its open-ended registration convenience is not needed. [Macro expansion](https://docs.rs/async-trait/latest/async_trait/#explanation) |
| Handwritten `BoxFuture` / `Pin<Box<dyn Future>>` | The same future-allocation choice with more explicit plumbing; not selected for the shared interface |
| Independent backend matches in CLI, TUI, config and schema | Rejected. An enum is useful only when the full implementation list is confined to composition and shared callers use the contracts |
| Handwritten polling/state-machine interfaces | Can avoid future boxing, but add complexity not justified by this built-in set |
| Dynamic plugin loading or generated dispatch framework | Not required. Use ordinary Rust enums and traits; do not add a macro dependency merely to conceal a small set of delegation arms |

A new datasource needs its crate and tests, a Cargo dependency, provider/executor variants, forwarding arms and a catalog entry. Rebuild the binary to use it. The compiler checks exhaustive dispatch; tests still need to check descriptors and behavior.

Enums must fit their largest variant. Generated futures must fit their branch state. Both can grow, neither has a guaranteed stable layout, and generics can increase compile time and binary size. Measure before claiming a speedup. [Rust type layout](https://doc.rust-lang.org/reference/type-layout.html)

SDKs, buffers, strings, channels and spawned tasks may still allocate. Leave native clients' internal boxing alone. If executor or future size becomes a measured problem, reconsider its storage; record a change of dispatch strategy in a new ADR.

## Unchanged contracts

`check` reports the access it tested. Resource capabilities distinguish paging from live following and reject unsupported operations. Providers keep native query and offset semantics; browsing must not acknowledge messages, commit offsets or replay operations silently.

The selected session still owns reusable native clients and their driver/heartbeat tasks. Query cancellation, transport reuse, PostgreSQL's no-idle-transaction rule, stale-result rejection, bounded shutdown and provider-native keepalive policies remain as specified in ADR-0001. Replacing an enum value is not a substitute for asynchronous shutdown. Concrete persistent-client work remains separate from changing dispatch.

Keep the TOML, CLI, deadlines and offline catalog described in the [README](../../README.md#configuration). The provider interface needs no `async-trait` dependency.
