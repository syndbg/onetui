# Contributing to OneTUI

OneTUI is working toward v0.1.0. Check [architecture decisions](docs/adr/0002-use-static-enum-dispatch-for-built-in-providers.md) before changing behavior. Discuss new datasources or larger design changes before implementing them.

## Local setup

Install rustup, Make, Bash and Docker with Compose. The repository pins the Rust toolchain. OpenSSL is needed for the Qdrant TLS tests; Go is needed for workflow linting.

Run from the repository root:

```sh
make dev-up
make run
make dev-down
```

The local databases contain disposable test data. `make dev-down` removes their containers, network and temporary data. See [hack/README.md](hack/README.md) for endpoints, fixture credentials and troubleshooting. Never run write-dependent tests against a real datasource.

## Changes and pull requests

Keep each change focused. Implement connector behavior and tests in its owning package; core and TUI code must not gain backend-specific branches. PostgreSQL and Qdrant have separate test suites, not a shared test loop.

Add regression coverage for bugs and relevant boundary/error tests for features. Update documentation and configuration examples with the code. Describe each setting's purpose, supported values, default and usage; distinguish implemented behavior from planned support.

Before submitting a pull request, run:

```sh
make verify
make test-integration
make workflow-lint
```

`verify` builds, checks formatting and Clippy, validates shell syntax and runs default tests. `test-integration` starts fresh fixtures, runs the ignored package-owned tests and cleans up. It refuses existing fixture containers: finish your development session with `make dev-down` before running it. Default Cargo tests alone do not cover the live integrations.

Explain the problem, scope, behavior changes and tests in the pull request. Call out compatibility changes and any unverified behavior. Do not include secrets or unrelated edits. CI definitions cover PR and main builds, with Linux fixture tests; local success is not evidence of hosted CI success.

OneTUI uses [Apache-2.0](LICENSE). Do not change versions or publish artifacts as part of an unrelated feature change.

## Releases

Development target: v0.1.0, stored as `0.1.0` in Cargo. It is not yet a published release. Versioning follows [Semantic Versioning](https://semver.org/); update Cargo.toml and regenerate Cargo.lock together. A prerelease such as `v0.1.0-rc.1` must correspond to Cargo version `0.1.0-rc.1`.

### Publish through GitHub's UI

1. Confirm PR/main validation is green for the commit being released. OneTUI uses [Apache-2.0](LICENSE).
2. Update the package version in a reviewed change. Run `make verify`, `make workflow-lint` and `make test-integration` from a clean checkout. Check `make release-check TAG=v0.1.0` (substitute the intended version).
3. In **Releases → Draft a new release**, choose/create the exact `v<version>` tag at the reviewed main commit. Write release notes containing only shipped functionality and known limitations. Mark prerelease versions as prereleases.
4. Click **Publish release**. The `release: published` workflow checks out the tagged revision, rejects tag/Cargo-version mismatches, reruns validation, and builds native archives for Linux x86_64 (`x86_64-unknown-linux-gnu`) and macOS arm64 (`aarch64-apple-darwin`).
5. Wait for the Release workflow to succeed and verify the two `.tar.gz` assets and their `.sha256` files. Both builds must pass before assets are uploaded. A published release is visible while builds run; do not announce it until assets are complete.

No workflow creates a release, tag, version-bump commit or crates.io publication. PR/main workflows have read-only repository permissions. Only the final release-upload job has `contents: write`; build jobs do not receive that permission. These definitions have not yet been exercised on GitHub.

### Local packaging and verification

```sh
make package TAG=v0.1.0
cd dist
shasum -a 256 -c onetui-v0.1.0-aarch64-apple-darwin.tar.gz.sha256
```

Use the filename matching your native platform. Packaging derives the target from rustc, builds explicitly for it, smoke-checks `--version`, and bundles the binary, README and LICENSE. It refuses to overwrite existing archives/checksums. Move an old local artifact aside before rebuilding; `dist/` is ignored by Git.

Artifacts are initially unsigned/unnotarized. Linux binaries use the runner's GNU libc. Compatibility with older distributions or musl is unverified. Intel macOS, Linux arm64 and Windows binaries are not part of this initial workflow. Compatibility and signing remain release-readiness work.

If validation fails, fix it through review and use the corrected release version/tag; do not silently move a published tag. If only upload fails, inspect existing assets before rerunning the upload job: uploads deliberately do not overwrite published files. GitHub can partially upload a batch, so recovery may require removing incomplete assets through the Releases UI before retrying. Nothing uploads to an external telemetry or package service.

References: [release workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#release), [workflow token permissions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#permissions), [hosted runner platforms](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
