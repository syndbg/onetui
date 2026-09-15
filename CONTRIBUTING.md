# Contributing to OneTUI

Check relevant ADRs before larger changes.

## Local setup

Install rustup, Make, Bash and Docker Compose. Rust is pinned in [rust-toolchain.toml](rust-toolchain.toml). Native builds need a C/C++ compiler, CMake and Perl; fixtures need OpenSSL, workflow lint needs Go.

On macOS, install Xcode Command Line Tools and `brew install cmake`; SASL/Kerberos comes from the system. On Debian/Ubuntu, install `build-essential cmake perl pkg-config libcurl4-openssl-dev libsasl2-dev libsasl2-modules-gssapi-mit`.

Use `make dev-up`, then `make run`. See [local fixtures](hack/README.md) and `make help`. `make dev-down` deletes the disposable data.

## Changes and pull requests

Keep changes focused and tests in their owning package. Do not combine datasource suites in shared backend loops. Add regression coverage for bugs; update affected usage docs and config examples. Use `onetui schema` for exhaustive settings reference.

Before submitting:

```sh
make verify
make test-integration
make workflow-lint
```

Integration tests require fresh disposable fixtures and refuse an existing development setup. Use `make dev-down` first only when its data can be deleted. Never run write-dependent tests against real datasources.

PRs should state the problem, changes, checks and any compatibility risks. Report unrun checks; do not weaken tests to pass. Docs-only changes need content/link checks and `git diff --check`.

## Releases

Target: v0.1.0. Follow Semantic Versioning; update Cargo.toml and Cargo.lock together. Tags must exactly match Cargo's version with a `v` prefix.

### Publish through GitHub's UI

1. Use a reviewed main commit with passing checks. Run `make release-check TAG=v0.1.0` (substitute the intended version).
2. In **Releases → Draft a new release**, select/create that tag at the reviewed commit. Describe shipped changes; mark prereleases.
3. Publish. Wait for the [release workflow](.github/workflows/release.yaml) and verify the Linux x86_64 archive and its SHA-256 file before announcing. All CI jobs use Ubuntu 24.04. macOS users build locally or use Homebrew.

Never move a published tag. Uploads refuse overwrites; inspect partial assets before retrying. Keep the license and third-party notices with redistributed binaries.

### Local packaging and verification

```sh
make package TAG=v0.1.0
cd dist
shasum -a 256 -c onetui-v0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
```

Use the filename matching your platform. Packaging refuses existing artifacts. Releases are unsigned/unnotarized; older GNU libc and musl compatibility is unverified. Review native dependency licenses when updating them.

Linux binaries dynamically link system SASL (`libsasl2-2` on Debian/Ubuntu); GSSAPI also needs `libsasl2-modules-gssapi-mit`. Kerberos libraries are not bundled.

### Homebrew

The formula lives in [syndbg/homebrew-tap](https://github.com/syndbg/homebrew-tap). Use that repository's checks when changing it. After installing through the tap, run `brew test --HEAD syndbg/tap/onetui` to check the installed CLI and offline catalog.

After publishing a release, update the tap's formula with its source archive URL and verified SHA-256. Keep `head` for development builds. Test the source build and installed CLI before publishing the formula update. The OneTUI release workflow does not update the tap automatically.

### Validation status

