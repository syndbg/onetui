# Contributing to OneTUI

Check the relevant [ADRs](docs/adr/) before larger changes.

## Local setup

Install rustup, Make, Bash and Docker Compose. Rust is pinned in [rust-toolchain.toml](rust-toolchain.toml). Native builds need a C/C++ compiler, CMake and Perl; fixtures need OpenSSL, workflow lint needs Go.

On macOS, install Xcode Command Line Tools and `brew install cmake`; SASL/Kerberos comes from the system. On Debian/Ubuntu, install `build-essential cmake perl pkg-config libcurl4-openssl-dev libsasl2-dev libsasl2-modules-gssapi-mit`.

Use `make run` to open the app. For demos, use `make dev-up`, then `make dev-run`. See [local fixtures](hack/README.md) and `make help`. `make dev-down` deletes the disposable data.

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
3. Publish. Wait for the [release workflow](.github/workflows/release.yaml) and verify the Linux x86_64 and macOS Apple Silicon/Intel archives, `.deb`, `.rpm`, installer and SHA-256 files before announcing. PR and main CI use Ubuntu 24.04. Releases also use native macOS 15 runners. The RPM builds in a Fedora 43 container on Ubuntu because its SASL library ABI differs from Ubuntu's. Assets upload only after every platform job succeeds.

Never move a published tag. Uploads refuse overwrites; inspect partial assets before retrying. Keep the license and third-party notices with redistributed binaries.

### Local packaging and verification

```sh
make package TAG=v0.1.0
cd dist
shasum -a 256 -c onetui-v0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
```

Use the filename matching your platform. Packaging refuses existing artifacts. macOS release builds target macOS 15 or newer, use system libraries, and check the extracted binary's version and offline schema. They are not Developer ID signed or notarized. Linux packages are unsigned. Older GNU libc and musl compatibility is unverified. Review native dependency licenses when updating them.

After `make package`, use `make package-deb TAG=v0.1.0` on Debian/Ubuntu or `make package-rpm TAG=v0.1.0` on Fedora. These use pinned nFPM through Go. Never repackage the Ubuntu binary as a Fedora RPM. The release workflow installs each package and checks its version and offline schema before uploading.

Linux binaries dynamically link system libraries. Ubuntu archive installs need `libc6` (2.39+), `libgcc-s1`, `libstdc++6`, `libcurl4t64`, `libsasl2-2` and `libgssapi-krb5-2`. GSSAPI also needs `libsasl2-modules-gssapi-mit`. The Debian/RPM packages declare their runtime dependencies. Kerberos libraries are not bundled.

### Homebrew

The formula lives in [syndbg/homebrew-tap](https://github.com/syndbg/homebrew-tap). Use that repository's checks when changing it. After installing through the tap, run `brew test --HEAD syndbg/tap/onetui` to check the installed CLI and offline catalog.

After publishing a release, update the tap's formula with its source archive URL and verified SHA-256. Keep `head` for development builds. Test the source build and installed CLI before publishing the formula update. The OneTUI release workflow does not update the tap automatically.
