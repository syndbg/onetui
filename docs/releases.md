# Releases

Current target: **v0.1.0**. Cargo stores `0.1.0`; being the development target does not mean this version has been published or the TUI is ready. Versioning follows [Semantic Versioning](https://semver.org/); update Cargo.toml and regenerate Cargo.lock together. A prerelease such as `v0.1.0-rc.1` must correspond to Cargo version `0.1.0-rc.1`.

## Publish through GitHub's UI

1. Choose the license before public distribution and confirm PR/main validation is green for the commit being released.
2. Update the package version in a reviewed change. Run `make verify`, `make workflow-lint` and `make test-integration` from a clean checkout. Check `make release-check TAG=v0.1.0` (substitute the intended version).
3. In **Releases → Draft a new release**, choose/create the exact `v<version>` tag at the reviewed main commit. Write release notes containing only shipped functionality and known limitations. Mark prerelease versions as prereleases.
4. Click **Publish release**. The `release: published` workflow checks out the tagged revision, rejects tag/Cargo-version mismatches, reruns validation, and builds native archives for Linux x86_64 (`x86_64-unknown-linux-gnu`) and macOS arm64 (`aarch64-apple-darwin`).
5. Wait for the Release workflow to succeed and verify the two `.tar.gz` assets and their `.sha256` files. Both builds must pass before assets are uploaded. A published release is visible while builds run; do not announce it until assets are complete.

No workflow creates a release, tag, version-bump commit or crates.io publication. PR/main workflows have read-only repository permissions. Only the final release-upload job has `contents: write`; build jobs do not receive that permission. These definitions have not yet been exercised on GitHub.

## Local packaging and verification

```sh
make package TAG=v0.1.0
cd dist
shasum -a 256 -c bpearl-v0.1.0-aarch64-apple-darwin.tar.gz.sha256
```

Use the filename matching your native platform. Packaging derives the target from rustc, builds explicitly for it, smoke-checks `--version`, and bundles the binary plus README. It refuses to overwrite existing archives/checksums. Move an old local artifact aside before rebuilding; `dist/` is ignored by Git.

Artifacts are initially unsigned/unnotarized. Linux binaries use the runner's GNU libc, not a promise of portability to older distributions or musl. Intel macOS, Linux arm64 and Windows binaries are not part of this initial workflow. Compatibility, signing and license decisions remain release-readiness work.

If validation fails, fix it through review and use the corrected release version/tag; do not silently move a published tag. If only upload fails, inspect existing assets before rerunning the upload job: uploads deliberately do not overwrite published files. GitHub can partially upload a batch, so recovery may require removing incomplete assets through the Releases UI before retrying. Nothing uploads to an external telemetry or package service.

References: [release workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#release), [workflow token permissions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#permissions), [hosted runner platforms](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
