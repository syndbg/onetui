#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
case "${1:-}" in
    check|package) ;;
    *) printf 'Usage: bash hack/release.sh {check|package} v<version>\n' >&2; exit 2 ;;
esac

# Cargo validates SemVer; compare its parsed package version, not a second version source.
package_id=$(cargo pkgid --locked)
version=${package_id##*#}
version=${version##*@}
tag=${2:-}
if [[ "$tag" != "v$version" ]]; then
    printf 'Release tag must match Cargo.toml exactly: v%s\n' "$version" >&2
    exit 1
fi
if [[ "$1" == check ]]; then
    printf 'Release version verified: %s\n' "$tag"
    exit 0
fi

target=$(rustc -vV | awk '$1 == "host:" {print $2}')
archive="bpearl-${tag}-${target}.tar.gz"
if [[ -e "dist/$archive" || -e "dist/$archive.sha256" ]]; then
    printf 'Release archive already exists; refusing to overwrite dist/%s.\n' "$archive" >&2
    exit 1
fi
# An explicit native target prevents ambient CARGO_BUILD_TARGET from mislabelling the archive.
cargo build --release --locked --target "$target"
"target/$target/release/bpearl" --version
mkdir -p dist
tar -czf "dist/$archive" -C "$repo_root/target/$target/release" bpearl -C "$repo_root" README.md
(cd dist && shasum -a 256 "$archive" > "$archive.sha256")
printf 'Created dist/%s and its SHA-256 file.\n' "$archive"
