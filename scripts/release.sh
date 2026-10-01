#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
case "${1:-}" in
    check|package|deb|rpm|arch) ;;
    *) printf 'Usage: bash scripts/release.sh {check|package|deb|rpm|arch} v<version>\n' >&2; exit 2 ;;
esac

# Cargo validates SemVer; compare its parsed package version, not a second version source.
package_id=$(cargo pkgid --locked)
version=${package_id##*#}
version=${version##*@}
tag=${2:-}
git_hash=$(git rev-parse --short HEAD)
expected_version="onetui $version ($git_hash)"
if [[ "$tag" != "v$version" ]]; then
    printf 'Release tag must match Cargo.toml exactly: v%s\n' "$version" >&2
    exit 1
fi
grep -qx "pkgver=$version" packaging/arch/PKGBUILD || { printf 'packaging/arch/PKGBUILD pkgver must be %s.\n' "$version" >&2; exit 1; }
if [[ "$1" == check ]]; then
    printf 'Release version verified: %s\n' "$tag"
    exit 0
fi

target=$(rustc -vV | awk '$1 == "host:" {print $2}')
if [[ "$1" == arch ]]; then
    [[ "$target" == x86_64-unknown-linux-gnu && -r /etc/os-release ]] || { printf 'Build Arch packages on Arch Linux x86_64.\n' >&2; exit 1; }
    source /etc/os-release
    [[ "$ID" == arch ]] || { printf 'Build Arch packages on Arch Linux x86_64.\n' >&2; exit 1; }
    stage=$(mktemp -d)
    trap 'rm -rf "$stage"' EXIT
    cp packaging/arch/PKGBUILD "$stage/"
    package_list=$(cd "$stage" && makepkg --packagelist)
    # makepkg may also list a separate debug package. Distribute the main package.
    asset=${package_list%%$'\n'*}
    asset=${asset##*/}
    mkdir -p dist
    [[ ! -e "dist/$asset" && ! -e "dist/$asset.sha256" ]] || { printf 'Refusing to overwrite dist/%s.\n' "$asset" >&2; exit 1; }
    (cd "$stage" && makepkg --noconfirm)
    [[ "$("$stage/src/onetui/target/release/onetui" --version)" == "$expected_version" ]] || { printf 'Arch package version does not match Cargo and Git.\n' >&2; exit 1; }
    cp "$stage/$asset" dist/
    (cd dist && sha256sum "$asset" > "$asset.sha256")
    printf 'Created dist/%s and its SHA-256 file.\n' "$asset"
    exit 0
fi
if [[ "$1" == deb || "$1" == rpm ]]; then
    [[ "$target" == x86_64-unknown-linux-gnu ]] || { printf 'Linux packages require a native Linux x86_64 build.\n' >&2; exit 1; }
    format=$1
    # Debian and Fedora use different SASL library ABIs. Never rewrap one binary for both.
    source /etc/os-release
    case "$format:$ID" in
        deb:ubuntu|deb:debian|rpm:almalinux|rpm:rocky|rpm:rhel|rpm:fedora) ;;
        *) printf 'Build deb on Debian/Ubuntu and rpm on AlmaLinux/Rocky/RHEL/Fedora.\n' >&2; exit 1 ;;
    esac
    [[ "$(target/x86_64-unknown-linux-gnu/release/onetui --version)" == "$expected_version" ]] || { printf 'Run make package from the matching version and commit first.\n' >&2; exit 1; }
    export ONETUI_PACKAGE_VERSION="$version"
    export ONETUI_PACKAGE_BINARY=target/x86_64-unknown-linux-gnu/release/onetui
    # Require what the binary links, not the build host's glibc, so older distros stay installable.
    ONETUI_GLIBC_VERSION=$(objdump -T "$ONETUI_PACKAGE_BINARY" | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -n 1)
    export ONETUI_GLIBC_VERSION=${ONETUI_GLIBC_VERSION#GLIBC_}
    mkdir -p dist
    asset="onetui-${tag}-x86_64.$format"
    [[ ! -e "dist/$asset" && ! -e "dist/$asset.sha256" ]] || { printf 'Refusing to overwrite dist/%s.\n' "$asset" >&2; exit 1; }
    GOTOOLCHAIN=go1.26.8 go run github.com/goreleaser/nfpm/v2/cmd/nfpm@v2.47.0 package --config nfpm.yaml --packager "$format" --target "dist/$asset"
    (cd dist && shasum -a 256 "$asset" > "$asset.sha256")
    exit 0
fi
archive="onetui-${tag}-${target}.tar.gz"
if [[ -e "dist/$archive" || -e "dist/$archive.sha256" ]]; then
    printf 'Release archive already exists; refusing to overwrite dist/%s.\n' "$archive" >&2
    exit 1
fi
# An explicit native target prevents ambient CARGO_BUILD_TARGET from mislabelling the archive.
cargo build --release --locked --target "$target" --target-dir target
[[ "$("target/$target/release/onetui" --version)" == "$expected_version" ]] || { printf 'Built binary version does not match Cargo and Git.\n' >&2; exit 1; }
mkdir -p dist
tar -czf "dist/$archive" -C "$repo_root/target/$target/release" onetui -C "$repo_root" README.md LICENSE THIRD_PARTY_NOTICES.md
(cd dist && shasum -a 256 "$archive" > "$archive.sha256")
printf 'Created dist/%s and its SHA-256 file.\n' "$archive"
