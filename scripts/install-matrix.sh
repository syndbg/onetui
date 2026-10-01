#!/usr/bin/env bash
# Install each release asset on the distros it targets and run `onetui --version`.
# Uses dist/ when it holds the tag's assets, else downloads them from the GitHub release.
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tag=${1:?Usage: bash scripts/install-matrix.sh v<version>}
dir=$repo_root/hack/install-matrix
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

# kind:base-image, one Dockerfile.<kind> per kind.
matrix=(
    deb:debian:12 deb:debian:13 deb:ubuntu:22.04 deb:ubuntu:24.04
    rpm:fedora:latest rpm:almalinux:9 rpm:rockylinux:9
    arch:archlinux tar:debian:12 tar:debian:13 tar:ubuntu:22.04 tar:ubuntu:24.04
)
patterns=(
    "onetui-$tag-x86_64.deb" "onetui-$tag-x86_64.rpm" "onetui-$tag-x86_64-unknown-linux-gnu.tar.gz"
    "onetui-${tag#v}-*-x86_64.pkg.tar.zst"
)

for asset in "${patterns[@]}"; do
    if compgen -G "$repo_root/dist/$asset" >/dev/null; then cp "$repo_root"/dist/$asset "$stage/"
    else gh release download "$tag" --pattern "$asset" --dir "$stage"; fi
done

pids=()
for entry in "${matrix[@]}"; do
    kind=${entry%%:*}
    base=${entry#*:}
    name=$kind-${base//[:\/]/_}
    ctx=$stage/$name
    mkdir "$ctx"
    case $kind in
        deb) cp "$stage"/*.deb "$ctx/" ;;
        rpm) cp "$stage"/*.rpm "$ctx/" ;;
        tar) cp "$stage"/*.tar.gz "$ctx/" ;;
        arch) cp "$stage"/*.pkg.tar.zst "$ctx/" ;;
    esac
    (docker build --platform linux/amd64 --progress=plain -f "$dir/Dockerfile.$kind" \
        --build-arg "BASE=$base" "$ctx" >"$stage/$name.log" 2>&1) &
    pids+=("$name:$!")
done

failed=0
for p in "${pids[@]}"; do
    name=${p%%:*}
    if wait "${p#*:}"; then printf 'PASS %s\n' "$name"
    else printf 'FAIL %s\n' "$name"; tail -n 8 "$stage/$name.log" | sed 's/^/    /'; failed=1; fi
done
exit "$failed"
