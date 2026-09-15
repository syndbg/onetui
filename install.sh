#!/bin/sh
set -eu

usage() {
    printf '%s\n' 'Usage: sh install.sh [--version vX.Y.Z] [--prefix DIR]' \
        'Installs the latest stable release into ~/.local/bin by default.' \
        'Run again to upgrade. No sudo, configuration changes or background updates.'
}
fail() { printf 'onetui installer: %s\n' "$*" >&2; exit 1; }

tag=latest
prefix=${HOME:?HOME must be set}/.local
while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--prefix)
            [ "$#" -ge 2 ] || fail "$1 requires a value"
            case "$1" in --version) tag=$2 ;; --prefix) prefix=$2 ;; esac
            shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) fail "unknown option: $1" ;;
    esac
done
case "$prefix" in /*) ;; *) fail '--prefix must be an absolute directory' ;; esac
for tool in curl tar mktemp install; do
    command -v "$tool" >/dev/null 2>&1 || fail "$tool is required"
done
case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) target=x86_64-unknown-linux-gnu ;;
    Darwin/arm64) target=aarch64-apple-darwin ;;
    Darwin/x86_64) target=x86_64-apple-darwin ;;
    *) fail 'no release binary for this OS/architecture; build from source' ;;
esac

releases=https://github.com/syndbg/onetui/releases
if [ "$tag" = latest ]; then
    resolved=$(curl --proto '=https' --proto-redir '=https' -fsSL --connect-timeout 15 --max-time 60 \
        -o /dev/null -w '%{url_effective}' "$releases/latest")
    case "$resolved" in "$releases/tag/"*) tag=${resolved##*/} ;; *) fail 'cannot resolve the latest release' ;; esac
fi
# Restrict the tag to a filename-safe SemVer spelling before constructing URLs.
printf '%s\n' "$tag" | LC_ALL=C grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$' || fail 'expected --version vX.Y.Z (optionally a prerelease)'
archive=onetui-${tag}-${target}.tar.gz
work=$(mktemp -d)
staged=
cleanup() {
    [ -z "$staged" ] || rm -f -- "$staged"
    rm -rf -- "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for asset in "$archive" "$archive.sha256"; do
    curl --proto '=https' --proto-redir '=https' -fsSL --connect-timeout 15 --max-time 300 \
        "$releases/download/$tag/$asset" -o "$work/$asset"
done
expected=$(awk 'NR == 1 {print $1}' "$work/$archive.sha256")
printf '%s\n' "$expected" | LC_ALL=C grep -Eq '^[0-9a-fA-F]{64}$' || fail 'invalid SHA-256 file'
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$work/$archive")
elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$work/$archive")
else
    fail 'sha256sum or shasum is required'
fi
[ "$expected" = "${actual%% *}" ] || fail 'SHA-256 mismatch; existing installation is unchanged'
# Extract only known members. Never unpack arbitrary release paths into the prefix.
tar -xzf "$work/$archive" -C "$work" onetui LICENSE THIRD_PARTY_NOTICES.md
for member in onetui LICENSE THIRD_PARTY_NOTICES.md; do
    [ -f "$work/$member" ] && [ ! -L "$work/$member" ] || fail "invalid archive member: $member"
done
[ "$("$work/onetui" --version)" = "onetui ${tag#v}" ] || fail 'binary cannot run or its version does not match the release'
mkdir -p "$prefix/bin" "$prefix/share/licenses/onetui"
destination=$prefix/bin/onetui
[ ! -L "$destination" ] || fail 'refusing to replace a symlink; upgrade through its package manager'
[ ! -d "$destination" ] || fail 'installation target is a directory'
staged=$(mktemp "$prefix/bin/.onetui.XXXXXX")
install -m 755 "$work/onetui" "$staged"
install -m 644 "$work/LICENSE" "$work/THIRD_PARTY_NOTICES.md" "$prefix/share/licenses/onetui/"
# Rename within the same filesystem so failed downloads never destroy the old binary.
mv -f -- "$staged" "$destination"
staged=
printf 'Installed %s at %s\nAdd %s/bin to PATH if needed.\n' "$tag" "$destination" "$prefix"
