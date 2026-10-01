---
status: accepted
date: 2026-10-01
---

# ADR-0020: Build Linux packages per library ABI

## Decision

The release workflow builds the Linux archive and `.deb` on Ubuntu 22.04 and the `.rpm` on AlmaLinux 9. Both hosts have older glibc than the supported targets, so the binaries run on Debian 12+, Ubuntu 22.04+, Fedora and EL 9+. Package glibc requirements come from the binary's highest `GLIBC_` symbol, not from the build host.

Debian and RPM distros use different `libsasl2` sonames (`.so.2` and `.so.3`). The `.tar.gz` therefore targets Debian and Ubuntu only. The release workflow also builds a native Arch Linux `.pkg.tar.zst` with `packaging/arch/PKGBUILD`, which links Arch's own libraries. The PKGBUILD remains available for local source builds.

`make test-install` installs each prebuilt asset on every supported distro in Docker, including the Arch package. The release workflow runs it before attaching assets.

## Context

Release v0.2.0 built on Ubuntu 24.04 and Fedora 43. The binary needed glibc 2.39 and the `.rpm` required the build host's glibc 2.42. Both failed on Debian 12, Ubuntu 22.04 and EL 9. The tarball failed on Arch and Fedora with `libsasl2.so.2: cannot open shared object file`.

Rejected: a static or musl build, because rdkafka's GSSAPI support needs the system SASL and Kerberos libraries. Rejected: one rpm per distro release, because EL 9 sonames match later Fedora and EL releases.
