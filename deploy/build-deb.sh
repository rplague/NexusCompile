#!/usr/bin/env bash
#
# NexusCompile deb package build script
#
# Assembles the .deb directly with dpkg-deb --build.
# Runs on Debian/Ubuntu hosts (requires dpkg-deb).
#
# Package/binary names are derived automatically from Cargo.toml [package]:
#   name = "NexusCompile"
#     -> PKG  = nexuscompile   (Debian package / systemd unit name)
#     -> BIN  = NexusCompile   (installed to /usr/bin/)
# After changing the Cargo.toml name, this script and the systemd unit follow automatically.
#
# Usage: ./build-deb.sh
# Output: ../target/packaging/<pkg>_<version>_<arch>.deb
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

# Read only fields inside the [package] section to avoid picking up dependency tables.
read_package_field() {
    awk -v key="$1" '
        /^\[/ { in_pkg = ($0 == "[package]"); next }
        in_pkg && $0 ~ "^" key " *=" {
            if (match($0, /"[^"]*"/)) {
                print substr($0, RSTART + 1, RLENGTH - 2)
                exit
            }
        }
    ' "${PROJECT_DIR}/Cargo.toml"
}

NAME="$(read_package_field name)"
VERSION="$(read_package_field version)"
[ -n "${NAME}" ] || { echo "[build-deb] 无法从 Cargo.toml 提取包名" >&2; exit 1; }
[ -n "${VERSION}" ] || { echo "[build-deb] 无法从 Cargo.toml 提取版本" >&2; exit 1; }

# Debian package name rules: lowercase letters, digits, '-', '+', '.';
# uppercase -> lowercase, '_' -> '-'
PKG="$(printf '%s' "${NAME}" | tr '[:upper:]_' '[:lower:]-')"
BIN="${NAME}"
UNIT_NAME="${PKG}"
SERVICE_USER="nexusnet"

ARCH="$(dpkg --print-architecture)"

OUT_DIR="${PROJECT_DIR}/target/packaging"
STAGE="${OUT_DIR}/${PKG}_${VERSION}_${ARCH}"
BINARY="${PROJECT_DIR}/target/release/${BIN}"
WRAPPER="${PROJECT_DIR}/target/release/nexus-rustc"
DEB="${OUT_DIR}/${PKG}_${VERSION}_${ARCH}.deb"

echo "[build-deb] 包名=${PKG} 二进制=${BIN} 版本=${VERSION} 架构=${ARCH}"

# Preflight checks
[ -x "${BINARY}" ] || { echo "[build-deb] 未找到二进制 ${BINARY}，请先: cargo build --release" >&2; exit 1; }
[ -x "${WRAPPER}" ] || { echo "[build-deb] 未找到 wrapper ${WRAPPER}，请先: cargo build --release" >&2; exit 1; }

# Render placeholders in packaging assets
render() {
    sed -e "s/@PKG@/${PKG}/g" \
        -e "s/@BIN@/${BIN}/g" \
        -e "s/@USER@/${SERVICE_USER}/g" \
        -e "s/@VERSION@/${VERSION}/g" "$1"
}

# Clean and prepare the staging directory
rm -rf "${STAGE}"
mkdir -p "${STAGE}/DEBIAN" \
         "${STAGE}/usr/bin" \
         "${STAGE}/usr/lib/${PKG}" \
         "${STAGE}/lib/systemd/system" \
         "${STAGE}/usr/lib/tmpfiles.d"

# Binaries
install -m 0755 "${BINARY}" "${STAGE}/usr/bin/${BIN}"
install -m 0755 "${WRAPPER}" "${STAGE}/usr/bin/nexus-rustc"
install -m 0755 "${SCRIPT_DIR}/install-toolchain" "${STAGE}/usr/lib/${PKG}/install-toolchain"

# Packaging assets
render "${SCRIPT_DIR}/service.service"       > "${STAGE}/lib/systemd/system/${UNIT_NAME}.service"
render "${SCRIPT_DIR}/service.tmpfiles.conf" > "${STAGE}/usr/lib/tmpfiles.d/${PKG}.conf"
chmod 0644 "${STAGE}/lib/systemd/system/${UNIT_NAME}.service" \
           "${STAGE}/usr/lib/tmpfiles.d/${PKG}.conf"

# Maintainer scripts
render "${PROJECT_DIR}/deb/postinst"  > "${STAGE}/DEBIAN/postinst"
render "${PROJECT_DIR}/deb/prerm"     > "${STAGE}/DEBIAN/prerm"
render "${PROJECT_DIR}/deb/postrm"    > "${STAGE}/DEBIAN/postrm"
render "${PROJECT_DIR}/deb/config"    > "${STAGE}/DEBIAN/config"
render "${PROJECT_DIR}/deb/templates" > "${STAGE}/DEBIAN/templates"
chmod 0755 "${STAGE}/DEBIAN/postinst" "${STAGE}/DEBIAN/prerm" "${STAGE}/DEBIAN/postrm" "${STAGE}/DEBIAN/config"
chmod 0644 "${STAGE}/DEBIAN/templates"

# Compute Installed-Size in KB
INSTALLED_SIZE="$(du -sk --exclude=DEBIAN "${STAGE}" | cut -f1)"

# Generate control
cat > "${STAGE}/DEBIAN/control" <<EOF
Package: ${PKG}
Version: ${VERSION}
Section: net
Priority: optional
Architecture: ${ARCH}
Maintainer: OAHD
Depends: libc6 (>= 2.31), debconf (>= 0.5) | debconf-2.0, curl, ca-certificates, gcc, binutils, libc6-dev, util-linux
Installed-Size: ${INSTALLED_SIZE}
Description: ${NAME} - NexusNet distributed compile node
 Implements the CBOR v2 protocol between NexusNet nodes and sidecars/backends,
 and provides the nexus-rustc local wrapper.
 Runs as the dedicated NexusNet user nexusnet; logs are managed by journald.
 The Debian package can install a private Rust toolchain for the sidecar.
EOF

# Package (root-owner-group makes files owned by root, no fakeroot needed)
dpkg-deb --build --root-owner-group "${STAGE}" "${DEB}" >/dev/null

echo "[build-deb] 产物: ${DEB}"
echo "[build-deb] 包内容预览:"
dpkg-deb -c "${DEB}"
