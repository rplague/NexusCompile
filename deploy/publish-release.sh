#!/usr/bin/env bash
#
# Idempotent release: upload files from <dist-dir> as assets to Gitea Release v<Cargo.toml version>
#
# Usage: REPO=<owner/repo> TOKEN=<token> ./publish-release.sh <dist-dir>
#
# Behavior:
#   - Look up the Release by tag and create it if missing; on concurrent 409, re-query
#   - Skip assets that already exist; upload the rest
#   - Safe to re-run; the final state is consistent
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

DIST_DIR="${1:-}"
REPO="${REPO:?需要环境变量 REPO (owner/repo)}"
TOKEN="${TOKEN:?需要环境变量 TOKEN}"

if [ -z "${DIST_DIR}" ] || [ ! -d "${DIST_DIR}" ]; then
    echo "用法: REPO=... TOKEN=... $0 <dist目录>" >&2
    exit 1
fi

VERSION="$(awk -v key="version" '
    /^\[/ { in_pkg = ($0 == "[package]"); next }
    in_pkg && $0 ~ "^" key " *=" {
        if (match($0, /"[^"]*"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }
' "${PROJECT_DIR}/Cargo.toml")"
[ -n "${VERSION}" ] || { echo "无法从 Cargo.toml 提取版本" >&2; exit 1; }

TAG="v${VERSION}"
API="${GITEA_API_BASE:-https://git.oahd.cn/api/v1}/repos/${REPO}"
AUTH=(-H "Authorization: token ${TOKEN}")
CODE=""
BODY=""
RELEASE_ID=""
ASSETS_JSON=""

# Unified request: status code into CODE, response body into BODY
api() {
    local method="$1" url="$2"; shift 2
    local resp
    resp=$(curl -s -w '\n%{http_code}' -X "$method" "${AUTH[@]}" "$@" "$url")
    CODE=$(printf '%s\n' "$resp" | tail -n1)
    BODY=$(printf '%s\n' "$resp" | sed '$d')
}

id_from() {
    printf '%s' "$1" | grep -oE '"id"[[:space:]]*:[[:space:]]*[0-9]+' | head -1 | grep -oE '[0-9]+'
}

find_release_id() {
    api GET "$API/releases/tags/$TAG"
    [ "$CODE" = "200" ] && id_from "$BODY"
}

# Find or create the Release
RELEASE_ID="$(find_release_id)"
if [ -z "${RELEASE_ID}" ]; then
    api POST "$API/releases" -H 'Content-Type: application/json' \
        -d "{\"tag_name\":\"${TAG}\",\"name\":\"${TAG}\",\"target_commitish\":\"${GITHUB_SHA:-}\"}"
    case "$CODE" in
        201) RELEASE_ID="$(id_from "$BODY")" ;;
        409)
            for _ in 1 2 3 4 5; do
                sleep 1
                RELEASE_ID="$(find_release_id)"
                [ -n "${RELEASE_ID}" ] && break
            done ;;
        *) echo "创建 Release 失败: HTTP ${CODE}" >&2; exit 1 ;;
    esac
fi
[ -n "${RELEASE_ID}" ] || { echo "RELEASE_ID 为空" >&2; exit 1; }
echo "RELEASE_ID=${RELEASE_ID}"

# Fetch the asset list
api GET "$API/releases/${RELEASE_ID}/assets?limit=50"
[ "$CODE" = "200" ] || { echo "查询资产失败: HTTP ${CODE}" >&2; exit 1; }
ASSETS_JSON="$BODY"

asset_exists() { printf '%s' "$ASSETS_JSON" | grep -Fq "\"name\":\"$1\""; }

upload_asset() {
    local file="$1" name="$2"
    if asset_exists "$name"; then
        echo "跳过已存在: ${name}"
        return 0
    fi
    api POST "$API/releases/${RELEASE_ID}/assets?name=$name" \
        -H 'Content-Type: application/octet-stream' --data-binary @"$file"
    if [ "$CODE" = "201" ]; then
        echo "已上传: ${name}"
        return 0
    fi
    # Concurrent upload of the same name: re-query once and treat an existing asset as success
    api GET "$API/releases/${RELEASE_ID}/assets?limit=50"
    ASSETS_JSON="$BODY"
    if asset_exists "$name"; then
        echo "已存在(并发): ${name}"
        return 0
    fi
    echo "上传失败 ${name}: HTTP ${CODE}" >&2
    exit 1
}

shopt -s nullglob
files=("${DIST_DIR}"/*)
[ ${#files[@]} -gt 0 ] || { echo "目录为空: ${DIST_DIR}" >&2; exit 1; }
for f in "${files[@]}"; do
    upload_asset "$f" "$(basename "$f")"
done
