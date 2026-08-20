#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_PROTOC='libprotoc 31.1'
readonly EXPECTED_PROTOC_GEN_GO='protoc-gen-go v1.36.9'
readonly EXPECTED_PROTOC_GEN_GO_GRPC='protoc-gen-go-grpc 1.5.1'
readonly MODULE='github.com/onelastleaf/go-plugin-sdk'
readonly PROTOCOL_PACKAGE="${MODULE}/protocol"

if [[ $# -ne 2 ]]; then
  echo "usage: $0 /path/to/onelastleaf <published-protocol-sha256>" >&2
  exit 2
fi

readonly OLL_ROOT=$1
readonly PUBLISHED_FINGERPRINT=$2
readonly SDK_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if [[ ! -f "${OLL_ROOT}/build.rs" || ! -d "${OLL_ROOT}/proto/oll" ]]; then
  echo "not an onelastleaf source checkout: ${OLL_ROOT}" >&2
  exit 2
fi
if [[ ! ${PUBLISHED_FINGERPRINT} =~ ^[0-9a-f]{64}$ ]]; then
  echo 'published protocol fingerprint must be 64 lower-case hexadecimal characters' >&2
  exit 2
fi

check_version() {
  local command_name=$1
  local expected=$2
  local actual
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "missing required generator: ${command_name}" >&2
    exit 2
  fi
  actual=$("${command_name}" --version)
  if [[ ${actual} != "${expected}" ]]; then
    echo "${command_name} version is '${actual}', expected '${expected}'" >&2
    exit 2
  fi
}

check_version protoc "${EXPECTED_PROTOC}"
check_version protoc-gen-go "${EXPECTED_PROTOC_GEN_GO}"
check_version protoc-gen-go-grpc "${EXPECTED_PROTOC_GEN_GO_GRPC}"

work_dir=$(mktemp -d)
trap 'rm -rf -- "${work_dir}"' EXIT
mkdir -p "${work_dir}/proto/oll" "${work_dir}/generated"

mapfile -t canonical_sources < <(find "${OLL_ROOT}/proto/oll" -maxdepth 1 -type f -name '*.proto' -print | LC_ALL=C sort)
if [[ ${#canonical_sources[@]} -eq 0 ]]; then
  echo 'onelastleaf checkout contains no protocol sources' >&2
  exit 2
fi
protoc --fatal_warnings -I "${OLL_ROOT}/proto" \
  --include_imports \
  --descriptor_set_out="${work_dir}/oll-protocol.pb" \
  "${canonical_sources[@]}"
computed_fingerprint=$(sha256sum "${work_dir}/oll-protocol.pb" | cut -d ' ' -f 1)
if [[ ${computed_fingerprint} != "${PUBLISHED_FINGERPRINT}" ]]; then
  echo "published fingerprint ${PUBLISHED_FINGERPRINT} does not match checkout fingerprint ${computed_fingerprint}" >&2
  exit 1
fi

readonly PROTO_FILES=(common.proto config.proto document.proto plugin.proto)
for file in "${PROTO_FILES[@]}"; do
  install -m 0644 "${OLL_ROOT}/proto/oll/${file}" "${work_dir}/proto/oll/${file}"
done

readonly GO_MAPPINGS=(
  --go_opt="Moll/common.proto=${PROTOCOL_PACKAGE}"
  --go_opt="Moll/config.proto=${PROTOCOL_PACKAGE}"
  --go_opt="Moll/document.proto=${PROTOCOL_PACKAGE}"
  --go_opt="Moll/plugin.proto=${PROTOCOL_PACKAGE}"
)
readonly GRPC_MAPPINGS=(
  --go-grpc_opt="Moll/common.proto=${PROTOCOL_PACKAGE}"
  --go-grpc_opt="Moll/config.proto=${PROTOCOL_PACKAGE}"
  --go-grpc_opt="Moll/document.proto=${PROTOCOL_PACKAGE}"
  --go-grpc_opt="Moll/plugin.proto=${PROTOCOL_PACKAGE}"
)
readonly STAGED_SOURCES=(
  "${work_dir}/proto/oll/common.proto"
  "${work_dir}/proto/oll/config.proto"
  "${work_dir}/proto/oll/document.proto"
  "${work_dir}/proto/oll/plugin.proto"
)

protoc -I "${work_dir}/proto" \
  --go_out="${work_dir}/generated" \
  --go_opt="module=${MODULE}" \
  "${GO_MAPPINGS[@]}" \
  --go-grpc_out="${work_dir}/generated" \
  --go-grpc_opt="module=${MODULE}" \
  "${GRPC_MAPPINGS[@]}" \
  "${STAGED_SOURCES[@]}"

for file in "${PROTO_FILES[@]}"; do
  install -m 0644 "${work_dir}/proto/oll/${file}" "${SDK_ROOT}/proto/oll/${file}"
  generated_name=${file%.proto}.pb.go
  install -m 0644 "${work_dir}/generated/protocol/${generated_name}" "${SDK_ROOT}/protocol/${generated_name}"
done
install -m 0644 "${work_dir}/generated/protocol/plugin_grpc.pb.go" "${SDK_ROOT}/protocol/plugin_grpc.pb.go"

sed -i -E \
  "s/(const ProtocolSchemaSHA256 = \")[0-9a-f]{64}(\")/\1${PUBLISHED_FINGERPRINT}\2/" \
  "${SDK_ROOT}/protocol_version.go"

echo "updated Go protocol bindings for ${PUBLISHED_FINGERPRINT}"
