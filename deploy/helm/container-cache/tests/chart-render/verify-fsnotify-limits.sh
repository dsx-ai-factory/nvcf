#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Regression test for nvbugs#6471949: on first bring-up of the NVMesh-backed
# cache volumes, nvcf-container-cache pods logged
#   failed to create fsnotify watcher: too many open files
# and needed a manual restart to recover.
#
# The part of that the chart controls is whether the cache pods get any
# inotify headroom of their own. This asserts the rendered StatefulSet raises
# the per-pod inotify instance limit above the common kernel default (128)
# rather than rendering nothing on the subject at all. It makes no claim about
# the CSI mount churn; it pins the chart output.
#
# Run from the chart subtree (same convention as the sibling scripts here,
# e.g. verify-monitoring.sh):
#   bash tests/chart-render/verify-fsnotify-limits.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_FILE="$(mktemp)"
PVC_FILE="$(mktemp)"
trap 'rm -f "${DEFAULT_FILE}" "${PVC_FILE}"' EXIT

cd "${ROOT_DIR}"

helm template c ./deploy > "${DEFAULT_FILE}"
helm template c ./deploy \
  --set persistentVolumeClaim.storageClassName=nvcf-sc \
  --set replicaCount=3 > "${PVC_FILE}"

sysctl_value() {
  local file="$1" name="$2"
  yq -r "select(.kind == \"StatefulSet\") | ((.spec.template.spec.securityContext.sysctls // []) | map(select(.name == \"${name}\")) | .[0].value // \"\")" "${file}"
}

init_container_text() {
  local file="$1"
  yq -r 'select(.kind == "StatefulSet") | [.spec.template.spec.initContainers[] | ((.command // []) | join(" ")) + " " + ((.args // []) | join(" "))] | join(" ")' "${file}"
}

assert_inotify_limit() {
  local file="$1" label="$2" value
  value="$(sysctl_value "${file}" "fs.inotify.max_user_instances")"
  if [ -z "${value}" ] || [ "${value}" = "null" ]; then
    case "$(init_container_text "${file}")" in
      *fs.inotify.max_user_instances*)
        echo "OK: ${label}: inotify instance limit raised by an init container"
        return 0
        ;;
    esac
    echo "FAILED: ${label}: the rendered StatefulSet sets no per-pod inotify limit (fs.inotify.max_user_instances) and no init container that raises one" >&2
    exit 1
  fi
  case "${value}" in
    ''|*[!0-9]*)
      echo "FAILED: ${label}: fs.inotify.max_user_instances is not a plain integer: ${value}" >&2
      exit 1
      ;;
  esac
  if [ "${value}" -le 128 ]; then
    echo "FAILED: ${label}: fs.inotify.max_user_instances is ${value}, not above the common kernel default of 128" >&2
    exit 1
  fi
  echo "OK: ${label}: fs.inotify.max_user_instances=${value}"
}

assert_inotify_limit "${DEFAULT_FILE}" "default values"
assert_inotify_limit "${PVC_FILE}" "persistent cache volumes"

echo "PASS: container-cache fsnotify limit render tests"
