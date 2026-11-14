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

# nvbugs#6471949, second symptom, as reported: the pods came up and ran, and
# only later did pod1 and pod2 log
#   failed to create fsnotify watcher: too many open files
# while pod0 stayed healthy. That ordering is the signature of a budget shared
# between the replicas rather than a per-pod one: fs.inotify.max_user_instances
# is accounted per (uid, user namespace), every cache pod here runs as uid 1000,
# and the rendered pod does not ask for its own user namespace, so the value in
# the pod securityContext is not a private allowance for that pod. Raising the
# number therefore does not stop the second and third replica on a node from
# exhausting the same budget and needing the manual restart the report
# describes. Require the headroom to be delivered per pod: the sysctl paired
# with hostUsers: false (pod gets its own user namespace, where the limit is
# genuinely its own), or an init container that raises the node's
# /proc/sys/fs/inotify budget for all of them.
host_users() {
  local value
  # Read the raw value instead of `// "unset"`: that coalesce treats a literal
  # `false` as absent, which is exactly the value being asserted on here.
  value="$(yq -r 'select(.kind == "StatefulSet") | .spec.template.spec.hostUsers' "$1")"
  if [ -z "${value}" ] || [ "${value}" = "null" ]; then
    echo "unset"
  else
    echo "${value}"
  fi
}

node_level_raise() {
  case "$(init_container_text "$1")" in
    */proc/sys/fs/inotify*) return 0 ;;
  esac
  return 1
}

assert_per_pod_inotify_headroom() {
  local file="$1" label="$2" value hu
  value="$(sysctl_value "${file}" "fs.inotify.max_user_instances")"
  hu="$(host_users "${file}")"
  if node_level_raise "${file}"; then
    echo "OK: ${label}: an init container raises the inotify budget via /proc/sys"
    return 0
  fi
  if [ -n "${value}" ] && [ "${value}" != "null" ] && [ "${hu}" = "false" ]; then
    echo "OK: ${label}: pod sysctl ${value} with hostUsers=false (pod owns its inotify budget)"
    return 0
  fi
  echo "FAILED: ${label}: fs.inotify.max_user_instances=${value:-<unset>} is set on the pod but hostUsers=${hu}: the cache pods share the node's per-uid inotify budget (all run as uid 1000 in the host user namespace), so later replicas still log 'failed to create fsnotify watcher: too many open files' as reported" >&2
  exit 1
}

assert_per_pod_inotify_headroom "${DEFAULT_FILE}" "default values"
assert_per_pod_inotify_headroom "${PVC_FILE}" "persistent cache volumes"

echo "PASS: container-cache fsnotify limit render tests"
