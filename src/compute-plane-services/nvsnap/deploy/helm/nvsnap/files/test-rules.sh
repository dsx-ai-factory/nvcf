#!/bin/sh
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Checks and unit-tests the NvSnap alert rules with promtool: a local
# promtool when there is one, else the pinned Prometheus image.
set -eu
cd "$(dirname "$0")"
PROMETHEUS_IMAGE=${PROMETHEUS_IMAGE:-prom/prometheus:v3.1.0}
if command -v promtool >/dev/null 2>&1; then
  run() { promtool "$@"; }
else
  run() { docker run --rm -v "$PWD:/rules:ro" -w /rules --entrypoint promtool "$PROMETHEUS_IMAGE" "$@"; }
fi
run check rules prometheus-rules.yaml
run test rules prometheus-rules.test.yaml
