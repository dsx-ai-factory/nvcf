#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: scripts/check_buf_breaking.sh <base-ref> [repo-relative-schema-directory-or-file]" >&2
  exit 2
fi
if ! command -v buf >/dev/null 2>&1; then
  echo "buf is required on PATH" >&2
  exit 127
fi

workspace="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo="$(git -C "$workspace" rev-parse --show-toplevel)"
prefix="$(git -C "$workspace" rev-parse --show-prefix)"
schemas="${2:-${prefix}crates/proto/proto}"
base="$(git -C "$repo" rev-parse --verify --end-of-options "$1^{commit}")"
baseline="$(mktemp -d)"
trap 'rm -rf "$baseline"' EXIT

# Export only the base schemas. Candidate config cannot weaken this check,
# and a missing base/schema is an error rather than a skipped comparison.
candidate="$repo/$schemas"
against="$baseline"
if [[ "$schemas" == *.proto ]]; then
  mkdir "$baseline/base" "$baseline/candidate"
  name="$(basename "$schemas")"
  git -C "$repo" show "$base:$schemas" > "$baseline/base/$name"
  cp "$candidate" "$baseline/candidate/$name"
  candidate="$baseline/candidate"
  against="$baseline/base"
else
  git -C "$repo" archive "$base:$schemas" | tar -x -C "$baseline"
fi
buf breaking "$candidate" \
  --against "$against" \
  --config '{"version":"v2","breaking":{"use":["FILE"]}}'
