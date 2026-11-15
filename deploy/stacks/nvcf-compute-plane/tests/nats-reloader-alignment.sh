#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compute_stack_dir="$(cd "$script_dir/.." && pwd)"
compute_state="$compute_stack_dir/helmfile.d/01-dependencies.yaml.gotmpl"
golden_state="$compute_stack_dir/testdata/golden/local/01-dependencies.yaml-dynamo-operator/dynamo-platform/charts/nats/templates/stateful-set.yaml"

extract_tag_after() {
  local file="$1"
  local marker="$2"

  awk -v marker="$marker" '
    index($0, marker) { found = 1 }
    found && $1 == "tag:" {
      value = $0
      if (value ~ /default "/) {
        sub(/^.*default "/, "", value)
        sub(/".*$/, "", value)
      } else {
        sub(/^[[:space:]]*tag:[[:space:]]*/, "", value)
        gsub(/"/, "", value)
        sub(/[[:space:]]*$/, "", value)
      }
      print value
      exit
    }
  ' "$file"
}

compute_tag="$(extract_tag_after "$compute_state" 'Keep the bundled Dynamo NATS reloader aligned')"

if [[ -z "$compute_tag" ]]; then
  printf 'ERROR: could not resolve the NATS reloader pin from the compute stack.\n' >&2
  exit 1
fi

# Other component directories on a maintenance branch are historical snapshots,
# so the in-tree NATS chart is not compared. Cross-stack consistency with the
# Self-Managed reloader is enforced by the release inventory merger.

if ! grep -Fq "image: docker.io/natsio/nats-server-config-reloader:$compute_tag" "$golden_state"; then
  printf 'ERROR: compute-plane golden render does not use NATS reloader %s.\n' \
    "$compute_tag" >&2
  exit 1
fi

printf 'nats-reloader-alignment: OK (%s)\n' "$compute_tag"
