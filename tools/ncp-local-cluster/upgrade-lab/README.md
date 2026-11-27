# Upgrade transition lab

Audit the exact inputs for `umbrella-1.0.1-to-1.0.4` or `umbrella-1.0.4-to-1.1.1` as reviewed planning inputs. This module implements `fetch`, `audit`,
`probe`, `check-recovery` and `rehearse --phase prepare`. It does not create
clusters or execute upgrades.

The lab-owned contracts, artifact lock and reports live in
[`transitions/`](transitions/README.md). They are not required deployment inputs.
See [the rehearsal procedure](../../../docs/dev/upgrade-transitions.md).
No new third-party dependencies are required.
Phase 1 accepts planning inputs and dependency contracts.
Artifact probes, render preparation and recovery checks remain developer helpers.
Their execution and complete route qualification are separate work.

## Prepare and compare

Run from the repository root. Every output directory must be new; keep its
parent private. Fetch uses the existing authenticated `gh` CLI to download
original release manifests, asset indexes, bundles and stack inventories.
Route versions identify umbrella releases.
Version 1.0.1 is an explicit legacy baseline without an umbrella manifest.
Umbrella 1.0.4 selects control 1.0.10, compute 1.0.5 and Observability 1.0.0.
Umbrella 1.1.1 selects control 1.1.9, compute 1.1.1 and Observability 1.1.0.
The loader checks all three stacks against pinned manifests and documentation matrices.

```sh
go -C tools/ncp-local-cluster/upgrade-lab build -o /tmp/nvcf-upgrade-lab .
go -C tools/docs-version-sync build -o /tmp/nvcf-bundle-verifier .
umask 077
export UPGRADE_RECORD="$(mktemp -d)"

/tmp/nvcf-upgrade-lab fetch --route umbrella-1.0.1-to-1.0.4 \
  --out "$UPGRADE_RECORD/inputs-101-104"
/tmp/nvcf-upgrade-lab audit --route umbrella-1.0.1-to-1.0.4 \
  --inputs "$UPGRADE_RECORD/inputs-101-104" --out "$UPGRADE_RECORD/audit-101-104"

/tmp/nvcf-upgrade-lab fetch --route umbrella-1.0.4-to-1.1.1 \
  --out "$UPGRADE_RECORD/inputs-104-111"
/tmp/nvcf-upgrade-lab audit --route umbrella-1.0.4-to-1.1.1 \
  --inputs "$UPGRADE_RECORD/inputs-104-111" --out "$UPGRADE_RECORD/audit-104-111"
```

Fetch verifies remote manifest bytes at their pinned commits.
It then verifies original release assets and inventories by SHA-256.
The asset index must bind the same manifest, bundle and three inventories.
Audit rejects changed manifests or selections that differ from those manifests.
Old stack-only route IDs and the superseded umbrella 1.1.0 target fail validation. Their checkpoints cannot resume these routes. Audit rechecks the release assets and inventories and writes `audit.json` and
`components.md`, including provenance and declared execution blockers.
Comparison retains each consumer's helper versions. Multiple versions within
one consumer require render review; the tool does not guess a version pairing.
An audit succeeds when comparison completes, including backward or unknown
selections. It never enables execution or evaluates runtime postconditions.
Comparison includes optional releases. Backward counts remain inventory
findings until effective values and rendered manifests establish applicability.
The JSON report labels its scope `release-inventories`. Added/removed counts
and blank cells describe inventory entries, not runtime discovery. Review its
`runtime_artifact_checks` against application defaults, workload launch
specifications and inline hooks before qualification.
Reports keep `conditional_checks` separate from `execution_blockers`; the lab
records these rules but does not evaluate them. Unknown applicability must be
resolved before the affected action.

## Probe artifact access

Probe needs Docker CLI, Helm and git. It downloads chart archives, records
their SHA-256 values and resolves linux/amd64 image manifest digests. It does
not deploy workloads. Disabled optional releases remain visible as skipped;
all required inventory artifacts are checked conservatively, including
operator-created images. This does not replace a profile render or node pull
test. A failure is an access blocker, not proof that an image does not exist.
Probe does not promote optional artifacts to required from site values. Check
enabled optional artifacts separately. A skipped optional artifact does not establish that the selected profile omits it.

Bundled charts are read with `git archive` from the exact owning stack commit.
The source repository must contain those commits. No chart templates are
patched. The archive digest records the source bytes separately from a
downloaded Helm package digest.

For authenticated HTTP chart repositories, set
`UPGRADE_LAB_HELM_REPOSITORY_CONFIG` to a private JSON Helm repositories file.
Register the exact original repository URL under an alias and populate its
index using the same `HELM_CACHE_HOME`. Probe looks up that exact URL; it does
not silently substitute a mirror. Keep usernames/passwords in that file,
never in command arguments. The file must not be committed.

```sh
export UPGRADE_LAB_SOURCE_REPO="$PWD"
# Optional: export UPGRADE_LAB_HELM_REPOSITORY_CONFIG=/private/repositories.json
/tmp/nvcf-upgrade-lab probe --route umbrella-1.0.4-to-1.1.1 \
  --inputs "$UPGRADE_RECORD/inputs-104-111" \
  --out "$UPGRADE_RECORD/access-104-111" --timeout 30s
```

An access probe can report `running` while incomplete. Only `complete: true` and
`status: passed` establish a completed access check. Exit code 1 blocks the
rehearsal. Qualification remains `unqualified` even when every probe passes.
Raw responses, including possible signed URLs, remain in owner-only files
under `private/`. Do not publish them.

## Export and render a rehearsal

`rehearse --phase prepare` verifies each umbrella endpoint's original bundle
with the native `docs-version-sync umbrella verify-bundle` command before
extraction. It checks the pinned manifest, asset index, bundle and inventories,
plus the product and stack tags. Each endpoint gets an isolated source tree.
The legacy source uses three Git archives from its independently locked stack
commits because it has no umbrella bundle. Only new environment and secret files are added. Chart and stack source
files are not patched.

Provide a private JSON configuration with these fields:

| Field | Value |
| --- | --- |
| `source_repository` | Absolute path to a Git repository containing the locked tags and commits |
| `inputs` | Absolute path to the verified release assets and inventories from `fetch` |
| `bundle_verifier` | Absolute path to the native `docs-version-sync` executable |
| `helmfile` | Absolute path to the Helmfile executable |
| `overlays.source` | Entries for `control-plane`, `compute-plane` and `observability` |
| `overlays.target` | Entries for the same three planes |
| `compute_registrations` | Two entries with `cluster_name`, `nca_id` and an absolute registration `values` path |

Each overlay entry has `values` and `secrets`, both absolute paths. They are
copied to `environments/rehearsal.yaml` and
`secrets/rehearsal-secrets.yaml`. Use native environment and chart values for
registry credentials, baseline resource sizing and feature flags. Keep the
configuration and its source files outside the repository. Helm and other
native prerequisites must already be available on `PATH`.
Only Linux hosts with GNU `env -C` are supported for this command.

Use the source baseline's original registration files for both endpoint
renders. The lab copies and hashes them, sets `CLUSTER_NAME`, `NCA_ID` and
`OUTPUT_DIR` explicitly, and renders both compute clusters at each endpoint.
It also renders Observability with the explicit `control` profile and the
`compute` profile for each retained compute cluster. Complete preparation
produces twelve render digests across both endpoints. These are final endpoint
renders; they do not qualify temporary combinations during the hop.
Do not supply replacement registrations to make a target render pass.
Before source baseline registration, omit `compute_registrations`. The lab
still exports all sources and renders both control planes and Observability
stacks, then stops with `blocked` because compute render evidence is missing.
This partial preparation cannot establish complete profile applicability.

```sh
/tmp/nvcf-upgrade-lab rehearse --route umbrella-1.0.4-to-1.1.1 \
  --run-config /private/upgrade-run.json \
  --run-dir /private/new-upgrade-run --phase prepare
```

Each command has a bounded timeout. Source archives, rendered manifests and
raw output remain private. An empty run-owned kubeconfig prevents use of the
operator's default context. `record.json` records the lab binary, Helmfile,
configuration, contract, original release assets, bundle verifier, inventory
identities, legacy source archives, overlays and
render hashes. A failed operation leaves a `blocked` record and stops later
commands. Existing run directories are never overwritten. Resolve the cause
and use a new directory for another preparation attempt.
Helm configuration, repository indexes, caches and temporary files use private
run directories. Set `HELM_PLUGINS` explicitly if a selected stack needs an
already installed plugin.

`prepared` means exports and renders completed. Review effective values,
artifact identities, HA placement, disabled optional workloads and migration
ordering before installing anything. It does not establish artifact pullability,
HA behavior, mixed-fleet compatibility or recovery. Automated `baseline`,
`backup`, `apply`, `restore` and `verify` phases are not implemented; those
phase names fail before any command runs. Follow the manual rehearsal
procedure for the remaining qualification work.

## Checks

The [recovery profile](transitions/recovery-prerequisites.md) requires a frozen
native stream/consumer baseline and a private JSON checkpoint. Its fields are
`schema_version: 1`, `route`, `contract_sha256`, `lock_sha256`,
`baseline_sha256`, `server_image_digest`, `barrier` and `samples`.
The barrier records `id`, `recorded_at`, `callers_paused` and `writers_paused`.
Each of the two samples records `observed_at` and the complete native
`streams`, including each stream's `name`, `config`, `state` and `consumers`.
Consumer observations include their `stream_name`, `name`, `config` and
native pending/acknowledgement/redelivery counts. Observations must follow
the pause barrier and be no older than five minutes.
Use the same native monitoring representation for the baseline and samples.
Request full configurations with `jsz?accounts=true&streams=true&consumers=true&config=true`
from every member and select each stream's leader observation. Older NATS CLI
versions can omit configuration fields that monitoring includes. Do not remove
those fields to make a comparison pass.

```sh
/tmp/nvcf-upgrade-lab check-recovery --route umbrella-1.0.4-to-1.1.1 \
  --baseline /private/frozen-nats-inventory.json \
  --checkpoint /private/paused-source-checkpoint.json \
  --out /private/new-recovery-check
```

The command checks supplied evidence and records `unqualified`. It does not
stop writers, read the cluster, execute restore or enable an upgrade. Native
sequence/cursor resets after drained-memory cold restore must be recorded;
nonempty or in-flight memory state remains outside this recovery profile.

## Module verification

From this module:

```sh
go test -race ./...
go vet ./...
```

The existing Go tools CI discovers this module automatically. The real audit
BDD uses existing DSL steps and needs a directory containing both routes'
verified inputs, plus a new output directory:

Return to the repository root before the following BDD commands.

```sh
export UPGRADE_LAB=/tmp/nvcf-upgrade-lab
export TRANSITION_CONTRACTS="$PWD/tools/ncp-local-cluster/upgrade-lab/transitions"
export TRANSITION_INPUTS=/private/verified-inputs
export TRANSITION_OUTPUT="$(mktemp -d)"
cd tests/bdd
go test -run '^TestUpgradeTransitionAudit$' -count=1 -v
```

This BDD exercises the actual audit binary, not a deployment. Cluster,
migration, workload and recovery acceptance remain separate results.
