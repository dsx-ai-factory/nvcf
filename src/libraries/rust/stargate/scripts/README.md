# Repository Scripts

The `scripts/` directory contains supported repository checks, local workflow
helpers, and their Python unit tests.

## Manual Python Test Suite

Run all Python support-script tests with:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

This suite is intentionally manual. Individual repository gates run the focused
test modules they own, such as the coverage-policy tests in
`scripts/check_coverage_quality.sh`.

Use the discovery command after changing Python support scripts, their tests,
Buildkite quality checks, Kubernetes integration helpers, benchmark profiling
helpers, or PR reporting utilities.

## Tilt Lifecycle

Use the Make targets rather than calling `tilt up` directly:

```bash
make tilt-up-kind
make tilt-up-docker-desktop
make tilt-up
```

The generic `make tilt-up` target snapshots the current kubectl context before
starting Tilt, so teardown remains pinned to the same cluster.

Run the CI integration suite through the same lifecycle wrapper:

```bash
python3 scripts/run_tilt.py ci --context kind-kind --timeout 30m
```

The runner supports `up` and `ci`. When Tilt exits successfully, fails, or is
interrupted, it invokes `tilt down --delete-namespaces`, preserving compatible
context, namespace, Tiltfile, and Tiltfile arguments, then removes the matching
CoreDNS rewrite. This removes the Kubernetes resources and cluster-wide DNS
side effect owned by that Tilt instance. The selected context is also applied
to kubectl calls made by the local integration and manifest-rendering helpers.

## PR Checks

The required Bazel workflow checks protobuf compatibility before selecting
changed subtrees. Runtime compatibility tests run in the Stargate row.

Run the support-script tests after changing repository tooling:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
```

## Protocol compatibility

With Buf on `PATH`, run `scripts/check_buf_breaking.sh origin/main` from this
directory's parent. It checks the Stargate registration and gateway client
protobuf APIs against the supplied Git
base with strict `FILE` rules and no deletion exceptions. Candidate Buf
configuration cannot disable the check. CI supplies the pull request or
merge queue base, or the previous commit for a push. CI also checks the gateway
server schema in `nvcf-core` and the client copy in `llm-api-gateway` against
that base. Historical gateway releases have no release matrix in this gate;
existing drift between copies is not reconciled by these checks.

Stargate schemas are compared against every supported stable Stargate release,
including backports. Releases are selected by UTC publication date from the
upstream GitHub release inventory. The rolling support window is 90 days. Older versions
still deployed or needed for rollback must remain in
`compatibility_retained_releases.json`; remove them only after verified migration
and closure of the rollback window. Missing inventory or retained-release
metadata fails CI. The selected tag, publication date, and cutoff appear in the
job log, along with the full supported tag list.

`cargo test --locked -p stargate --test compatibility -- --test-threads=1`
decodes released stats values, then re-encodes them with the current client for
gRPC registration and HTTP/QUIC streaming.
CI encodes these stats using the selected oldest release's protobuf schema and
passes the hex payload through `STARGATE_COMPAT_STATS_HEX`. Local runs without
that variable use the frozen v0.8.0 fixture.
It checks power-of-n, both Pulsar defaults, and their explicit mean-based modes
with old-only, mixed, and new-only backends, then restores the older backend
and withdraws the newer one. Both backends must serve requests in the mixed
state. An absent aggregate maximum falls back to the positive finite last mean.
Cluster aggregation treats missing or invalid backend maxima as absent. This
also covers current Pylons that have not established a maximum.

This checks released stats values with current registration envelopes and
transports. It does not send an untouched historical registration frame or run
historical Pylon/Stargate binaries. Restoring an older backend is a producer
replacement scenario; consumer downgrade, relay compatibility, and production
rolling upgrades require separate qualification.

## Last-Cluster End-to-End Check

`scripts/e2e-last-cluster.sh` runs the LLM API gateway, Stargate, two Pylons,
and two mock-dynamo backends on loopback and checks that
`last_cluster_affinity` keeps a returning session on its last cluster after
overflow.

```bash
scripts/e2e-last-cluster.sh
```

Run `scripts/e2e-last-cluster.sh --help` for the scenario, options, and
environment variables.
