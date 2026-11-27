# Runtime prerequisites for future execution

This reference describes checks outside Phase 1 planning acceptance.
Use the exact selected artifacts and the published procedure for each source profile.

## Artifact coverage

Original inventories can omit launch helpers, inline hooks and operator-created workloads.
Inspect effective values, chart templates and actual Pod specifications before execution.
Legacy NVCT helpers require separate original-reference identity and access checks.
Registry access from an operator workstation does not prove compute-node pulls.

## Source and target behavior

- Preserve source registration, credentials, trust and workload identities.
- Complete source SIS lookup prerequisites before the backup boundary.
- Retain the first hop's legacy OpenBao bridge requirements.
- The second hop retains the versioned JWT lifecycle while changing its chart and migration helper.
- Review `highAvailability.mode`, replica sizing, storage and placement explicitly.
- Inspect admission policies, callback configuration and enabled optional consumers.
- Preserve source Observability during intermediate states when the selected procedure requires it.
- Final umbrella 1.1.1 selects Observability 1.1.0, including its monitoring-label changes.

The [published second-hop guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md)
owns its command sequence and documented limits.
The [current findings](current-findings.md) summarize unresolved planning inputs.
The lab does not execute these checks or establish live readiness.
