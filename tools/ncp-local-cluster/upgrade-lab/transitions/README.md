# Umbrella upgrade planning inputs

The active lab routes are legacy NVCF 1.0.1 to umbrella 1.0.4, then umbrella 1.0.4 to 1.1.1.
Umbrella 1.0.4 is the required release stop for earlier 1.0.x releases.
The target manifest's `previous_version` records release ancestry, not another required stop.

| Endpoint | Kind | Control / Compute / Observability |
| --- | --- | --- |
| NVCF 1.0.1 | Legacy baseline without an umbrella manifest | 1.0.1 / 1.0.0 / 1.0.0 |
| Umbrella 1.0.4 | Released manifest | 1.0.10 / 1.0.5 / 1.0.0 |
| Umbrella 1.1.1 | Released manifest | 1.1.9 / 1.1.1 / 1.1.0 |

| Route | Contract | Component report |
| --- | --- | --- |
| `umbrella-1.0.1-to-1.0.4` | [Contract](umbrella-1.0.1-to-1.0.4.json) | [Report](umbrella-1.0.1-to-1.0.4-components.md) |
| `umbrella-1.0.4-to-1.1.1` | [Contract](umbrella-1.0.4-to-1.1.1.json) | [Report](umbrella-1.0.4-to-1.1.1-components.md) |

## Identity and comparison

The [artifact lock](artifact-lock.json) pins original tags, commits, asset IDs and SHA-256 digests.
The [1.0.4](manifests/1.0.4.json) and [1.1.1](manifests/1.1.1.json) copies retain released manifest bytes.
The legacy baseline uses its frozen matrix and original stack inventories.
It has no synthetic released manifest.

Run the [fetch and audit commands](../README.md) against original release assets.
Fetch checks manifests, asset indexes, bundles and original inventories.
Audit writes component comparisons without evaluating runtime readiness.
The [provenance](artifact-provenance.json) binds both reports to current lock and contract digests.
The superseded 1.1.0 target and old evidence are no longer active inputs.

| Route | Changed | Added | Removed | Backward | Unordered |
| --- | --- | --- | --- | --- | --- |
| 1.0.1 to 1.0.4 | 55 | 9 | 0 | 0 | 0 |
| 1.0.4 to 1.1.1 | 58 | 6 | 3 | 0 | 3 |

Counts describe inventory entries, including optional components.
Zero backward entries does not prove runtime compatibility.
Review same-version location changes, multiple image variants and removed consumers separately.
The audit does not infer runtime absence from a missing inventory entry.

## Published guidance and lab acceptance

The [1.1.1 release notes](https://github.com/dsx-ai-factory/nvcf/blob/2034fdcabd36f9119d4be9ff1ecd4ca9dd3e06b6/deploy/releases/1.1.1/notes.md)
report single-cluster and multi-cluster AWS validation of the published guides.
The [1.0.4 to 1.1.1 guide](https://github.com/dsx-ai-factory/nvcf/blob/517ebf322e217adfb274a34ea5c5ed03ff74b34e/docs/overview/release-notes/1.0.4-to-1.1.1-upgrade.md)
also records limits, including the untested control 1.1.9 with compute 1.0.5 upgrade window.
Single-member OpenBao is not covered. Cross-cluster AWS compute metrics remain experimental.

These lab contracts remain `unqualified` with `supported_execution_enabled: false`.
Reviewing their inputs does not qualify a lab deployment or WorkloadGate integration.
See the [current findings](current-findings.md) and [planning reference](../../../../docs/dev/upgrade-transitions.md).
Complete execution, recovery rehearsals and operator-effort comparisons require separate planning.
